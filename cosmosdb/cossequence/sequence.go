package cossequence

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/data/azcosmos"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/tpm-az-common/cosmosdb/cosutil"
)

const (
	SequenceDefaultPKey              = "cos-sequence"
	SequenceIdDefaultPrefix          = "seq:"
	SequenceDefaultCreateDescription = "sequence missing and created"
)

type NextValOptions struct {
	Pkey              string
	SeqIdPrefix       string
	SeqId             string
	CreateIfMissing   bool
	CreateDescription string
}

type NextValOption func(*NextValOptions)

func WithPartitionKey(s string) NextValOption {
	return func(opts *NextValOptions) {
		opts.Pkey = s
	}
}

func WithSeqId(s string) NextValOption {
	return func(opts *NextValOptions) {
		opts.SeqId = s
	}
}

func WithSeqIdPrefix(s string) NextValOption {
	return func(opts *NextValOptions) {
		opts.SeqIdPrefix = s
	}
}

func WithCreateIfMissing(b bool, d string) NextValOption {
	return func(opts *NextValOptions) {
		opts.CreateIfMissing = b
		opts.CreateDescription = d
	}
}

type Sequence struct {
	PKey        string `yaml:"pkey,omitempty" mapstructure:"pkey,omitempty" json:"pkey,omitempty"`
	Id          string `yaml:"id,omitempty" mapstructure:"id,omitempty" json:"id,omitempty"`
	Description string `yaml:"description,omitempty" mapstructure:"description,omitempty" json:"description,omitempty"`
	Value       int    `yaml:"value,omitempty" mapstructure:"value,omitempty" json:"value,omitempty"`
}

type StoredSequence struct {
	*Sequence
	ETag azcore.ETag
}

func (ctx *Sequence) ToJSON() ([]byte, error) {
	return json.Marshal(ctx)
}

func (ctx *Sequence) MustToJSON() []byte {
	b, err := json.Marshal(ctx)
	if err != nil {
		panic(err)
	}

	return b
}

func DeserializeContext(b []byte) (*Sequence, error) {
	ctx := Sequence{}
	err := json.Unmarshal(b, &ctx)
	if err != nil {
		return nil, err
	}

	return &ctx, nil
}

func (ctx *Sequence) Valid() bool {
	return true
}

// NextValUpsert old next val with optimistic locking. The new default one is the one with patch operation.
func NextValUpsert(ctx context.Context, client *azcosmos.ContainerClient, nextValOpts ...NextValOption) (int, error) {

	opts := NextValOptions{Pkey: SequenceDefaultPKey, SeqIdPrefix: SequenceIdDefaultPrefix, CreateIfMissing: true, CreateDescription: SequenceDefaultCreateDescription}
	for _, o := range nextValOpts {
		o(&opts)
	}

	opts.SeqId = fmt.Sprintf("%s%s", opts.SeqIdPrefix, opts.SeqId)
	if opts.SeqId == "" || opts.Pkey == "" {
		panic(fmt.Errorf("sequence missing core params - pkey: %s, id: %s", opts.Pkey, opts.SeqId))
	}

	storedSeq, err := FindSequenceById(ctx, client, opts.Pkey, opts.SeqId)
	if err != nil && (err != cosutil.EntityNotFound || !opts.CreateIfMissing) {
		return -1, err
	}

	if err != nil {
		seq := Sequence{PKey: opts.Pkey, Id: opts.SeqId, Value: 1, Description: opts.CreateDescription}
		stSeq, err := InsertSequence(ctx, client, &seq)
		if err != nil {
			return -1, err
		}

		return stSeq.Value, nil
	}

	storedSeq.Sequence.Value++
	ok, err := storedSeq.Upsert(ctx, client)
	if err != nil {
		return -1, err
	}

	if !ok {
		panic("unexpected error")
	}

	return storedSeq.Value, nil
}

func NextVal(ctx context.Context, client *azcosmos.ContainerClient, nextValOpts ...NextValOption) (int, error) {

	opts := NextValOptions{Pkey: SequenceDefaultPKey, SeqIdPrefix: SequenceIdDefaultPrefix, CreateIfMissing: true, CreateDescription: SequenceDefaultCreateDescription}
	for _, o := range nextValOpts {
		o(&opts)
	}

	opts.SeqId = fmt.Sprintf("%s%s", opts.SeqIdPrefix, opts.SeqId)
	if opts.SeqId == "" || opts.Pkey == "" {
		panic(fmt.Errorf("sequence missing core params - pkey: %s, id: %s", opts.Pkey, opts.SeqId))
	}

	patch := azcosmos.PatchOperations{}
	patch.AppendIncrement("/value", 1)
	// patch.SetCondition("from c where c.id='TOK'")
	itemOptions := azcosmos.ItemOptions{ /* EnableContentResponseOnWrite: true */ }
	resp, err := client.PatchItem(ctx, azcosmos.NewPartitionKeyString(opts.Pkey), opts.SeqId, patch, &itemOptions)
	if err != nil {
		err = cosutil.MapAzCoreError(err)
		if err != cosutil.EntityNotFound || !opts.CreateIfMissing {
			return -1, err
		}

		seq := Sequence{PKey: opts.Pkey, Id: opts.SeqId, Value: 1, Description: opts.CreateDescription}
		stSeq, err := InsertSequence(ctx, client, &seq)
		if err != nil {
			return -1, err
		}

		return stSeq.Value, nil
	}

	e, err := DeserializeContext(resp.Value)
	if err != nil {
		return -1, err
	}

	return e.Value, nil

	/*
		if err != nil && err != cosutil.EntityNotFound {
			return -1, err
		}

		if err != nil {
			seq := Sequence{Id: seqId, Value: 1, Description: "next val generated"}
			stSeq, err := InsertSequence(ctx, client, &seq)
			if err != nil {
				return -1, err
			}

			return stSeq.Value, nil
		}

		storedSeq.Sequence.Value++
		ok, err := storedSeq.Upsert(ctx, client)
		if err != nil {
			return -1, err
		}

		if !ok {
			panic("unexpected error")
		}

		return storedSeq.Value, nil
	*/
}

func InsertSequence(ctx context.Context, client *azcosmos.ContainerClient, tokCtx *Sequence) (StoredSequence, error) {
	resp, err := client.CreateItem(ctx, azcosmos.NewPartitionKeyString(tokCtx.PKey), tokCtx.MustToJSON(), nil)
	if err != nil {
		return StoredSequence{}, cosutil.MapAzCoreError(err)
	}

	return StoredSequence{Sequence: tokCtx, ETag: resp.ETag}, nil
}

func FindSequenceById(ctx context.Context, client *azcosmos.ContainerClient, seqPkey, Name string) (StoredSequence, error) {
	resp, err := client.ReadItem(ctx, azcosmos.NewPartitionKeyString(seqPkey), Name, nil)
	if err != nil {
		return StoredSequence{nil, ""}, cosutil.MapAzCoreError(err)
	}

	e, err := DeserializeContext(resp.Value)
	return StoredSequence{Sequence: e, ETag: resp.ETag}, err
}

func (e *StoredSequence) Upsert(ctx context.Context, client *azcosmos.ContainerClient) (bool, error) {

	b, err := e.ToJSON()
	if err != nil {
		return false, err
	}

	opts := &azcosmos.ItemOptions{IfMatchEtag: &e.ETag}
	resp, err := client.UpsertItem(ctx, azcosmos.NewPartitionKeyString(e.PKey), b, opts)
	if err != nil {
		return false, cosutil.MapAzCoreError(err)
	}

	e.ETag = resp.ETag
	return true, nil
}
