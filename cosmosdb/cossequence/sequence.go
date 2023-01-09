package cossequence

import (
	"context"
	"encoding/json"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/data/azcosmos"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/tpm-az-common/cosmosdb/cosutil"
)

type Sequence struct {
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

func NextVal(ctx context.Context, client *azcosmos.ContainerClient, seqId string) (int, error) {

	storedSeq, err := FindSequenceById(ctx, client, seqId)
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
}

func InsertSequence(ctx context.Context, client *azcosmos.ContainerClient, tokCtx *Sequence) (StoredSequence, error) {
	resp, err := client.CreateItem(ctx, azcosmos.NewPartitionKeyString(tokCtx.Id), tokCtx.MustToJSON(), nil)
	if err != nil {
		return StoredSequence{}, cosutil.MapAzCoreError(err)
	}

	return StoredSequence{Sequence: tokCtx, ETag: resp.ETag}, nil
}

func FindSequenceById(ctx context.Context, client *azcosmos.ContainerClient, Name string) (StoredSequence, error) {
	resp, err := client.ReadItem(ctx, azcosmos.NewPartitionKeyString(Name), Name, nil)
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
	resp, err := client.UpsertItem(ctx, azcosmos.NewPartitionKeyString(e.Id), b, opts)
	if err != nil {
		return false, cosutil.MapAzCoreError(err)
	}

	e.ETag = resp.ETag
	return true, nil
}
