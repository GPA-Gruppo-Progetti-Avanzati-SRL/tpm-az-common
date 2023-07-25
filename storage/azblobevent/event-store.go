package azblobevent

import (
	"context"
	"encoding/json"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/data/azcosmos"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/tpm-az-common/cosmosdb/cosutil"
)

type EventDocument struct {
	Id            string `mapstructure:"id,omitempty" yaml:"id,omitempty" json:"id,omitempty"`
	PKey          string `mapstructure:"pkey,omitempty" yaml:"pkey,omitempty" json:"pkey,omitempty"`
	Typ           string `mapstructure:"eventType,omitempty" yaml:"eventType,omitempty" json:"eventType,omitempty"`
	Status        string `mapstructure:"status,omitempty" yaml:"status,omitempty" json:"status,omitempty"`
	AccountName   string `mapstructure:"account-name,omitempty" yaml:"account-name,omitempty" json:"account-name,omitempty"`
	ContainerName string `mapstructure:"container,omitempty" yaml:"container,omitempty" json:"container,omitempty"`
	BlobName      string `mapstructure:"blob-name,omitempty" yaml:"blob-name,omitempty" json:"blob-name,omitempty"`
	ContentType   string `mapstructure:"contentType,omitempty" yaml:"contentType,omitempty" json:"contentType,omitempty"`
	ContentLength int64  `mapstructure:"contentLength,omitempty" yaml:"contentLength,omitempty" json:"contentLength,omitempty"`
	BlobType      string `mapstructure:"blobType,omitempty" yaml:"blobType,omitempty" json:"blobType,omitempty"`
	Url           string `mapstructure:"url,omitempty" yaml:"url,omitempty" json:"url,omitempty"`
	Ts            string `mapstructure:"eventTime,omitempty" yaml:"eventTime,omitempty" json:"eventTime,omitempty"`
	TTL           int    `mapstructure:"ttl,omitempty" yaml:"ttl,omitempty" json:"ttl,omitempty"`
}

const (
	CosCollectionId = "events"
	CosPartitionKey = "blob-event"
)

type StoredEventDocument struct {
	*EventDocument
	ETag  azcore.ETag `mapstructure:"_etag,omitempty" yaml:"_etag,omitempty" json:"_etag,omitempty"`
	Rid   azcore.ETag `mapstructure:"_rid,omitempty" yaml:"_rid,omitempty" json:"_rid,omitempty"`
	Self  azcore.ETag `mapstructure:"_self,omitempty" yaml:"_self,omitempty" json:"_self,omitempty"`
	CosTs int64       `mapstructure:"_ts,omitempty" yaml:"_ts,omitempty" json:"_ts,omitempty"`
}

func (c *EventDocument) ToJSON() ([]byte, error) {
	return json.Marshal(c)
}

func (c *EventDocument) MustToJSON() []byte {
	b, err := json.Marshal(c)
	if err != nil {
		panic(err)
	}

	return b
}

func DeserializeEventDocument(b []byte) (*EventDocument, error) {
	ctx := EventDocument{}
	err := json.Unmarshal(b, &ctx)
	if err != nil {
		return nil, err
	}

	ctx.PKey = CosPartitionKey
	return &ctx, nil
}

func InsertEventDocument(ctx context.Context, client *azcosmos.ContainerClient, tokCtx *EventDocument) (StoredEventDocument, error) {
	resp, err := client.CreateItem(ctx, azcosmos.NewPartitionKeyString(tokCtx.PKey), tokCtx.MustToJSON(), nil)
	if err != nil {
		return StoredEventDocument{}, cosutil.MapAzCoreError(err)
	}

	return StoredEventDocument{EventDocument: tokCtx, ETag: resp.ETag}, nil
}

func DeleteEventDocument(ctx context.Context, client *azcosmos.ContainerClient, pkey, id string) (bool, error) {
	var err error
	_, err = client.DeleteItem(ctx, azcosmos.NewPartitionKeyString(pkey), id, nil)
	if err != nil {
		return false, cosutil.MapAzCoreError(err)
	}

	return true, nil
}

func ReplaceEventDocument(ctx context.Context, client *azcosmos.ContainerClient, tok *EventDocument) (StoredEventDocument, error) {
	b, err := tok.ToJSON()
	if err != nil {
		return StoredEventDocument{}, err
	}

	resp, err := client.ReplaceItem(ctx, azcosmos.NewPartitionKeyString(tok.PKey), tok.Id, b, nil)
	if err != nil {
		return StoredEventDocument{}, cosutil.MapAzCoreError(err)
	}

	return StoredEventDocument{EventDocument: tok, ETag: resp.ETag}, nil
}

func UpdateEventDocumentStatus(ctx context.Context, client *azcosmos.ContainerClient, pkey, id, status string) error {
	patch := azcosmos.PatchOperations{}
	patch.AppendSet("/status", status)
	// patch.SetCondition("from c where c.id='TOK'")
	itemOptions := azcosmos.ItemOptions{ /* EnableContentResponseOnWrite: true */ }
	_, err := client.PatchItem(ctx, azcosmos.NewPartitionKeyString(pkey), id, patch, &itemOptions)
	return err
}

func FindEventDocumentById(ctx context.Context, client *azcosmos.ContainerClient, pkey, id string) (StoredEventDocument, error) {
	resp, err := client.ReadItem(ctx, azcosmos.NewPartitionKeyString(pkey), id, nil)
	if err != nil {
		return StoredEventDocument{EventDocument: nil, ETag: "", Rid: "", Self: "", CosTs: 0}, cosutil.MapAzCoreError(err)
	}

	e, err := DeserializeEventDocument(resp.Value)
	return StoredEventDocument{EventDocument: e, ETag: resp.ETag}, err
}

func (e *StoredEventDocument) Replace(ctx context.Context, client *azcosmos.ContainerClient) (bool, error) {

	b, err := e.ToJSON()
	if err != nil {
		return false, err
	}

	opts := &azcosmos.ItemOptions{IfMatchEtag: &e.ETag}
	resp, err := client.ReplaceItem(ctx, azcosmos.NewPartitionKeyString(e.PKey), e.Id, b, opts)
	if err != nil {
		return false, cosutil.MapAzCoreError(err)
	}

	e.ETag = resp.ETag
	return true, nil
}

func (e *StoredEventDocument) Delete(ctx context.Context, client *azcosmos.ContainerClient) (bool, error) {

	var err error

	opts := &azcosmos.ItemOptions{IfMatchEtag: &e.ETag}
	_, err = client.DeleteItem(ctx, azcosmos.NewPartitionKeyString(e.PKey), e.Id, opts)
	if err != nil {
		return false, cosutil.MapAzCoreError(err)
	}

	return true, nil
}

func (e *StoredEventDocument) Upsert(ctx context.Context, client *azcosmos.ContainerClient) (bool, error) {
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

func FindEventDocuments(client *azcosmos.ContainerClient) ([]StoredEventDocument, error) {
	pk := azcosmos.NewPartitionKeyString(CosPartitionKey)

	var result []StoredEventDocument
	qo := azcosmos.QueryOptions{PageSizeHint: 10}
	queryPager := client.NewQueryItemsPager("select * from c where c.status = 'todo'", pk, &qo)
	for queryPager.More() {
		queryResponse, err := queryPager.NextPage(context.Background())
		if err != nil {
			return nil, err
		}

		for _, item := range queryResponse.Items {
			var itemResponseBody StoredEventDocument
			err = json.Unmarshal(item, &itemResponseBody)
			if err != nil {
				return nil, err
			}

			result = append(result, itemResponseBody)
		}
	}

	return result, nil
}
