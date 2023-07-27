package azblobevent

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/data/azcosmos"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/tpm-az-common/cosmosdb/cosutil"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/tpm-common/util"
)

type CosmosDbDocumentMetadata struct {
	ETag  azcore.ETag `mapstructure:"_etag,omitempty" yaml:"_etag,omitempty" json:"_etag,omitempty"`
	Rid   azcore.ETag `mapstructure:"_rid,omitempty" yaml:"_rid,omitempty" json:"_rid,omitempty"`
	Self  azcore.ETag `mapstructure:"_self,omitempty" yaml:"_self,omitempty" json:"_self,omitempty"`
	CosTs int64       `mapstructure:"_ts,omitempty" yaml:"_ts,omitempty" json:"_ts,omitempty"`
}

type EventDocumentAnnotationNote struct {
	Typ     string `mapstructure:"type,omitempty" yaml:"type,omitempty" json:"type,omitempty"`
	Code    string `mapstructure:"code,omitempty" yaml:"code,omitempty" json:"code,omitempty"`
	Message string `mapstructure:"message,omitempty" yaml:"message,omitempty" json:"message,omitempty"`
}

type EventDocumentAnnotation struct {
	Id   string                        `mapstructure:"id,omitempty" yaml:"id,omitempty" json:"id,omitempty"`
	PKey string                        `mapstructure:"pkey,omitempty" yaml:"pkey,omitempty" json:"pkey,omitempty"`
	Name string                        `mapstructure:"name,omitempty" yaml:"name,omitempty" json:"name,omitempty"`
	Typ  string                        `mapstructure:"type,omitempty" yaml:"type,omitempty" json:"type,omitempty"`
	Note []EventDocumentAnnotationNote `mapstructure:"notes,omitempty" yaml:"notes,omitempty" json:"notes,omitempty"`
	TTL  int                           `mapstructure:"ttl,omitempty" yaml:"ttl,omitempty" json:"ttl,omitempty"`
}

type StoredEventDocumentAnnotation struct {
	*EventDocumentAnnotation
	CosmosDbDocumentMetadata
}

func (c *EventDocumentAnnotation) MustToJSON() []byte {
	b, err := json.Marshal(c)
	if err != nil {
		panic(err)
	}

	return b
}

func DeserializeEventDocumentAnnotation(b []byte) (*EventDocumentAnnotation, error) {
	ctx := EventDocumentAnnotation{}
	err := json.Unmarshal(b, &ctx)
	if err != nil {
		return nil, err
	}

	ctx.PKey = CosPartitionKey
	return &ctx, nil
}

func InsertEventDocumentAnnotation(ctx context.Context, client *azcosmos.ContainerClient, evtPkey, evtId string, tokCtx *EventDocumentAnnotation) (StoredEventDocumentAnnotation, error) {
	tokCtx.PKey = fmt.Sprintf("note:%s:%s", evtPkey, evtId)
	tokCtx.Id = util.NewObjectId().String()

	resp, err := client.CreateItem(ctx, azcosmos.NewPartitionKeyString(tokCtx.PKey), tokCtx.MustToJSON(), nil)
	if err != nil {
		return StoredEventDocumentAnnotation{}, cosutil.MapAzCoreError(err)
	}

	return StoredEventDocumentAnnotation{EventDocumentAnnotation: tokCtx, CosmosDbDocumentMetadata: CosmosDbDocumentMetadata{ETag: resp.ETag}}, nil
}
