package azblobevent_test

import (
	"context"
	"errors"
	"github.com/Azure/azure-sdk-for-go/sdk/data/azcosmos"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/tpm-az-common/cosmosdb/coslks"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/tpm-az-common/storage/azblobevent"
	"github.com/stretchr/testify/require"
	"os"
	"testing"
)

var evts = []byte(`
[
  {
    "topic": "/subscriptions/80d4f652-4caf-4ba5-8a61-900659986846/resourceGroups/GECT/providers/Microsoft.Storage/storageAccounts/gecttest",
    "subject": "/blobServices/default/containers/test-leas-events/blobs/test.txt",
    "eventType": "Microsoft.Storage.BlobCreated",
    "id": "2cb94a7d-f01e-0017-5521-baabe1063553",
    "data": {
      "api": "PutBlob",
      "clientRequestId": "d12d6687-6112-4b7a-8c21-b4cce72f301d",
      "requestId": "2cb94a7d-f01e-0017-5521-baabe1000000",
      "eTag": "0x8DB883894F72BD2",
      "contentType": "text/plain",
      "contentLength": 10,
      "blobType": "BlockBlob",
      "url": "https://gecttest.blob.core.windows.net/test-leas-events/test.txt",
      "sequencer": "000000000000000000000000000044FB00000000002a37b7",
      "storageDiagnostics": {
        "batchId": "5a5f87ad-6006-0058-0021-badab5000000"
      }
    },
    "dataVersion": "",
    "metadataVersion": "1",
    "eventTime": "2023-07-19T09:14:40.1839833Z"
  }
]
`)

func TestMain(m *testing.M) {

	cfg := coslks.Config{
		CosmosName: "default",
		Endpoint:   os.Getenv("AZCOMMON_COS_ENDPOINT"),
		AccountKey: os.Getenv("AZCOMMON_COS_ACCTKEY"),
		DB: coslks.KeyNamePair{
			Id:   "default",
			Name: os.Getenv("AZCOMMON_COS_DBNAME"),
		},
		Collections: coslks.CollectionsCfg{
			{
				Id:   "events",
				Name: "tokens",
			},
		},
	}

	dbName := os.Getenv("AZCOMMON_COS_DBNAME")
	collectionName := TokensCollectionName
	if cfg.Endpoint == "" {
		panic(errors.New("CosmosDb endpoint not set.... use env var AZCOMMON_COS_ENDPOINT"))
	}

	if cfg.AccountKey == "" {
		panic(errors.New("CosmosDb account-key not set.... use env var AZCOMMON_COS_ACCTKEY"))
	}

	if dbName == "" {
		panic(errors.New("CosmosDb db-name not set.... use env var AZCOMMON_COS_DBNAME"))
	}

	lks, err := coslks.Initialize([]coslks.Config{cfg})
	if err != nil {
		panic(err)
	}

	c, err := lks[0].NewClient(false)
	if err != nil {
		panic(err)
	}

	cli, err = c.NewContainer(dbName, collectionName)
	if err != nil {
		panic(err)
	}

	exitVal := m.Run()
	os.Exit(exitVal)
}

const (
	TokensCollectionName = "tokens"
	DeleteOnExit         = false
)

var cli *azcosmos.ContainerClient

func TestBlobEvent(t *testing.T) {
	evts, err := azblobevent.DeserializeEvents(evts)
	require.NoError(t, err)

	for _, e := range evts {

		evt := e

		b, err := e.BlobInfo()
		require.NoError(t, err)

		eDoc := azblobevent.EventDocument{
			Id:            evt.Id,
			PKey:          azblobevent.CosPartitionKey,
			Typ:           evt.Typ,
			Status:        "todo",
			AccountName:   b.AccountName,
			ContainerName: b.ContainerName,
			BlobName:      b.BlobName,
			ContentType:   b.ContentType,
			ContentLength: b.Size,
			BlobType:      evt.Data.BlobType,
			Url:           evt.Data.Url,
			Ts:            evt.Ts,
			TTL:           -1,
		}

		t.Log("Insert event document of id ", evt.Id)
		_, err = azblobevent.InsertEventDocument(context.Background(), cli, &eDoc)
		require.NoError(t, err)

		defer func(evt *azblobevent.EventDocument) {
			if DeleteOnExit {
				t.Log("delete event document of id ", evt.Id)
				_, _ = azblobevent.DeleteEventDocument(context.Background(), cli, azblobevent.CosPartitionKey, evt.Id)
			}
		}(&eDoc)

		evts, err := azblobevent.FindEventDocuments(cli)
		require.NoError(t, err)

		for _, e := range evts {
			t.Log(e.EventDocument)
		}
	}
}
