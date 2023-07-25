package coslease_test

import (
	"context"
	"errors"
	"github.com/Azure/azure-sdk-for-go/sdk/data/azcosmos"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/tpm-az-common/cosmosdb/coslease"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/tpm-az-common/cosmosdb/coslks"
	"github.com/stretchr/testify/require"
	"os"
	"testing"
	"time"
)

const (
	PartitionKey = "1234"
	ObjectId     = "5678"

	LeaseCollectionName = "tokens"
)

var cli *azcosmos.ContainerClient

func TestMain(m *testing.M) {

	cfg := coslks.Config{
		Endpoint:   os.Getenv("AZCOMMON_COS_ENDPOINT"),
		AccountKey: os.Getenv("AZCOMMON_COS_ACCTKEY"),
	}

	dbName := os.Getenv("AZCOMMON_COS_DBNAME")
	collectionName := LeaseCollectionName
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

func TestLease(t *testing.T) {
	t.Logf("acquire lease on object %s:%s", PartitionKey, ObjectId)
	lh, err := coslease.AcquireLease(context.Background(), cli, coslease.LeaseTypeBlobEvent, PartitionKey, ObjectId, false)
	require.NoError(t, err)

	t.Logf("release lease on object %s:%s", PartitionKey, ObjectId)
	err = lh.Release()
	require.NoError(t, err)

	t.Logf("acquire lease on object %s:%s", PartitionKey, ObjectId)
	_, err = coslease.AcquireLease(context.Background(), cli, coslease.LeaseTypeBlobEvent, PartitionKey, ObjectId, false)
	require.NoError(t, err)

	t.Log("waiting 10 secs for not expired test...")
	time.Sleep(time.Second * 10)
	t.Logf("try to acquire lease on object %s:%s.... and should fail", PartitionKey, ObjectId)
	_, err = coslease.AcquireLease(context.Background(), cli, coslease.LeaseTypeBlobEvent, PartitionKey, ObjectId, false)
	require.Error(t, err)
	t.Log(err)

	t.Log("waiting 70 secs for lease expiration...")
	time.Sleep(time.Second * 70)
	t.Logf("acquire lease on object %s:%s", PartitionKey, ObjectId)
	lh, err = coslease.AcquireLease(context.Background(), cli, coslease.LeaseTypeBlobEvent, PartitionKey, ObjectId, true)
	require.NoError(t, err)

	t.Log("waiting 100 secs before release...")
	time.Sleep(time.Second * 100)
	t.Logf("release lease on object %s:%s", PartitionKey, ObjectId)
	err = lh.Release()
	require.NoError(t, err)
}
