package cossequence_test

import (
	"context"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/tpm-az-common/cosmosdb/coslks"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/tpm-az-common/cosmosdb/cossequence"
	"github.com/stretchr/testify/require"
	"os"
	"testing"
)

const (
	DbName         = "rtp_bconn_db"
	CollectionName = "sequence"
	// DbName         = "projectADB1"

	AZCOMMON_CDB_ENDPOINT = "AZCOMMON_CDB_ENDPOINT"
	AZCOMMON_CDB_ACCTKEY  = "AZCOMMON_CDB_ACCTKEY"
)

func TestSequence(t *testing.T) {
	cfg := &coslks.Config{
		Endpoint:   os.Getenv(AZCOMMON_CDB_ENDPOINT),
		AccountKey: os.Getenv(AZCOMMON_CDB_ACCTKEY),
	}

	require.NotEmpty(t, cfg.Endpoint, "CosmosDb endpoint not set.... use env var "+AZCOMMON_CDB_ENDPOINT)
	require.NotEmpty(t, cfg.AccountKey, "CosmosDb account-key not set.... use env var "+AZCOMMON_CDB_ACCTKEY)

	lks, err := coslks.NewLinkedServiceWithConfig(*cfg)
	require.NoError(t, err)

	c, err := lks.NewClient(true)
	require.NoError(t, err)

	client, err := c.NewContainer(DbName, CollectionName)
	require.NoError(t, err)

	seqVal, err := cossequence.NextValUpsert(context.Background(), client, cossequence.WithSeqId("TIK"))
	require.NoError(t, err)

	t.Logf("upsert result: %d", seqVal)

	seqVal, err = cossequence.NextVal(context.Background(), client, cossequence.WithSeqId("TAK"))
	require.NoError(t, err)

	t.Logf("upsert result: %d", seqVal)
}
