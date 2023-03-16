package costextfile_test

import (
	"context"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/tpm-az-common/cosmosdb/coslks"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/tpm-az-common/cosmosdb/costextfile"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"github.com/stretchr/testify/require"
	"os"
	"testing"
)

const (
	DbName         = "rtp_bconn_db"
	CollectionName = "tokens"

	AZCOMMON_CDB_ENDPOINT = "AZCOMMON_CDB_ENDPOINT"
	AZCOMMON_CDB_ACCTKEY  = "AZCOMMON_CDB_ACCTKEY"
)

func TestFile(t *testing.T) {
	log.Logger = log.Output(zerolog.ConsoleWriter{Out: os.Stderr})

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

	stf := costextfile.StoredFile{
		File: &costextfile.File{
			Id:        "id",
			Path:      "path",
			Filename:  "filename",
			Prty:      "",
			Status:    costextfile.FileStatus{},
			NumDups:   0,
			RowsStats: costextfile.RowsStat{},
			Events:    nil,
			TTL:       120,
		},
		ETag: "",
	}

	_, err = stf.Upsert(context.Background(), client)
	require.NoError(t, err)
}
