package cosops

import (
	"context"
	"github.com/Azure/azure-sdk-for-go/sdk/data/azcosmos"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/tpm-az-common/cosmosdb/coslks"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/tpm-common/util"
	"github.com/rs/zerolog/log"
)

func DeleteAll(lks *coslks.LinkedService, dbName, collectionName, queryText string, opts ...Option) (int, error) {
	const semLogContext = "cos-ops::delete-all"
	var deleteOpts = opts

	cli, err := lks.GetCosmosDbContainer(dbName, collectionName, false)
	if err != nil {
		log.Error().Err(err).Str("coll-id", collectionName).Str("query", queryText).Msg(semLogContext)
		return 0, err
	}

	dv := &DeleteVisitor{cli: cli, logger: util.GeometricTraceLogger{}}
	deleteOpts = append(deleteOpts, WithVisitor(dv))

	return ReadAndVisit(lks, dbName, collectionName, queryText, deleteOpts...)
}

type DeleteVisitor struct {
	cli     *azcosmos.ContainerClient
	logger  util.GeometricTraceLogger
	numDels int
}

func (v *DeleteVisitor) Count() int {
	return v.numDels
}

func (v *DeleteVisitor) Visit(phase string, df DataFrame) error {

	const semLogContext = "cos-ops::delete-visitor"
	if v.logger.CheckAndSetOnOff() {
		v.logger.LogEvent(log.Trace().Int("num-dels", v.numDels).Str("id", df.id).Str("pkey", df.pkey), semLogContext)
	}

	_, err := v.cli.DeleteItem(context.Background(), azcosmos.NewPartitionKeyString(df.pkey), df.id, nil)
	if err == nil {
		v.numDels++
	}
	return err
}
