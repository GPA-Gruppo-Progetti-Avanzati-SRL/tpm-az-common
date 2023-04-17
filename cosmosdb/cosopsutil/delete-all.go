package cosopsutil

import (
	"context"
	"github.com/Azure/azure-sdk-for-go/sdk/data/azcosmos"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/tpm-az-common/cosmosdb/coslks"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/tpm-common/util"
	"github.com/rs/zerolog/log"
)

type DeleteVisitor struct {
	cli     *azcosmos.ContainerClient
	logger  util.GeometricTraceLogger
	numDels int
}

func (v *DeleteVisitor) Count() int {
	return v.numDels
}

func (v *DeleteVisitor) Process(phase string, df PipelineDataFrame) error {

	const semLogContext = "cos-ops-util::delete-visitor"
	if v.logger.CheckAndSetOnOff() {
		v.logger.LogEvent(log.Trace().Str("id", df.id).Str("pkey", df.pkey), semLogContext)
	}

	_, err := v.cli.DeleteItem(context.Background(), azcosmos.NewPartitionKeyString(df.pkey), df.id, nil)
	if err == nil {
		v.numDels++
	}
	return err
}

func DeleteAll(lks *coslks.LinkedService, collectionId, queryText string, opts ...Option) (int, error) {
	const semLogContext = "cos-util::delete-all"

	deleteOptions := DeleteAllDefaultOptions
	for _, o := range opts {
		o(&deleteOptions)
	}

	cli, err := coslks.GetCosmosDbContainer("default", collectionId, false)
	if err != nil {
		log.Error().Err(err).Str("coll-id", collectionId).Str("query", queryText).Msg(semLogContext)
		return 0, err
	}

	dv := DeleteVisitor{cli: cli, logger: util.GeometricTraceLogger{}}

	pr, err := NewPagedReader(lks, collectionId, queryText, opts...)
	if err != nil {
		log.Error().Err(err).Str("coll-id", collectionId).Str("query", queryText).Msg(semLogContext)
		return 0, err
	}

	numberOfRowsDeleted := 0
	rows, err := pr.Read()
	if err != nil {
		log.Error().Err(err).Str("coll-id", collectionId).Str("query", queryText).Msg(semLogContext)
		return 0, err
	}

	hasNext := true
	for hasNext {
		var ndels int
		var err error
		if deleteOptions.Concurrency > 1 {
			ndels, err = deleteDocumentsPipeline(&dv, rows, deleteOptions.Concurrency)
		} else {
			ndels, err = deleteDocuments(&dv, rows)
		}

		numberOfRowsDeleted += ndels
		if err != nil {
			np, nr := pr.Count()
			log.Error().Err(err).Int("num-pages", np).Int("num-matches", nr).Int("num-dels", numberOfRowsDeleted).Str("coll-id", collectionId).Str("query", queryText).Msg(semLogContext)
			return numberOfRowsDeleted, err
		}

		if pr.HasNext() {
			rows, err = pr.Read()
		} else {
			hasNext = false
		}

		if err != nil {
			np, nr := pr.Count()
			log.Error().Err(err).Int("num-pages", np).Int("num-matches", nr).Str("coll-id", collectionId).Str("query", queryText).Msg(semLogContext)
			return numberOfRowsDeleted, err
		}
	}

	np, nr := pr.Count()
	log.Info().Err(err).Int("num-pages", np).Int("num-matches", nr).Int("num-dels", numberOfRowsDeleted).Str("coll-id", collectionId).Str("query", queryText).Msg(semLogContext)

	return numberOfRowsDeleted, nil
}

func deleteDocumentsPipeline(dfp DataframeProcessor, rows []Document, concurrency int) (int, error) {
	err := rowPipeline(rows, dfp, WithConcurrency(concurrency))
	return dfp.Count(), err
}

func deleteDocuments(dfp DataframeProcessor, rows []Document) (int, error) {

	const semLogContext = "delete"
	for _, r := range rows {
		err := dfp.Process("", PipelineDataFrame{id: r.Id, pkey: r.PKey})
		if err != nil {
			return dfp.Count(), err
		}
	}

	return dfp.Count(), nil
}
