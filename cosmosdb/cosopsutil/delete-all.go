package cosopsutil

import (
	"context"
	"github.com/Azure/azure-sdk-for-go/sdk/data/azcosmos"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/tpm-az-common/cosmosdb/coslks"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/tpm-common/util"
	"github.com/rs/zerolog/log"
)

func DeleteAll(lks *coslks.LinkedService, collectionId, queryText string) (int, error) {
	const semLogContext = "cos-util::delete-all"

	cli, err := coslks.GetCosmosDbContainer("default", collectionId, false)
	if err != nil {
		log.Error().Err(err).Str("coll-id", collectionId).Str("query", queryText).Msg(semLogContext)
		return 0, err
	}

	pr, err := NewPagedReader(lks, collectionId, queryText)
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

	logger := util.GeometricTraceLogger{}
	hasNext := true
	for hasNext {
		for i, r := range rows {
			logger.LogEvent(log.Trace().Str("row-id", r.Id).Int("num-row", i), semLogContext)
			_, err = cli.DeleteItem(context.Background(), azcosmos.NewPartitionKeyString(r.PKey), r.Id, nil)
			if err != nil {
				np, nr := pr.Count()
				log.Error().Err(err).Int("num-pages", np).Int("num-matches", nr).Int("num-dels", numberOfRowsDeleted).Str("coll-id", collectionId).Str("query", queryText).Msg(semLogContext)
				return numberOfRowsDeleted, err
			}
			numberOfRowsDeleted++
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
