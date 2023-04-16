package cosutil

import (
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/tpm-az-common/cosmosdb/coslks"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/tpm-az-common/cosmosdb/cosquery"
	"github.com/rs/zerolog/log"
)

func ReadAll(lks *coslks.LinkedService, collectionId, queryText string) ([]Document, error) {
	const semLogContext = "cos-util::read-all"

	qc, err := cosquery.NewClientInstance(
		cosquery.ResponseDecoderFunc(QueryResponseDecoderFunc),
		cosquery.WithConnectionString(lks.ConnectionString()),
		cosquery.WithDbName(lks.DbName()),
		cosquery.WithCollectionName(lks.CollectionName(collectionId)),
		cosquery.WithQueryText(queryText),
	)

	if err != nil {
		log.Error().Err(err).Str("coll-id", collectionId).Str("query", queryText).Msg(semLogContext)
		return nil, err
	}

	resp, err := qc.Execute()
	if err != nil {
		log.Error().Err(err).Err(err).Str("coll-id", collectionId).Str("query", queryText).Msg(semLogContext)
		return nil, err
	}

	var docs []Document
	hasNext := true
	for hasNext {
		if resp.NumDocs() > 0 {
			qr := resp.(*QueryResponse)
			for _, d := range qr.Documents {
				docs = append(docs, d)
			}
		}

		if qc.HasNext() {
			resp, err = qc.Next()
		} else {
			hasNext = false
		}

		if err != nil {
			return nil, err
		}
	}

	log.Info().Str("coll-id", collectionId).Str("query", queryText).Int("num-docs", len(docs)).Msg(semLogContext)
	return docs, nil
}
