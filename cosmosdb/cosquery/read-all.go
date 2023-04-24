package cosquery

import (
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/tpm-az-common/cosmosdb/coslks"
	"github.com/rs/zerolog/log"
)

func ReadAll(lks *coslks.LinkedService, dbName string, collectionName, queryText string, opts ...ReaderOption) ([]Document, error) {
	const semLogContext = "cos-util::read-all"

	readerOpts := ReaderDefaultOptions
	for _, o := range opts {
		o(&readerOpts)
	}

	qc, err := NewClientInstance(
		readerOpts.DecoderFunc,
		WithConnectionString(lks.ConnectionString()),
		WithDbName(dbName),
		WithCollectionName(collectionName),
		WithQueryText(queryText),
		WithPageSize(readerOpts.PageSize),
	)

	if err != nil {
		log.Error().Err(err).Str("coll-id", collectionName).Str("query", queryText).Msg(semLogContext)
		return nil, err
	}

	resp, err := qc.Execute()
	if err != nil {
		log.Error().Err(err).Err(err).Str("coll-id", collectionName).Str("query", queryText).Msg(semLogContext)
		return nil, err
	}

	var docs []Document
	hasNext := true
	for hasNext {
		for _, d := range resp.Docs {
			docs = append(docs, d)
		}

		if (readerOpts.Limit > 0 && readerOpts.Limit >= len(docs)) || !qc.HasNext() {
			hasNext = false
		} else {
			resp, err = qc.Next()
		}

		if err != nil {
			return nil, err
		}
	}

	log.Info().Str("coll-id", collectionName).Str("query", queryText).Int("num-docs", len(docs)).Msg(semLogContext)
	return docs, nil
}
