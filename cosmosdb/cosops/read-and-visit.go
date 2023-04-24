package cosops

import (
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/tpm-az-common/cosmosdb/coslks"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/tpm-az-common/cosmosdb/cosquery"
	"github.com/rs/zerolog/log"
)

const (
	semLogContainer = "cnt"
	semLogQuery     = "query"
)

func ReadAndVisit(lks *coslks.LinkedService, dbName, collectionName, queryText string, opts ...Option) (int, error) {
	const semLogContext = "cos-ops::delete-all"

	cmdOptions := ReadAndVisitDefaultOptions
	for _, o := range opts {
		o(&cmdOptions)
	}

	pr, err := cosquery.NewPagedReader(lks, dbName, collectionName, queryText, cosquery.WithReaderPageSize(cmdOptions.PageSize), cosquery.WithReaderResponseDecoderFunc(cosquery.DocumentKeyQueryResponseDecoderFunc("pkey", "id")))
	if err != nil {
		log.Error().Err(err).Str("coll-id", collectionName).Str("query", queryText).Msg(semLogContext)
		return 0, err
	}

	numberOfRowsAffected := 0
	rows, err := pr.Read()
	if err != nil {
		log.Error().Err(err).Str("coll-id", collectionName).Str("query", queryText).Msg(semLogContext)
		return 0, err
	}

	hasNext := true
	for hasNext {
		var ndocs int
		var err error
		if cmdOptions.Concurrency > 1 {
			ndocs, err = visitDocumentsPipeline(cmdOptions.Visitor, rows, cmdOptions.Concurrency)
		} else {
			ndocs, err = visitDocuments(cmdOptions.Visitor, rows, &cmdOptions)
		}

		if err != nil {
			np, nr := pr.Count()
			log.Error().Err(err).Int("num-pages", np).Int("num-matches", nr).Int("num-docs", ndocs).Str("coll-id", collectionName).Str("query", queryText).Msg(semLogContext)
			return nr, err
		}

		if pr.HasNext() {
			rows, err = pr.Read()
		} else {
			hasNext = false
		}

		if err != nil {
			np, nr := pr.Count()
			log.Error().Err(err).Int("num-pages", np).Int("num-matches", nr).Str("coll-id", collectionName).Str("query", queryText).Msg(semLogContext)
			return numberOfRowsAffected, err
		}
	}

	np, nr := pr.Count()
	log.Info().Err(err).Int("num-pages", np).Int("num-matches", nr).Str("coll-id", collectionName).Str("query", queryText).Msg(semLogContext)

	return nr, nil
}

func visitDocumentsPipeline(dfp Visitor, rows []cosquery.Document, concurrency int) (int, error) {
	err := rowPipeline(rows, dfp, WithConcurrency(concurrency))
	return dfp.Count(), err
}

func visitDocuments(dfp Visitor, rows []cosquery.Document, opts *Options) (int, error) {

	const semLogContext = "cos-ops::visit-documents"
	for _, r := range rows {
		pk, id := r.GetKeys()
		err := dfp.Visit("", DataFrame{id: id, pkey: pk})
		if err != nil {
			return dfp.Count(), err
		}
	}

	return dfp.Count(), nil
}
