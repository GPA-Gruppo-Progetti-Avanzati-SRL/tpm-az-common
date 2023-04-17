package cosopsutil

import (
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/tpm-az-common/cosmosdb/coslks"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/tpm-az-common/cosmosdb/cosquery"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/tpm-common/util"
	"github.com/rs/zerolog/log"
)

type PagedReader struct {
	qc         cosquery.QueryClient
	pageNumber int
	numReads   int
	logger     util.GeometricTraceLogger
}

func (pr *PagedReader) Count() (int, int) {
	return pr.pageNumber, pr.numReads
}

func (pr *PagedReader) HasNext() bool {
	return pr.qc.HasNext()
}

func (pr *PagedReader) Read() ([]Document, error) {
	const semLogContext = "page-reader::read"
	var err error
	var resp cosquery.Response

	if pr.pageNumber <= 0 {
		resp, err = pr.qc.Execute()
	} else {
		resp, err = pr.qc.Next()
	}

	if err != nil {
		log.Error().Err(err).Msg(semLogContext)
		return nil, err
	}

	var docs []Document
	if resp.NumDocs() > 0 {
		pr.pageNumber++
		qr := resp.(*QueryResponse)
		for i, d := range qr.Documents {
			pr.numReads++
			if pr.logger.CheckAndSetOnOff() {
				pr.logger.LogEvent(log.Trace().Int("num-reads", i).Int("page-number", pr.pageNumber), semLogContext)
			}
			docs = append(docs, d)
		}
	}

	log.Info().Int("page-number", pr.pageNumber).Int("page-reads", len(docs)).Int("total-num-reads", pr.numReads).Msg(semLogContext)
	return docs, nil
}

func NewPagedReader(lks *coslks.LinkedService, collectionId, queryText string, opts ...Option) (*PagedReader, error) {
	const semLogContext = "page-reade::new"

	queryOpts := PagedReaderDefaultOptions
	for _, o := range opts {
		o(&queryOpts)
	}

	qc, err := cosquery.NewClientInstance(
		cosquery.ResponseDecoderFunc(QueryResponseDecoderFunc),
		cosquery.WithConnectionString(lks.ConnectionString()),
		cosquery.WithDbName(lks.DbName()),
		cosquery.WithCollectionName(lks.CollectionName("files")),
		cosquery.WithQueryText(queryText),
		cosquery.WithPageSize(queryOpts.PageSize),
	)

	if err != nil {
		log.Error().Err(err).Str("coll-id", collectionId).Str("query", queryText).Msg(semLogContext)
		return nil, err
	}

	return &PagedReader{qc: qc, logger: util.GeometricTraceLogger{}}, nil
}
