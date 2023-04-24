package cosquery

import (
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/tpm-az-common/cosmosdb/coslks"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/tpm-common/util"
	"github.com/rs/zerolog/log"
)

/*
 * Options
 */

type ReaderOptions struct {
	PageSize    int
	Limit       int
	DecoderFunc ResponseDecoderFunc
}

type ReaderOption func(opts *ReaderOptions)

var ReaderDefaultOptions = ReaderOptions{
	PageSize:    500,
	DecoderFunc: DocumentMapResponseDecoderFunc,
	Limit:       0,
}

func WithReaderPageSize(s int) ReaderOption {
	return func(opts *ReaderOptions) {
		if s > 0 {
			if opts.Limit > 0 && s > opts.Limit {
				opts.PageSize = opts.Limit
			} else {
				opts.PageSize = s
			}
		}
	}
}

func WithReaderLimit(s int) ReaderOption {
	return func(opts *ReaderOptions) {
		opts.Limit = s
		if s > 0 && s < opts.PageSize {
			opts.PageSize = opts.Limit
		}
	}
}

func WithReaderResponseDecoderFunc(f ResponseDecoderFunc) ReaderOption {
	return func(opts *ReaderOptions) {
		opts.DecoderFunc = f
	}
}

/*
 * Response object
 */

/*
type PagedReaderResponse struct {
	RespRid                                           string `json:"_rid"`
	RespCount                                         int    `json:"_count"`
	Documents []interface{}        `json:"Documents"`
}

func (dr *PagedReaderResponse) Rid() string {
	return dr.RespRid
}

func (dr *PagedReaderResponse) Count() int {
	return dr.RespCount
}

func (dr *PagedReaderResponse) NumDocs() int {
	return len(dr.Documents)
}

func (dr *PagedReaderResponse) PageNumber() int {
	return 1
}

func PagedReaderResponseDecoderFunc(resp *gocosmos.RespQueryDocs) (Response, error) {
	e := &PagedReaderResponse{}
	if resp != nil {
		e.Documents = resp.Documents
		e.RespCount = resp.Count
	}
	return e, nil
}
*/
/*
 * PageReader
 */

type PagedReader struct {
	qc         QueryClient
	pageNumber int
	numReads   int
	logger     util.GeometricTraceLogger
	limit      int
}

func (pr *PagedReader) Count() (int, int) {
	return pr.pageNumber, pr.numReads
}

func (pr *PagedReader) HasNext() bool {
	if pr.limit > 0 && pr.numReads >= pr.limit {
		return false
	}

	return pr.qc.HasNext()
}

func (pr *PagedReader) Read() ([]Document, error) {
	const semLogContext = "page-reader::read"
	var err error
	var resp Response

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
	if len(resp.Docs) > 0 {
		pr.pageNumber++
		pr.numReads += len(resp.Docs)
		docs = resp.Docs
	}

	if pr.logger.CheckAndSetOnOff() {
		log.Info().Int("page-number", pr.pageNumber).Int("page-reads", len(docs)).Int("total-num-reads", pr.numReads).Msg(semLogContext)
	}
	return docs, nil
}

func NewPagedReader(lks *coslks.LinkedService, dbName, collectionName, queryText string, opts ...ReaderOption) (*PagedReader, error) {
	const semLogContext = "page-reade::new"

	queryOpts := ReaderDefaultOptions
	for _, o := range opts {
		o(&queryOpts)
	}

	qc, err := NewClientInstance(
		queryOpts.DecoderFunc,
		WithConnectionString(lks.ConnectionString()),
		WithDbName(dbName),
		WithCollectionName(collectionName),
		WithQueryText(queryText),
		WithPageSize(queryOpts.PageSize),
	)

	if err != nil {
		log.Error().Err(err).Str("container", collectionName).Str("query", queryText).Msg(semLogContext)
		return nil, err
	}

	return &PagedReader{qc: qc, logger: util.GeometricTraceLogger{}, limit: queryOpts.Limit}, nil
}
