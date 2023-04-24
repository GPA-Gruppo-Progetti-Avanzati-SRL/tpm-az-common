package cosquery

import (
	"errors"
	"fmt"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/tpm-az-common/cosmosdb/cosutil"
	"github.com/btnguyen2k/gocosmos"
	"github.com/opentracing/opentracing-go"
	"github.com/rs/zerolog/log"
	"net/http"
	"time"
)

const DefaultThrottleThinkTime = 2

type QueryClient struct {
	client         *gocosmos.RestClient
	dbName         string
	collectionName string
	query          string
	pageSize       int

	withTrace   bool
	traceOpName string
	span        opentracing.Span
	thinkTime   int

	pageNumber        int
	queryRequest      gocosmos.QueryReq
	continuationToken string

	responseDecoder ResponseDecoder
}

type Option func(o *QueryClient)

func WithCollectionName(n string) Option {
	return func(o *QueryClient) {
		o.collectionName = n
	}
}

func WithDbName(n string) Option {
	return func(o *QueryClient) {
		o.dbName = n
	}
}

func WithConnectionString(cs string) Option {
	return func(o *QueryClient) {
		var err error
		o.client, err = gocosmos.NewRestClient(nil, cs)
		if err != nil {
			log.Error().Err(err).Send()
		}
	}
}

func WithPageSize(siz int) Option {
	return func(o *QueryClient) {
		o.pageSize = siz
	}
}

func WithQueryText(q string) Option {
	return func(o *QueryClient) {
		o.query = q
	}
}

func WithTrace(parentSpan opentracing.Span, opn string) Option {
	return func(o *QueryClient) {
		o.withTrace = true
		o.span = parentSpan
		if opn != "" {
			o.traceOpName = opn
		}
	}
}

func WithThinkTime(siz int) Option {
	return func(o *QueryClient) {
		o.thinkTime = siz
	}
}

func NewClientInstance(responseDecoder ResponseDecoder, opts ...Option) (QueryClient, error) {
	q := QueryClient{responseDecoder: responseDecoder, pageNumber: -1, traceOpName: "cos-query"}
	for _, o := range opts {
		o(&q)
	}

	if !q.valid() {
		return q, errors.New("query client invalid")
	}
	return q, nil
}

func (s *QueryClient) valid() bool {

	v := s.client != nil
	if s.responseDecoder == nil {
		s.responseDecoder = ResponseDecoderFunc(DocumentMapResponseDecoderFunc)
	}

	return v
}

func (s *QueryClient) Close() {
	if s.span != nil {
		s.span.Finish()
	}
}

func (s *QueryClient) PageNumber() int {
	return s.pageNumber
}

func (s *QueryClient) HasNext() bool {
	return s.continuationToken != ""
}

func (s *QueryClient) TraceOperationName(pageNumber int) string {
	o := s.traceOpName

	if o != "" && pageNumber >= 0 {
		o = fmt.Sprintf("%s-page-%03d", o, pageNumber)
	}

	return o
}

func (s *QueryClient) Execute() (Response, error) {

	s.pageNumber = 0
	s.continuationToken = ""

	if s.withTrace {
		if s.span != nil {
			parentCtx := s.span.Context()
			s.span = opentracing.StartSpan(
				s.TraceOperationName(s.pageNumber),
				opentracing.ChildOf(parentCtx),
			)
		} else {
			s.span = opentracing.StartSpan(s.TraceOperationName(-1))
		}
	}

	s.queryRequest = gocosmos.QueryReq{
		DbName:                s.dbName,
		CollName:              s.collectionName,
		MaxItemCount:          s.pageSize,
		CrossPartitionEnabled: true,
		ConsistencyLevel:      "Eventual",
		Query:                 s.query,
	}

	return s.executeQuery()
}

func (s *QueryClient) Next() (Response, error) {

	if s.thinkTime > 0 {
		time.Sleep(time.Duration(s.thinkTime) * time.Second)
	}

	s.pageNumber++
	if s.continuationToken != "" {
		s.queryRequest.ContinuationToken = s.continuationToken
	} else {
		return Response{}, errors.New("no continuation token present")
	}

	return s.executeQuery()
}

func (s *QueryClient) executeQuery() (Response, error) {

	if s.withTrace {
		var parentCtx opentracing.SpanContext
		if s.span != nil {
			parentCtx = s.span.Context()
		}

		span := opentracing.StartSpan(
			s.TraceOperationName(s.pageNumber),
			opentracing.ChildOf(parentCtx),
		)
		defer span.Finish()
	}

	resp := s.client.QueryDocuments(s.queryRequest)
	for resp.StatusCode == 429 {
		log.Warn().Err(resp.Error()).Send()
		if s.thinkTime > 0 {
			time.Sleep(time.Duration(s.thinkTime) * time.Second)
		} else {
			time.Sleep(DefaultThrottleThinkTime * time.Second)
		}
		resp = s.client.QueryDocuments(s.queryRequest)
	}

	s.continuationToken = resp.ContinuationToken

	if s.withTrace {
		s.span.SetTag(cosutil.HttStatusCodeTraceTag, resp.StatusCode)
		s.span.SetTag("req.limit", s.pageSize)
		s.span.SetTag("req.page-number", s.pageNumber)
	}

	switch resp.StatusCode {
	case http.StatusOK:
		r, err := s.responseDecoder.Decode(resp)
		if err != nil {
			return Response{}, err
		}

		if s.withTrace {
			s.span.SetTag("query.num-docs", len(r.Docs))
			s.span.SetTag("query.count", r.RespCount)

			if resp.ContinuationToken != "" {
				s.span.SetTag("continuation", resp.ContinuationToken)
			}
		}
		return r, nil
	case http.StatusNotFound:
		r, err := s.responseDecoder.Decode(resp)
		if err != nil {
			return Response{}, err
		}
		return r, nil
	}

	return Response{}, resp.Error()
}
