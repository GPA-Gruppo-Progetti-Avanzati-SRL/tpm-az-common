package cosquery

import (
	"bytes"
	"encoding/json"
	"github.com/btnguyen2k/gocosmos"
)

type ResponseDecoder interface {
	Decode(resp *gocosmos.RespQueryDocs) (Response, error)
}

type Response interface {
	Rid() string
	Count() int
	NumDocs() int
}

type ResponseDecoderFunc func(resp *gocosmos.RespQueryDocs) (Response, error)

func (f ResponseDecoderFunc) Decode(resp *gocosmos.RespQueryDocs) (Response, error) {
	return f(resp)
}

type DefaultResponse struct {
	RespRid   string                   `json:"_rid"`
	RespCount int                      `json:"_count"`
	Documents []map[string]interface{} `json:"Documents"`
}

func (dr *DefaultResponse) Rid() string {
	return dr.RespRid
}

func (dr *DefaultResponse) Count() int {
	return dr.RespCount
}

func (dr *DefaultResponse) NumDocs() int {
	return len(dr.Documents)
}

func DefaultResponseDecoderFunc(resp *gocosmos.RespQueryDocs) (Response, error) {
	e := &DefaultResponse{}
	err := json.NewDecoder(bytes.NewReader(resp.RespBody)).Decode(e)
	return e, err
}

/*
type Document struct {
	DocId      string `json:"idUnivoco"`
	BlobSize   int    `json:"fullContentSize"`
	DocName    string `json:"nomeDocumento"`
	DocType    string `json:"tipoDocumento"`
	NumPratica string `json:"numeroPratica"`
}

type Response struct {
	Rid       string     `json:"_rid"`
	Count     int        `json:"_count"`
	Documents []Document `json:"Documents"`
}

func UnmarshallResponse(resp *gocosmos.RespQueryDocs) (Response, error) {
	e := Response{}
	if err := json.NewDecoder(bytes.NewReader(resp.RespBody)).Decode(&e); err != nil {
		return e, err
	}

	return e, nil
}
*/
