package cosquery

import (
	"errors"
	"fmt"
	"github.com/btnguyen2k/gocosmos"
)

type ResponseDecoder interface {
	Decode(resp *gocosmos.RespQueryDocs) (Response, error)
}

type Document interface {
	GetKeys() (string, string)
}

type DocumentMap map[string]interface{}

func (d DocumentMap) GetKeys() (string, string) {
	panic(errors.New("GetKeys not implemented in ResponseDocumentImpl"))
	return "", ""
}

type DocumentKey struct {
	Id   string `yaml:"id" mapstructure:"id" json:"id"`
	PKey string `yaml:"pkey" mapstructure:"pkey" json:"pkey"`
}

func (d DocumentKey) GetKeys() (string, string) {
	return d.PKey, d.Id
}

/*
type Response interface {
	Rid() string
	Count() int
	NumDocs() int
	PageNumber() int
	Documents() []ResponseDocument
}
*/

type ResponseDecoderFunc func(resp *gocosmos.RespQueryDocs) (Response, error)

func (f ResponseDecoderFunc) Decode(resp *gocosmos.RespQueryDocs) (Response, error) {
	return f(resp)
}

type Response struct {
	RespRid   string     `yaml:"_rid" mapstructure:"_rid" json:"_rid"`
	RespCount int        `yaml:"_count" mapstructure:"_count" json:"_count"`
	Docs      []Document `yaml:"documents,omitempty" mapstructure:"documents,omitempty" json:"documents,omitempty"`
}

/*
func (dr *DefaultResponse) Rid() string {
	return dr.RespRid
}

func (dr *DefaultResponse) Count() int {
	return dr.RespCount
}

func (dr *DefaultResponse) NumDocs() int {
	return len(dr.Docs)
}

func (dr *DefaultResponse) PageNumber() int {
	return 1
}

func (dr *DefaultResponse) Documents() []interface{} {
	return dr.Docs
}
*/

func DocumentMapResponseDecoderFunc(resp *gocosmos.RespQueryDocs) (Response, error) {
	e := Response{}
	if resp != nil {
		e.RespCount = resp.Count
		for _, d := range resp.Documents {
			switch typedDoc := d.(type) {
			case map[string]interface{}:
				e.Docs = append(e.Docs, DocumentMap(typedDoc))
			case gocosmos.DocInfo:
				e.Docs = append(e.Docs, DocumentMap(typedDoc.AsMap()))
			}
		}
	}
	return e, nil
}

func DocumentKeyQueryResponseDecoderFunc(pkeyFieldName, idFieldName string) func(resp *gocosmos.RespQueryDocs) (Response, error) {
	return func(resp *gocosmos.RespQueryDocs) (Response, error) {
		e := Response{}
		if resp != nil {
			var err error
			for _, d := range resp.Documents {
				if m, ok := d.(map[string]interface{}); ok {
					newd := DocumentKey{Id: m[idFieldName].(string), PKey: m[pkeyFieldName].(string)}
					e.Docs = append(e.Docs, newd)
				} else {
					err = fmt.Errorf("unrecognized document type %T", d)
					return e, err
				}
			}

			e.RespCount = resp.Count
		}
		return e, nil
	}
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
