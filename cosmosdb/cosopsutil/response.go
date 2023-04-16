package cosopsutil

import (
	"fmt"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/tpm-az-common/cosmosdb/cosquery"
	"github.com/btnguyen2k/gocosmos"
)

type Document struct {
	Id   string `json:"id"`
	PKey string `json:"pkey"`
}

type QueryResponse struct {
	RespRid   string     `json:"_rid"`
	RespCount int        `json:"_count"`
	Documents []Document `json:"Documents"`
}

func (dr *QueryResponse) Rid() string {
	return dr.RespRid
}

func (dr *QueryResponse) Count() int {
	return dr.RespCount
}

func (dr *QueryResponse) NumDocs() int {
	return len(dr.Documents)
}

func QueryResponseDecoderFunc(resp *gocosmos.RespQueryDocs) (cosquery.Response, error) {
	var err error
	e := &QueryResponse{}
	for _, d := range resp.Documents {
		if m, ok := d.(map[string]interface{}); ok {
			newd := Document{Id: m["id"].(string), PKey: m["pkey"].(string)}
			e.Documents = append(e.Documents, newd)
		} else {
			err = fmt.Errorf("unrecognized document type %T", d)
			return e, err
		}
	}

	e.RespCount = len(e.Documents)
	// err := json.NewDecoder(bytes.NewReader(resp.RespBody)).Decode(e)
	return e, nil
}
