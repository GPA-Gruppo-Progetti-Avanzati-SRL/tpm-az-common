package costextfile

import (
	"encoding/json"
	"fmt"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/rs/zerolog/log"
)

const (
	RowNeverExpireTTL   = -1
	RowDefaultExpireTTL = 3600 * 24 * 30 // Default value is set to 30 days. It will enforced if the ttl value provided has not been set... 0
)

type RowStatus struct {
	Code string `yaml:"cd,omitempty" mapstructure:"cd,omitempty" json:"cd,omitempty"`
	Text string `yaml:"text,omitempty" mapstructure:"text,omitempty" json:"text,omitempty"`
}

type Row struct {
	Id        string                 `yaml:"id,omitempty" mapstructure:"id,omitempty" json:"id,omitempty"`
	PKey      string                 `yaml:"pkey,omitempty" mapstructure:"pkey,omitempty" json:"pkey,omitempty"`
	FileId    string                 `yaml:"file-id,omitempty" mapstructure:"file-id,omitempty" json:"file-id,omitempty"`
	Raw       string                 `yaml:"raw,omitempty" mapstructure:"raw,omitempty" json:"raw,omitempty"`
	Status    RowStatus              `yaml:"status,omitempty" mapstructure:"status,omitempty" json:"status,omitempty"`
	RowNumber int                    `yaml:"row-num,omitempty" mapstructure:"row-num,omitempty" json:"row-num,omitempty"`
	Data      map[string]interface{} `yaml:"data,omitempty" mapstructure:"data,omitempty" json:"data,omitempty"`
	TTL       int                    `yaml:"ttl,omitempty" mapstructure:"ttl,omitempty" json:"ttl,omitempty"`
}

func (r *Row) enforceDefaultValues() {

	const semLogContext = "cos-text-row::set-defaults"

	r.PKey = r.FileId
	if r.PKey == "" {
		log.Warn().Str("file-id", r.FileId).Msgf(semLogContext+" no file-id provided... using default (%s)", FilePartitionKey)
	}

	if r.Id == "" {
		r.Id = fmt.Sprintf("%s-%d", r.FileId, r.RowNumber)
		log.Warn().Int("row-num", r.RowNumber).Str("file-id", r.FileId).Msgf(semLogContext+" no row-id provided... using filed id and row number (%s)", r.Id)
	}

	if r.TTL == 0 {
		r.TTL = RowDefaultExpireTTL
	}
}

func (f *Row) ToJson() ([]byte, error) {

	const semLogContext = "cos-text-row::to-json"
	b, err := json.Marshal(f)
	return b, err
}

func RowFromJson(b []byte) (*Row, error) {
	const semLogContext = "cos-text-row::from-json"
	f := Row{}
	err := json.Unmarshal(b, &f)
	return &f, err
}

func (f *Row) MustToJson() []byte {

	const semLogContext = "cos-text-row::must-to-json"
	b, err := json.Marshal(f)
	if err != nil {
		log.Error().Err(err).Str("id", f.Id).Str("pkey", f.PKey).Msg(semLogContext)
		panic(err)
	}

	return b
}

type StoredRow struct {
	*Row
	ETag azcore.ETag
}
