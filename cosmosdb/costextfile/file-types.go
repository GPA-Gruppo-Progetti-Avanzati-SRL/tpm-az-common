package costextfile

import (
	"encoding/json"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/rs/zerolog/log"
	"path/filepath"
)

const (
	FilePartitionKey     = "cos-text-file"
	FileNeverExpireTTL   = -1
	FileDefaultExpireTTL = 3600 * 24 * 30 // Default value is set to 30 days. It will enforced if the ttl value provided has not been set... 0
)

type RowsStat struct {
	Total     int `yaml:"total,omitempty" mapstructure:"total,omitempty" json:"total,omitempty"`
	Processed int `yaml:"processed,omitempty" mapstructure:"processed,omitempty" json:"processed,omitempty"`
	Valid     int `yaml:"valid,omitempty" mapstructure:"valid,omitempty" json:"valid,omitempty"`
	Failed    int `yaml:"failed,omitempty" mapstructure:"failed,omitempty" json:"failed,omitempty"`
}

type Event struct {
	Duration int `yaml:"duration,omitempty" mapstructure:"duration,omitempty" json:"duration,omitempty"`
	Path     int `yaml:"path,omitempty" mapstructure:"path,omitempty" json:"path,omitempty"`
	Ts       int `yaml:"ts,omitempty" mapstructure:"ts,omitempty" json:"ts,omitempty"`
	Status   int `yaml:"status,omitempty" mapstructure:"status,omitempty" json:"status,omitempty"`
}

/*
 * Note. The TTL could be put On (no default)
 */

type File struct {
	Id        string   `yaml:"id,omitempty" mapstructure:"id,omitempty" json:"id,omitempty"`
	PKey      string   `yaml:"pkey,omitempty" mapstructure:"pkey,omitempty" json:"pkey,omitempty"`
	Path      string   `yaml:"path,omitempty" mapstructure:"path,omitempty" json:"path,omitempty"`
	Filename  string   `yaml:"filename,omitempty" mapstructure:"filename,omitempty" json:"filename,omitempty"`
	Prty      string   `yaml:"prty,omitempty" mapstructure:"prty,omitempty" json:"prty,omitempty"`
	Status    string   `yaml:"status,omitempty" mapstructure:"status,omitempty" json:"status,omitempty"`
	NumDups   int      `yaml:"num-dups,omitempty" mapstructure:"num-dups,omitempty" json:"num-dups,omitempty"`
	RowsStats RowsStat `yaml:"rows-stats,omitempty" mapstructure:"rows-stats,omitempty" json:"rows-stats,omitempty"`
	TTL       int      `yaml:"ttl,omitempty" mapstructure:"ttl,omitempty" json:"ttl,omitempty"`
}

func (f *File) enforceDefaultValues() error {
	const semLogContext = "cos-text-file::set-defaults"

	f.PKey = FilePartitionKey

	if f.TTL == 0 {
		f.TTL = FileDefaultExpireTTL
	}

	if f.Path == "" && f.Filename == "" && f.Id == "" {
		log.Error().Msgf(semLogContext + " no filename or path or id provided")
	}

	if f.Filename == "" && f.Path != "" {
		f.Filename = filepath.Base(f.Path)
	}

	if f.Id == "" {
		f.Id = f.Filename
		log.Warn().Str("filename", f.Filename).Msgf(semLogContext+" no file-id provided... using filename (%s)", f.Id)
	} else {
		if f.Filename == "" {
			f.Filename = f.Id
		}
	}

	return nil
}

func (f *File) ToJson() ([]byte, error) {

	const semLogContext = "cos-text-file::to-json"
	b, err := json.Marshal(f)
	return b, err
}

func FileFromJson(b []byte) (*File, error) {
	const semLogContext = "cos-text-file::from-json"
	f := File{}
	err := json.Unmarshal(b, &f)
	return &f, err
}

func (f *File) MustToJson() []byte {

	const semLogContext = "cos-text-file::must-to-json"
	b, err := json.Marshal(f)
	if err != nil {
		log.Error().Err(err).Str("id", f.Id).Str("pkey", f.PKey).Msg(semLogContext)
		panic(err)
	}

	return b
}

type StoredFile struct {
	*File
	ETag azcore.ETag
}
