package costextfile

import (
	"encoding/json"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/rs/zerolog/log"
	"path/filepath"
	"time"
)

const (
	FilePartitionKey     = "cos-text-file"
	FileNeverExpireTTL   = -1
	FileDefaultExpireTTL = 3600 * 24 * 30 // Default value is set to 30 days. It will enforced if the ttl value provided has not been set... 0

	StatusAccepted     = "accepted"
	StatusRefused      = "refused"
	StatusDone         = "done"
	StatusUploaded     = "uploaded"
	StatusWorking      = "working"
	StatusEmpty        = "empty"
	StatusCreated      = "created"
	StatusProduced     = "produced"
	StatusFailed       = "failed"
	StatusAcceptedText = "Accepted"
	StatusRefusedText  = "Refused"
	StatusDoneText     = "Done"
	StatusUploadedText = "Uploaded"
	StatusWorkingText  = "Working"
	StatusEmptyText    = "Empty"
	StatusCreatedText  = "Created"
	StatusProducedText = "Produced"
	StatusFailedText   = "Failed"
)

// Note: omitempty removed because they are counters and the 0 value is a meaningful value.

type RowsStat struct {
	Total     int `yaml:"total" mapstructure:"total" json:"total"`
	Processed int `yaml:"processed,omitempty" mapstructure:"processed,omitempty" json:"processed,omitempty"`
	Valid     int `yaml:"valid" mapstructure:"valid" json:"valid"`
	Failed    int `yaml:"failed" mapstructure:"failed" json:"failed"`
}

type Event struct {
	Status   FileStatus `yaml:"status,omitempty" mapstructure:"status,omitempty" json:"status,omitempty"`
	Duration int        `yaml:"duration,omitempty" mapstructure:"duration,omitempty" json:"duration,omitempty"`
	Ts       string     `yaml:"ts,omitempty" mapstructure:"ts,omitempty" json:"ts,omitempty"`
}

type FileStatus struct {
	Code   string `yaml:"cd,omitempty" mapstructure:"cd,omitempty" json:"cd,omitempty"`
	Reason string `yaml:"rsn,omitempty" mapstructure:"rsn,omitempty" json:"rsn,omitempty"`
	Text   string `yaml:"text,omitempty" mapstructure:"text,omitempty" json:"text,omitempty"`
}

/*
 * Note. The TTL could be put On (no default)
 */

type File struct {
	Id        string     `yaml:"id,omitempty" mapstructure:"id,omitempty" json:"id,omitempty"`
	PKey      string     `yaml:"pkey,omitempty" mapstructure:"pkey,omitempty" json:"pkey,omitempty"`
	Path      string     `yaml:"path,omitempty" mapstructure:"path,omitempty" json:"path,omitempty"`
	Filename  string     `yaml:"filename,omitempty" mapstructure:"filename,omitempty" json:"filename,omitempty"`
	Prty      string     `yaml:"prty,omitempty" mapstructure:"prty,omitempty" json:"prty,omitempty"`
	Status    FileStatus `yaml:"status,omitempty" mapstructure:"status,omitempty" json:"status,omitempty"`
	NumDups   int        `yaml:"num-dups,omitempty" mapstructure:"num-dups,omitempty" json:"num-dups,omitempty"`
	RowsStats RowsStat   `yaml:"rows-stats" mapstructure:"rows-stats" json:"rows-stats"`
	Events    []Event    `yaml:"events" mapstructure:"events" json:"events"`
	TTL       int        `yaml:"ttl,omitempty" mapstructure:"ttl,omitempty" json:"ttl,omitempty"`
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

func (f *File) AddEvent(evt Event, overrideStatus bool) {
	const semLogContext = "cos-text-file::add-event"
	evt.Ts = time.Now().Format(time.RFC3339Nano)
	f.Events = append(f.Events, evt)
	if overrideStatus {
		f.Status = evt.Status
	}
}

type StoredFile struct {
	*File
	ETag azcore.ETag
}
