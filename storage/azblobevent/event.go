package azblobevent

import (
	"encoding/json"
	"errors"
	"fmt"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/tpm-az-common/storage/azbloblks"
	"github.com/rs/zerolog/log"
	"strings"
)

const (
	BlobCreated = "Microsoft.Storage.BlobCreated"
	BlobDeleted = "Microsoft.Storage.BlobDeleted"
)

type Diagnostics struct {
	BatchId string `mapstructure:"batchId,omitempty" yaml:"batchId,omitempty" json:"batchId,omitempty"`
}

type Data struct {
	Api             string      `mapstructure:"api,omitempty" yaml:"api,omitempty" json:"api,omitempty"`
	ClientRequestId string      `mapstructure:"clientRequestId,omitempty" yaml:"clientRequestId,omitempty" json:"clientRequestId,omitempty"`
	RequestId       string      `mapstructure:"requestId,omitempty" yaml:"requestId,omitempty" json:"requestId,omitempty"`
	ETag            string      `mapstructure:"eTag,omitempty" yaml:"eTag,omitempty" json:"eTag,omitempty"`
	ContentType     string      `mapstructure:"contentType,omitempty" yaml:"contentType,omitempty" json:"contentType,omitempty"`
	ContentLength   int         `mapstructure:"contentLength,omitempty" yaml:"contentLength,omitempty" json:"contentLength,omitempty"`
	BlobType        string      `mapstructure:"blobType,omitempty" yaml:"blobType,omitempty" json:"blobType,omitempty"`
	Url             string      `mapstructure:"url,omitempty" yaml:"url,omitempty" json:"url,omitempty"`
	Sequencer       string      `mapstructure:"sequencer,omitempty" yaml:"sequencer,omitempty" json:"sequencer,omitempty"`
	Diagnostics     Diagnostics `mapstructure:"storageDiagnostics,omitempty" yaml:"storageDiagnostics,omitempty" json:"storageDiagnostics,omitempty"`
}

type Event struct {
	Topic           string `mapstructure:"topic,omitempty" yaml:"topic,omitempty" json:"topic,omitempty"`
	Subject         string `mapstructure:"subject,omitempty" yaml:"subject,omitempty" json:"subject,omitempty"`
	Typ             string `mapstructure:"eventType,omitempty" yaml:"eventType,omitempty" json:"eventType,omitempty"`
	Id              string `mapstructure:"id,omitempty" yaml:"id,omitempty" json:"id,omitempty"`
	Data            Data   `mapstructure:"data,omitempty" yaml:"data,omitempty" json:"data,omitempty"`
	DataVersion     string `mapstructure:"dataVersion,omitempty" yaml:"dataVersion,omitempty" json:"dataVersion,omitempty"`
	MetadataVersion string `mapstructure:"metadataVersion,omitempty" yaml:"metadataVersion,omitempty" json:"metadataVersion,omitempty"`
	Ts              string `mapstructure:"eventTime,omitempty" yaml:"eventTime,omitempty" json:"eventTime,omitempty"`
}

func (evt *Event) IsZero() bool {
	const semLogContext = "az-blob-event::is-zero"
	if evt.Data.Url != "" {
		_, account, container, pathInfo, _, ok := azbloblks.ParseBlobUrl(evt.Data.Url)
		if ok {
			return false
		}

		log.Trace().Str("account", account).Str("container", container).Str("path-info", pathInfo).Msg(semLogContext)
	}

	return true
}

func (evt *Event) BlobInfo() (azbloblks.BlobInfo, error) {
	const semLogContext = "az-blob-event::blob-info"

	_, account, container, pathInfo, _, ok := azbloblks.ParseBlobUrl(evt.Data.Url)
	if !ok {
		return azbloblks.BlobInfo{}, fmt.Errorf("cannot parse blob url %s", (evt.Data.Url))
	}

	bi := azbloblks.BlobInfo{
		Exists:        true,
		AccountName:   account,
		ContainerName: container,
		BlobName:      strings.TrimPrefix(pathInfo, "/"),
		FileName:      "",
		Body:          nil,
		Tags:          nil,
		ContentType:   evt.Data.ContentType,
		Size:          int64(evt.Data.ContentLength),
		ETag:          "",
		LeaseState:    "",
	}

	log.Trace().Str("account", account).Str("container", container).Str("path-info", pathInfo).Msg(semLogContext)
	return bi, nil
}

func DeserializeEvents(b []byte) ([]Event, error) {
	events := make([]Event, 0, 0)
	err := json.Unmarshal(b, &events)
	if err != nil {
		return nil, err
	}

	if len(events) > 0 {
		if events[0].IsZero() {
			err = errors.New("data cannot be recognized as an array of valid blob events")
		}
	}

	return events, err
}
