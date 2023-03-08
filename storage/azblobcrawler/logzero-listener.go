package azblobcrawler

import (
	"github.com/rs/zerolog/log"
	"time"
)

type logZeroListener struct {
}

func (l *logZeroListener) Accept(blob CrawledBlob) (time.Duration, bool) {
	const semLogContext = "log-zero-listener::accept"
	log.Info().Str("path-id", blob.PathId).Str("blob-name", blob.BlobInfo.BlobName).Msg(semLogContext)
	return 0, true
}

func (l *logZeroListener) Process(blob CrawledBlob) error {
	const semLogContext = "log-zero-listener::process"
	log.Info().Str("path-id", blob.PathId).Str("blob-name", blob.BlobInfo.BlobName).Msg(semLogContext)
	return nil
}

func (l *logZeroListener) Start() {
	const semLogContext = "log-zero-listener::start"
	log.Info().Msg(semLogContext)
}

func (l *logZeroListener) Close() {
	const semLogContext = "log-zero-listener::close"
	log.Info().Msg(semLogContext)
}
