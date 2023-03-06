package azblobcrawler

import (
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/tpm-az-common/storage/azbloblks"
	"github.com/rs/zerolog/log"
)

type logZeroListener struct {
}

func (l *logZeroListener) Accept(blob azbloblks.BlobInfo) bool {
	const semLogContext = "log-zero-listener::accept"
	log.Info().Str("blob-name", blob.BlobName).Msg(semLogContext)
	return true
}

func (l *logZeroListener) Process(blob CrawledBlob) error {
	const semLogContext = "log-zero-listener::process"
	log.Info().Str("blob-name", blob.BlobInfo.BlobName).Msg(semLogContext)
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
