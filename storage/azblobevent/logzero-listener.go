package azblobevent

import (
	"github.com/rs/zerolog/log"
	"time"
)

type logZeroListener struct {
}

func (l *logZeroListener) Accept(blob CrawledEvent) (time.Duration, bool) {
	const semLogContext = "log-zero-listener::accept"
	log.Info().Msg(semLogContext)
	return 0, true
}

func (l *logZeroListener) Process(blob CrawledEvent) error {
	const semLogContext = "log-zero-listener::process"
	log.Info().Msg(semLogContext)
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
