package azblobevent_test

import (
	"context"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/tpm-az-common/cosmosdb/coslks"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/tpm-az-common/storage/azblobevent"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v2"
	"os"
	"sync"
	"testing"
	"time"
)

const (
	TargetContainer     = "lks-container"
	DropContainerOnExit = false

	AZCommonBlobAccountNameEnvVarName = "AZCOMMON_BLOB_ACCOUNTNAME"
	AZCommonBlobAccountKeyEnvVarName  = "AZCOMMON_BLOB_ACCTKEY"
)

var (
	crawlerCfg = azblobevent.Config{
		CosName:      "default",
		TickInterval: time.Second * 5,
		ExitOnNop:    true,
		ExitOnErr:    true,
	}
)

func TestCrawler(t *testing.T) {
	//ctx := context.Background()
	log.Logger = log.Output(zerolog.ConsoleWriter{Out: os.Stderr})

	b, err := yaml.Marshal(crawlerCfg)
	require.NoError(t, err)

	t.Log(string(b))

	var wg sync.WaitGroup
	crawler, err := azblobevent.NewInstance(&crawlerCfg, &wg, azblobevent.WithQuitChannel(make(chan error, 2)), azblobevent.WithListener(&testListener{}))
	require.NoError(t, err)
	defer crawler.Stop()

	crawler.Start()
	wg.Wait()
}

type testListener struct {
}

func (l *testListener) Accept(blob azblobevent.CrawledEvent) (time.Duration, bool) {
	const semLogContext = "test-listener::accept"
	log.Info().Msg(semLogContext)
	return 0, true
}

func (l *testListener) Process(ce azblobevent.CrawledEvent) error {
	const semLogContext = "test-listener::process"
	log.Info().Msg(semLogContext)

	defer func() {
		if ce.LeaseHandler != nil {
			err := ce.LeaseHandler.Release()
			if err != nil {
				log.Error().Err(err).Msg(semLogContext + " releasing lease")
			}
		}
	}()

	cnt, err := coslks.GetCosmosDbContainer(ce.CosName, azblobevent.CosCollectionId, false)
	if err != nil {
		return err
	}

	err = azblobevent.UpdateEventDocumentStatus(context.Background(), cnt, ce.PKey, ce.Id, "done")
	if err != nil {
		return err
	}

	return nil
}

func (l *testListener) Close() {
	const semLogContext = "test-listener::close"
	log.Info().Msg(semLogContext)
}

func (l *testListener) Start() {
	const semLogContext = "test-listener::start"
	log.Info().Msg(semLogContext)
}
