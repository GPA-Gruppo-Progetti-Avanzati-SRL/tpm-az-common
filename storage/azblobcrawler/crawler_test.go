package azblobcrawler_test

import (
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/tpm-az-common/storage/azblobcrawler"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/tpm-az-common/storage/azbloblks"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/tpm-az-common/storage/azstoragecfg"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v2"
	"os"
	"regexp"
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
	crawlerCfg = azblobcrawler.Config{
		StorageName: "test",
		Mode:        azblobcrawler.ModeTag,
		TagName:     "status",
		Tags: []azblobcrawler.TagValue{
			{
				Value: "ready",
				Id:    azblobcrawler.TagValueReady,
			},
		},
		Paths: []azblobcrawler.Path{
			{
				Id:          "cvm2leas-pattern",
				Container:   TargetContainer,
				NamePattern: "^(?:[A-Za-z0-9]*/)?([A-Za-z0-9]{1,6})_([0-9]{4}\\-[0-9]{2}\\-[0-9]{2}_[0-9]{2}\\.[0-9]{2}\\.[0-9]{2})_(CVM2LEAS).csv$",
				Regexp:      regexp.MustCompile("^(?:[A-Za-z0-9]*/)?([A-Za-z0-9]{1,6})_([0-9]{4}\\-[0-9]{2}\\-[0-9]{2}_[0-9]{2}\\.[0-9]{2}\\.[0-9]{2})_(CVM2LEAS).csv$"),
			},
		},
		TickInterval: time.Second * 5,
		DownloadPath: "/tmp",
		ExitOnNop:    true,
		ExitOnErr:    true,
	}
)

func TestCrawler(t *testing.T) {
	//ctx := context.Background()
	log.Logger = log.Output(zerolog.ConsoleWriter{Out: os.Stderr})

	stgConfig := azstoragecfg.Config{
		Name:       "test",
		Account:    os.Getenv(AZCommonBlobAccountNameEnvVarName),
		AccountKey: os.Getenv(AZCommonBlobAccountKeyEnvVarName),
		AuthMode:   azstoragecfg.AuthModeAccountKey,
	}

	require.NotEmpty(t, stgConfig.Name, "blob storage account-name not set.... use env var "+AZCommonBlobAccountNameEnvVarName)
	require.NotEmpty(t, stgConfig.AccountKey, "blob storage account-key not set.... use env var "+AZCommonBlobAccountKeyEnvVarName)

	_, err := azbloblks.Initialize([]azstoragecfg.Config{stgConfig})
	require.NoError(t, err)

	b, err := yaml.Marshal(crawlerCfg)
	require.NoError(t, err)

	t.Log(string(b))

	var wg sync.WaitGroup
	crawler, err := azblobcrawler.NewInstance(&crawlerCfg, &wg, azblobcrawler.WithQuitChannel(make(chan error, 2)), azblobcrawler.WithListener(&testListener{}))
	require.NoError(t, err)
	defer crawler.Stop()

	crawler.Start()
	wg.Wait()
}

type testListener struct {
}

func (l *testListener) Accept(blob azblobcrawler.CrawledBlob) (time.Duration, bool) {
	const semLogContext = "test-listener::accept"
	log.Info().Str("path-id", blob.PathId).Str("blob-name", blob.BlobInfo.BlobName).Msg(semLogContext)
	return 0, true
}

func (l *testListener) Process(blob azblobcrawler.CrawledBlob) error {
	const semLogContext = "test-listener::process"
	log.Info().Str("path-id", blob.PathId).Str("blob-name", blob.BlobInfo.BlobName).Msg(semLogContext)

	if blob.LeaseHandler != nil {
		blob.LeaseHandler.Release(azbloblks.WithTag(azbloblks.BlobTag{
			Key:   "status",
			Value: "done",
		}))
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
