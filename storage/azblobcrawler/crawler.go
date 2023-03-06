package azblobcrawler

import (
	"errors"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/tpm-az-common/storage/azbloblks"
	"github.com/rs/zerolog/log"
	"path/filepath"
	"sync"
	"time"
)

type CrawledBlob struct {
	PathId       string
	BlobInfo     azbloblks.BlobInfo
	LeaseHandler *azbloblks.LeaseHandler
}

type Crawler struct {
	cfg *Config

	quitc       chan struct{}
	parentQuitc chan error
	wg          *sync.WaitGroup

	listener Listener
}

type Listener interface {
	Accept(blob CrawledBlob) bool
	Process(blob CrawledBlob) error
	Start()
	Close()
}

type Option func(c *Crawler)

func WithListener(l Listener) Option {
	return func(c *Crawler) {
		c.listener = l
	}
}

func WithQuitChannel(qc chan error) Option {
	return func(c *Crawler) {
		c.parentQuitc = qc
	}
}

func NewInstance(cfg *Config, wg *sync.WaitGroup, opts ...Option) (Crawler, error) {

	const semLogContext = "azb-crawler::new"
	_, err := azbloblks.GetLinkedService(cfg.StorageName)
	if err != nil {
		log.Error().Err(err).Msg(semLogContext)
		return Crawler{}, err
	}

	c := Crawler{cfg: cfg, wg: wg, quitc: make(chan struct{})}
	for _, o := range opts {
		o(&c)
	}

	if c.listener == nil {
		c.listener = &logZeroListener{}
	}
	return c, nil
}

func (c *Crawler) Start() {
	const semLogContext = "azb-crawler::start"
	log.Info().Msg(semLogContext)
	c.wg.Add(1)
	go c.doWorkLoop()
}

func (c *Crawler) Stop() {
	const semLogContext = "azb-crawler::stop"
	log.Info().Msg(semLogContext)
	close(c.quitc)
}

func (c *Crawler) WorkerTerminated() {
	const semLogContext = "azb-crawler::terminated"
	log.Info().Msg(semLogContext)
	c.parentQuitc <- errors.New("worker has terminated")
}

func (c *Crawler) doWorkLoop() {
	const semLogContext = "azb-crawler::work-loop"
	log.Info().Float64("tickInterval-secs", c.cfg.TickInterval.Seconds()).Msg(semLogContext)

	crawledBlob, ok, err := c.next()
	if c.shouldExit(!ok, err != nil) {
		log.Info().Msg(semLogContext + " crawler terminating...")
		c.WorkerTerminated()
		c.wg.Done()
		return
	}

	c.listener.Start()

	if ok {
		_ = c.processBlob(crawledBlob)
	}

	ticker := time.NewTicker(c.cfg.TickInterval)
	for {
		select {
		case <-ticker.C:
			crawledBlob, ok, err = c.next()
			if c.shouldExit(!ok, err != nil) {
				log.Info().Msg(semLogContext + " crawler terminating...")
				ticker.Stop()
				c.listener.Close()
				c.WorkerTerminated()
				c.wg.Done()
				return
			}

			if ok {
				_ = c.processBlob(crawledBlob)
			}

		case <-c.quitc:
			log.Info().Msg(semLogContext + " ending...")
			ticker.Stop()
			c.listener.Close()
			c.wg.Done()
			return
		}
	}
}

func (c *Crawler) next() (CrawledBlob, bool, error) {
	const semLogContext = "azb-crawler::next"

	var crawledBlob CrawledBlob
	var ok bool
	var err error

	switch c.cfg.Mode {
	case ModeTag:
		crawledBlob, ok, err = c.nextByTag()
	default:
		log.Warn().Msg(semLogContext + " unrecognized mode")
	}

	if err != nil {
		return crawledBlob, false, err
	}

	if ok {
		lks, _ := azbloblks.GetLinkedService(c.cfg.StorageName)
		b2, err := lks.DownloadToFile(crawledBlob.BlobInfo.ContainerName, crawledBlob.BlobInfo.BlobName, filepath.Join(c.cfg.DownloadPath, crawledBlob.BlobInfo.BlobName))
		if err != nil {
			return CrawledBlob{}, false, err
		}

		crawledBlob.BlobInfo.FileName = b2.FileName
		return crawledBlob, true, nil
	}

	return CrawledBlob{}, ok, nil
}

func (c *Crawler) shouldExit(isNop bool, isError bool) bool {
	const semLogContext = "azb-crawler::should-exit"

	doExit := false
	if isError {
		if c.cfg.ExitOnErr {
			doExit = true
		}
	}

	if isNop {
		log.Info().Msg(semLogContext + " crawler no blobs left to process...")
		if c.cfg.ExitOnNop {
			doExit = true
		}
	}

	return doExit
}

func (c *Crawler) nextByTag() (CrawledBlob, bool, error) {
	const semLogContext = "azb-crawler::next-by-tag"

	lks, _ := azbloblks.GetLinkedService(c.cfg.StorageName)

	tag, ok := c.cfg.GetTagByType(QueryTag)
	if !ok {
		log.Error().Msg(semLogContext + " query tag not found")
	}

	taggedBlobs, err := lks.ListBlobByTag("", tag.Name, tag.Value, 10)
	if err != nil {
		log.Error().Err(err).Msg(semLogContext + " query tag not found")
		return CrawledBlob{}, false, err
	}

	for _, b := range taggedBlobs {
		for _, p := range c.cfg.Paths {
			if (p.Container != b.ContainerName && p.Container != "*") || !p.Regexp.Match([]byte(b.BlobName)) {
				continue
			}

			b1, err := lks.GetBlobInfo(b.ContainerName, b.BlobName)
			if err != nil {
				log.Error().Err(err).Str("blob-name", b.BlobName).Str("container", b.ContainerName).Msg(semLogContext + " impossible to get ")
				return CrawledBlob{}, false, err
			}

			log.Info().Str("tag-name", tag.Name).Str("tag-value", tag.Value).Str("container", b1.ContainerName).Str("blob-name", b1.BlobName).Str("lease-state", b1.LeaseState).Msg(semLogContext + " blob found")
			if b1.LeaseState != azbloblks.LeaseStateExpired && b1.LeaseState != azbloblks.LeaseStateAvailable {
				log.Info().Str("container", b.ContainerName).Str("blob-name", b.BlobName).Str("lease-state", b1.LeaseState).Msg(semLogContext + " blob not available")
				continue
			}

			crawledBlob := CrawledBlob{PathId: p.Id, BlobInfo: b1}

			if !c.listener.Accept(crawledBlob) {
				log.Info().Str("container", b.ContainerName).Str("blob-name", b.BlobName).Msg(semLogContext + " blob not accepted by listener")
				continue
			}

			leaseHandler, err := lks.AcquireLease(b1.ContainerName, b1.BlobName, 60, true)
			if err != nil {
				log.Info().Err(err).Str("container", b.ContainerName).Str("blob-name", b.BlobName).Str("lease-state", b1.LeaseState).Msg(semLogContext + " lease cannot be acquired")
			}

			crawledBlob.LeaseHandler = leaseHandler
			return crawledBlob, true, nil
		}
	}

	log.Info().Str("tag-name", tag.Name).Str("tag-value", tag.Value).Msg(semLogContext + " no blobs found by tag")
	return CrawledBlob{}, false, nil
}

func (c *Crawler) processBlob(crawledBlob CrawledBlob) error {
	const semLogContext = "azb-crawler::process-blob"

	log.Info().Str("blob-info", crawledBlob.BlobInfo.BlobName).Msg(semLogContext + " ...enqueuing")
	c.listener.Process(crawledBlob)
	return nil
}
