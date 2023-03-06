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
	Accept(blob azbloblks.BlobInfo) bool
	Process(CrawledBlob) error
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

	ticker := time.NewTicker(c.cfg.TickInterval)
	for {
		select {
		case <-ticker.C:
			blobInfo, lh, ok, err := c.next()
			doExit := false
			if err != nil {
				log.Error().Err(err).Msg(semLogContext)
				if c.cfg.ExitOnErr {
					doExit = true
				}
			}
			if !ok {
				log.Info().Msg(semLogContext + " crawler no blobs left to process...")
				if c.cfg.ExitOnNop {
					doExit = true
				}
			}

			if doExit {
				log.Info().Msg(semLogContext + " crawler terminating...")
				ticker.Stop()
				c.listener.Close()
				c.WorkerTerminated()
				c.wg.Done()
				return
			}

			_ = c.processBlob(blobInfo, lh)

		case <-c.quitc:
			log.Info().Msg(semLogContext + " ending...")
			ticker.Stop()
			c.listener.Close()
			c.wg.Done()
			return
		}
	}
}

func (c *Crawler) next() (azbloblks.BlobInfo, *azbloblks.LeaseHandler, bool, error) {
	const semLogContext = "azb-crawler::next"

	var b azbloblks.BlobInfo
	var lh *azbloblks.LeaseHandler
	var ok bool
	var err error

	switch c.cfg.Mode {
	case ModeTag:
		b, lh, ok, err = c.nextByTag()
	default:
		log.Warn().Msg(semLogContext + " unrecognized mode")
	}

	if err != nil {
		return b, nil, false, err
	}

	if ok {
		lks, _ := azbloblks.GetLinkedService(c.cfg.StorageName)
		b2, err := lks.DownloadToFile(b.ContainerName, b.BlobName, filepath.Join(c.cfg.DownloadPath, b.BlobName))
		if err != nil {
			return b, nil, false, err
		}

		b.FileName = b2.FileName
		return b, lh, true, nil
	}

	return azbloblks.BlobInfo{}, lh, ok, nil
}

func (c *Crawler) nextByTag() (azbloblks.BlobInfo, *azbloblks.LeaseHandler, bool, error) {
	const semLogContext = "azb-crawler::next-by-tag"

	lks, _ := azbloblks.GetLinkedService(c.cfg.StorageName)

	tag, ok := c.cfg.GetTagByType(QueryTag)
	if !ok {
		log.Error().Msg(semLogContext + " query tag not found")
	}

	taggedBlobs, err := lks.ListBlobByTag("", tag.Name, tag.Value, 10)
	if err != nil {
		log.Error().Err(err).Msg(semLogContext + " query tag not found")
		return azbloblks.BlobInfo{}, nil, false, err
	}

	for _, b := range taggedBlobs {
		for _, p := range c.cfg.Paths {
			if (p.Container != b.ContainerName && p.Container != "*") || !p.Regexp.Match([]byte(b.BlobName)) {
				continue
			}

			b1, err := lks.GetBlobInfo(b.ContainerName, b.BlobName)
			if err != nil {
				log.Error().Err(err).Str("blob-name", b.BlobName).Str("container", b.ContainerName).Msg(semLogContext + " impossible to get ")
				return azbloblks.BlobInfo{}, nil, false, err
			}

			log.Info().Str("tag-name", tag.Name).Str("tag-value", tag.Value).Str("container", b1.ContainerName).Str("blob-name", b1.BlobName).Str("lease-state", b1.LeaseState).Msg(semLogContext + " blob found")
			if b1.LeaseState != azbloblks.LeaseStateExpired && b1.LeaseState != azbloblks.LeaseStateAvailable {
				log.Info().Str("container", b.ContainerName).Str("blob-name", b.BlobName).Str("lease-state", b1.LeaseState).Msg(semLogContext + " blob not available")
				continue
			}

			if !c.listener.Accept(b1) {
				log.Info().Str("container", b.ContainerName).Str("blob-name", b.BlobName).Msg(semLogContext + " blob not accepted by listener")
				continue
			}

			leaseHandler, err := lks.AcquireLease(b1.ContainerName, b1.BlobName, 60, true)
			if err != nil {
				log.Info().Err(err).Str("container", b.ContainerName).Str("blob-name", b.BlobName).Str("lease-state", b1.LeaseState).Msg(semLogContext + " lease cannot be acquired")
			}

			return b1, leaseHandler, true, nil
		}
	}

	log.Info().Str("tag-name", tag.Name).Str("tag-value", tag.Value).Msg(semLogContext + " no blobs found by tag")
	return azbloblks.BlobInfo{}, nil, false, nil
}

func (c *Crawler) processBlob(blobInfo azbloblks.BlobInfo, lh *azbloblks.LeaseHandler) error {
	const semLogContext = "azb-crawler::process-blob"

	log.Info().Str("blob-info", blobInfo.BlobName).Msg(semLogContext + " ...enqueuing")
	c.listener.Process(CrawledBlob{BlobInfo: blobInfo, LeaseHandler: lh})
	return nil
}
