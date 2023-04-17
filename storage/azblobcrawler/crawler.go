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
	BlobId        string                  `mapstructure:"id,omitempty" yaml:"id,omitempty" json:"id,omitempty"`
	NameGroups    []string                `mapstructure:"name-groups,omitempty" yaml:"name-groups,omitempty" json:"name-groups,omitempty"`
	PathId        string                  `mapstructure:"path-id,omitempty" yaml:"path-id,omitempty" json:"path-id,omitempty"`
	BlobInfo      azbloblks.BlobInfo      `mapstructure:"info,omitempty" yaml:"info,omitempty" json:"info,omitempty"`
	ThinkTime     time.Duration           `mapstructure:"think-time,omitempty" yaml:"think-time,omitempty" json:"think-time,omitempty"`
	LeaseHandler  *azbloblks.LeaseHandler `mapstructure:"-" yaml:"-" json:"-"`
	ListenerIndex int                     `mapstructure:"-" yaml:"-" json:"-"`
}

type Crawler struct {
	cfg *Config

	quitc       chan struct{}
	parentQuitc chan error
	wg          *sync.WaitGroup

	listeners []Listener
}

var CrawledBlobZero = CrawledBlob{ListenerIndex: -1}

type Listener interface {
	Accept(blob CrawledBlob) (time.Duration, bool)
	Process(blob CrawledBlob) error
	Start()
	Close()
}

type Option func(c *Crawler)

func WithListener(l Listener) Option {
	return func(c *Crawler) {
		if l != nil {
			c.listeners = append(c.listeners, l)
		}
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

	if len(c.listeners) == 0 {
		c.listeners = append(c.listeners, &logZeroListener{})
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

	crawledBlob, err := c.next()
	if c.shouldExit(crawledBlob.ListenerIndex < 0, err != nil) {
		log.Info().Msg(semLogContext + " crawler terminating...")
		c.WorkerTerminated()
		c.wg.Done()
		return
	}

	for i := range c.listeners {
		log.Info().Int("listener", i).Msg(semLogContext + " starting crawler listener")
		c.listeners[i].Start()
	}

	if crawledBlob.ListenerIndex >= 0 {
		_ = c.processBlob(crawledBlob)
	}

	ticker := time.NewTicker(c.cfg.TickInterval)
	for {
		select {
		case <-ticker.C:
			crawledBlob, err = c.next()
			if c.shouldExit(crawledBlob.ListenerIndex < 0, err != nil) {
				log.Info().Msg(semLogContext + " crawler terminating...")
				ticker.Stop()
				for i := range c.listeners {
					log.Info().Int("listener", i).Msg(semLogContext + " closing crawler listener")
					c.listeners[i].Close()
				}
				c.WorkerTerminated()
				c.wg.Done()
				return
			}

			if crawledBlob.ListenerIndex >= 0 {
				_ = c.processBlob(crawledBlob)
			}

		case <-c.quitc:
			log.Info().Msg(semLogContext + " ending...")
			ticker.Stop()
			for i := range c.listeners {
				log.Info().Int("listener", i).Msg(semLogContext + " closing crawler listener")
				c.listeners[i].Close()
			}
			c.wg.Done()
			return
		}
	}
}

func (c *Crawler) next() (CrawledBlob, error) {
	const semLogContext = "azb-crawler::next"

	var crawledBlob CrawledBlob
	var err error

	switch c.cfg.Mode {
	case ModeTag:
		crawledBlob, err = c.nextByTag()
	default:
		log.Warn().Msg(semLogContext + " unrecognized mode")
	}

	if err != nil {
		return CrawledBlobZero, err
	}

	if crawledBlob.ListenerIndex >= 0 {
		lks, _ := azbloblks.GetLinkedService(c.cfg.StorageName)
		b2, err := lks.DownloadToFile(crawledBlob.BlobInfo.ContainerName, crawledBlob.BlobInfo.BlobName, filepath.Join(c.cfg.DownloadPath, crawledBlob.BlobInfo.BlobName))
		if err != nil {
			return CrawledBlobZero, err
		}

		crawledBlob.BlobInfo.FileName = b2.FileName
		return crawledBlob, nil
	}

	return CrawledBlobZero, nil
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

func (c *Crawler) nextByTag() (CrawledBlob, error) {
	const semLogContext = "azb-crawler::next-by-tag"

	lks, _ := azbloblks.GetLinkedService(c.cfg.StorageName)

	tag, ok := c.cfg.GetTagByType(TagValueReady)
	if !ok {
		log.Error().Str("tag-type", string(TagValueReady)).Msg(semLogContext + " query tag not found")
		return CrawledBlobZero, errors.New("query tag not found")
	}

	taggedBlobs, err := lks.ListBlobByTag("", c.cfg.TagName, tag.Value, 10)
	if err != nil {
		log.Error().Err(err).Msg(semLogContext + " query tag not found")
		return CrawledBlobZero, err
	}

	for _, b := range taggedBlobs {
		for _, p := range c.cfg.Paths {
			if (p.Container != b.ContainerName && p.Container != "*") || !p.Regexp.Match([]byte(b.BlobName)) {
				continue
			}

			matches := p.Regexp.FindAllStringSubmatch(b.BlobName, -1)
			var blobNameParts []string
			if len(matches[0]) > 0 {
				for i := 1; i < len(matches[0]); i++ {
					blobNameParts = append(blobNameParts, matches[0][i])
				}
			}

			b1, err := lks.GetBlobInfo(b.ContainerName, b.BlobName)
			if err != nil {
				log.Error().Err(err).Str("blob-name", b.BlobName).Str("container", b.ContainerName).Msg(semLogContext + " impossible to get info")
				return CrawledBlobZero, err
			}

			log.Info().Str("tag-name", c.cfg.TagName).Str("tag-value", tag.Value).Str("container", b1.ContainerName).Str("blob-name", b1.BlobName).Str("lease-state", b1.LeaseState).Msg(semLogContext + " blob found")
			if b1.LeaseState != azbloblks.LeaseStateExpired && b1.LeaseState != azbloblks.LeaseStateAvailable {
				log.Info().Str("container", b.ContainerName).Str("blob-name", b.BlobName).Str("lease-state", b1.LeaseState).Msg(semLogContext + " blob not available")
				continue
			}

			crawledBlob := CrawledBlob{BlobId: b1.Id(), PathId: p.Id, BlobInfo: b1, NameGroups: blobNameParts, ListenerIndex: -1}

			for i := range c.listeners {
				crawledBlob.ThinkTime, ok = c.listeners[i].Accept(crawledBlob)
				if ok {
					log.Info().Str("container", b.ContainerName).Int("listener", i).Float64("expected-duration-s", crawledBlob.ThinkTime.Seconds()).Str("blob-name", b.BlobName).Msg(semLogContext + " blob accepted by listener")
					crawledBlob.ListenerIndex = i
					break
				} else {
					log.Info().Str("container", b.ContainerName).Int("listener", i).Str("blob-name", b.BlobName).Msg(semLogContext + " blob NOT accepted by listener")
				}
			}

			if crawledBlob.ListenerIndex < 0 {
				log.Info().Str("container", b.ContainerName).Str("blob-name", b.BlobName).Msg(semLogContext + " blob not accepted by any listener")
				continue
			}

			leaseHandler, err := lks.AcquireLease(b1.ContainerName, b1.BlobName, 60, true)
			if err != nil {
				log.Info().Err(err).Str("container", b.ContainerName).Str("blob-name", b.BlobName).Str("lease-state", b1.LeaseState).Msg(semLogContext + " lease cannot be acquired")
				continue
			}

			crawledBlob.LeaseHandler = leaseHandler
			return crawledBlob, nil
		}
	}

	log.Info().Str("tag-name", c.cfg.TagName).Str("tag-value", tag.Value).Msg(semLogContext + " no blobs found by tag")
	return CrawledBlobZero, nil
}

func (c *Crawler) processBlob(crawledBlob CrawledBlob) error {
	const semLogContext = "azb-crawler::process-blob"

	log.Info().Str("blob-info", crawledBlob.BlobInfo.BlobName).Msg(semLogContext + " ...enqueuing")
	c.listeners[crawledBlob.ListenerIndex].Process(crawledBlob)
	if crawledBlob.ThinkTime > 0 {
		log.Info().Float64("think-time-secs", crawledBlob.ThinkTime.Seconds()).Msg(semLogContext + " sleeping as instructed by listener")
		time.Sleep(crawledBlob.ThinkTime)
	}
	return nil
}
