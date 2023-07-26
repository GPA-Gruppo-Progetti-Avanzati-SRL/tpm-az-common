package azblobevent

import (
	"context"
	"errors"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/tpm-az-common/cosmosdb/coslease"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/tpm-az-common/cosmosdb/coslks"
	"github.com/rs/zerolog/log"
	"sync"
	"time"
)

type CrawledEvent struct {
	EventDocument
	CosName string `mapstructure:"cos-name,omitempty" yaml:"cos-name,omitempty" json:"cos-name,omitempty"`
	// ProcessedEventTtl int                    `mapstructure:"processed-event-ttl,omitempty" yaml:"processed-event-ttl,omitempty" json:"processed-event-ttl,omitempty"`
	// ErrorEventTtl     int                    `mapstructure:"error-event-ttl,omitempty" yaml:"error-event-ttl,omitempty" json:"error-event-ttl,omitempty"`
	ThinkTime     time.Duration          `mapstructure:"think-time,omitempty" yaml:"think-time,omitempty" json:"think-time,omitempty"`
	LeaseHandler  *coslease.LeaseHandler `mapstructure:"-" yaml:"-" json:"-"`
	ListenerIndex int                    `mapstructure:"-" yaml:"-" json:"-"`
}

type Crawler struct {
	cfg *Config

	quitc       chan struct{}
	parentQuitc chan error
	wg          *sync.WaitGroup

	listeners []Listener
}

var CrawledEventZero = CrawledEvent{ListenerIndex: -1}

type Listener interface {
	Accept(blob CrawledEvent) (time.Duration, bool)
	Process(blob CrawledEvent) error
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

	const semLogContext = "azb-event-crawler::new"
	_, err := coslks.GetLinkedService(cfg.CosName)
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
	const semLogContext = "azb-event-crawler::start"
	log.Info().Msg(semLogContext)
	c.wg.Add(1)
	go c.doWorkLoop()
}

func (c *Crawler) Stop() {
	const semLogContext = "azb-event-crawler::stop"
	log.Info().Msg(semLogContext)
	close(c.quitc)
}

func (c *Crawler) WorkerTerminated() {
	const semLogContext = "azb-event-crawler::terminated"
	log.Info().Msg(semLogContext)
	c.parentQuitc <- errors.New("worker has terminated")
}

func (c *Crawler) doWorkLoop() {
	const semLogContext = "azb-event-crawler::work-loop"
	log.Info().Float64("tickInterval-secs", c.cfg.TickInterval.Seconds()).Msg(semLogContext)

	crawledBlob, err := c.next()
	if err != nil {
		log.Error().Err(err).Msg(semLogContext)
	}
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
		_ = c.processEvent(crawledBlob)
	}

	ticker := time.NewTicker(c.cfg.TickInterval)
	for {
		select {
		case <-ticker.C:
			crawledBlob, err = c.next()
			if err != nil {
				log.Error().Err(err).Msg(semLogContext)
			}
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
				_ = c.processEvent(crawledBlob)
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

/*
func (c *Crawler) next() (CrawledEvent, error) {
	const semLogContext = "azb-event-crawler::next"

	var crawledBlob CrawledEvent
	var err error

	crawledBlob, err = c.nextByTag()
	if err != nil {
		return CrawledEventZero, err
	}

	return crawledBlob, nil
}
*/

func (c *Crawler) shouldExit(isNop bool, isError bool) bool {
	const semLogContext = "azb-event-crawler::should-exit"

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

func (c *Crawler) next() (CrawledEvent, error) {
	const semLogContext = "azb-event-crawler::next"

	cnt, err := coslks.GetCosmosDbContainer(c.cfg.CosName, CosCollectionId, false)
	if err != nil {
		return CrawledEventZero, err
	}

	docs, err := FindEventDocuments(cnt)
	if err != nil {
		return CrawledEventZero, err
	}

	for _, d := range docs {
		if d.Typ != BlobCreated {
			d.Status = EventDocumentStatusDiscarded
			d.TTL = AdaptTtl(c.cfg.DiscardedEventTtl)
			if _, err = d.Replace(context.Background(), cnt); err != nil {
				log.Warn().Err(err).Msg(semLogContext)
			}

			continue
		}

		// Blob Create Items.
		ok, err := coslease.CanAcquireLease(context.Background(), cnt, coslease.LeaseTypeBlobEvent, d.PKey, d.Id)
		if err != nil {
			return CrawledEventZero, err
		}

		if !ok {
			continue
		}

		crawledEvt := CrawledEvent{
			EventDocument: *d.EventDocument,
			CosName:       c.cfg.CosName,
			ThinkTime:     0,
			ListenerIndex: -1,
		}

		for i := range c.listeners {
			crawledEvt.ThinkTime, ok = c.listeners[i].Accept(crawledEvt)
			if ok {
				log.Info().Int("listener", i).Float64("expected-duration-s", crawledEvt.ThinkTime.Seconds()).Msg(semLogContext + " event accepted by listener")
				crawledEvt.ListenerIndex = i
				break
			} else {
				log.Info().Int("listener", i).Msg(semLogContext + " event NOT accepted by listener")
			}
		}

		if crawledEvt.ListenerIndex < 0 {
			log.Info().Msg(semLogContext + " blob not accepted by any listener")
			d.Status = EventDocumentStatusSkipped
			d.TTL = AdaptTtl(c.cfg.SkippedEventTtl)
			if _, err = d.Replace(context.Background(), cnt); err != nil {
				log.Warn().Err(err).Msg(semLogContext)
			}
			continue
		}

		leaseHandler, err := coslease.AcquireLease(context.Background(), cnt, coslease.LeaseTypeBlobEvent, d.PKey, d.Id, true)
		if err != nil {
			log.Info().Err(err).Msg(semLogContext + " lease cannot be acquired")
			continue
		}

		crawledEvt.LeaseHandler = leaseHandler
		return crawledEvt, nil
	}

	log.Info().Msg(semLogContext + " no events found")
	return CrawledEventZero, nil

}

func (c *Crawler) processEvent(crawledBlob CrawledEvent) error {
	const semLogContext = "azb-event-crawler::process-blob"

	log.Info().Msg(semLogContext + " ...enqueuing")
	err := c.listeners[crawledBlob.ListenerIndex].Process(crawledBlob)
	if err != nil {
		return err
	}
	if crawledBlob.ThinkTime > 0 {
		log.Info().Float64("think-time-secs", crawledBlob.ThinkTime.Seconds()).Msg(semLogContext + " sleeping as instructed by listener")
		time.Sleep(crawledBlob.ThinkTime)
	}
	return nil
}
