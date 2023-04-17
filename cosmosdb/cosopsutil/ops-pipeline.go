package cosopsutil

import (
	"errors"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/tpm-common/util"
	"github.com/rs/zerolog/log"
	"sync"
	"time"
)

type DataframeProcessor interface {
	Process(phase string, df PipelineDataFrame) error
	Count() int
}

type PipelineDataFrame struct {
	id   string
	pkey string
	err  error
}

type Pipeline struct {
	concurrency int
	paths       chan PipelineDataFrame
	errs        chan error
	done        chan struct{}
}

func rowPipeline(docs []Document, p DataframeProcessor, opts ...Option) error {

	const semLogContext = "cos-pipeline::run"

	pipelineOpts := DefaultPipelineOptions
	for _, o := range opts {
		o(&pipelineOpts)
	}

	log.Info().Int("concurrency", pipelineOpts.Concurrency).Msg("starting doc pipeline...")

	done := make(chan struct{})
	defer close(done)

	downloadInbound, errc := sourcePipeline(done, docs, p)
	downloadOutbound := make(chan PipelineDataFrame)
	var wg sync.WaitGroup

	wg.Add(pipelineOpts.Concurrency)
	for i := 0; i < pipelineOpts.Concurrency; i++ {
		idGoroutine := i
		go func() {
			processDataFrame(idGoroutine, done, downloadInbound, downloadOutbound, p) // HLc
			wg.Done()
		}()
	}
	go func() {
		wg.Wait()
		close(downloadOutbound) // HLc
	}()

	err := reducePipeline(downloadOutbound, p)
	if err != nil {
		log.Error().Msg(semLogContext)
	}
	log.Trace().Msg(semLogContext + " ..... end of work")
	// Check whether the Walk failed.
	if err := <-errc; err != nil { // HLerrc )
		return err
	}

	return nil
}

func sourcePipeline(done <-chan struct{}, docs []Document, p DataframeProcessor) (<-chan PipelineDataFrame, <-chan error) {

	const semLogContext = "cvm-2-lease-listener-pipeline::source"

	paths := make(chan PipelineDataFrame)
	errc := make(chan error, 1)

	go func() {

		defer close(paths)
		rowNumber := 0
		for _, d := range docs {

			rowNumber++
			select {
			case paths <- PipelineDataFrame{id: d.Id, pkey: d.PKey}:
			case <-done:
				log.Trace().Msg("data source cancelled")
				errc <- errors.New("data source cancelled")
				return
			}
		}

		errc <- nil

	}()
	return paths, errc
}

func processDataFrame(idGo int, done chan struct{}, inBound <-chan PipelineDataFrame, outBound chan<- PipelineDataFrame, p DataframeProcessor) {

	const semLogContext = "cos-pipeline::process-dataframe"
	const semLogNumDataFrames = "num-data-frames"

	numDataFrames := 0
	logger := util.GeometricTraceLogger{}
	for dataframe := range inBound {
		numDataFrames++
		if logger.CheckAndSetOnOff() {
			logger.LogEvent(log.Trace().Int("id-go", idGo).Int(semLogNumDataFrames, numDataFrames), semLogContext)
		}

		err := p.Process("process-data-frame", dataframe)
		dataframe.err = err

		select {
		case outBound <- dataframe:
		case <-done:
			log.Trace().Int(semLogNumDataFrames, numDataFrames).Msg(semLogContext + " done signal... termination of download procedure")
			return
		}
	}

	log.Trace().Int("id-go", idGo).Int(semLogNumDataFrames, numDataFrames).Msg(semLogContext + " inbound messages consumed")
}

func reducePipeline(outBound chan PipelineDataFrame, p DataframeProcessor) error {
	const semLogContext = "cos-pipeline::reduce"

	beginOfProcessing := time.Now()

	numDf := 0
	logger := util.GeometricTraceLogger{}
	for dataframe := range outBound {
		if logger.CheckAndSetOnOff() {
			log.Trace().Int("df-num", numDf).Str("df-id", dataframe.id).Msg(semLogContext)
		}
	}

	log.Info().Float64("elapsed", time.Since(beginOfProcessing).Seconds()).Msg(semLogContext)
	return nil
}
