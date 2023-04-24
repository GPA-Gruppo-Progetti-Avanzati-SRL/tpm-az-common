package cosops

import (
	"errors"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/tpm-az-common/cosmosdb/cosquery"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/tpm-common/util"
	"github.com/rs/zerolog/log"
	"sync"
)

type Pipeline struct {
	concurrency int
	paths       chan DataFrame
	errs        chan error
	done        chan struct{}
}

func rowPipeline(docs []cosquery.Document, p Visitor, opts ...Option) error {

	const semLogContext = "cos-pipeline::run"

	pipelineOpts := ReadAndVisitDefaultOptions
	for _, o := range opts {
		o(&pipelineOpts)
	}

	log.Info().Int("concurrency", pipelineOpts.Concurrency).Msg(semLogContext + " starting...")

	done := make(chan struct{})
	defer close(done)

	downloadInbound, errc := sourcePipeline(done, docs, p)
	downloadOutbound := make(chan DataFrame)
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

func sourcePipeline(done <-chan struct{}, docs []cosquery.Document, p Visitor) (<-chan DataFrame, <-chan error) {

	const semLogContext = "cvm-2-lease-listener-pipeline::source"

	paths := make(chan DataFrame)
	errc := make(chan error, 1)

	go func() {

		defer close(paths)
		rowNumber := 0
		for _, d := range docs {

			pk, id := d.GetKeys()
			rowNumber++
			select {
			case paths <- DataFrame{id: id, pkey: pk}:
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

func processDataFrame(idGo int, done chan struct{}, inBound <-chan DataFrame, outBound chan<- DataFrame, p Visitor) {

	const semLogContext = "cos-pipeline::process-dataframe"
	const semLogNumDataFrames = "num-data-frames"

	numDataFrames := 0
	logger := util.GeometricTraceLogger{}
	for dataframe := range inBound {
		numDataFrames++
		if logger.CheckAndSetOnOff() {
			logger.LogEvent(log.Trace().Int("id-go", idGo).Int(semLogNumDataFrames, numDataFrames), semLogContext)
		}

		err := p.Visit("process-data-frame", dataframe)
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

func reducePipeline(outBound chan DataFrame, p Visitor) error {
	const semLogContext = "cos-pipeline::reduce"

	numDf := 0
	logger := util.GeometricTraceLogger{}
	for dataframe := range outBound {
		numDf++
		if logger.CheckAndSetOnOff() {
			logger.LogEvent(log.Trace().Int("df-num", numDf).Str("df-id", dataframe.id), semLogContext)
		}
	}

	log.Info().Int("num-dataframes", numDf).Msg(semLogContext + " reduced")

	return nil
}
