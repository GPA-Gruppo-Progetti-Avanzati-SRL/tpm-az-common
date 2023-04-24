package main

import (
	"errors"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/tpm-az-common/cosmosdb/coslks"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/tpm-az-common/cosmosdb/cosops"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/tpm-az-common/cosmosdb/cosquery"
	varResolver "github.com/GPA-Gruppo-Progetti-Avanzati-SRL/tpm-common/util/vars"
	"github.com/rs/zerolog/log"
	"time"
)

func executeSelectAndDeleteCommand(args CmdLineArgs, opNdx int) error {
	const semLogContext = "cos-cli::select-delete-command"

	lks, err := coslks.GetLinkedService(args.Broker)
	if err != nil {
		return err
	}

	if args.Operations[opNdx].CtxQueryText == "" {
		err = executeSelectDeleteOperation(lks, args.Db, args.Operations[opNdx].Container, args.Operations[opNdx].QueryText, args.Operations[opNdx].PrintTemplate, cosops.WithPageSize(args.Operations[opNdx].PageSize), cosops.WithConcurrency(args.Operations[opNdx].ConcurrencyLevel))
		return err
	}

	var ctxDocs []cosquery.Document
	ctxDocs, err = cosquery.ReadAll(lks, args.Db, args.Operations[opNdx].Container, args.Operations[opNdx].CtxQueryText)
	if err != nil {
		log.Error().Err(err).Str(semLogContainer, args.Operations[opNdx].Container).Str(semLogCtxQuery, args.Operations[opNdx].CtxQueryText).Msg(semLogContext)
		return err
	}

	for _, d := range ctxDocs {
		log.Info().Interface("context-document", d).Msg(semLogContext)
		if m, ok := d.(cosquery.DocumentMap); ok {
			f := varResolver.SimpleMapResolver(m, "")
			qt, err := varResolver.ResolveVariables(args.Operations[opNdx].QueryText, varResolver.DollarVariableReference, f, true)
			if err != nil {
				return err
			}

			err = executeSelectDeleteOperation(lks, args.Db, args.Operations[opNdx].Container, qt, args.Operations[opNdx].PrintTemplate, cosops.WithPageSize(args.Operations[opNdx].PageSize), cosops.WithConcurrency(args.Operations[opNdx].ConcurrencyLevel))
			if err != nil {
				return err
			}
		} else {
			err = errors.New("the document returned is not a map")
			log.Error().Err(err).Str(semLogContainer, args.Operations[opNdx].Container).Str(semLogCtxQuery, args.Operations[opNdx].CtxQueryText).Msg(semLogContext)
			return err
		}
	}

	/*
			numberOfRowsAffected := 0
			beginOfProcessing := time.Now()
			defer func(start time.Time) {
				log.Info().Int("num-rows-affected", numberOfRowsAffected).Float64("elapsed", time.Since(beginOfProcessing).Seconds()).Msg(semLogContext)
			}(beginOfProcessing)


		numberOfRowsAffected, err = cosops.DeleteAll(lks, args.Db, args.Operations[opNdx].Container, args.Operations[opNdx].QueryText, cosops.WithPageSize(args.Operations[opNdx].PageSize), cosops.WithConcurrency(args.Operations[opNdx].ConcurrencyLevel))
		if err != nil {
			log.Error().Err(err).Msg(semLogContext)
		} else {
			log.Info().Err(err).Int("num-rows-affected", numberOfRowsAffected).Msg(semLogContext)
		}
	*/
	return nil
}

func executeSelectDeleteOperation(lks *coslks.LinkedService, dbName, container, queryText, printTemplate string, opts ...cosops.Option) error {

	const semLogContext = "cos-cli::execute-select-delete"
	log.Info().Str(semLogContainer, container).Str(semLogQuery, queryText).Msg(semLogContext)

	var err error
	numberOfRowsAffected := 0
	beginOfProcessing := time.Now()
	defer func(start time.Time) {
		log.Info().Int("num-rows-affected", numberOfRowsAffected).Float64("elapsed", time.Since(beginOfProcessing).Seconds()).Msg(semLogContext)
	}(beginOfProcessing)

	log.Trace().Str(semLogQuery, queryText).Str(semLogContainer, container).Msg(semLogContext)

	numberOfRowsAffected, err = cosops.DeleteAll(lks, dbName, container, queryText, opts...)
	if err != nil {
		log.Error().Err(err).Msg(semLogContext)
	} else {
		log.Info().Err(err).Int("num-rows-affected", numberOfRowsAffected).Msg(semLogContext)
	}

	return err

}
