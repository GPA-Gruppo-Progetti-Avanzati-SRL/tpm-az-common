package main

import (
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/tpm-az-common/cosmosdb/coslks"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"os"
)

const (
	semLogContainer = "cnt"
	semLogQuery     = "query"
	semLogCtxQuery  = "context-query"
)

func main() {
	const semLogContext = "cos-cli::main"
	log.Logger = log.Output(zerolog.ConsoleWriter{Out: os.Stderr})

	args, err := ParseCmdLineArgs()
	if err != nil {
		log.Fatal().Err(err).Msg(semLogContext)
	}

	if args.LksConfig != nil {
		_, err := coslks.Initialize([]coslks.Config{*args.LksConfig})
		if err != nil {
			log.Fatal().Err(err).Msg(semLogContext)
		}
	}

	args.Log(semLogContext)

	for i, op := range args.Operations {
		switch op.Cmd {
		case CmdSelect:
			err = executeSelectCommand(args, i)
		case CmdSelectDelete:
			err = executeSelectAndDeleteCommand(args, i)
		}

		if err != nil {
			log.Fatal().Err(err).Msg(semLogContext)
		}
	}

}
