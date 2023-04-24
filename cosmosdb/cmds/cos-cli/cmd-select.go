package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/tpm-az-common/cosmosdb/coslks"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/tpm-az-common/cosmosdb/cosquery"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/tpm-common/util/templateutil"
	varResolver "github.com/GPA-Gruppo-Progetti-Avanzati-SRL/tpm-common/util/vars"
	"github.com/rs/zerolog/log"
)

func executeSelectCommand(args CmdLineArgs, opNdx int) error {
	const semLogContext = "cos-cli::select-command"

	log.Trace().Str(semLogQuery, args.Operations[opNdx].QueryText).Str(semLogContainer, args.Operations[opNdx].Container).Msg(semLogContext)

	lks, err := coslks.GetLinkedService(args.Broker)
	if err != nil {
		return err
	}

	if args.Operations[opNdx].CtxQueryText == "" {
		err = executeSelectOperation(lks, args.Db, args.Operations[opNdx].Container, args.Operations[opNdx].QueryText, args.Operations[opNdx].PrintTemplate, cosquery.WithReaderPageSize(args.Operations[opNdx].PageSize), cosquery.WithReaderLimit(args.Operations[opNdx].Limit))
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

			err = executeSelectOperation(lks, args.Db, args.Operations[opNdx].Container, qt, args.Operations[opNdx].PrintTemplate, cosquery.WithReaderPageSize(args.Operations[opNdx].PageSize), cosquery.WithReaderLimit(args.Operations[opNdx].Limit))
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
		pr, err := cosquery.NewPagedReader(lks, args.Db, args.Operations[opNdx].Container, args.Operations[opNdx].QueryText, cosquery.WithReaderPageSize(args.Operations[opNdx].PageSize))
		if err != nil {
			log.Error().Err(err).Str(semLogContainer, args.Operations[opNdx].Container).Str(semLogQuery, args.Operations[opNdx].QueryText).Msg(semLogContext)
			return err
		}

		rows, err := pr.Read()
		if err != nil {
			log.Error().Err(err).Str(semLogContainer, args.Operations[opNdx].Container).Str(semLogQuery, args.Operations[opNdx].QueryText).Msg(semLogContext)
			return err
		}

		hasNext := true
		tmpl, err := templateutil.Parse([]templateutil.Info{
			{Name: "print", Content: args.Operations[opNdx].PrintTemplate},
		}, nil)

		for hasNext {

			for _, r := range rows {
				if m, ok := r.(cosquery.DocumentMap); ok {
					jsonData, err := json.Marshal(r)
					if err != nil {
						log.Error().Err(err).Str(semLogContainer, args.Operations[opNdx].Container).Str(semLogQuery, args.Operations[opNdx].QueryText).Msg(semLogContext)
						return err
					}

					m["json"] = string(jsonData)
					b, err := templateutil.Process(tmpl, m, false)
					if err != nil {
						log.Error().Err(err).Str(semLogContainer, args.Operations[opNdx].Container).Str(semLogQuery, args.Operations[opNdx].QueryText).Msg(semLogContext)
						return err
					}

					fmt.Println(string(b))
				} else {
					log.Error().Err(err).Str(semLogContainer, args.Operations[opNdx].Container).Str(semLogQuery, args.Operations[opNdx].QueryText).Msg(semLogContext + " document is not a map")
				}
			}

			if pr.HasNext() {
				rows, err = pr.Read()
			} else {
				hasNext = false
			}

			if err != nil {
				log.Error().Err(err).Str(semLogContainer, args.Operations[opNdx].Container).Str(semLogQuery, args.Operations[opNdx].QueryText).Msg(semLogContext)
				return err
			}
		}

		np, nr := pr.Count()
		log.Info().Err(err).Int("num-pages", np).Int("num-matches", nr).Str(semLogContainer, args.Operations[opNdx].Container).Str(semLogQuery, args.Operations[opNdx].QueryText).Msg(semLogContext)

		/*
			files, err := cosopsutil.ReadAll(lks, args.Db, args.Container, args.QueryText)
			if err != nil {
				log.Error().Err(err).Msg(semLogContext)
				return err
			}

			log.Info().Int("returned-count", len(files)).Msg(semLogContext)
			for i, f := range files {
				log.Info().Int("i", i).Str("pkey", f.PKey).Str("id", f.Id).Msg(semLogContext)
			}

	*/
	return nil
}

func executeSelectOperation(lks *coslks.LinkedService, dbName, container, queryText, printTemplate string, opts ...cosquery.ReaderOption) error {

	const semLogContext = "cos-cli::execute-select"
	log.Info().Str(semLogContainer, container).Str(semLogQuery, queryText).Msg(semLogContext)

	pr, err := cosquery.NewPagedReader(lks, dbName, container, queryText, opts...)
	if err != nil {
		log.Error().Err(err).Str(semLogContainer, container).Str(semLogQuery, queryText).Msg(semLogContext)
		return err
	}

	rows, err := pr.Read()
	if err != nil {
		log.Error().Err(err).Str(semLogContainer, container).Str(semLogQuery, queryText).Msg(semLogContext)
		return err
	}

	hasNext := true
	tmpl, err := templateutil.Parse([]templateutil.Info{
		{Name: "print", Content: printTemplate},
	}, nil)

	for hasNext {

		for _, r := range rows {
			if m, ok := r.(cosquery.DocumentMap); ok {
				jsonData, err := json.Marshal(r)
				if err != nil {
					log.Error().Err(err).Str(semLogContainer, container).Str(semLogQuery, queryText).Msg(semLogContext)
					return err
				}

				m["json"] = string(jsonData)
				b, err := templateutil.Process(tmpl, m, false)
				if err != nil {
					log.Error().Err(err).Str(semLogContainer, container).Str(semLogQuery, queryText).Msg(semLogContext)
					return err
				}

				fmt.Println(string(b))
			} else {
				log.Error().Err(err).Str(semLogContainer, container).Str(semLogQuery, queryText).Msg(semLogContext + " document is not a map")
			}
		}

		if pr.HasNext() {
			rows, err = pr.Read()
		} else {
			hasNext = false
		}

		if err != nil {
			log.Error().Err(err).Str(semLogContainer, container).Str(semLogQuery, queryText).Msg(semLogContext)
			return err
		}
	}

	np, nr := pr.Count()
	log.Info().Err(err).Int("num-pages", np).Int("num-matches", nr).Str(semLogContainer, container).Str(semLogQuery, queryText).Msg(semLogContext)

	/*
		files, err := cosopsutil.ReadAll(lks, args.Db, args.Container, args.QueryText)
		if err != nil {
			log.Error().Err(err).Msg(semLogContext)
			return err
		}

		log.Info().Int("returned-count", len(files)).Msg(semLogContext)
		for i, f := range files {
			log.Info().Int("i", i).Str("pkey", f.PKey).Str("id", f.Id).Msg(semLogContext)
		}

	*/
	return nil
}
