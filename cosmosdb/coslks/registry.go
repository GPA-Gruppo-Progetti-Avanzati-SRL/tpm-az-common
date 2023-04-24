package coslks

import (
	"errors"
	"github.com/Azure/azure-sdk-for-go/sdk/data/azcosmos"
	"github.com/rs/zerolog/log"
)

type LinkedServices []*LinkedService

var theRegistry LinkedServices

func Initialize(cfgs []Config) (LinkedServices, error) {

	const semLogContext = "cos-registry::initialize"
	if len(cfgs) == 0 {
		log.Info().Msg(semLogContext + " no config provided....skipping")
		return nil, nil
	}

	if len(theRegistry) != 0 {
		log.Warn().Msg(semLogContext + " registry already configured.. overwriting")
	}

	log.Info().Int("no-linked-services", len(cfgs)).Msg(semLogContext)

	var r LinkedServices
	for _, kcfg := range cfgs {
		lks, err := NewLinkedServiceWithConfig(kcfg)
		if err != nil {
			return nil, err
		}

		r = append(r, lks)
		log.Info().Str("cos-name", kcfg.CosmosName).Msg(semLogContext + " cosmosdb instance configured")

	}

	theRegistry = r
	return r, nil
}

func GetLinkedService(cosName string) (*LinkedService, error) {

	const semLogContext = "cos-registry::get-lks"
	for _, cos := range theRegistry {
		if cos.cfg.CosmosName == cosName {
			return cos, nil
		}
	}

	err := errors.New("cosmosdb linked service not found by name " + cosName)
	log.Error().Err(err).Str("cos-name", cosName).Msg(semLogContext)
	return nil, err
}

func NewCosmosDbClient(cosName string, enableContentResponseOnWrite bool) (*azcosmos.Client, error) {

	lks, err := GetLinkedService(cosName)
	if err != nil {
		return nil, err
	}

	return lks.NewClient(enableContentResponseOnWrite)
}

func GetCosmosDbContainer(cosName string, dbName, collectionName string, enableContentResponseOnWrite bool) (*azcosmos.ContainerClient, error) {

	lks, err := GetLinkedService(cosName)
	if err != nil {
		return nil, err
	}

	cli, err := lks.NewClient(enableContentResponseOnWrite)
	if err != nil {
		return nil, err
	}

	container, err := cli.NewContainer(dbName, collectionName)
	return container, err
}
