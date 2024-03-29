package azbloblks

import (
	"errors"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/tpm-az-common/storage/azstoragecfg"
	"github.com/rs/zerolog/log"
)

type LinkedServices []*LinkedService

var theRegistry LinkedServices

func Initialize(cfgs []azstoragecfg.Config) (LinkedServices, error) {

	const semLogContext = "azb-registry::initialize"
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
		log.Info().Str("cos-name", kcfg.Name).Msg(semLogContext + " cosmosdb instance configured")

	}

	theRegistry = r
	return r, nil
}

func LookupNameByAccountName(n string) (string, error) {
	const semLogContext = "azb-registry::get-account-name-by-name"
	for _, stg := range theRegistry {
		if stg.AccountName == n {
			return stg.Name, nil
		}
	}

	err := errors.New("storage-account linked service not found by account-name " + n)
	log.Error().Err(err).Str("account-name", n).Msg(semLogContext)
	return "", err
}

func GetLinkedService(stgName string) (*LinkedService, error) {
	const semLogContext = "azb-registry::get-lks"
	for _, stg := range theRegistry {
		if stg.Name == stgName {
			return stg, nil
		}
	}

	err := errors.New("storage-account linked service not found by name " + stgName)
	log.Error().Err(err).Str("stg-name", stgName).Msg(semLogContext)
	return nil, err
}

func GetLinkedServiceByAccountName(accountName string) (*LinkedService, error) {
	const semLogContext = "azb-registry::get-lks-by-acct-name"
	for _, stg := range theRegistry {
		if stg.AccountName == accountName {
			return stg, nil
		}
	}

	err := errors.New("storage-account linked service not found by name " + accountName)
	log.Error().Err(err).Str("stg-name", accountName).Msg(semLogContext)
	return nil, err
}
