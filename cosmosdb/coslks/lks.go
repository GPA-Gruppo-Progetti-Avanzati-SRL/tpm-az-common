package coslks

import (
	"fmt"
	"github.com/Azure/azure-sdk-for-go/sdk/data/azcosmos"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/tpm-az-common/cosmosdb/cosutil"
	"github.com/rs/zerolog/log"
)

type LinkedService struct {
	cfg Config
}

func NewLinkedServiceWithConfig(cfg Config) (*LinkedService, error) {
	lks := LinkedService{cfg: cfg}
	return &lks, nil
}

func (lks *LinkedService) GetCollectionNameById(cId string, onNotFound string) string {
	n := lks.cfg.GetCollectionNameById(cId)
	if n != "" {
		return n
	}

	return onNotFound
}

func (lks *LinkedService) MustGetCollectionNameById(cId string) string {
	const semLogContext = "cos-lks::must-get-collection-name-by-id"
	n := lks.cfg.GetCollectionNameById(cId)
	if n == "" {
		panic(fmt.Errorf(semLogContext+" not found: %s", cId))
	}
	return n
}

func (lks *LinkedService) GetDbName() string {
	return lks.cfg.DB.Name
}

func (lks *LinkedService) GetDbNameById(dbId string, onNotFound string) string {
	n := lks.cfg.GetDbNameById(dbId)
	if n != "" {
		return n
	}

	return onNotFound
}

func (lks *LinkedService) MustGetDbNameById(dbId string) string {
	const semLogContext = "cos-lks::must-get-db-name-by-id"
	n := lks.cfg.GetDbNameById(dbId)
	if n == "" {
		panic(fmt.Errorf(semLogContext+" not found: %s", dbId))
	}

	return n
}

func (lks *LinkedService) ConnectionString() string {
	return cosutil.ConnectionStringFromEndpointAndAccountKey(lks.cfg.Endpoint, lks.cfg.AccountKey)
}

// NewClient the enableContentResponseOnWrite should be enabled if for example you need to do a patch operation and want the content back.
func (lks *LinkedService) NewClient(enableContentResponseOnWrite bool) (*azcosmos.Client, error) {

	const semLogContext = "cos-lks::new-client"
	cred, err := azcosmos.NewKeyCredential(lks.cfg.AccountKey)
	if err != nil {
		log.Error().Err(err).Msg(semLogContext)
		return nil, err
	}

	opts := azcosmos.ClientOptions{
		EnableContentResponseOnWrite: enableContentResponseOnWrite,
	}
	client, err := azcosmos.NewClientWithKey(lks.cfg.Endpoint, cred, &opts)
	return client, err
}

func (lks *LinkedService) GetCosmosDbContainer(dbName, collectionName string, enableContentResponseOnWrite bool) (*azcosmos.ContainerClient, error) {

	cli, err := lks.NewClient(enableContentResponseOnWrite)
	if err != nil {
		return nil, err
	}

	container, err := cli.NewContainer(dbName, collectionName)
	return container, err
}
