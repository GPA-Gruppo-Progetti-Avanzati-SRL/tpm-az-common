package coslks

import (
	"github.com/Azure/azure-sdk-for-go/sdk/data/azcosmos"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/tpm-az-common/cosmosdb/cosutil"
)

type LinkedService struct {
	cfg *Config
}

func NewInstanceWithConfig(cfg *Config) (*LinkedService, error) {
	lks := LinkedService{cfg: cfg}
	return &lks, nil
}

func (lks *LinkedService) DbName() string {
	return lks.cfg.DB.Name
}

func (lks *LinkedService) CollectionName(cId string) string {
	return lks.cfg.GetCollectionName(cId)
}

func (lks *LinkedService) ConnectionString() string {
	return cosutil.ConnectionStringFromEndpointAndAccountKey(lks.cfg.Endpoint, lks.cfg.AccountKey)
}

// NewClient the enableContentResponseOnWrite should be enabled if for example you need to do a patch operation and want the content back.
func (lks *LinkedService) NewClient(enableContentResponseOnWrite bool) (*azcosmos.Client, error) {
	cred, _ := azcosmos.NewKeyCredential(lks.cfg.AccountKey)

	opts := azcosmos.ClientOptions{
		EnableContentResponseOnWrite: enableContentResponseOnWrite,
	}
	client, err := azcosmos.NewClientWithKey(lks.cfg.Endpoint, cred, &opts)
	return client, err
}
