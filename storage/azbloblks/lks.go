package azbloblks

import (
	"errors"
	"fmt"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/tpm-az-common/storage/azstoragecfg"
)

type LinkedService struct {
	Name        string
	AccountName string
	Client      *azblob.Client
}

const (
	StorageAccountBlobBaseUrl = "https://%s.blob.core.windows.net/"
	StorageAccountBlobSasUrl  = "https://%s.blob.core.windows.net/?%s"
)

func NewLinkedServiceWithConfig(cfg azstoragecfg.Config) (*LinkedService, error) {

	var serviceClient *azblob.Client
	var err error

	switch cfg.AuthMode {
	case azstoragecfg.AuthModeAccountKey:
		cred, err := azblob.NewSharedKeyCredential(cfg.Account, cfg.AccountKey)
		if err != nil {
			return nil, err
		}

		serviceClient, err = azblob.NewClientWithSharedKeyCredential(fmt.Sprintf(StorageAccountBlobBaseUrl, cfg.Account), cred, nil)
		if err != nil {
			return nil, err
		}

	case azstoragecfg.AuthModeSasToken:
		serviceClient, err = azblob.NewClientWithNoCredential(fmt.Sprintf(StorageAccountBlobSasUrl, cfg.Account, cfg.SasToken), nil)
		if err != nil {
			return nil, err
		}

	case azstoragecfg.AuthModeConnectionString:
		// The azure portal doesn't provide the connection string as the matter of fact....
		serviceClient, err = azblob.NewClientFromConnectionString(cfg.ConnectionString, nil)
		if err != nil {
			return nil, err
		}

	default:
		return nil, errors.New("please specify a suitable authentication mode")
	}

	lks := &LinkedService{Name: cfg.Name, AccountName: cfg.Account, Client: serviceClient}
	return lks, nil
}

func NewLinkedService(account string, opts ...azstoragecfg.Option) (*LinkedService, error) {
	cfg := azstoragecfg.Config{Account: account}

	for _, o := range opts {
		o(&cfg)
	}

	return NewLinkedServiceWithConfig(cfg)
}
