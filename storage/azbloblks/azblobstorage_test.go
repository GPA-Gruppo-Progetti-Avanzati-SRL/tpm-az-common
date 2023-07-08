package azbloblks_test

import (
	"context"
	"fmt"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/tpm-az-common/storage/azbloblks"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/tpm-az-common/storage/azstoragecfg"
	"github.com/stretchr/testify/require"
	"os"
	"testing"
	"time"
)

const (
	TargetContainer     = "lks-container"
	DropContainerOnExit = false

	AZCommonBlobAccountNameEnvVarName     = "AZCOMMON_BLOB_ACCOUNTNAME"
	AZCommonBlobAccountKeyEnvVarName      = "AZCOMMON_BLOB_ACCTKEY"
	AZCommonBlobAccountKeySasTokenVarName = "AZCOMMON_BLOB_SASTOKEN"
	AZCommonBlobAuthModeVarName           = "AZCOMMON_BLOB_AUTHMODE"
)

var blobDataPattern = `This is my blob %d`

var blobLks *azbloblks.LinkedService

func TestMain(m *testing.M) {
	stgConfig := azstoragecfg.Config{
		Account:    os.Getenv(AZCommonBlobAccountNameEnvVarName),
		AccountKey: os.Getenv(AZCommonBlobAccountKeyEnvVarName),
		SasToken:   os.Getenv(AZCommonBlobAccountKeySasTokenVarName),
		AuthMode:   os.Getenv(AZCommonBlobAuthModeVarName),
	}

	if stgConfig.Account == "" {
		panic("blob storage account-name not set.... use env var " + AZCommonBlobAccountNameEnvVarName)
	}

	var err error

	if stgConfig.AuthMode == "" {
		stgConfig.AuthMode = azstoragecfg.AuthModeAccountKey
	}

	switch stgConfig.AuthMode {
	case azstoragecfg.AuthModeAccountKey:
		if stgConfig.AccountKey == "" {
			panic("blob storage account-key not set.... use env var " + AZCommonBlobAccountKeyEnvVarName)
		}
		blobLks, err = azbloblks.NewLinkedService(stgConfig.Account, azstoragecfg.WithAccountKey(stgConfig.AccountKey))

	case azstoragecfg.AuthModeSasToken:
		if stgConfig.SasToken == "" {
			panic("blob storage sas-token not set.... use env var " + AZCommonBlobAccountKeySasTokenVarName)
		}

		blobLks, err = azbloblks.NewLinkedService(stgConfig.Account, azstoragecfg.WithSasToken(stgConfig.SasToken))
	}

	if err != nil {
		panic(err)
	}

	exitVal := m.Run()
	os.Exit(exitVal)
}

func TestListBlobs(t *testing.T) {

	const testListBlobsContainer = "lks-cnt-1"
	const DropListBlobContainerOnExit = true

	ctx := context.Background()

	var err error
	err = blobLks.NewContainer(testListBlobsContainer, true)
	require.NoError(t, err)

	defer func() {
		if DropListBlobContainerOnExit {
			err = blobLks.DeleteContainer(testListBlobsContainer, false)
			require.NoError(t, err)
		}
	}()

	for i := 0; i < 6000; i++ {
		blobData := fmt.Sprintf(blobDataPattern, i)
		_, err = blobLks.UploadFromBuffer(context.Background(), testListBlobsContainer, fmt.Sprintf("blob-%d.txt", i), []byte(blobData))
		require.NoError(t, err)
	}

	opts := azblob.ListBlobsFlatOptions{}
	pager := blobLks.Client.NewListBlobsFlatPager(testListBlobsContainer, &opts)

	numBlobsRetrieved := 0
	for pager.More() {
		resp, err := pager.NextPage(ctx)
		require.NoError(t, err)
		for _, v := range resp.Segment.BlobItems {
			numBlobsRetrieved++
			t.Logf("[%d-%s] tier: %s, content-type: %s, content-length: %d, name: %s", numBlobsRetrieved, *v.Properties.BlobType, *v.Properties.AccessTier, *v.Properties.ContentType, *v.Properties.ContentLength, *v.Name)
		}
	}

}

func TestListBlobsByTag(t *testing.T) {

	var err error
	err = blobLks.NewContainer(TargetContainer, true)
	require.NoError(t, err)

	defer func() {
		if DropContainerOnExit {
			err = blobLks.DeleteContainer(TargetContainer, false)
			require.NoError(t, err)
		}
	}()

	taggedBlobs, err := blobLks.ListBlobByTag(TargetContainer, "status", "done", 10)
	require.NoError(t, err)
	t.Log(taggedBlobs)
}

func TestUploadBlob(t *testing.T) {
	ctx := context.Background()

	var err error

	defer func() {
		if DropContainerOnExit {
			err = blobLks.DeleteContainer(TargetContainer, false)
			require.NoError(t, err)
		}
	}()

	url, err := blobLks.UploadFromBuffer(ctx, TargetContainer, "test-blob-upload-2.txt", []byte(`Text data3`))
	require.NoError(t, err)

	t.Log(url)
}

func TestDownloadBlob(t *testing.T) {
	var err error

	_, err = blobLks.DownloadToBuffer(TargetContainer, "cortina-2021")
	require.NoError(t, err)

	blobExists, err := blobLks.BlobExists(TargetContainer, "cortina-2021")
	require.NoError(t, err)
	t.Log("blob exists? ", blobExists)
}

func TestAcquireBlob(t *testing.T) {

	var err error
	blobName := "cortina-2021"
	blobInfo, err := blobLks.GetBlobInfo(TargetContainer, blobName)
	require.NoError(t, err)
	t.Log(blobInfo)

	leaseHandler, err := blobLks.AcquireLease(TargetContainer, blobName, 60, true)
	require.NoError(t, err)

	defer leaseHandler.Release()

	for i := 0; i <= 20; i++ {
		blobInfo, err = blobLks.GetBlobInfo(TargetContainer, blobName)
		require.NoError(t, err)
		t.Log(blobInfo)
		t.Logf("[%d] sleeping....", i)
		time.Sleep(20 * time.Second)

		bi := azbloblks.BlobInfo{ContainerName: TargetContainer, BlobName: blobName, Tags: []azbloblks.BlobTag{{Key: "TAGleased", Value: fmt.Sprintf("tag-val-%d", i)}}}
		err = blobLks.SetBlobTags(bi, leaseHandler.LeaseId)
		require.NoError(t, err)

		if i == 6 {
			_, err := blobLks.AcquireLease(TargetContainer, blobName, 30, false)
			if err != nil {
				t.Log(err.Error())
			}
			require.Error(t, err)
		}
	}

	t.Logf("[EOL] sleeping....")
	time.Sleep(60 * time.Second)
	blobInfo, err = blobLks.GetBlobInfo(TargetContainer, blobName)
	require.NoError(t, err)
	t.Log(blobInfo)
}

func TestParseBlobUrl(t *testing.T) {

	u := `https://sgectngsa03azne.blob.core.windows.net/dms/378E978F-572C-4C4D-A849-32E70FFE715E.pdf?se=2023-01-24T11%3A19%3A12Z&sig=%2BIRHw%2BmxEA0tZC38XGZ3KZEnib9HwuwyaB1JghSmRvI%3D&sp=r&spr=https&sr=b&st=2023-01-24T11%3A04%3A12Z&sv=2019-12-12`
	scheme, account, container, pathInfo, queryString, ok := azbloblks.ParseBlobUrl(u)
	if ok {
		t.Log(scheme, account, container, pathInfo, queryString)
	} else {
		t.Log("not a blob url")
	}

	u = `whatever-else`
	scheme, account, container, pathInfo, queryString, ok = azbloblks.ParseBlobUrl(u)
	if ok {
		t.Log(scheme, account, container, pathInfo, queryString)
	} else {
		t.Log("not a blob url")
	}
}

func TestDownloadPreSigned(t *testing.T) {
	u := "https://sgectngsa01azne.blob.core.windows.net/gect/documents/0_2dab4d8e-15d1-4b59-87cb-95c30adf96f8/Bolletta+LUCE?se=2023-01-24T13%3A15%3A21Z&sig=Xfa1BFAhNw%2BIXrg18yf8EpkSvuZEjb9XOICzz99xZBo%3D&sp=r&spr=https&sr=b&st=2023-01-24T13%3A00%3A21Z&sv=2019-12-12"
	info, err := azbloblks.DownloadBlobFromPreSignedUrl(u, nil)
	require.NoError(t, err)

	t.Log(info)
}
