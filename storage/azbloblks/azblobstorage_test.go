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

	AZCommonBlobAccountNameEnvVarName = "AZCOMMON_BLOB_ACCOUNTNAME"
	AZCommonBlobAccountKeyEnvVarName  = "AZCOMMON_BLOB_ACCTKEY"
)

func TestListBlobs(t *testing.T) {
	ctx := context.Background()

	stgConfig := azstoragecfg.Config{
		Name:       os.Getenv(AZCommonBlobAccountNameEnvVarName),
		AccountKey: os.Getenv(AZCommonBlobAccountKeyEnvVarName),
	}

	require.NotEmpty(t, stgConfig.Name, "blob storage account-name not set.... use env var "+AZCommonBlobAccountNameEnvVarName)
	require.NotEmpty(t, stgConfig.AccountKey, "blob storage account-key not set.... use env var "+AZCommonBlobAccountKeyEnvVarName)

	azb, err := azbloblks.NewLinkedService(stgConfig.Name, azstoragecfg.WithAccountKey(stgConfig.AccountKey))
	require.NoError(t, err)

	err = azb.NewContainer(TargetContainer, true)
	require.NoError(t, err)

	defer func() {
		if DropContainerOnExit {
			err = azb.DeleteContainer(TargetContainer, false)
			require.NoError(t, err)
		}
	}()

	opts := azblob.ListBlobsFlatOptions{}
	pager := azb.Client.NewListBlobsFlatPager(TargetContainer, &opts)

	for pager.More() {
		resp, err := pager.NextPage(ctx)
		require.NoError(t, err)
		for _, v := range resp.Segment.BlobItems {
			t.Logf("[%s] tier: %s, content-type: %s, content-length: %d, name: %s", *v.Properties.BlobType, *v.Properties.AccessTier, *v.Properties.ContentType, *v.Properties.ContentLength, *v.Name)
		}
	}

	taggedBlobs, err := azb.ListBlobByTag(TargetContainer, "TagKey1", "MioValore", 10)
	require.NoError(t, err)
	t.Log(taggedBlobs)
}

func TestUploadBlob(t *testing.T) {
	ctx := context.Background()

	stgConfig := azstoragecfg.Config{
		Name:       os.Getenv(AZCommonBlobAccountNameEnvVarName),
		AccountKey: os.Getenv(AZCommonBlobAccountKeyEnvVarName),
	}

	require.NotEmpty(t, stgConfig.Name, "blob storage account-name not set.... use env var "+AZCommonBlobAccountNameEnvVarName)
	require.NotEmpty(t, stgConfig.AccountKey, "blob storage account-key not set.... use env var "+AZCommonBlobAccountKeyEnvVarName)

	azb, err := azbloblks.NewLinkedService(stgConfig.Name, azstoragecfg.WithAccountKey(stgConfig.AccountKey))
	require.NoError(t, err)

	defer func() {
		if DropContainerOnExit {
			err = azb.DeleteContainer(TargetContainer, false)
			require.NoError(t, err)
		}
	}()

	url, err := azb.UploadFromBuffer(ctx, TargetContainer, "test-blob-upload-2.txt", []byte(`Text data3`))
	require.NoError(t, err)

	t.Log(url)
}

func TestDownloadBlob(t *testing.T) {
	stgConfig := azstoragecfg.Config{
		Name:       os.Getenv(AZCommonBlobAccountNameEnvVarName),
		AccountKey: os.Getenv(AZCommonBlobAccountKeyEnvVarName),
	}

	require.NotEmpty(t, stgConfig.Name, "blob storage account-name not set.... use env var "+AZCommonBlobAccountNameEnvVarName)
	require.NotEmpty(t, stgConfig.AccountKey, "blob storage account-key not set.... use env var "+AZCommonBlobAccountKeyEnvVarName)

	azb, err := azbloblks.NewLinkedService(stgConfig.Name, azstoragecfg.WithAccountKey(stgConfig.AccountKey))
	require.NoError(t, err)

	_, err = azb.DownloadToBuffer(TargetContainer, "cortina-2021")
	require.NoError(t, err)

	blobExists, err := azb.BlobExists(TargetContainer, "cortina-2021")
	require.NoError(t, err)
	t.Log("blob exists? ", blobExists)
}

func TestAcquireBlob(t *testing.T) {
	stgConfig := azstoragecfg.Config{
		Name:       os.Getenv(AZCommonBlobAccountNameEnvVarName),
		AccountKey: os.Getenv(AZCommonBlobAccountKeyEnvVarName),
	}

	require.NotEmpty(t, stgConfig.Name, "blob storage account-name not set.... use env var "+AZCommonBlobAccountNameEnvVarName)
	require.NotEmpty(t, stgConfig.AccountKey, "blob storage account-key not set.... use env var "+AZCommonBlobAccountKeyEnvVarName)

	azb, err := azbloblks.NewLinkedService(stgConfig.Name, azstoragecfg.WithAccountKey(stgConfig.AccountKey))
	require.NoError(t, err)

	blobName := "cortina-2021"
	blobInfo, err := azb.GetBlobInfo(TargetContainer, blobName)
	require.NoError(t, err)
	t.Log(blobInfo)

	leaseHandler, err := azb.AcquireLease(TargetContainer, blobName, 60, true)
	require.NoError(t, err)

	defer leaseHandler.Close()

	for i := 0; i <= 20; i++ {
		blobInfo, err = azb.GetBlobInfo(TargetContainer, blobName)
		require.NoError(t, err)
		t.Log(blobInfo)
		t.Logf("[%d] sleeping....", i)
		time.Sleep(20 * time.Second)

		bi := azbloblks.BlobInfo{ContainerName: TargetContainer, BlobName: blobName, Tags: []azbloblks.BlobTag{{Key: "TAGleased", Value: fmt.Sprintf("tag-val-%d", i)}}}
		err = azb.SetBlobTags(bi, leaseHandler.LeaseId)
		require.NoError(t, err)

		if i == 6 {
			_, err := azb.AcquireLease(TargetContainer, blobName, 30, false)
			if err != nil {
				t.Log(err.Error())
			}
			require.Error(t, err)
		}
	}

	t.Logf("[EOL] sleeping....")
	time.Sleep(60 * time.Second)
	blobInfo, err = azb.GetBlobInfo(TargetContainer, blobName)
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

// These functions have not been tested....

func TestBlobListBlobsWithSAS(t *testing.T) {
	ctx := context.Background()

	stgConfig := azstoragecfg.Config{
		Name:       "",
		AccountKey: "",
	}

	azb, err := azbloblks.NewLinkedService(stgConfig.Name, azstoragecfg.WithSasToken("sv=2020-08-04&ss=bfqt&srt=co&sp=rwdlacupitfx&se=2021-12-31T21:26:45Z&st=2021-12-10T13:26:45Z&spr=https&sig=EG%2BJ5X4e0pzO5PUyQZsxzah8m1W6tX24hdxlr1KQj6M%3D"))
	require.NoError(t, err)

	err = azb.NewContainer(TargetContainer, true)
	require.NoError(t, err)

	defer func() {
		if DropContainerOnExit {
			err = azb.DeleteContainer(TargetContainer, false)
			require.NoError(t, err)
		}
	}()

	opts := azblob.ListBlobsFlatOptions{}
	pager := azb.Client.NewListBlobsFlatPager(TargetContainer, &opts)

	for pager.More() {
		resp, err := pager.NextPage(ctx)
		require.NoError(t, err)
		for _, _blob := range resp.Segment.BlobItems {
			fmt.Printf("%v", _blob.Name)
		}
	}
}
