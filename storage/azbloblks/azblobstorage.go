package azbloblks

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/blob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/bloberror"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/service"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/tpm-az-common/storage/azblobutil"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/tpm-common/util"
	"github.com/opentracing/opentracing-go"
	"github.com/rs/zerolog/log"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"strings"
)

type BlobTag struct {
	Key   string `mapstructure:"key,omitempty" yaml:"key,omitempty" json:"key,omitempty"`
	Value string `mapstructure:"value,omitempty" yaml:"value,omitempty" json:"value,omitempty"`
}

const (
	LeaseStateAvailable = "available" // The lease is unlocked and can be acquired. Allowed action: acquire.
	LeaseStateLeased    = "leased"    //  The lease is locked. Allowed actions: acquire (same lease ID only), renew, change, release, and break.
	LeaseStateExpired   = "expired"   //  The lease duration has expired. Allowed actions: acquire, renew, release, and break.
	LeaseStateBreaking  = "breaking"  //  The lease has been broken, but the lease will continue to be locked until the break period has expired. Allowed actions: release and break.
	LeaseStateBroken    = "broken"    // : The lease has been broken, and the break period has expired. Allowed actions: acquire, release, and break.
)

type BlobInfo struct {
	Exists        bool      `mapstructure:"exists,omitempty" yaml:"exists,omitempty" json:"exists,omitempty"`
	AccountName   string    `mapstructure:"account-name,omitempty" yaml:"account-name,omitempty" json:"account-name,omitempty"`
	ContainerName string    `mapstructure:"container-name,omitempty" yaml:"container-name,omitempty" json:"container-name,omitempty"`
	BlobName      string    `mapstructure:"blob-name,omitempty" yaml:"blob-name,omitempty" json:"blob-name,omitempty"`
	FileName      string    `mapstructure:"file-name,omitempty" yaml:"file-name,omitempty" json:"file-name,omitempty"`
	Body          []byte    `mapstructure:"body,omitempty" yaml:"body,omitempty" json:"body,omitempty"`
	Tags          []BlobTag `mapstructure:"tags,omitempty" yaml:"tags,omitempty" json:"tags,omitempty"`
	ContentType   string    `mapstructure:"content-type,omitempty" yaml:"content-type,omitempty" json:"content-type,omitempty"`
	Size          int64     `mapstructure:"size,omitempty" yaml:"size,omitempty" json:"size,omitempty"`
	ETag          string    `mapstructure:"etag,omitempty" yaml:"etag,omitempty" json:"etag,omitempty"`
	LeaseState    string    `mapstructure:"lease-state,omitempty" yaml:"lease-state,omitempty" json:"lease-state,omitempty"`
}

func (bi *BlobInfo) Id() string {
	id := filepath.Base(bi.BlobName)
	ext := filepath.Ext(bi.BlobName)
	if ext != "" {
		id = strings.TrimSuffix(id, ext)
	}

	return id
}

func (az *LinkedService) NewContainer(cntName string, noErrorIfPresent bool) error {

	const semLogContext = "azb-lks::new-container"
	var err error

	createOpts := &azblob.CreateContainerOptions{}
	_, err = az.Client.CreateContainer(context.Background(), cntName, createOpts)
	if err != nil {
		blobErr := azblobutil.MapError2AzBlobError(err)
		if blobErr.StatusCode == http.StatusConflict && blobErr.ErrorCode == string(bloberror.ContainerAlreadyExists) {
			if noErrorIfPresent {
				log.Info().Str("container", cntName).Msg("container already exists")
				err = nil
			} else {
				log.Error().Str("container", cntName).Int("status-code", blobErr.StatusCode).Str("err-code", blobErr.ErrorCode).Bool("mute-error", noErrorIfPresent).Msg(semLogContext + " container already exists")
			}
		} else {
			log.Error().Str("container", cntName).Int("status-code", blobErr.StatusCode).Str("err-code", blobErr.ErrorCode).Msg(semLogContext)
		}
	}

	return err
}

func (az *LinkedService) DeleteContainer(cntName string, noErrorIfMissing bool) error {
	const semLogContext = "azb-lks::delete-container"

	var err error
	deleteOpts := &azblob.DeleteContainerOptions{}
	_, err = az.Client.DeleteContainer(context.Background(), cntName, deleteOpts)
	if err != nil {
		blobErr := azblobutil.MapError2AzBlobError(err)
		if blobErr.StatusCode == http.StatusNotFound && blobErr.ErrorCode == string(bloberror.ContainerNotFound) {
			if noErrorIfMissing {
				log.Info().Str("container", cntName).Msg("container not found")
				err = nil
			} else {
				log.Error().Str("container", cntName).Int("status-code", blobErr.StatusCode).Str("err-code", blobErr.ErrorCode).Bool("mute-error", noErrorIfMissing).Msg(semLogContext + " container not found")
			}
		} else {
			log.Error().Str("container", cntName).Int("status-code", blobErr.StatusCode).Str("err-code", blobErr.ErrorCode).Msg(semLogContext)
		}
	}
	return err
}

func (az *LinkedService) BlobExists(cntName string, fn string) (bool, error) {

	_, ok, err := az.GetBlobProperties(cntName, fn)
	return ok, err
}

func (az *LinkedService) DeleteBlob(cntName string, fn string) error {
	blobClient := az.Client.ServiceClient().NewContainerClient(cntName).NewBlobClient(fn)
	_, err := blobClient.Delete(context.Background(), nil)
	if err != nil {
		return err
	}

	return nil
}

func (az *LinkedService) GetBlobProperties(cntName string, fn string) (blob.GetPropertiesResponse, bool, error) {
	blobClient := az.Client.ServiceClient().NewContainerClient(cntName).NewBlobClient(fn)

	opts := &blob.GetPropertiesOptions{}
	resp, err := blobClient.GetProperties(context.Background(), opts)
	if err != nil {
		blobErr := azblobutil.MapError2AzBlobError(err)
		if blobErr.ErrorCode == string(bloberror.BlobNotFound) {
			return resp, false, nil
		}

		return resp, false, err
	}

	return resp, true, nil
}

func (az *LinkedService) GetBlobInfo(cntName string, fn string) (BlobInfo, error) {
	blobClient := az.Client.ServiceClient().NewContainerClient(cntName).NewBlobClient(fn)

	opts := &blob.GetPropertiesOptions{}
	resp, err := blobClient.GetProperties(context.Background(), opts)
	if err != nil {
		blobErr := azblobutil.MapError2AzBlobError(err)
		if blobErr.ErrorCode == string(bloberror.BlobNotFound) {
			blobErr = nil
		}

		return BlobInfo{Exists: false, ContainerName: cntName, BlobName: fn}, blobErr
	}

	bi := BlobInfo{Exists: true, ContainerName: cntName, BlobName: fn, ContentType: *resp.ContentType, Size: *resp.ContentLength, ETag: string(*resp.ETag), LeaseState: string(*resp.LeaseState)}
	if resp.TagCount != nil && *resp.TagCount > 0 {
		tagResp, err := blobClient.GetTags(context.Background(), &blob.GetTagsOptions{})
		if err != nil {
			return bi, azblobutil.MapError2AzBlobError(err)
		}

		for _, ti := range tagResp.BlobTagSet {
			bi.Tags = append(bi.Tags, BlobTag{Key: *ti.Key, Value: *ti.Value})
		}
	}
	return bi, nil
}

func (az *LinkedService) DownloadToBuffer(cntName string, blobName string) (BlobInfo, error) {
	ctx := context.Background()

	blobClient := az.Client.ServiceClient().NewContainerClient(cntName).NewBlobClient(blobName)

	downloadStreamOpts := &azblob.DownloadStreamOptions{}
	downloadResponse, err := blobClient.DownloadStream(ctx, downloadStreamOpts)
	if err != nil {
		return BlobInfo{}, azblobutil.MapError2AzBlobError(err)
	}

	downloadedData := &bytes.Buffer{}

	readerOpts := &azblob.RetryReaderOptions{MaxRetries: 2}
	reader := downloadResponse.NewRetryReader(ctx, readerOpts)
	_, err = downloadedData.ReadFrom(reader)
	if err != nil {
		return BlobInfo{}, azblobutil.MapError2AzBlobError(err)
	}

	err = reader.Close()
	if err != nil {
		return BlobInfo{}, azblobutil.MapError2AzBlobError(err)
	}

	fi := BlobInfo{Body: downloadedData.Bytes(), ContainerName: cntName, BlobName: blobName}

	// log.Trace().Msg("download file from storage " + fn)
	return fi, nil
}

func (az *LinkedService) DownloadToFile(cntName string, blobName string, destFilename string) (BlobInfo, error) {

	const semLogContext = "azb-lks::download-file"
	log.Trace().Str("container-name", cntName).Str("blob-name", blobName).Str("dest-file", destFilename).Msg(semLogContext)

	ctx := context.Background()

	destFile, err := os.Create(destFilename)
	if err != nil {
		return BlobInfo{}, err
	}
	defer func(destFile *os.File) {
		err = destFile.Close()
		if err != nil {
			log.Error().Err(err).Msg(semLogContext + " error in closing downloaded file")
		}
	}(destFile)

	blobClient := az.Client.ServiceClient().NewContainerClient(cntName).NewBlobClient(blobName)

	downloadStreamOpts := &azblob.DownloadFileOptions{}
	_, err = blobClient.DownloadFile(ctx, destFile, downloadStreamOpts)
	if err != nil {
		return BlobInfo{}, azblobutil.MapError2AzBlobError(err)
	}

	fi := BlobInfo{Body: nil, ContainerName: cntName, BlobName: blobName, FileName: destFilename}

	// log.Trace().Msg("download file from storage " + fn)
	return fi, nil
}

/*
 *
 */

func (az *LinkedService) UploadFromBuffer(ctx context.Context, container, fn string, body []byte) (string, error) {

	var err error

	blobClient := az.Client.ServiceClient().NewContainerClient(container).NewBlockBlobClient(fn)

	uploadOptions := azblob.UploadBufferOptions{}
	_, err = blobClient.UploadBuffer(ctx, body, &uploadOptions)
	if err != nil {
		return "", azblobutil.MapError2AzBlobError(err)
	}

	return "", nil
}

func (az *LinkedService) UploadFromFile(ctx context.Context, cntName, blobName string, sourceFileName string, removeFile bool) (string, error) {

	const semLogContext = "azb-lks::upload-file"

	destFile, err := os.Open(sourceFileName)
	if err != nil {
		return "", err
	}
	defer func(file *os.File) {
		err = file.Close()
		if err != nil {
			log.Error().Err(err).Msg(semLogContext + " error in closing uploaded file")
		}
	}(destFile)

	if removeFile {
		defer func(name string) {
			err = os.Remove(name)
			if err != nil {
				log.Error().Err(err).Msg(semLogContext + " error in deleting uploaded file")
			}
		}(sourceFileName)
	}

	blobClient := az.Client.ServiceClient().NewContainerClient(cntName).NewBlockBlobClient(blobName)

	_, err = blobClient.UploadFile(context.TODO(), destFile,
		&azblob.UploadFileOptions{
			BlockSize:   int64(1024),
			Concurrency: uint16(3),
			// If Progress is non-nil, this function is called periodically as bytes are uploaded.
			Progress: func(bytesTransferred int64) {
				log.Trace().Err(err).Int64("bytes-transferred", bytesTransferred).Msg(semLogContext + " uploading....")
			},
		})

	if err != nil {
		return "", azblobutil.MapError2AzBlobError(err)
	}

	return "", nil
}

func (az *LinkedService) ListBlobs(cntName string, maxResults int32) ([]BlobInfo, error) {

	containerClient := az.Client.ServiceClient().NewContainerClient(cntName)

	opts := &azblob.ListBlobsFlatOptions{
		MaxResults: &maxResults,
	}
	pager := containerClient.NewListBlobsFlatPager(opts)

	var rl []BlobInfo
	for pager.More() {
		resp, err := pager.NextPage(context.TODO())
		if err != nil {
			return nil, azblobutil.MapError2AzBlobError(err)
		}
		for _, bi := range resp.Segment.BlobItems {
			blobInfo := BlobInfo{
				ContainerName: cntName,
				BlobName:      *bi.Name,
			}
			rl = append(rl, blobInfo)
		}
	}

	return rl, nil
}

func (az *LinkedService) ListBlobByTag(cntName string, tagName, tagValue string, maxResults int) ([]BlobInfo, error) {

	svcClient := az.Client.ServiceClient()

	whereCondition := fmt.Sprintf("\"%s\"='%s'", tagName, tagValue)

	maxResultOption := int32(maxResults)
	resp, err := svcClient.FilterBlobs(context.Background(), whereCondition, &service.FilterBlobsOptions{
		MaxResults: &maxResultOption,
	})

	if err != nil {
		return nil, azblobutil.MapError2AzBlobError(err)
	}

	var rl []BlobInfo
	for _, bi := range resp.Blobs {
		blobInfo := BlobInfo{
			ContainerName: *bi.ContainerName,
			BlobName:      *bi.Name,
		}
		for _, bt := range bi.Tags.BlobTagSet {
			blobTag := BlobTag{
				Key:   *bt.Key,
				Value: *bt.Value,
			}
			blobInfo.Tags = append(blobInfo.Tags, blobTag)
		}
		rl = append(rl, blobInfo)
	}

	return rl, nil
}

func (az *LinkedService) SetTags(container, blobNae string, tags []BlobTag, leaseId string) error {
	blobClient := az.Client.ServiceClient().NewContainerClient(container).NewBlobClient(blobNae)

	// new tags cannot be nil but is acceptable to be empty
	newTags := make(map[string]string)
	for _, bi := range tags {
		if newTags == nil {
			newTags = make(map[string]string)
		}
		newTags[bi.Key] = bi.Value
	}

	opts := blob.SetTagsOptions{}
	if leaseId != "" {
		opts.AccessConditions = &blob.AccessConditions{LeaseAccessConditions: &blob.LeaseAccessConditions{
			LeaseID: &leaseId,
		}}
	}

	_, err := blobClient.SetTags(context.Background(), newTags, &opts)
	if err != nil {
		return azblobutil.MapError2AzBlobError(err)
	}

	return nil
}

func (az *LinkedService) SetBlobTags(blobInfo BlobInfo, leaseId string) error {
	blobClient := az.Client.ServiceClient().NewContainerClient(blobInfo.ContainerName).NewBlobClient(blobInfo.BlobName)

	var newTags map[string]string
	for _, bi := range blobInfo.Tags {
		if newTags == nil {
			newTags = make(map[string]string)
		}
		newTags[bi.Key] = bi.Value
	}

	opts := blob.SetTagsOptions{}
	if leaseId != "" {
		opts.AccessConditions = &blob.AccessConditions{LeaseAccessConditions: &blob.LeaseAccessConditions{
			LeaseID: &leaseId,
		}}
	}

	_, err := blobClient.SetTags(context.Background(), newTags, &opts)
	if err != nil {
		return azblobutil.MapError2AzBlobError(err)
	}

	return nil
}

var StorageUrlPattern = regexp.MustCompile(`^(http|https)://([0-9a-zA-Z]*).blob.core.windows.net/([0-9a-zA-Z\-]*)([^?]*)(\\?.*)?`)

func IsBlobUrl(u string) bool {
	_, _, _, _, _, ok := ParseBlobUrl(u)
	return ok
}

func ParseBlobUrl(u string) (string, string, string, string, string, bool) {

	const semLogContext = "parse-blob-url"
	matches := StorageUrlPattern.FindStringSubmatch(u)
	if len(matches) != 6 {
		return "", "", "", "", "", false
	}

	/*
		scheme := matches[1]
		account := matches[2]
		container := matches[3]
		pathInfo := matches[4]
		queryString := matches[5]
	*/
	qs := strings.TrimLeft(matches[5], "?")
	pathInfo, err := url.PathUnescape(matches[4])
	if err != nil {
		log.Error().Err(err).Msg(semLogContext)
		return "", "", "", "", "", false
	}
	return matches[1], matches[2], matches[3], pathInfo, qs, true
}

func DownloadBlobFromPreSignedUrl(u string, span opentracing.Span) (BlobInfo, error) {

	scheme, account, container, pathInfo, sasToken, ok := ParseBlobUrl(u)
	if !ok {
		return BlobInfo{}, errors.New("unparsable url")
	}

	ctx := context.Background()
	u1 := fmt.Sprintf("%s://%s.blob.core.windows.net?%s", scheme, account, sasToken)
	serviceClient, err := azblob.NewClientWithNoCredential(u1, nil)
	if err != nil {
		return BlobInfo{}, err
	}

	downloadResponse, err := serviceClient.DownloadStream(ctx, container, strings.TrimLeft(pathInfo, "/"), nil)
	if err != nil {
		return BlobInfo{}, err
	}

	if span != nil {
		span.SetTag("blob.container", container)
		span.SetTag("blob.path", pathInfo)
		span.SetTag(util.HttpMethodTraceTag, "get")
		span.SetTag(util.HttStatusCodeTraceTag, downloadResponse.ErrorCode)
	}

	downloadedData := &bytes.Buffer{}
	readerOpts := &azblob.RetryReaderOptions{MaxRetries: 2}
	reader := downloadResponse.NewRetryReader(context.Background(), readerOpts)
	_, err = downloadedData.ReadFrom(reader)
	if err != nil {
		return BlobInfo{}, err
	}

	err = reader.Close()
	if err != nil {
		return BlobInfo{}, err
	}

	fi := BlobInfo{Body: downloadedData.Bytes(), ContainerName: container, BlobName: "filename"}

	// log.Trace().Msg("download file from storage " + fn)
	return fi, nil
}

func UploadBlobToPreSignedUrl(u string, blobData []byte, span opentracing.Span) error {

	scheme, account, container, pathInfo, sasToken, ok := ParseBlobUrl(u)
	if !ok {
		return fmt.Errorf("unparsable url: %s", u)
	}

	ctx := context.Background()
	u1 := fmt.Sprintf("%s://%s.blob.core.windows.net?%s", scheme, account, sasToken)
	serviceClient, err := azblob.NewClientWithNoCredential(u1, nil)
	if err != nil {
		return err
	}

	_, err = serviceClient.UploadBuffer(ctx, container, strings.TrimLeft(pathInfo, "/"), blobData, nil)
	if span != nil {
		span.SetTag("blob.container", container)
		span.SetTag("blob.path", pathInfo)
		span.SetTag(util.HttpMethodTraceTag, "get")
		span.SetTag(util.HttStatusCodeTraceTag, "test")
	}
	if err != nil {
		return err
	}

	return nil
}
