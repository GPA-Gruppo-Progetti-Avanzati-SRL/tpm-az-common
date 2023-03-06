package costextfile

import (
	"context"
	"github.com/Azure/azure-sdk-for-go/sdk/data/azcosmos"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/tpm-az-common/cosmosdb/cosutil"
)

func InsertFile(ctx context.Context, client *azcosmos.ContainerClient, f *File) (StoredFile, error) {

	f.enforceDefaultValues()

	resp, err := client.CreateItem(ctx, azcosmos.NewPartitionKeyString(f.PKey), f.MustToJson(), nil)
	if err != nil {
		return StoredFile{}, cosutil.MapAzCoreError(err)
	}

	return StoredFile{File: f, ETag: resp.ETag}, nil
}

func DeleteFile(ctx context.Context, client *azcosmos.ContainerClient, id string) (bool, error) {
	var err error
	_, err = client.DeleteItem(ctx, azcosmos.NewPartitionKeyString(FilePartitionKey), id, nil)
	if err != nil {
		return false, cosutil.MapAzCoreError(err)
	}

	return true, nil
}

func ReplaceFile(ctx context.Context, client *azcosmos.ContainerClient, f *File) (StoredFile, error) {

	f.enforceDefaultValues()

	b, err := f.ToJson()
	if err != nil {
		return StoredFile{}, err
	}

	resp, err := client.ReplaceItem(ctx, azcosmos.NewPartitionKeyString(FilePartitionKey), f.Id, b, nil)
	if err != nil {
		return StoredFile{}, cosutil.MapAzCoreError(err)
	}

	return StoredFile{File: f, ETag: resp.ETag}, nil
}

func FindFileById(ctx context.Context, client *azcosmos.ContainerClient, id string) (StoredFile, error) {
	resp, err := client.ReadItem(ctx, azcosmos.NewPartitionKeyString(FilePartitionKey), id, nil)
	if err != nil {
		return StoredFile{nil, ""}, cosutil.MapAzCoreError(err)
	}

	e, err := FileFromJson(resp.Value)
	return StoredFile{File: e, ETag: resp.ETag}, err
}

func (e *StoredFile) Replace(ctx context.Context, client *azcosmos.ContainerClient) (bool, error) {

	e.enforceDefaultValues()

	b, err := e.ToJson()
	if err != nil {
		return false, err
	}

	opts := &azcosmos.ItemOptions{IfMatchEtag: &e.ETag}
	resp, err := client.ReplaceItem(ctx, azcosmos.NewPartitionKeyString(e.PKey), e.Id, b, opts)
	if err != nil {
		return false, cosutil.MapAzCoreError(err)
	}

	e.ETag = resp.ETag
	return true, nil
}

func (e *StoredFile) Delete(ctx context.Context, client *azcosmos.ContainerClient) (bool, error) {

	var err error

	opts := &azcosmos.ItemOptions{IfMatchEtag: &e.ETag}
	_, err = client.DeleteItem(ctx, azcosmos.NewPartitionKeyString(FilePartitionKey), e.Id, opts)
	if err != nil {
		return false, cosutil.MapAzCoreError(err)
	}

	return true, nil
}

func (e *StoredFile) Upsert(ctx context.Context, client *azcosmos.ContainerClient) (bool, error) {

	e.enforceDefaultValues()

	b, err := e.ToJson()
	if err != nil {
		return false, err
	}

	opts := &azcosmos.ItemOptions{IfMatchEtag: &e.ETag}
	resp, err := client.UpsertItem(ctx, azcosmos.NewPartitionKeyString(e.PKey), b, opts)
	if err != nil {
		return false, cosutil.MapAzCoreError(err)
	}

	e.ETag = resp.ETag
	return true, nil
}
