package costextfile

import (
	"context"
	"github.com/Azure/azure-sdk-for-go/sdk/data/azcosmos"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/tpm-az-common/cosmosdb/cosutil"
)

func InsertRow(ctx context.Context, client *azcosmos.ContainerClient, r *Row) (StoredRow, error) {

	r.enforceDefaultValues()
	resp, err := client.CreateItem(ctx, azcosmos.NewPartitionKeyString(r.PKey), r.MustToJson(), nil)
	if err != nil {
		return StoredRow{}, cosutil.MapAzCoreError(err)
	}

	return StoredRow{Row: r, ETag: resp.ETag}, nil
}

func DeleteRow(ctx context.Context, client *azcosmos.ContainerClient, pkey, id string) (bool, error) {
	var err error
	_, err = client.DeleteItem(ctx, azcosmos.NewPartitionKeyString(pkey), id, nil)
	if err != nil {
		return false, cosutil.MapAzCoreError(err)
	}

	return true, nil
}

func ReplaceRow(ctx context.Context, client *azcosmos.ContainerClient, r *Row) (StoredRow, error) {

	r.enforceDefaultValues()

	b, err := r.ToJson()
	if err != nil {
		return StoredRow{}, err
	}

	resp, err := client.ReplaceItem(ctx, azcosmos.NewPartitionKeyString(r.PKey), r.Id, b, nil)
	if err != nil {
		return StoredRow{}, cosutil.MapAzCoreError(err)
	}

	return StoredRow{Row: r, ETag: resp.ETag}, nil
}

func FindRowById(ctx context.Context, client *azcosmos.ContainerClient, pkey, id string) (StoredRow, error) {
	resp, err := client.ReadItem(ctx, azcosmos.NewPartitionKeyString(pkey), id, nil)
	if err != nil {
		return StoredRow{nil, ""}, cosutil.MapAzCoreError(err)
	}

	e, err := RowFromJson(resp.Value)
	return StoredRow{Row: e, ETag: resp.ETag}, err
}

func (e *StoredRow) Replace(ctx context.Context, client *azcosmos.ContainerClient) (bool, error) {

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

func (e *StoredRow) Delete(ctx context.Context, client *azcosmos.ContainerClient) (bool, error) {

	var err error

	opts := &azcosmos.ItemOptions{IfMatchEtag: &e.ETag}
	_, err = client.DeleteItem(ctx, azcosmos.NewPartitionKeyString(e.PKey), e.Id, opts)
	if err != nil {
		return false, cosutil.MapAzCoreError(err)
	}

	return true, nil
}

func (e *StoredRow) Upsert(ctx context.Context, client *azcosmos.ContainerClient) (bool, error) {

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
