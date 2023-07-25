package coslease

import (
	"context"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/data/azcosmos"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/tpm-az-common/cosmosdb/cosutil"
)

type StoredLease struct {
	*Lease
	ETag azcore.ETag
}

func insertLease(ctx context.Context, client *azcosmos.ContainerClient, ls *Lease) (StoredLease, error) {
	resp, err := client.CreateItem(ctx, azcosmos.NewPartitionKeyString(ls.PKey), ls.MustToJSON(), nil)
	if err != nil {
		return StoredLease{}, cosutil.MapAzCoreError(err)
	}

	return StoredLease{Lease: ls, ETag: resp.ETag}, nil
}

func deleteLeaseOnLeasedObject(ctx context.Context, client *azcosmos.ContainerClient, lid string) (bool, error) {
	var err error

	_, err = client.DeleteItem(ctx, azcosmos.NewPartitionKeyString(lid), lid, nil)
	if err != nil {
		return false, cosutil.MapAzCoreError(err)
	}

	return true, nil
}

func replaceLease(ctx context.Context, client *azcosmos.ContainerClient, tok *Lease) (StoredLease, error) {
	b, err := tok.ToJSON()
	if err != nil {
		return StoredLease{}, err
	}

	resp, err := client.ReplaceItem(ctx, azcosmos.NewPartitionKeyString(tok.PKey), tok.Id, b, nil)
	if err != nil {
		return StoredLease{}, cosutil.MapAzCoreError(err)
	}

	return StoredLease{Lease: tok, ETag: resp.ETag}, nil
}

func findLeaseByLeasedObjectId(ctx context.Context, client *azcosmos.ContainerClient, lid string) (StoredLease, error) {

	resp, err := client.ReadItem(ctx, azcosmos.NewPartitionKeyString(lid), lid, nil)
	if err != nil {
		return StoredLease{nil, ""}, cosutil.MapAzCoreError(err)
	}

	e, err := DeserializeEventDocument(resp.Value)
	return StoredLease{Lease: e, ETag: resp.ETag}, err
}

func (e *StoredLease) replace(ctx context.Context, client *azcosmos.ContainerClient) (bool, error) {

	b, err := e.ToJSON()
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

func (e *StoredLease) delete(ctx context.Context, client *azcosmos.ContainerClient) (bool, error) {

	var err error

	opts := &azcosmos.ItemOptions{IfMatchEtag: &e.ETag}
	_, err = client.DeleteItem(ctx, azcosmos.NewPartitionKeyString(e.PKey), e.Id, opts)
	if err != nil {
		return false, cosutil.MapAzCoreError(err)
	}

	return true, nil
}

func (e *StoredLease) upsert(ctx context.Context, client *azcosmos.ContainerClient) (bool, error) {
	b, err := e.ToJSON()
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
