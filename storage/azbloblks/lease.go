package azbloblks

import (
	"context"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/to"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/lease"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/tpm-az-common/storage/azblobutil"
	"github.com/google/uuid"
	"github.com/rs/zerolog/log"
	"time"
)

type LeaseHandler struct {
	lks           *LinkedService
	LeaseId       string
	ContainerName string
	BlobName      string
	LeaseDuration int
	auto          bool
	autoRenewCh   chan struct{}
}

func (az *LinkedService) AcquireLease(cntName string, fn string, duration int, auto bool) (*LeaseHandler, error) {

	const semLogContext = "azb-lks::acquire-lease"

	blobClient := az.Client.ServiceClient().NewContainerClient(cntName).NewBlobClient(fn)
	leaseID := uuid.New().String()
	leaseClient, err := lease.NewBlobClient(blobClient, &lease.BlobClientOptions{LeaseID: to.Ptr(leaseID)})
	if err != nil {
		return nil, azblobutil.MapError2AzBlobError(err)
	}

	if duration > 0 {
		if duration < 15 {
			duration = 15
		}
		if duration > 60 {
			duration = 60
		}
	} else {
		duration = -1
	}

	durationOption := int32(duration)
	_, err = leaseClient.AcquireLease(context.Background(), durationOption, nil)
	if err != nil {
		log.Error().Err(err).Str("lease-id", leaseID).Msg(semLogContext)
		return nil, azblobutil.MapError2AzBlobError(err)
	}

	log.Info().Str("lease-id", leaseID).Int("duration", duration).Msg(semLogContext)
	lh := &LeaseHandler{lks: az, LeaseId: leaseID, ContainerName: cntName, BlobName: fn, LeaseDuration: duration, auto: auto, autoRenewCh: make(chan struct{})}
	if auto {
		go lh.renewLoop()
	}
	return lh, nil
}

func (az *LinkedService) ReleaseLease(cntName string, fn string, leaseID string) (bool, error) {

	const semLogContext = "azb-lks::release-lease"

	blobClient := az.Client.ServiceClient().NewContainerClient(cntName).NewBlobClient(fn)
	leaseClient, err := lease.NewBlobClient(blobClient, &lease.BlobClientOptions{LeaseID: to.Ptr(leaseID)})
	if err != nil {
		return false, azblobutil.MapError2AzBlobError(err)
	}

	_, err = leaseClient.ReleaseLease(context.Background(), &lease.BlobReleaseOptions{})
	if err != nil {
		log.Error().Err(err).Str("lease-id", leaseID).Msg(semLogContext)
		return false, azblobutil.MapError2AzBlobError(err)
	}

	log.Info().Str("lease-id", leaseID).Msg(semLogContext + " released")
	return true, nil
}

type ReleaseOptions struct {
	Tags []BlobTag
}

type ReleaseOption func(optss *ReleaseOptions)

func WithTag(t BlobTag) ReleaseOption {
	return func(opts *ReleaseOptions) {
		opts.Tags = append(opts.Tags, t)
	}
}

func (lh *LeaseHandler) Release(opts ...ReleaseOption) error {

	const semLogContext = "lease-handler::release"
	options := ReleaseOptions{}
	for _, o := range opts {
		o(&options)
	}

	var errTags error
	if len(options.Tags) > 0 {
		errTags = lh.lks.SetTags(lh.ContainerName, lh.BlobName, options.Tags, lh.LeaseId)
		if errTags != nil {
			log.Error().Err(errTags).Msg(semLogContext)
		}
	}

	_, errRelease := lh.lks.ReleaseLease(lh.ContainerName, lh.BlobName, lh.LeaseId)
	if errRelease != nil {
		log.Error().Err(errRelease).Msg(semLogContext)
	}

	if lh.auto {
		close(lh.autoRenewCh)
	}

	if errRelease != nil {
		return errRelease
	}

	return errTags
}

func (lh *LeaseHandler) renewLoop() {
	const semLogContext = "lease-handler::renew-loop"

	tickInterval := time.Second * time.Duration(float64(lh.LeaseDuration)*0.6)
	log.Info().Float64("tickInterval-secs", tickInterval.Seconds()).Msg(semLogContext + " starting...")

	ticker := time.NewTicker(tickInterval)
	var exitLoop bool
	for !exitLoop {
		select {
		case <-ticker.C:
			err := lh.lks.RenewLease(lh.ContainerName, lh.BlobName, lh.LeaseId)
			if err != nil {
				log.Error().Err(err)
			}
		case <-lh.autoRenewCh:
			ticker.Stop()
			exitLoop = true
		}
	}

	log.Info().Msg(semLogContext + " ended")
}

func (az *LinkedService) RenewLease(cntName string, fn string, leaseID string) error {

	const semLogContext = "lease-handler::renew"
	blobClient := az.Client.ServiceClient().NewContainerClient(cntName).NewBlobClient(fn)

	leaseClient, err := lease.NewBlobClient(blobClient, &lease.BlobClientOptions{LeaseID: to.Ptr(leaseID)})
	if err != nil {
		return azblobutil.MapError2AzBlobError(err)
	}

	log.Info().Str("lease-id", leaseID).Msg(semLogContext)

	_, err = leaseClient.RenewLease(context.Background(), &lease.BlobRenewOptions{})
	if err != nil {
		return azblobutil.MapError2AzBlobError(err)
	}

	// log.Trace().Interface("lease-resp", resp).Msg(semLogContext)
	return nil
}
