package coslease

import (
	"context"
	"errors"
	"fmt"
	"github.com/Azure/azure-sdk-for-go/sdk/data/azcosmos"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/tpm-az-common/cosmosdb/cosutil"
	"github.com/rs/zerolog/log"
	"time"
)

type LeaseHandler struct {
	cli         *azcosmos.ContainerClient
	Lease       Lease
	auto        bool
	autoRenewCh chan struct{}
}

func (lh *LeaseHandler) IsZero() bool {
	return lh.Lease.Id == ""
}

func CanAcquireLease(ctx context.Context, client *azcosmos.ContainerClient, typ, pkey, id string) (bool, error) {

	const semLogContext = "cos-lease::can-acquire-lease"

	l := NewLease(typ, pkey, id, 60)

	d, err := findLeaseByLeasedObjectId(context.Background(), client, l.Id)
	if err != nil {
		if err == cosutil.EntityNotFound {
			return true, nil
		}

		return false, err
	}

	return d.Acquirable(), nil
}

func AcquireLease(ctx context.Context, client *azcosmos.ContainerClient, typ, pkey, id string, auto bool) (*LeaseHandler, error) {

	const semLogContext = "cos-lease::acquire-lease"

	l := NewLease(typ, pkey, id, 60)

	d, err := findLeaseByLeasedObjectId(context.Background(), client, l.Id)
	if err != nil {
		if err == cosutil.EntityNotFound {
			_, err = insertLease(context.Background(), client, &l)
		}
		if err != nil {
			return nil, err
		}
	} else {
		if !d.Lease.Acquirable() {
			return nil, fmt.Errorf("lease cannot be acquired on event %s", d.Id)
		} else {
			d.Lease = &l
			ok, err := d.replace(context.Background(), client)
			if err != nil {
				return nil, err
			}

			if !ok {
				return nil, errors.New("cannot acquire lease... replace failed")
			}
		}
	}

	lh := LeaseHandler{
		cli:         client,
		Lease:       l,
		auto:        auto,
		autoRenewCh: make(chan struct{}),
	}

	if auto {
		go lh.renewLoop()
	}

	return &lh, nil
}

func (lh *LeaseHandler) Release() error {

	const semLogContext = "lease-handler::release"

	d, err := findLeaseByLeasedObjectId(context.Background(), lh.cli, lh.Lease.Id)
	if err != nil {
		if err == cosutil.EntityNotFound {
			return nil
		}

		return err
	}

	if d.Lease.LeaseId != lh.Lease.LeaseId {
		log.Warn().Msg(semLogContext + " lease id already been released")
	}

	d.Lease.Status = "available"
	_, err = d.replace(context.Background(), lh.cli)

	if lh.auto {
		close(lh.autoRenewCh)
	}

	return err
}

func (lh *LeaseHandler) renewLoop() {
	const semLogContext = "lease-handler::renew-loop"

	tickInterval := time.Second * time.Duration(float64(lh.Lease.Duration)*0.6)
	log.Info().Float64("tickInterval-secs", tickInterval.Seconds()).Msg(semLogContext + " starting...")

	ticker := time.NewTicker(tickInterval)
	var exitLoop bool
	for !exitLoop {
		select {
		case <-ticker.C:
			err := lh.RenewLease()
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

func (lh *LeaseHandler) RenewLease() error {
	const semLogContext = "lease-handler::renew"

	d, err := findLeaseByLeasedObjectId(context.Background(), lh.cli, lh.Lease.Id)
	if err != nil {
		return err
	}

	if d.Lease.LeaseId != lh.Lease.LeaseId {
		err := fmt.Errorf("lease-id on object %s: wanted %s, actual %s", lh.Lease.Id, lh.Lease.LeaseId, d.Lease.LeaseId)
		return err
	}

	d.Lease.Ts = time.Now().Format(time.RFC3339Nano)
	_, err = d.replace(context.Background(), lh.cli)
	return err
}
