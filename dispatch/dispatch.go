// Package dispatch identifies dates to reprocess, and feeds them into
// the reprocessing network.
package dispatch

import (
	"context"
	"errors"
	"log"
	"math/rand"
	"os"
	"time"

	"cloud.google.com/go/datastore"

	"github.com/m-lab/etl-gardener/cloud/ds"
)

// ###############################################################################
//  Ownership related code
// ###############################################################################

const (
	// DSNamespace is the namespace for all gardener related DataStore entities.
	DSNamespace = "Gardener"

	// DSOwnerLeaseName is the name of the single OwnerLease object.
	DSOwnerLeaseName = "OwnerLease"
)

var (
	// TestMode controls datastore retry delays to allow faster testing.
	TestMode = false
)

// Errors for leases
var (
	ErrLostLease          = errors.New("lost ownership lease")
	ErrNotOwner           = errors.New("owner does not match instance")
	ErrOwnershipRequested = errors.New("another instance has requested ownership")
	ErrInvalidState       = errors.New("invalid owner lease state")
	ErrNoSuchLease        = errors.New("lease does not exist")
	ErrNotAvailable       = errors.New("lease not available")
)

// OwnerLease is a DataStore record that controls ownership of the reprocessing task.
// An instance must own this before doing any reprocessing / dedupping operations.
// It should be periodically renewed to avoid another instance taking ownership
// without a handshake.
// TODO - should this have a mutex or other synchronization support?
type OwnerLease struct {
	Hostname        string    // Hostname of the owner.
	InstanceID      string    // instance ID of the owner.
	LeaseExpiration time.Time // Time that the lease will expire.
	NewInstanceID   string    // ID of instance trying to assume ownership.
}

// NewOwnerLease returns a properly initialized OwnerLease object.
func NewOwnerLease() OwnerLease {
	hostname := os.Getenv("HOSTNAME")
	instance := os.Getenv("GAE_INSTANCE")

	return OwnerLease{hostname, instance, time.Now().Add(5 * time.Minute), ""}
}

// Validate checks that fields have been initialized.
func (ol *OwnerLease) Validate() error {
	if ol.NewInstanceID != "" {
		return ErrOwnershipRequested
	}
	if ol.Hostname == "" || ol.InstanceID == "" {
		log.Printf("%+v\n", ol)
		return ErrInvalidState
	}
	return nil
}

// Renew renews the ownership lease for interval.
// The receiver must have Hostname and InstanceID already set.
// TODO - should this run on a timer, or in a go routine?
func (ol *OwnerLease) Renew(saver *ds.Saver, interval time.Duration) error {
	err := ol.Validate()
	if err != nil {
		log.Println(err)
		return err
	}
	ctx, cf := context.WithTimeout(context.Background(), 10*time.Second)
	defer cf()
	_, err = saver.Client.RunInTransaction(ctx, func(tx *datastore.Transaction) error {
		k := saver.NameKey(DSOwnerLeaseName)
		var lease OwnerLease
		err := tx.Get(k, &lease)
		if err != nil {
			return err
		}
		if lease.Hostname != ol.Hostname || lease.InstanceID != ol.InstanceID {
			log.Println(ol.InstanceID, "lost lease to", lease.InstanceID)
			return ErrLostLease
		}

		if lease.NewInstanceID != "" {
			log.Println(lease.InstanceID, "relinquishing ownership to", lease.NewInstanceID)
			if lease.LeaseExpiration.After(time.Now()) {
				lease.LeaseExpiration = time.Now()
				_, err = tx.Put(k, &lease)
				if err != nil {
					return err
				}
			}
			return ErrOwnershipRequested
		}

		lease.LeaseExpiration = time.Now().Add(interval)
		_, err = tx.Put(k, &lease)
		return err
	})
	return err
}

// TakeOwnershipIfAvailable assumes ownership if no-one else owns it.
func (ol *OwnerLease) TakeOwnershipIfAvailable(saver *ds.Saver, interval time.Duration) error {
	ctx, cf := context.WithTimeout(context.Background(), 10*time.Second)
	defer cf()
	_, err := saver.Client.RunInTransaction(ctx, func(tx *datastore.Transaction) error {
		k := saver.NameKey(DSOwnerLeaseName)
		var lease OwnerLease
		err := tx.Get(k, &lease)
		// If lease is expired, or doesn't exist, go ahead and try to take it.
		if err == datastore.ErrNoSuchEntity || lease.LeaseExpiration.Before(time.Now()) {
			ol.LeaseExpiration = time.Now().Add(interval)
			ol.NewInstanceID = ""
			tx.Put(k, ol)
			return nil
		}
		return ErrNotAvailable
	})
	return err
}

// RequestLease sets the NewInstanceID field to indicate that we want ownership.
func (ol *OwnerLease) RequestLease(saver *ds.Saver) error {
	ctx, cf := context.WithTimeout(context.Background(), 10*time.Second)
	defer cf()
	_, err := saver.Client.RunInTransaction(ctx, func(tx *datastore.Transaction) error {
		k := saver.NameKey(DSOwnerLeaseName)
		var lease OwnerLease
		err := tx.Get(k, &lease)
		if err != nil {
			return err
		}
		if lease.NewInstanceID == "" {
			lease.NewInstanceID = ol.InstanceID
			log.Println(ol.InstanceID, "requesting lease from", lease.InstanceID)
			_, err = tx.Put(k, &lease)
		}
		return err
	})
	return err
}

// WaitForOwnership retries TakeOwnershipIfAvailable until timeout or success.
func (ol *OwnerLease) WaitForOwnership(saver *ds.Saver, interval time.Duration) error {
	for timeout := time.Now().Add(2 * time.Minute); time.Now().Before(timeout); {
		log.Println("Trying again to get ownership", ol.InstanceID)
		err := ol.TakeOwnershipIfAvailable(saver, interval)
		if err != ErrNotAvailable {
			return err
		}
		if TestMode {
			time.Sleep(time.Duration(1) * time.Second)
		} else {
			time.Sleep(time.Duration(5+rand.Intn(10)) * time.Second)
		}
	}

	return ErrNotAvailable
}

// TakeOwnership attempts to take ownership of the lease.
// Expected to be attempted at startup, and process should fail health check
// if this fails repeatedly.
func (ol *OwnerLease) TakeOwnership(saver *ds.Saver, interval time.Duration) error {
	if ol.Validate() != nil {
		return ol.Validate()
	}
	err := ol.TakeOwnershipIfAvailable(saver, interval)
	if err == nil {
		return err
	}
	err = ol.RequestLease(saver)
	if err != nil {
		return err
	}
	err = ol.WaitForOwnership(saver, interval)
	return err
}

// DeleteLease deletes the lease iff held by ol.
func (ol *OwnerLease) DeleteLease(saver *ds.Saver) error {
	ctx, cf := context.WithTimeout(context.Background(), 10*time.Second)
	defer cf()
	_, err := saver.Client.RunInTransaction(ctx, func(tx *datastore.Transaction) error {
		k := saver.NameKey(DSOwnerLeaseName)
		var lease OwnerLease
		err := tx.Get(k, &lease)
		if err != nil {
			return err
		}
		if lease.Hostname == ol.Hostname && lease.InstanceID == ol.InstanceID {
			err = tx.Delete(k)
			return err
		}
		return ErrNotOwner
	})
	return err
}

// ###############################################################################
//  Dispatch interface and related code
// ###############################################################################
