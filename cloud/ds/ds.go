// Package ds defines utilities for working with datastore.
package ds

import (
	"context"
	"errors"
	"log"
	"os"
	"time"

	"cloud.google.com/go/datastore"
	"google.golang.org/api/option"
)

// ###############################################################################
// All DataStore related code and variables
// ###############################################################################

// Saver is poorly named.  It is used for various DataStore load/save operations
// Pass by value
type Saver struct {
	Namespace string
	Kind      string
	Client    *datastore.Client
}

// NewSaver creates a new saver using the provided namespace and kind.
func NewSaver(namespace, kind string, opts ...option.ClientOption) (Saver, error) {
	var err error
	var client *datastore.Client
	project, ok := os.LookupEnv("PROJECT")
	if !ok {
		return Saver{}, errors.New("PROJECT env var not set")
	}
	log.Println(project)
	client, err = datastore.NewClient(context.Background(), project, opts...)
	if err != nil {
		return Saver{}, err
	}
	return Saver{namespace, kind, client}, nil
}

// NameKey creates a full key using the Saver settings.
func (s Saver) NameKey(name string) *datastore.Key {
	k := datastore.NameKey(s.Kind, name, nil)
	k.Namespace = s.Namespace
	return k
}

// Load retrieves an arbitrary record from datastore.
func (s Saver) Load(name string, obj interface{}) error {
	k := s.NameKey(name)
	log.Printf("%+v\n", k)
	return s.Client.Get(context.Background(), k, obj)
}

// Save stores an arbitrary object to kind/key in the default namespace.
// If a record already exists, then it is overwritten.
// TODO(gfr) Make an upsert version of this:
// https://cloud.google.com/datastore/docs/concepts/entities
func (s Saver) Save(key string, obj interface{}) error {
	k := s.NameKey(key)
	_, err := s.Client.Put(context.Background(), k, obj)
	return err
}

// OwnerLease is a DataStore record that controls ownership of the reprocessing task.
// An instance must own this before doing any reprocessing / dedupping operations.
// It should be periodically renewed to avoid another instance taking ownership.
// TODO - should this have a mutex or other synchronization support?
type OwnerLease struct {
	Hostname        string    // Hostname of the owner.
	InstanceID      string    // instance ID of the owner.
	LeaseExpiration time.Time // Time that the lease will expire.
}

// Renew renews the ownership lease for X minutes.
// TODO - should this run on a timer, or in a go routine?
func (ol *OwnerLease) Renew() {

	// TODO -
}

// TakeOwnership attempts to take ownership of the lease.
// Expected to be attempted at startup, and process should fail health check
// if this fails.
func TakeOwnership() (OwnerLease, error) {

	return OwnerLease{}, nil
}
