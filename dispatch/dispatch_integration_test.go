package dispatch_test

import (
	"log"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/m-lab/etl-gardener/cloud/ds"
	"github.com/m-lab/etl-gardener/dispatch"
	"google.golang.org/api/option"
)

func init() {
	// Always prepend the filename and line number.
	log.SetFlags(log.LstdFlags | log.Lshortfile)
	dispatch.TestMode = true
}

func MLabTestAuth() []option.ClientOption {
	opts := []option.ClientOption{}
	if os.Getenv("TRAVIS") != "" {
		authOpt := option.WithCredentialsFile("../../travis-testing.key")
		opts = append(opts, authOpt)
	}

	return opts
}

// TODO - use random saver namespace to avoid collisions between multiple testers.
// TODO - use datastore emulator
func TestOwnerLease_TakeOwnership(t *testing.T) {
	os.Setenv("PROJECT", "mlab-testing")
	saver1, err := ds.NewSaver("gardener", "test", MLabTestAuth()...)
	saver2, err := ds.NewSaver("gardener", "test", MLabTestAuth()...)
	if err != nil {
		t.Fatal(err)
	}
	// TODO - should use emulator
	ol1 := dispatch.OwnerLease{"host1", "instance1", time.Time{}, ""}
	if ol1.Validate() != nil {
		log.Println(ol1.Validate())
	}
	ol2 := dispatch.OwnerLease{"host2", "instance2", time.Time{}, ""}
	if ol2.Validate() != nil {
		log.Println(ol2.Validate())
	}

	err = ol1.TakeOwnership(&saver1, 10*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	log.Println("instance1 has ownership")

	var ol2err error
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		ol2err = ol2.TakeOwnership(&saver2, 10*time.Second)
		wg.Done()
	}()

	time.Sleep(2 * 2 * time.Second)
	// This should notice the request, and relinquish ownership.
	err = ol1.Renew(&saver1, 2*time.Second)
	if err != dispatch.ErrOwnershipRequested {
		log.Println(err)
	}

	wg.Wait()
	if ol2err != nil {
		t.Fatal(ol2err)
	}

	err = ol1.Renew(&saver1, 2*time.Second)
	if err != dispatch.ErrLostLease {
		t.Fatal("Should have lost lease:", err)
	}
	err = ol1.DeleteLease(&saver1)
	if err != dispatch.ErrNotOwner {
		t.Error(err)
	}
	err = ol2.DeleteLease(&saver2)
	if err != err {
		t.Error(err)
	}
}
