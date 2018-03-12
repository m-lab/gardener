package dispatch_test

import (
	"log"
	"testing"
	"time"

	"google.golang.org/api/option"

	"github.com/m-lab/etl-gardener/cloud/tq"
	"github.com/m-lab/etl-gardener/dispatch"
)

func init() {
	// Always prepend the filename and line number.
	log.SetFlags(log.LstdFlags | log.Lshortfile)
}

func TestDispatcherLifeCycle(t *testing.T) {
	// Use a fake client so we intercept all the http ops.
	client, counter := tq.DryRunQueuerClient()

	// With time.Now(), this shouldn't send any requests.
	// Inject fake client for bucket ops, so it doesn't hit the backends at all.
	d, err := dispatch.NewDispatcher(client, "project", "queue-base-", 4, time.Now(), option.WithHTTPClient(client))
	if err != nil {
		t.Fatal(err)
	}

	d.Add("gs://foobar/ndt/2001/01/01/")

	// This waits for all work to complete, then closes all channels.
	d.Terminate()

	// Test prefix should have triggered a single task queue check, so count should be 2.
	if counter.Count() != 6 {
		t.Errorf("Count was %d instead of 6", counter.Count())
	}

	err = d.Add("gs://foobar/ndt/2001/01/01/")
	if err != dispatch.ErrTerminating {
		t.Error("Should get", dispatch.ErrTerminating)
	}
}