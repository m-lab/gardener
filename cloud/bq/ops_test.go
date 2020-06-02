package bq_test

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/m-lab/etl-gardener/cloud/bq"
	"github.com/m-lab/etl-gardener/tracker"
	"github.com/m-lab/go/rtx"
)

func TestTemplate(t *testing.T) {
	job := tracker.NewJob("bucket", "ndt", "tcpinfo", time.Date(2019, 3, 4, 0, 0, 0, 0, time.UTC))
	q, err := bq.NewQuerier(job, "fake-project")
	rtx.Must(err, "NewQuerier failed")
	qs := q.DedupQuery()
	if !strings.Contains(qs, "uuid") {
		t.Error("query should contain keep.uuid:\n", q)
	}
	if !strings.Contains(qs, `"2019-03-04"`) {
		t.Error(`query should contain "2019-03-04":\n`, q)
	}
	if !strings.Contains(qs, "ParseInfo.TaskFileName") {
		t.Error("query should contain ParseInfo.TaskFileName:\n", q)
	}
	// TODO check final WHERE clause.
	if !strings.Contains(qs, "target.ParseInfo.ParseTime = keep.ParseTime") {
		t.Error("query should contain target.ParseInfo.ParseTime = ... :\n", qs)
	}
}

// NOTE: This validates queries against actual tables in mlab-testing.  It only
// runs Dryrun queries, so it does not modify the tables.
func TestValidateQueries(t *testing.T) {
	if testing.Short() {
		t.Log("Skipping test for --short")
	}
	ctx := context.Background()
	dataTypes := []string{"tcpinfo", "annotation", "ndt7"}
	// Test for each datatype
	for _, dataType := range dataTypes {
		job := tracker.NewJob("bucket", "ndt", dataType, time.Date(2019, 3, 4, 0, 0, 0, 0, time.UTC))
		qp, err := bq.NewQuerier(job, "mlab-testing")
		if err != nil {
			t.Fatal(dataType, err)
		}
		t.Run(dataType+":dedup", func(t *testing.T) {
			t.Log(t.Name())
			j, err := qp.Dedup(ctx, true)
			if err != nil {
				t.Fatal(t.Name(), err, qp.DedupQuery())
			}
			status := j.LastStatus()
			if status.Err() != nil {
				t.Fatal(t.Name(), err, qp.DedupQuery())
			}
		})
	}
}
