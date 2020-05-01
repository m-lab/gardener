package tracker_test

import (
	"testing"

	"github.com/m-lab/etl-gardener/tracker"
)

func TestStatusUpdate(t *testing.T) {
	s := tracker.NewStatus()
	s.Update(tracker.Parsing, "init done")
	if s.History[0].LastUpdate != "init done" {
		t.Error(s.History[0])
	}
}
