package schedule_test

import (
	"context"
	"log"
	"testing"
	"time"

	"github.com/m-lab/etl-gardener/schedule"
)

func init() {
	// Always prepend the filename and line number.
	log.SetFlags(log.LstdFlags | log.Lshortfile | log.Lmicroseconds)
}

func TestNewEvent(t *testing.T) {
	ctx := context.Background()

	start := time.Now()

	var event schedule.Event

	interval := 5 * time.Millisecond
	fuzz := 2 * time.Millisecond
	f1 := func(ctx context.Context) {
		next := event.NextTime()
		diff := next.Sub(start)
		// Ensure that the ticks are regular within fuzz.
		if diff%interval > fuzz {
			t.Fatal("Incorrect interval", diff)
		}
	}
	_, err := schedule.NewEvent(ctx, "bad", f1, start, interval, 2*interval)
	if err == nil {
		t.Error("Expected error")
	}
	event, err = schedule.NewEvent(ctx, "test1", f1, start, interval, fuzz)
	if err != nil {
		t.Fatal(err)
	}
	if !event.Active() {
		t.Error("Should be active")
	}
	time.Sleep(20 * interval)
	event.Cancel()
	// Should be inactive after Cancel.
	if event.Active() {
		t.Error("Should not be active after Cancel")
	}
	time.Sleep(5 * interval)

	// Should be no further f() calls after Cancel.
	if event.Active() {
		t.Error("Should not be active after Cancel")
	}

	if event.Status() != "" {
		t.Error("Expected empty status", event.Status())
	}
	if event.Name() != "test1" {
		t.Error("Expected test1:", event.Name())
	}

	// Test too long since start
	_, err = schedule.NewEvent(ctx, "bad", f1, start, interval, fuzz)
	if err == nil {
		t.Error("Expected error")
	}
}

func TestSingleEvent(t *testing.T) {
	ctx := context.Background()

	start := time.Now()

	var event schedule.Event

	fuzz := 2 * time.Millisecond
	f1 := func(ctx context.Context) {
		next := event.NextTime()
		diff := next.Sub(start)
		// Ensure that the ticks are regular within fuzz.
		if diff > fuzz {
			t.Fatal("Incorrect interval", diff)
		}
	}
	// Test single event.
	event, err := schedule.NewEvent(ctx, "bad", f1, time.Now(), 0, fuzz)
	if err != nil {
		t.Fatal(err)
	}
	time.Sleep(5 * fuzz)
	if event.Active() {
		t.Error("Should be inactive now")
	}
}

func TestNilFunc(t *testing.T) {
	ctx := context.Background()

	_, err := schedule.NewEvent(ctx, "bad", nil, time.Now(), 0, 0)
	if err == nil {
		t.Error("Expected error")
	}
}
