// Package schedule runs arbitrary functions at scheduled times and intervals.
// It's main purpose is to allow construction of a status page for
// recent and pending events.
package schedule

import (
	"context"
	"errors"
	"math/rand"
	"sync"
	"time"
)

// Event describes the interface for an event.
type Event interface {
	// Name returns the name of the event.
	Name() string
	// NextTime returns the earliest time that the event will trigger again.
	// It returns the zero time if the event is inactive.
	NextTime() time.Time
	// Active returns true iff the event will fire again.
	Active() bool
	// Cancel cancels future occurances of the event.
	Cancel()
	// Status returns a string summarizing the status of the event.
	Status() string // Possibly HTML
}

// RegularEvent describes an event that occurs regularly,
// at or near a particular time (or times).
type RegularEvent struct {
	ctx    context.Context // Context for running f
	cancel func()

	name     string        // constant
	interval time.Duration // typically Hour (or multiple), Day (or multiple)

	// If non-zero, then add this much fuzz to the execution time.
	// This does NOT lead to poisson distribution.
	fuzz time.Duration

	// nextTick and timer are protected by stateLock
	stateLock sync.Mutex
	nextTick  time.Time   // Beginning of next time interval, e.g. 02:30 UTC
	timer     *time.Timer // Timer that triggers the next execution cycle.

	// runLock prevents concurrent execution of f()
	runLock sync.Mutex
	f       func(context.Context)
}

// ErrBadParemeter is returned if parameters are invalid.
var ErrBadParemeter = errors.New("bad parameter")

// NewEvent creates a new RegularEvent
func NewEvent(ctx context.Context, name string, f func(context.Context), first time.Time, interval, fuzz time.Duration) (Event, error) {
	if f == nil {
		return nil, ErrBadParemeter
	}
	if interval != 0 && (time.Since(first) > interval/2+fuzz || fuzz > interval) {
		return nil, ErrBadParemeter
	}
	ctx, cancel := context.WithCancel(ctx)
	e := RegularEvent{ctx: ctx, cancel: cancel, name: name, f: f, nextTick: first, interval: interval, fuzz: fuzz}

	// Set up the first event.
	next := e.nextTick
	if e.fuzz > 0 {
		next = next.Add(time.Duration(rand.Intn(int(e.fuzz))))
	}
	e.timer = time.AfterFunc(next.Sub(time.Now()), e.run)

	return &e, nil
}

// Name implements Event.Name
func (e *RegularEvent) Name() string {
	return e.name
}

// Active implements Event.Active
func (e *RegularEvent) Active() bool {
	e.stateLock.Lock()
	defer e.stateLock.Unlock()
	return e.timer != nil
}

// NextTime implements Event.NextTime
func (e *RegularEvent) NextTime() time.Time {
	e.stateLock.Lock()
	defer e.stateLock.Unlock()
	return e.nextTick
}

// Cancel implements Event.Cancel
func (e *RegularEvent) Cancel() {
	e.stateLock.Lock()
	defer e.stateLock.Unlock()
	e.timer.Stop()
	e.cancel()
	e.timer = nil
}

// Status implements Event.Status
func (e *RegularEvent) Status() string {
	e.stateLock.Lock()
	defer e.stateLock.Unlock()
	return ""
}

func (e *RegularEvent) run() {
	// This prevents creating a new timer when there is a race with Cancel.
	if e.ctx.Err() != nil {
		return
	}
	// Latency from scheduled time is 100-200 usec on workstation, but 1-4 msec on macbook,
	e.stateLock.Lock()
	// Set up the next run.
	if e.interval > 0 && e.timer != nil {
		e.nextTick = e.nextTick.Add(e.interval)
		next := e.nextTick
		if e.fuzz > 0 {
			next = next.Add(time.Duration(rand.Intn(int(e.fuzz))))
		}
		e.timer = time.AfterFunc(next.Sub(time.Now()), e.run)
		//log.Println("next", next)
	} else {
		e.timer = nil
	}
	e.stateLock.Unlock()

	// Prevents concurrent execution of f.
	e.runLock.Lock()
	// Additional 150 usec latency (on workstation) from top of run()
	e.f(e.ctx)
	e.runLock.Unlock()
}
