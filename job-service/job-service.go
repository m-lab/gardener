// Package job provides an http handler to serve up jobs to ETL parsers.
package job

import (
	"context"
	"errors"
	"log"
	"net/http"
	"reflect"
	"sync"
	"time"

	"github.com/m-lab/etl-gardener/config"
	"github.com/m-lab/etl-gardener/persistence"
	"github.com/m-lab/etl-gardener/tracker"
)

// ErrMoreJSON is returned when response from gardener has unknown fields.
var ErrMoreJSON = errors.New("JSON body not completely consumed")

// ErrNilParameter is returned on disallowed nil parameter.
var ErrNilParameter = errors.New("nil parameter not allowed")

type jobAdder interface {
	AddJob(job tracker.Job) error
	LastJob() tracker.Job // temporary
}

// Service contains all information needed to provide a job service.
// It iterates through successive dates, processing that date from
// all TypeSources in the source bucket.
type Service struct {
	jobSpecs []tracker.JobWithTarget // The job prefixes to be iterated through.

	saver persistence.Saver // This injected to implement persistence.

	startDate time.Time // The date to restart at.

	// Optional jobAdder to add jobs to.
	jobAdder jobAdder

	// All fields above are const after initialization.
	// All fields below are protected by *lock*
	lock sync.Mutex

	// The Date is exported for persistence.  It is the only field
	// that is recovered after restart.  All others are injected
	// from config.
	Date time.Time // The date currently being dispatched.

	nextIndex int // index of TypeSource to dispatch next.
}

func (svc *Service) advanceDate() {
	date := svc.Date.UTC().AddDate(0, 0, 1).Truncate(24 * time.Hour)
	// Start over when we reach yesterday.
	if time.Since(date) < 36*time.Hour {
		date = svc.startDate
	}
	svc.Date = date
	svc.nextIndex = 0
}

// NextJob returns a tracker.Job to dispatch.
func (svc *Service) NextJob(ctx context.Context) tracker.JobWithTarget {
	svc.lock.Lock()
	defer svc.lock.Unlock()
	job := svc.jobSpecs[svc.nextIndex]
	job.Date = svc.Date

	svc.nextIndex++
	if svc.nextIndex >= len(svc.jobSpecs) {
		svc.advanceDate()
		if svc.saver != nil {
			// Note that this will block other calls to NextJob
			ctx, cf := context.WithTimeout(ctx, 5*time.Second)
			defer cf()
			err := svc.saver.Save(ctx, svc)
			if err != nil {
				log.Println(err)
			}
		}
	}
	return job
}

// JobHandler handle requests for new jobs.
// TODO - should update tracker instance.
func (svc *Service) JobHandler(resp http.ResponseWriter, req *http.Request) {
	// Must be a post because it changes state.
	if req.Method != http.MethodPost {
		resp.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	job := svc.NextJob(req.Context())
	err := svc.jobAdder.AddJob(job.Job)
	if err != nil {
		log.Println(err, job)
		resp.WriteHeader(http.StatusInternalServerError)
		_, err = resp.Write([]byte("Job already exists.  Try again."))
		if err != nil {
			log.Println(err)
		}
		return
	}

	log.Println("Dispatching", job.Job)
	_, err = resp.Write(job.Marshal())
	if err != nil {
		log.Println(err)
		// This should precede the Write(), but the Write failed, so this
		// is likely ok.
		resp.WriteHeader(http.StatusInternalServerError)
		return
	}
}

// Recover the processing date.
// Not thread-safe - should be called before activating service.
func (svc *Service) recoverDate(ctx context.Context) {
	// Default to tracker.LastJob date.
	// Deprecated
	svc.Date = svc.jobAdder.LastJob().Date

	// If saver provided, try to recover date from saver.
	if svc.saver != nil {
		ctx, cf := context.WithTimeout(ctx, 5*time.Second)
		defer cf()
		err := svc.saver.Fetch(ctx, svc)
		if err != nil {
			log.Println(err)
		}
	}

	// Adjust if Date is too early.
	if svc.Date.Before(svc.startDate) {
		svc.Date = svc.startDate
	}
}

// ErrInvalidStartDate is returned if startDate is time.Time{}
var ErrInvalidStartDate = errors.New("Invalid start date")

// NewJobService creates the default job service.
func NewJobService(tk jobAdder, startDate time.Time,
	targetBase string, sources []config.SourceConfig,
	saver persistence.Saver,
) (*Service, error) {
	if startDate.Equal(time.Time{}) {
		return nil, ErrInvalidStartDate
	}
	if tk == nil {
		return nil, ErrNilParameter
	}
	// The service cycles through the jobSpecs.  Each spec is a job (bucket/exp/type) and a target GCS bucket or BQ table.
	specs := make([]tracker.JobWithTarget, 0)
	for _, s := range sources {
		log.Println(s)
		job := tracker.Job{
			Bucket:     s.Bucket,
			Experiment: s.Experiment,
			Datatype:   s.Datatype,
			Filter:     s.Filter,
			Date:       time.Time{}, // This is not used.
		}
		// TODO - handle gs:// targets
		jt, err := job.Target(targetBase + "." + s.Target)
		if err != nil {
			log.Println(err, targetBase+s.Target)
			continue
		}
		specs = append(specs, jt)
	}
	if len(specs) < 1 {
		log.Fatal("No jobs specified")
	}

	svc := Service{jobAdder: tk,
		saver:     saver,
		startDate: startDate,
		nextIndex: 0,
		jobSpecs:  specs,
	}

	svc.recoverDate(context.Background())

	return &svc, nil
}

// ---------- Implement persistence.StateObject -------------

// GetName implements StateObject.GetName
func (svc *Service) GetName() string {
	return "singleton" // There is only one job service.
}

// GetKind implements StateObject.GetKind
func (svc *Service) GetKind() string {
	return reflect.TypeOf(svc).Name()
}
