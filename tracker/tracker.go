// Package tracker tracks status of all jobs, and handles persistence.
//
// Concurrency properties:
//  1. The job map is protected by a Mutex, but lock is only required
//     to get a copy or set the Status value, so there is minimal
//     contention.
//  2. Status objects are persisted to a Saver by a separate
//     goroutine that periodically updates any modified Status objects.
//     The Status's updatetime is used to determine whether it needs
//     to be saved.
package tracker

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"os"
	"sync"
	"time"

	"cloud.google.com/go/datastore"
	"github.com/googleapis/google-cloud-go-testing/datastore/dsiface"

	"github.com/m-lab/etl-gardener/metrics"
	"github.com/m-lab/go/logx"
)

// Tracker keeps track of all the jobs in flight.
// Only tracker functions should access any of the fields.
type Tracker struct {
	// client dsiface.Client
	// dsKey  *datastore.Key
	saver  Saver
	ticker *time.Ticker

	// The lock should be held whenever accessing the jobs JobMap
	lock         sync.Mutex
	lastModified time.Time

	// These are the stored values.
	lastJob Job    // The last job that was added/initialized.
	jobs    JobMap // Map from Job to Status.

	// Time after which stale job should be ignored or replaced.
	expirationTime time.Duration
	// Delay before removing Complete jobs.
	cleanupDelay time.Duration
}

type Saver interface {
	Save(ctx context.Context, src interface{}) error
	Load(ctx context.Context) (JobMap, Job, error)
}

type DatastoreSaver struct {
	client dsiface.Client
	key    *datastore.Key
}

func NewDatastoreSaver(client dsiface.Client, key *datastore.Key) *DatastoreSaver {
	return &DatastoreSaver{
		client: client,
		key:    key,
	}
}

func (ds *DatastoreSaver) Save(ctx context.Context, state interface{}) error {
	_, err := ds.client.Put(ctx, ds.key, state)
	return err
}

func (ds *DatastoreSaver) Load(ctx context.Context) (JobMap, Job, error) {
	return loadJobMap(ctx, ds.client, ds.key)
}

type LocalSaver struct {
	key *datastore.Key
}

func NewLocalSaver(dsKey *datastore.Key) *LocalSaver {
	return &LocalSaver{
		key: dsKey,
	}
}

func (ls *LocalSaver) fname() string {
	return ls.key.Kind + "-" + ls.key.Name // NOTE: this is not generic for all key types.
}

func (ls *LocalSaver) Save(ctx context.Context, state interface{}) error {
	f, err := os.OpenFile(ls.fname(), os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return err
	}
	b, err := json.Marshal(state)
	if err != nil {
		return err
	}
	_, err = f.Write(b)
	return err
}

func (ls *LocalSaver) Load(ctx context.Context) (JobMap, Job, error) {
	b, err := ioutil.ReadFile(ls.fname())
	if err != nil {
		return nil, Job{}, err
	}

	state := saverStruct{Jobs: make([]byte, 0)}
	err = json.Unmarshal(b, &state)
	if err != nil {
		return nil, Job{}, err
	}
	return loadJobMapFromState(state)
}

// InitTracker recovers the Tracker state from a Client object.
// May return error if recovery fails.
func InitTracker(
	ctx context.Context,
	saver Saver,
	saveInterval time.Duration, expirationTime time.Duration, cleanupDelay time.Duration) (*Tracker, error) {

	jobMap, lastJob, err := saver.Load(ctx)
	if err != nil {
		log.Println(err)
		jobMap = make(JobMap, 100)
	}
	for j, s := range jobMap {
		// Update the metrics for all jobs still in flight or failed.
		if !s.isDone() {
			metrics.StartedCount.WithLabelValues(j.Experiment, j.Datatype).Inc()
			metrics.TasksInFlight.WithLabelValues(j.Experiment, j.Datatype, s.Label()).Inc()
		}
	}
	t := Tracker{
		saver: saver, lastModified: time.Now(),
		lastJob: lastJob, jobs: jobMap,
		expirationTime: expirationTime, cleanupDelay: cleanupDelay}
	if saver != nil && saveInterval > 0 {
		t.saveEvery(saveInterval)
	}
	return &t, nil
}

// NumJobs returns the number of jobs in flight.  This includes
// jobs in "Complete" state that have not been removed from saver.
func (tr *Tracker) NumJobs() int {
	tr.lock.Lock()
	defer tr.lock.Unlock()
	return len(tr.jobs)
}

// NumFailed returns the number of failed jobs.
func (tr *Tracker) NumFailed() int {
	jobs, _, _ := tr.GetState()
	counts := make(map[State]int, 20)

	for _, s := range jobs {
		counts[s.State()]++
	}
	return counts[Failed]
}

// Sync snapshots the full job state and saves it to the datastore client IFF it has changed.
// Returns time last saved, which may or may not be updated.
func (tr *Tracker) Sync(ctx context.Context, lastSave time.Time) (time.Time, error) {
	jobs, lastInit, lastMod := tr.GetState()
	if lastMod.Before(lastSave) {
		logx.Debug.Println("Skipping save", lastMod, lastSave)
		return lastSave, nil
	}

	jsonJobs, err := jobs.MarshalJSON()
	if err != nil {
		return lastSave, err
	}

	// Save the full state.
	lastTry := time.Now()
	state := saverStruct{time.Now(), lastInit, jsonJobs}
	ctx, cf := context.WithTimeout(ctx, 10*time.Second)
	defer cf()
	err = tr.saver.Save(ctx, &state)

	if err != nil {
		return lastSave, err
	}
	return lastTry, nil
}

func (tr *Tracker) saveEvery(interval time.Duration) {
	tr.ticker = time.NewTicker(interval)
	go func() {
		lastSave := time.Time{}
		// TODO - is there a better source for this context?
		ctx := context.Background()
		for range tr.ticker.C {
			var err error
			lastSave, err = tr.Sync(ctx, lastSave)
			if err != nil {
				log.Println(err)
			}
		}
	}()
}

// GetStatus retrieves the status of an existing job.
// Note that the returned object is a shallow copy, and the History
// field shares the slice objects with the JobMap.
func (tr *Tracker) GetStatus(job Job) (Status, error) {
	tr.lock.Lock()
	defer tr.lock.Unlock()
	status, ok := tr.jobs[job]
	if !ok {
		return Status{}, ErrJobNotFound
	}
	return status, nil
}

// AddJob adds a new job to the Tracker.
// May return ErrJobAlreadyExists if job already exists and is still in flight.
func (tr *Tracker) AddJob(job Job) error {
	status := NewStatus()

	tr.lock.Lock()
	defer tr.lock.Unlock()
	s, ok := tr.jobs[job]
	if ok {
		if s.isDone() {
			log.Println("Restarting completed job", job)
		} else if s.State() == Failed {
			// If job didn't complete, the InFlight metric needs to be updated.
			metrics.TasksInFlight.WithLabelValues(job.Experiment, job.Datatype, s.Label()).Dec()
			log.Println("Restarting failed job", job)
		} else {
			return ErrJobAlreadyExists
		}
	}

	tr.lastJob = job
	tr.lastModified = time.Now()
	metrics.StartedCount.WithLabelValues(job.Experiment, job.Datatype).Inc()
	tr.jobs[job] = status
	status.updateMetrics(job)
	return nil
}

// UpdateJob updates an existing job.
// May return ErrJobNotFound if job no longer exists.
func (tr *Tracker) UpdateJob(job Job, new Status) error {
	tr.lock.Lock()
	defer tr.lock.Unlock()
	old, ok := tr.jobs[job]
	if !ok {
		return ErrJobNotFound
	}

	if old.State() != new.State() {
		log.Println(job, old.LastStateInfo(), "->", new.State())
		new.updateMetrics(job)
	}

	tr.lastModified = time.Now()
	// When jobs are done, we update stats and may remove them from tracker.
	if new.isDone() {
		metrics.CompletedCount.WithLabelValues(job.Experiment, job.Datatype).Inc()

		// This could be done by GetStatus, but would change behaviors slightly.
		if tr.cleanupDelay == 0 {
			delete(tr.jobs, job)
			return nil
		}
	}
	tr.jobs[job] = new
	return nil
}

// SetDetail updates a job's detail message in memory.
func (tr *Tracker) SetDetail(job Job, detail string) error {
	// NOTE: This is not a deep copy.  Shares the History elements.
	status, err := tr.GetStatus(job)
	if err != nil {
		return err
	}
	status.SetDetail(detail)
	status.UpdateCount++
	return tr.UpdateJob(job, status)
}

// SetStatus updates a job's state in memory.
// It may or may not change the job state.  If it does change state,
// the detail string is applied to the last state, not the new state.
func (tr *Tracker) SetStatus(job Job, state State, detail string) error {
	// NOTE: This is not a deep copy.  Shares the History elements.
	status, err := tr.GetStatus(job)
	if err != nil {
		metrics.WarningCount.WithLabelValues(job.Experiment, job.Datatype, "NoSuchJob").Inc()
		return err
	}
	last := status.LastStateInfo()
	status.SetDetail(detail)

	if state != last.State {
		status.NewState(state)

		if state == ParseComplete {
			// TODO enable this once we have file or byte counts.
			// Alternatively, incorporate this into the next Action!
			// Update the metrics, even if there is an error, since the files were submitted to the queue already.
			// metrics.FilesPerDateHistogram.WithLabelValues(job.Datatype, strconv.Itoa(job.Date.Year())).Observe(float64(fileCount))
			// metrics.BytesPerDateHistogram.WithLabelValues(t.Experiment, strconv.Itoa(t.Date.Year())).Observe(float64(byteCount))
		}
	}
	status.UpdateCount++
	return tr.UpdateJob(job, status)
}

// Heartbeat updates a job's heartbeat time.
func (tr *Tracker) Heartbeat(job Job) error {
	status, err := tr.GetStatus(job)
	if err != nil {
		return err
	}
	status.HeartbeatTime = time.Now()
	return tr.UpdateJob(job, status)
}

// SetJobError updates a job's error fields, and handles persistence.
func (tr *Tracker) SetJobError(job Job, errString string) error {
	status, err := tr.GetStatus(job)
	if err != nil {
		return err
	}
	oldState := status.State()
	job.failureMetric(oldState, errString)
	status.NewState(Failed)
	// Set the final detail to include the prior state and error message.
	status.SetDetail(fmt.Sprintf("%s: %s", oldState, errString))

	return tr.UpdateJob(job, status)
}

// GetState returns the full job map, last initialized Job, and last mod time.
// It also cleans up any expired jobs from the tracker.
func (tr *Tracker) GetState() (JobMap, Job, time.Time) {
	tr.lock.Lock()
	defer tr.lock.Unlock()
	m := make(JobMap, len(tr.jobs))
	for j, s := range tr.jobs {
		// Remove any obsolete jobs.
		updateTime := s.DetailTime()
		if (tr.expirationTime > 0 && time.Since(updateTime) > tr.expirationTime) ||
			(s.isDone() && time.Since(updateTime) > tr.cleanupDelay) {
			if !s.isDone() {
				// If job didn't complete, the InFlight metric needs to be updated.
				metrics.TasksInFlight.WithLabelValues(j.Experiment, j.Datatype, s.Label()).Dec()
				log.Println("Deleting stale job", j, time.Since(updateTime), tr.cleanupDelay)
			}
			tr.lastModified = time.Now()
			delete(tr.jobs, j)
		} else {
			m[j] = s
		}
	}
	return m, tr.lastJob, tr.lastModified
}

// WriteHTMLStatusTo writes out the status of all jobs to the html writer.
func (tr *Tracker) WriteHTMLStatusTo(ctx context.Context, w io.Writer) error {
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	// TODO - add the lastInit job.
	jobs, _, _ := tr.GetState()

	fmt.Fprint(w, "<div>Tracker State</div>\n")

	return jobs.WriteHTML(w)
}

// LastJob returns the last Job successfully added with AddJob
func (tr *Tracker) LastJob() Job {
	return tr.lastJob
}
