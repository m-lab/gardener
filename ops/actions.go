package ops

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"os"
	"strings"
	"time"

	"cloud.google.com/go/bigquery"
	"github.com/googleapis/google-cloud-go-testing/bigquery/bqiface"
	"google.golang.org/api/googleapi"

	"github.com/m-lab/etl-gardener/cloud"
	"github.com/m-lab/etl-gardener/cloud/bq"
	"github.com/m-lab/etl-gardener/metrics"
	"github.com/m-lab/etl-gardener/tracker"
)

func newStateFunc(detail string) ActionFunc {
	return func(ctx context.Context, j tracker.Job) *Outcome {
		return Success(j, detail)
	}
}

// NewStandardMonitor creates the standard monitor that handles several state transitions.
func NewStandardMonitor(ctx context.Context, config cloud.BQConfig, tk *tracker.Tracker) (*Monitor, error) {
	m, err := NewMonitor(ctx, config, tk)
	if err != nil {
		return nil, err
	}
	m.AddAction(tracker.ParseComplete,
		nil,
		newStateFunc("-"),
		tracker.Deduplicating,
		"Changing to Deduplicating")
	// Hack to handle old jobs from previous gardener implementations
	m.AddAction(tracker.Stabilizing,
		nil,
		newStateFunc("-"),
		tracker.Deduplicating,
		"Changing to Deduplicating")
	m.AddAction(tracker.Deduplicating,
		nil,
		dedupFunc,
		tracker.Copying,
		"Deduplicating")
	m.AddAction(tracker.Copying,
		nil,
		copyFunc,
		tracker.Cleaning,
		"Copying")
	m.AddAction(tracker.Cleaning,
		nil,
		cleanupFunc,
		tracker.Complete,
		"Cleaning")
	return m, nil
}

// Waits for bqjob to complete, handles backoff and job updates.
// If Outcome is Success, status will be non-nil, and non-error.
func waitAndCheck(ctx context.Context, bqJob bqiface.Job, j tracker.Job, label string) (*bigquery.JobStatus, *Outcome) {
	status, err := bqJob.Wait(ctx)
	if err != nil {
		switch typedErr := err.(type) {
		case *googleapi.Error:
			if typedErr.Code == http.StatusBadRequest &&
				strings.Contains(typedErr.Error(), "streaming buffer") {
				log.Println(typedErr)
				metrics.WarningCount.WithLabelValues(
					j.Experiment, j.Datatype,
					label+"WaitingForStreamingBuffer").Inc()

				// Leave in current state, Wait a while and try again.
				return nil, Retry(j, err, "waiting for empty streaming buffer")
			}
			log.Println(typedErr, typedErr.Code)
		default:
			// We don't know the problem...
		}
		// Not googleapi.Error, OR not streaming buffer problem.
		log.Println(label, err)
		metrics.WarningCount.WithLabelValues(
			j.Experiment, j.Datatype,
			label+"UnknownError").Inc()
		// This will terminate this job.
		return status, Failure(j, err, "unknown error")
	}
	if status.Err() != nil {
		err := status.Err()
		log.Println(label, err)
		metrics.WarningCount.WithLabelValues(
			j.Experiment, j.Datatype,
			label+"UnknownStatusError").Inc()

		// This will terminate this job.
		return status, Failure(j, err, "unknown error")
	}
	return status, Success(j, "-")
}

// TODO improve test coverage?
func dedupFunc(ctx context.Context, j tracker.Job) *Outcome {
	// This is the delay since entering the dedup state, due to monitor delay
	// and retries.
	// delay := time.Since(s.LastStateChangeTime()).Round(time.Minute)

	var bqJob bqiface.Job
	var msg string
	// TODO pass in the JobWithTarget, and get the base from the target.
	qp, err := bq.NewQuerier(j, os.Getenv("PROJECT"))
	if err != nil {
		log.Println(err)
		// This terminates this job.
		return Failure(j, err, "-")
	}
	bqJob, err = qp.Run(ctx, "dedup", false)
	if err != nil {
		log.Println(err)
		// Try again soon.
		return Retry(j, err, "-")
	}
	status, outcome := waitAndCheck(ctx, bqJob, j, "Dedup")
	if !outcome.IsDone() {
		return outcome
	}

	// Dedup job was successful.  Handle the statistics, metrics, tracker update.
	stats := status.Statistics
	switch details := stats.Details.(type) {
	case *bigquery.QueryStatistics:
		cleanupTime := stats.EndTime.Sub(stats.StartTime)
		metrics.QueryCostHistogram.WithLabelValues(j.Datatype, "dedup").Observe(float64(details.SlotMillis) / 1000.0)
		msg = fmt.Sprintf("Dedup took %s (after xxx waiting), %5.2f Slot Minutes, %d Rows affected, %d MB Processed, %d MB Billed",
			cleanupTime.Round(100*time.Millisecond),
			float64(details.SlotMillis)/60000, details.NumDMLAffectedRows,
			details.TotalBytesProcessed/1000000, details.TotalBytesBilled/1000000)
		log.Println(msg)
		log.Printf("Dedup %s: %+v\n", j, details)
	default:
		log.Printf("Could not convert to QueryStatistics: %+v\n", status.Statistics.Details)
		msg = "Could not convert Detail to QueryStatistics"
	}
	return Success(j, msg)
}

// TODO This is costly.  Consider using decorated table delete.
// TODO improve test coverage?
func cleanupFunc(ctx context.Context, j tracker.Job) *Outcome {
	var bqJob bqiface.Job
	// TODO pass in the JobWithTarget, and get the base from the target.
	qp, err := bq.NewQuerier(j, os.Getenv("PROJECT"))
	if err != nil {
		log.Println(err)
		// This terminates this job.
		return Failure(j, err, "-")
	}
	bqJob, err = qp.Run(ctx, "cleanup", false)
	if err != nil {
		log.Println(err)
		// Try again soon.
		return Retry(j, err, "-")
	}
	status, outcome := waitAndCheck(ctx, bqJob, j, "Cleanup")
	if !errors.Is(outcome, IsDone) {
		return outcome
	}

	var msg string
	// Dedup job was successful.  Handle the statistics, metrics, tracker update.
	stats := status.Statistics
	switch details := stats.Details.(type) {
	case *bigquery.QueryStatistics:
		cleanupTime := stats.EndTime.Sub(stats.StartTime)
		metrics.QueryCostHistogram.WithLabelValues(j.Datatype, "cleanup").Observe(float64(details.SlotMillis) / 1000.0)
		msg = fmt.Sprintf("Cleanup took %s (after xxx waiting), %5.2f Slot Minutes, %d Rows affected, %d MB Processed, %d MB Billed",
			cleanupTime.Round(100*time.Millisecond),
			float64(details.SlotMillis)/60000, details.NumDMLAffectedRows,
			details.TotalBytesProcessed/1000000, details.TotalBytesBilled/1000000)
		log.Println(msg)
		log.Printf("Cleanup %s: %+v\n", j, details)
	default:
		log.Printf("Could not convert to QueryStatistics: %+v\n", status.Statistics.Details)
		msg = "Could not convert Detail to QueryStatistics"
	}
	return Success(j, msg)
}

// TODO improve test coverage?
func copyFunc(ctx context.Context, j tracker.Job) *Outcome {
	// This is the delay since entering the dedup state, due to monitor delay
	// and retries.
	// delay := time.Since(s.LastStateChangeTime()).Round(time.Minute)

	var bqJob bqiface.Job
	// TODO pass in the JobWithTarget, and get the base from the target.
	qp, err := bq.NewQuerier(j, os.Getenv("PROJECT"))
	if err != nil {
		log.Println(err)
		// This terminates this job.
		return Failure(j, err, "-")
	}
	bqJob, err = qp.Copy(ctx, false)
	if err != nil {
		log.Println(err)
		// Try again soon.
		return Retry(j, err, "-")
	}
	status, outcome := waitAndCheck(ctx, bqJob, j, "Copy")
	if !outcome.IsDone() {
		return outcome
	}

	var msg string
	stats := status.Statistics
	if stats != nil {
		copyTime := stats.EndTime.Sub(stats.StartTime)
		msg = fmt.Sprintf("Copy took %s (after xxx waiting), %d MB Processed",
			copyTime.Round(100*time.Millisecond),
			stats.TotalBytesProcessed/1000000)
		log.Println(msg)
	}
	return Success(j, msg)
}
