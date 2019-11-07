# Gardener Task Tracker

The tracker keeps track of the state of all parsing activities, persists
the data in datastore, and recovers tracker state from datastore on
startup or recovery.

The tracker is used by other components of Gardener to decide:

1. what jobs to do next,
2. when a job has failed and needs to be recovered,
3. when postprocessing actions should be initiated.

The tracker provides an API to the other Gardener components to answer
questions about the system state.

1.
1.
1.
