package queue

import "github.com/riverqueue/river/rivertype"

type JobStatus struct {
	JobID  int64
	Status rivertype.JobState
}
