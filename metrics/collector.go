package metrics

import "time"

// Iteration captures measurements for a single benchmark operation.
type Iteration struct {
	Scenario      string
	Transport     string        // "rest" or "grpc"
	ObjectSize    int64         // bytes; 0 for head/list
	Concurrency   int
	IterationNum  int
	TTFB          time.Duration // time to first byte; 0 if not applicable
	TotalLatency  time.Duration // end-to-end wall time
	BytesXferred  int64         // 0 for head/list
	Error         string        // empty on success
}
