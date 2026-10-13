package queue

var constructQueue = NewQueue

// NewPool constructs a Ring-backed Queue, starts it and panics on configuration
// error. Nonpositive size selects runtime.NumCPU. Options are applied after the
// initial worker/count choices and can override them, including WithWorker.
// Release the returned queue when finished.
func NewPool(size int64, opts ...Option) *Queue {
	o := []Option{
		WithWorkerCount(size),
		WithWorker(NewRing(opts...)),
	}
	o = append(
		o,
		opts...,
	)

	q, err := constructQueue(o...)
	if err != nil {
		panic(err)
	}

	q.Start()

	return q
}
