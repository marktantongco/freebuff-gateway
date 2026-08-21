package queue

import (
	"context"
	"fmt"
	"math"
	"sync"
	"sync/atomic"
	"time"
)

// WorkerPool manages a pool of workers that consume jobs from the queue.
type WorkerPool struct {
	queue      *Queue
	workers    int
	ctx        context.Context
	cancel     context.CancelFunc
	wg         sync.WaitGroup
	active     int64 // atomic: currently running jobs
	completed  int64 // atomic: total completed by workers
	errors     int64 // atomic: total errors by workers
	onError    func(job *Job, err error)
	onComplete func(job *Job)
	started    bool
	mu         sync.Mutex
}

// WorkerPoolConfig configures the worker pool.
type WorkerPoolConfig struct {
	Workers    int              `json:"workers"`
	Queue      *Queue           `json:"-"`
	OnComplete func(*Job)       `json:"-"`
	OnError    func(*Job, error) `json:"-"`
}

// DefaultWorkerPoolConfig returns sensible defaults.
func DefaultWorkerPoolConfig(q *Queue) WorkerPoolConfig {
	return WorkerPoolConfig{
		Workers: 10,
		Queue:   q,
	}
}

// NewWorkerPool creates a new worker pool.
func NewWorkerPool(config WorkerPoolConfig) *WorkerPool {
	if config.Workers <= 0 {
		config.Workers = 10
	}
	if config.Queue == nil {
		config.Queue = NewQueue(DefaultQueueConfig())
	}

	ctx, cancel := context.WithCancel(context.Background())

	return &WorkerPool{
		queue:      config.Queue,
		workers:    config.Workers,
		ctx:        ctx,
		cancel:     cancel,
		onComplete: config.OnComplete,
		onError:    config.OnError,
	}
}

// Start begins consuming jobs from the queue.
func (wp *WorkerPool) Start() {
	wp.mu.Lock()
	if wp.started {
		wp.mu.Unlock()
		return
	}
	wp.started = true
	wp.mu.Unlock()

	for i := 0; i < wp.workers; i++ {
		wp.wg.Add(1)
		go wp.worker(i)
	}
}

// Stop signals all workers to stop and waits for them to finish.
func (wp *WorkerPool) Stop() {
	wp.cancel()
	wp.queue.Close()
	wp.wg.Wait()
}

// Stats returns worker pool statistics.
func (wp *WorkerPool) Stats() WorkerStats {
	return WorkerStats{
		Workers:   int64(wp.workers),
		Active:    atomic.LoadInt64(&wp.active),
		Completed: atomic.LoadInt64(&wp.completed),
		Errors:    atomic.LoadInt64(&wp.errors),
	}
}

func (wp *WorkerPool) worker(id int) {
	defer wp.wg.Done()

	for {
		select {
		case <-wp.ctx.Done():
			return
		default:
		}

		job := wp.queue.TryDequeue()
		if job == nil {
			// Brief sleep to avoid busy-wait
			select {
			case <-wp.ctx.Done():
				return
			case <-time.After(50 * time.Millisecond):
			}
			continue
		}

		wp.executeJob(id, job)
	}
}

func (wp *WorkerPool) executeJob(id int, job *Job) {
	atomic.AddInt64(&wp.active, 1)
	defer atomic.AddInt64(&wp.active, -1)

	// Create job-scoped context with timeout
	jobCtx := wp.ctx
	var cancel context.CancelFunc

	if job.Timeout > 0 {
		jobCtx, cancel = context.WithTimeout(jobCtx, job.Timeout)
		defer cancel()
	} else {
		jobCtx, cancel = context.WithCancel(jobCtx)
		defer cancel()
	}

	// Check deadline
	if !job.Deadline.IsZero() {
		dCtx, dCancel := context.WithDeadline(jobCtx, job.Deadline)
		jobCtx = dCtx
		defer dCancel()
	}

	// Execute
	err := job.fn(jobCtx)

	if err != nil {
		retrying := wp.queue.Fail(job.ID, err)
		if retrying {
			// Job was re-enqueued, log retry
			fmt.Printf("[worker-%d] job %s retrying (attempt %d/%d): %v\n",
				id, job.Name, job.RetryCount, job.MaxRetries, err)
			return
		}
		atomic.AddInt64(&wp.errors, 1)
		if wp.onError != nil {
			wp.onError(job, err)
		}
		return
	}

	wp.queue.Complete(job.ID)
	atomic.AddInt64(&wp.completed, 1)
	if wp.onComplete != nil {
		wp.onComplete(job)
	}
}

// WorkerStats contains worker pool metrics.
type WorkerStats struct {
	Workers   int64 `json:"workers"`
	Active    int64 `json:"active"`
	Completed int64 `json:"completed"`
	Errors    int64 `json:"errors"`
}

// --- Retry Delay Calculator ---

// CalculateRetryDelay computes the delay before the next retry.
func CalculateRetryDelay(job *Job) time.Duration {
	if job.RetryCount == 0 {
		return 0
	}

	delay := float64(job.Backoff.Initial) * math.Pow(job.Backoff.Factor, float64(job.RetryCount-1))

	if job.Backoff.Jitter > 0 {
		jitter := delay * job.Backoff.Jitter
		delay = delay - jitter/2 + jitter*float64(time.Now().UnixNano()%1000)/1000.0
	}

	maxDelay := float64(job.Backoff.Max)
	if maxDelay > 0 && delay > maxDelay {
		delay = maxDelay
	}

	return time.Duration(delay)
}
