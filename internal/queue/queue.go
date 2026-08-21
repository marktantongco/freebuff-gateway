package queue

import (
	"container/heap"
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"sync"
	"sync/atomic"
	"time"
)

// Priority levels (lower number = higher priority).
const (
	PriorityUrgent   = 0
	PriorityHigh     = 1
	PriorityNormal   = 2
	PriorityLow      = 3
	PriorityBulk     = 4
)

var (
	ErrQueueFull    = errors.New("queue: queue is full")
	ErrJobTimeout   = errors.New("queue: job timed out")
	ErrJobCancelled = errors.New("queue: job cancelled")
	ErrJobFailed    = errors.New("queue: job failed after retries")
)

// JobState represents the lifecycle state of a job.
type JobState int32

const (
	JobStatePending  JobState = iota
	JobStateRunning
	JobStateDone
	JobStateFailed
	JobStateCancelled
	JobStateRetrying
)

func (s JobState) String() string {
	switch s {
	case JobStatePending:
		return "pending"
	case JobStateRunning:
		return "running"
	case JobStateDone:
		return "done"
	case JobStateFailed:
		return "failed"
	case JobStateCancelled:
		return "cancelled"
	case JobStateRetrying:
		return "retrying"
	default:
		return "unknown"
	}
}

// JobFunc is the function executed by the worker for a job.
type JobFunc func(ctx context.Context) error

// Job represents a unit of work in the queue.
type Job struct {
	ID         string            `json:"id"`
	Name       string            `json:"name"`
	Priority   int               `json:"priority"`
	State      JobState          `json:"state"`
	Error      string            `json:"error,omitempty"`
	Metadata   map[string]string `json:"metadata,omitempty"`

	// Scheduling
	CreatedAt  time.Time     `json:"created_at"`
	StartedAt  *time.Time    `json:"started_at,omitempty"`
	FinishedAt *time.Time    `json:"finished_at,omitempty"`
	Deadline   time.Time     `json:"deadline,omitempty"`
	Timeout    time.Duration `json:"timeout,omitempty"`

	// Retry
	MaxRetries int           `json:"max_retries"`
	RetryCount int           `json:"retry_count"`
	RetryDelay time.Duration `json:"retry_delay,omitempty"`
	Backoff    BackoffConfig  `json:"backoff,omitempty"`

	// Internal
	fn       JobFunc
	index    int // heap index
	queuePos int // position in the priority queue
}

// IsTerminal returns true if the job is in a final state.
func (j *Job) IsTerminal() bool {
	return j.State == JobStateDone || j.State == JobStateFailed || j.State == JobStateCancelled
}

// Duration returns how long the job took (or has been running).
func (j *Job) Duration() time.Duration {
	if j.FinishedAt != nil {
		return j.FinishedAt.Sub(j.CreatedAt)
	}
	if j.StartedAt != nil {
		return time.Since(*j.StartedAt)
	}
	return time.Since(j.CreatedAt)
}

// BackoffConfig controls retry delay growth.
type BackoffConfig struct {
	Initial  time.Duration `json:"initial"`
	Max      time.Duration `json:"max"`
	Factor   float64       `json:"factor"`
	Jitter   float64       `json:"jitter"` // 0-1, fraction of delay to randomize
}

// DefaultBackoff returns a sensible default backoff.
func DefaultBackoff() BackoffConfig {
	return BackoffConfig{
		Initial: 500 * time.Millisecond,
		Max:     30 * time.Second,
		Factor:  2.0,
		Jitter:  0.1,
	}
}

// QueueConfig configures the job queue.
type QueueConfig struct {
	MaxSize     int           `json:"max_size"`
	MaxRetries  int           `json:"max_retries"`
	DefaultTimeout time.Duration `json:"default_timeout"`
	DefaultBackoff BackoffConfig `json:"default_backoff"`
}

// DefaultQueueConfig returns sensible defaults.
func DefaultQueueConfig() QueueConfig {
	return QueueConfig{
		MaxSize:        10000,
		MaxRetries:     3,
		DefaultTimeout:  5 * time.Minute,
		DefaultBackoff: DefaultBackoff(),
	}
}

// --- Priority Queue (min-heap by priority, then by creation time) ---

type jobHeap []*Job

func (h jobHeap) Len() int { return len(h) }

func (h jobHeap) Less(i, j int) bool {
	if h[i].Priority != h[j].Priority {
		return h[i].Priority < h[j].Priority
	}
	return h[i].CreatedAt.Before(h[j].CreatedAt)
}

func (h jobHeap) Swap(i, j int) {
	h[i], h[j] = h[j], h[i]
	h[i].index = i
	h[j].index = j
}

func (h *jobHeap) Push(x interface{}) {
	job := x.(*Job)
	job.index = len(*h)
	*h = append(*h, job)
}

func (h *jobHeap) Pop() interface{} {
	old := *h
	n := len(old)
	job := old[n-1]
	old[n-1] = nil
	job.index = -1
	*h = old[:n-1]
	return job
}

// --- Queue ---

// Queue is a priority job queue with retry and deadline support.
type Queue struct {
	mu       sync.Mutex
	cond     *sync.Cond
	heap     jobHeap
	jobs     map[string]*Job
	config   QueueConfig
	count    int64 // atomic: total jobs ever enqueued
	drained  int64 // atomic: total jobs completed
	failed   int64 // atomic: total jobs failed
	retrying int64 // atomic: total retries
	cancelled int64 // atomic: total cancelled
	closed   bool
}

// NewQueue creates a new job queue.
func NewQueue(config QueueConfig) *Queue {
	q := &Queue{
		jobs:   make(map[string]*Job),
		config: config,
	}
	q.cond = sync.NewCond(&q.mu)
	heap.Init(&q.heap)
	return q
}

// Enqueue adds a job to the queue. Returns the job ID.
func (q *Queue) Enqueue(ctx context.Context, name string, priority int, fn JobFunc, opts ...JobOption) (string, error) {
	q.mu.Lock()
	defer q.mu.Unlock()

	if q.closed {
		return "", errors.New("queue: queue is closed")
	}

	if q.heap.Len() >= q.config.MaxSize {
		return "", ErrQueueFull
	}

	job := &Job{
		ID:         generateID(),
		Name:       name,
		Priority:   priority,
		State:      JobStatePending,
		CreatedAt:  time.Now(),
		MaxRetries: q.config.MaxRetries,
		RetryDelay: q.config.DefaultBackoff.Initial,
		Backoff:    q.config.DefaultBackoff,
		Timeout:    q.config.DefaultTimeout,
		Metadata:   make(map[string]string),
		fn:         fn,
	}

	for _, opt := range opts {
		opt(job)
	}

	q.jobs[job.ID] = job
	heap.Push(&q.heap, job)
	atomic.AddInt64(&q.count, 1)

	// Signal waiting workers
	q.cond.Signal()

	return job.ID, nil
}

// Dequeue removes and returns the highest-priority job.
// Blocks if the queue is empty. Returns nil if the queue is closed.
func (q *Queue) Dequeue() *Job {
	q.mu.Lock()
	defer q.mu.Unlock()

	for q.heap.Len() == 0 && !q.closed {
		q.cond.Wait()
	}

	if q.heap.Len() == 0 {
		return nil
	}

	job := heap.Pop(&q.heap).(*Job)
	job.State = JobStateRunning
	now := time.Now()
	job.StartedAt = &now
	return job
}

// TryDequeue attempts to dequeue without blocking.
func (q *Queue) TryDequeue() *Job {
	q.mu.Lock()
	defer q.mu.Unlock()

	if q.heap.Len() == 0 || q.closed {
		return nil
	}

	job := heap.Pop(&q.heap).(*Job)
	job.State = JobStateRunning
	now := time.Now()
	job.StartedAt = &now
	return job
}

// Complete marks a job as done.
func (q *Queue) Complete(jobID string) {
	q.mu.Lock()
	defer q.mu.Unlock()

	job, ok := q.jobs[jobID]
	if !ok {
		return
	}

	job.State = JobStateDone
	now := time.Now()
	job.FinishedAt = &now
	atomic.AddInt64(&q.drained, 1)
}

// Fail marks a job as failed and optionally re-enqueues for retry.
func (q *Queue) Fail(jobID string, err error) bool {
	q.mu.Lock()
	defer q.mu.Unlock()

	job, ok := q.jobs[jobID]
	if !ok {
		return false
	}

	job.Error = err.Error()

	if job.RetryCount < job.MaxRetries {
		job.RetryCount++
		job.State = JobStateRetrying
		job.StartedAt = nil
		job.FinishedAt = nil
		atomic.AddInt64(&q.retrying, 1)

		// Re-enqueue with updated priority (same or lower)
		heap.Push(&q.heap, job)
		q.cond.Signal()
		return true
	}

	job.State = JobStateFailed
	now := time.Now()
	job.FinishedAt = &now
	atomic.AddInt64(&q.failed, 1)
	return false
}

// Cancel removes a pending job or marks a running job as cancelled.
func (q *Queue) Cancel(jobID string) bool {
	q.mu.Lock()
	defer q.mu.Unlock()

	job, ok := q.jobs[jobID]
	if !ok {
		return false
	}

	if job.IsTerminal() {
		return false
	}

	if job.State == JobStatePending {
		// Remove from heap
		heap.Remove(&q.heap, job.index)
	}

	job.State = JobStateCancelled
	now := time.Now()
	job.FinishedAt = &now
	atomic.AddInt64(&q.cancelled, 1)
	return true
}

// Get returns a job by ID.
func (q *Queue) Get(jobID string) *Job {
	q.mu.Lock()
	defer q.mu.Unlock()
	return q.jobs[jobID]
}

// Stats returns queue statistics.
func (q *Queue) Stats() QueueStats {
	q.mu.Lock()
	defer q.mu.Unlock()

	return QueueStats{
		Pending:    q.heap.Len(),
		Total:      atomic.LoadInt64(&q.count),
		Completed:  atomic.LoadInt64(&q.drained),
		Failed:     atomic.LoadInt64(&q.failed),
		Retries:    atomic.LoadInt64(&q.retrying),
		Cancelled:  atomic.LoadInt64(&q.cancelled),
		StoredJobs: len(q.jobs),
	}
}

// Close signals the queue to stop accepting and wake all workers.
func (q *Queue) Close() {
	q.mu.Lock()
	q.closed = true
	q.mu.Unlock()
	q.cond.Broadcast()
}

// Len returns the number of pending jobs.
func (q *Queue) Len() int {
	q.mu.Lock()
	defer q.mu.Unlock()
	return q.heap.Len()
}

// QueueStats contains queue metrics.
type QueueStats struct {
	Pending    int   `json:"pending"`
	Total      int64 `json:"total"`
	Completed  int64 `json:"completed"`
	Failed     int64 `json:"failed"`
	Retries    int64 `json:"retries"`
	Cancelled  int64 `json:"cancelled"`
	StoredJobs int   `json:"stored_jobs"`
}

// JobOption is a functional option for configuring jobs.
type JobOption func(*Job)

// WithPriority sets the job priority.
func WithPriority(p int) JobOption {
	return func(j *Job) { j.Priority = p }
}

// WithTimeout sets the job timeout.
func WithTimeout(d time.Duration) JobOption {
	return func(j *Job) { j.Timeout = d }
}

// WithDeadline sets the job deadline.
func WithDeadline(t time.Time) JobOption {
	return func(j *Job) { j.Deadline = t }
}

// WithMaxRetries sets the max retry count.
func WithMaxRetries(n int) JobOption {
	return func(j *Job) { j.MaxRetries = n }
}

// WithRetryDelay sets the base retry delay.
func WithRetryDelay(d time.Duration) JobOption {
	return func(j *Job) { j.RetryDelay = d }
}

// WithBackoff sets the backoff configuration.
func WithBackoff(b BackoffConfig) JobOption {
	return func(j *Job) { j.Backoff = b }
}

// WithMetadata adds metadata to the job.
func WithMetadata(k, v string) JobOption {
	return func(j *Job) { j.Metadata[k] = v }
}

func generateID() string {
	b := make([]byte, 8)
	rand.Read(b)
	return hex.EncodeToString(b)
}

func init() {
	// Ensure heap interface is satisfied
	var _ heap.Interface = &jobHeap{}
}
