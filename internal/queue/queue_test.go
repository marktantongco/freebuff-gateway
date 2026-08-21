package queue

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// --- Queue Tests ---

func TestQueueEnqueueDequeue(t *testing.T) {
	q := NewQueue(DefaultQueueConfig())
	defer q.Close()

	ctx := context.Background()
	id, err := q.Enqueue(ctx, "test-job", PriorityNormal, func(ctx context.Context) error {
		return nil
	})
	if err != nil {
		t.Fatalf("enqueue failed: %v", err)
	}
	if id == "" {
		t.Fatal("expected non-empty job ID")
	}

	job := q.TryDequeue()
	if job == nil {
		t.Fatal("expected non-nil job")
	}
	if job.ID != id {
		t.Fatalf("expected ID %s, got %s", id, job.ID)
	}
	if job.State != JobStateRunning {
		t.Fatalf("expected state Running, got %s", job.State)
	}
}

func TestQueuePriorityOrder(t *testing.T) {
	q := NewQueue(DefaultQueueConfig())
	defer q.Close()

	ctx := context.Background()
	// Enqueue in reverse priority order
	q.Enqueue(ctx, "low", PriorityLow, func(ctx context.Context) error { return nil })
	q.Enqueue(ctx, "urgent", PriorityUrgent, func(ctx context.Context) error { return nil })
	q.Enqueue(ctx, "normal", PriorityNormal, func(ctx context.Context) error { return nil })
	q.Enqueue(ctx, "high", PriorityHigh, func(ctx context.Context) error { return nil })

	expected := []string{"urgent", "high", "normal", "low"}
	for _, exp := range expected {
		job := q.TryDequeue()
		if job == nil {
			t.Fatalf("expected job for %s", exp)
		}
		if job.Name != exp {
			t.Fatalf("expected %s, got %s", exp, job.Name)
		}
	}
}

func TestQueueFIFOWithinPriority(t *testing.T) {
	q := NewQueue(DefaultQueueConfig())
	defer q.Close()

	ctx := context.Background()
	q.Enqueue(ctx, "first", PriorityNormal, func(ctx context.Context) error { return nil })
	q.Enqueue(ctx, "second", PriorityNormal, func(ctx context.Context) error { return nil })
	q.Enqueue(ctx, "third", PriorityNormal, func(ctx context.Context) error { return nil })

	expected := []string{"first", "second", "third"}
	for _, exp := range expected {
		job := q.TryDequeue()
		if job.Name != exp {
			t.Fatalf("expected %s, got %s", exp, job.Name)
		}
	}
}

func TestQueueCompleteAndFail(t *testing.T) {
	q := NewQueue(QueueConfig{MaxSize: 100, MaxRetries: 0})
	defer q.Close()

	ctx := context.Background()
	id, _ := q.Enqueue(ctx, "test", PriorityNormal, func(ctx context.Context) error { return nil })
	job := q.TryDequeue()

	q.Complete(id)
	if job.State != JobStateDone {
		t.Fatalf("expected Done, got %s", job.State)
	}
	if job.FinishedAt == nil {
		t.Fatal("expected FinishedAt to be set")
	}

	// Test failure
	id2, _ := q.Enqueue(ctx, "fail", PriorityNormal, func(ctx context.Context) error { return errors.New("boom") })
	job2 := q.TryDequeue()
	retrying := q.Fail(id2, errors.New("boom"))
	if retrying {
		t.Fatal("expected no retry (max retries = 0 by default)")
	}
	if job2.State != JobStateFailed {
		t.Fatalf("expected Failed, got %s", job2.State)
	}
}

func TestQueueRetry(t *testing.T) {
	q := NewQueue(QueueConfig{
		MaxSize:    100,
		MaxRetries: 2,
	})

	ctx := context.Background()
	id, _ := q.Enqueue(ctx, "retry-job", PriorityNormal, func(ctx context.Context) error {
		return errors.New("transient")
	})
	q.TryDequeue()

	// First failure → should retry
	retrying := q.Fail(id, errors.New("transient"))
	if !retrying {
		t.Fatal("expected retry")
	}

	job := q.Get(id)
	if job.RetryCount != 1 {
		t.Fatalf("expected retry count 1, got %d", job.RetryCount)
	}

	// Dequeue the retried job
	job2 := q.TryDequeue()
	if job2 == nil || job2.ID != id {
		t.Fatal("expected retried job")
	}

	// Second failure → should retry again
	retrying = q.Fail(id, errors.New("transient"))
	if !retrying {
		t.Fatal("expected second retry")
	}

	// Third failure → should fail permanently
	job3 := q.TryDequeue()
	retrying = q.Fail(id, errors.New("transient"))
	if retrying {
		t.Fatal("expected no more retries")
	}
	if job3.State != JobStateFailed {
		t.Fatalf("expected Failed, got %s", job3.State)
	}
}

func TestQueueCancel(t *testing.T) {
	q := NewQueue(DefaultQueueConfig())
	defer q.Close()

	ctx := context.Background()
	id, _ := q.Enqueue(ctx, "cancel-me", PriorityNormal, func(ctx context.Context) error { return nil })

	// Cancel before dequeue
	ok := q.Cancel(id)
	if !ok {
		t.Fatal("expected cancel to succeed")
	}

	job := q.Get(id)
	if job.State != JobStateCancelled {
		t.Fatalf("expected Cancelled, got %s", job.State)
	}

	// Should not appear in dequeue
	job2 := q.TryDequeue()
	if job2 != nil && job2.ID == id {
		t.Fatal("cancelled job should not be dequeued")
	}
}

func TestQueueFull(t *testing.T) {
	q := NewQueue(QueueConfig{MaxSize: 2})
	defer q.Close()

	ctx := context.Background()
	q.Enqueue(ctx, "a", PriorityNormal, func(ctx context.Context) error { return nil })
	q.Enqueue(ctx, "b", PriorityNormal, func(ctx context.Context) error { return nil })

	_, err := q.Enqueue(ctx, "c", PriorityNormal, func(ctx context.Context) error { return nil })
	if !errors.Is(err, ErrQueueFull) {
		t.Fatalf("expected ErrQueueFull, got %v", err)
	}
}

func TestQueueStats(t *testing.T) {
	q := NewQueue(QueueConfig{MaxSize: 100, MaxRetries: 1})
	defer q.Close()

	ctx := context.Background()
	id, _ := q.Enqueue(ctx, "a", PriorityNormal, func(ctx context.Context) error { return nil })
	q.TryDequeue()
	q.Complete(id)

	stats := q.Stats()
	if stats.Total != 1 {
		t.Fatalf("expected Total=1, got %d", stats.Total)
	}
	if stats.Completed != 1 {
		t.Fatalf("expected Completed=1, got %d", stats.Completed)
	}
}

func TestJobOptions(t *testing.T) {
	q := NewQueue(DefaultQueueConfig())
	defer q.Close()

	ctx := context.Background()
	deadline := time.Now().Add(time.Hour)
	id, _ := q.Enqueue(ctx, "opts", PriorityHigh,
		func(ctx context.Context) error { return nil },
		WithTimeout(10*time.Second),
		WithDeadline(deadline),
		WithMaxRetries(5),
		WithRetryDelay(time.Second),
		WithMetadata("key", "value"),
	)

	job := q.Get(id)
	if job.Priority != PriorityHigh {
		t.Fatalf("expected PriorityHigh, got %d", job.Priority)
	}
	if job.Timeout != 10*time.Second {
		t.Fatalf("expected 10s timeout, got %v", job.Timeout)
	}
	if job.MaxRetries != 5 {
		t.Fatalf("expected 5 max retries, got %d", job.MaxRetries)
	}
	if job.Metadata["key"] != "value" {
		t.Fatalf("expected metadata key=value")
	}
}

func TestJobIsTerminal(t *testing.T) {
	j := &Job{State: JobStateDone}
	if !j.IsTerminal() {
		t.Fatal("expected Done to be terminal")
	}
	j.State = JobStateRunning
	if j.IsTerminal() {
		t.Fatal("expected Running to not be terminal")
	}
}

func TestJobDuration(t *testing.T) {
	now := time.Now()
	completed := now.Add(5 * time.Second)
	j := &Job{
		CreatedAt:  now,
		FinishedAt: &completed,
	}
	d := j.Duration()
	if d != 5*time.Second {
		t.Fatalf("expected 5s duration, got %v", d)
	}
}

// --- Worker Pool Tests ---

func TestWorkerPoolProcessesJobs(t *testing.T) {
	q := NewQueue(DefaultQueueConfig())
	wp := NewWorkerPool(WorkerPoolConfig{
		Workers: 2,
		Queue:   q,
	})

	var count int64
	var wg sync.WaitGroup
	n := 10

	for i := 0; i < n; i++ {
		wg.Add(1)
		_ = i
		q.Enqueue(context.Background(), "job", PriorityNormal, func(ctx context.Context) error {
			defer wg.Done()
			atomic.AddInt64(&count, 1)
			return nil
		})
	}

	wp.Start()
	wg.Wait()
	wp.Stop()

	if atomic.LoadInt64(&count) != int64(n) {
		t.Fatalf("expected %d jobs, got %d", n, count)
	}
}

func TestWorkerPoolRetry(t *testing.T) {
	q := NewQueue(QueueConfig{MaxSize: 100, MaxRetries: 1})
	wp := NewWorkerPool(WorkerPoolConfig{
		Workers: 1,
		Queue:   q,
	})

	var attempts int64
	q.Enqueue(context.Background(), "flaky", PriorityNormal, func(ctx context.Context) error {
		n := atomic.AddInt64(&attempts, 1)
		if n <= 1 {
			return errors.New("transient error")
		}
		return nil
	})

	wp.Start()
	time.Sleep(200 * time.Millisecond)
	wp.Stop()

	if atomic.LoadInt64(&attempts) != 2 {
		t.Fatalf("expected 2 attempts, got %d", attempts)
	}

	stats := q.Stats()
	if stats.Completed != 1 {
		t.Fatalf("expected 1 completed, got %d", stats.Completed)
	}
}

func TestWorkerPoolConcurrency(t *testing.T) {
	q := NewQueue(DefaultQueueConfig())
	workers := 5
	wp := NewWorkerPool(WorkerPoolConfig{
		Workers: workers,
		Queue:   q,
	})

	var maxConcurrent int64
	var current int64
	var wg sync.WaitGroup
	n := 20

	for i := 0; i < n; i++ {
		wg.Add(1)
		q.Enqueue(context.Background(), "concurrent", PriorityNormal, func(ctx context.Context) error {
			defer wg.Done()
			cur := atomic.AddInt64(&current, 1)
			// Update max
			for {
				old := atomic.LoadInt64(&maxConcurrent)
				if cur <= old {
					break
				}
				if atomic.CompareAndSwapInt64(&maxConcurrent, old, cur) {
					break
				}
			}
			time.Sleep(10 * time.Millisecond)
			atomic.AddInt64(&current, -1)
			return nil
		})
	}

	wp.Start()
	wg.Wait()
	wp.Stop()

	mc := atomic.LoadInt64(&maxConcurrent)
	if mc > int64(workers) {
		t.Fatalf("max concurrent %d exceeds worker count %d", mc, workers)
	}
}

func TestWorkerPoolCallbacks(t *testing.T) {
	q := NewQueue(DefaultQueueConfig())
	var completedCount int64
	var errorCount int64

	wp := NewWorkerPool(WorkerPoolConfig{
		Workers: 2,
		Queue:   q,
		OnComplete: func(job *Job) {
			atomic.AddInt64(&completedCount, 1)
		},
		OnError: func(job *Job, err error) {
			atomic.AddInt64(&errorCount, 1)
		},
	})

	q.Enqueue(context.Background(), "ok", PriorityNormal, func(ctx context.Context) error {
		return nil
	})
	q.Enqueue(context.Background(), "fail", PriorityNormal, func(ctx context.Context) error {
		return errors.New("fail")
	})

	wp.Start()
	time.Sleep(100 * time.Millisecond)
	wp.Stop()

	if atomic.LoadInt64(&completedCount) != 1 {
		t.Fatalf("expected 1 completion callback, got %d", completedCount)
	}
	if atomic.LoadInt64(&errorCount) != 1 {
		t.Fatalf("expected 1 error callback, got %d", errorCount)
	}
}

// --- Rate Limiter Tests ---

func TestTokenBucketBasic(t *testing.T) {
	tb := NewTokenBucket(10, 10) // 10/sec, burst 10

	// Should allow burst
	for i := 0; i < 10; i++ {
		if !tb.Allow() {
			t.Fatalf("expected Allow at %d", i)
		}
	}

	// Should deny after burst
	if tb.Allow() {
		t.Fatal("expected deny after burst")
	}

	// Wait for refill
	time.Sleep(150 * time.Millisecond)
	if !tb.Allow() {
		t.Fatal("expected Allow after refill")
	}
}

func TestTokenBucketWait(t *testing.T) {
	tb := NewTokenBucket(100, 1) // fast refill
	tb.Allow()                    // consume token

	start := time.Now()
	err := tb.Wait()
	elapsed := time.Since(start)

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if elapsed > 100*time.Millisecond {
		t.Fatalf("waited too long: %v", elapsed)
	}
}

func TestTokenBucketAvailable(t *testing.T) {
	tb := NewTokenBucket(10, 10)
	tb.AllowN(3)
	avail := tb.Available()
	if avail < 6.5 || avail > 8 { // Allow for some refill
		t.Fatalf("expected ~7 available, got %v", avail)
	}
}

func TestSlidingWindowBasic(t *testing.T) {
	sw := NewSlidingWindow(3, time.Second)

	if !sw.Allow() {
		t.Fatal("expected Allow 1")
	}
	if !sw.Allow() {
		t.Fatal("expected Allow 2")
	}
	if !sw.Allow() {
		t.Fatal("expected Allow 3")
	}
	if sw.Allow() {
		t.Fatal("expected Deny after 3")
	}

	remaining := sw.Remaining()
	if remaining != 0 {
		t.Fatalf("expected 0 remaining, got %d", remaining)
	}
}

func TestSlidingWindowEviction(t *testing.T) {
	sw := NewSlidingWindow(2, 50*time.Millisecond)

	sw.Allow()
	sw.Allow()
	if sw.Allow() {
		t.Fatal("expected Deny")
	}

	// Wait for window to slide
	time.Sleep(60 * time.Millisecond)
	if !sw.Allow() {
		t.Fatal("expected Allow after window slide")
	}
}

func TestRateLimiterComposite(t *testing.T) {
	rl := NewRateLimiter("test", 100, 10, 100, time.Minute)

	// Should allow initial requests
	for i := 0; i < 10; i++ {
		if !rl.Allow() {
			t.Fatalf("expected Allow at %d", i)
		}
	}

	if rl.Name() != "test" {
		t.Fatalf("expected name 'test', got %s", rl.Name())
	}

	stats := rl.Stats()
	if stats.Name != "test" {
		t.Fatalf("expected stats name 'test', got %s", stats.Name)
	}
}

func TestRateLimiterManager(t *testing.T) {
	mgr := NewRateLimiterManager()

	// Register per-provider limiters
	mgr.Register("openai", NewRateLimiter("openai", 10, 10, 100, time.Minute))
	mgr.Register("anthropic", NewRateLimiter("anthropic", 5, 5, 50, time.Minute))

	if !mgr.Allow("openai") {
		t.Fatal("expected Allow for openai")
	}
	if !mgr.Allow("unknown") {
		t.Fatal("expected Allow for unregistered provider")
	}

	stats := mgr.Stats()
	if len(stats) != 2 {
		t.Fatalf("expected 2 stats, got %d", len(stats))
	}
}

// --- Circuit Breaker Tests ---

func TestCircuitBreakerClosed(t *testing.T) {
	cb := NewCircuitBreaker("test", CircuitBreakerConfig{
		FailureThreshold: 3,
		SuccessThreshold: 2,
		Timeout:          100 * time.Millisecond,
		MaxHalfOpen:      2,
	})

	// Successful requests keep it closed
	for i := 0; i < 5; i++ {
		err := cb.Execute(func() error { return nil })
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
	}

	if cb.State() != CircuitClosed {
		t.Fatalf("expected Closed, got %s", cb.State())
	}
}

func TestCircuitBreakerOpens(t *testing.T) {
	cb := NewCircuitBreaker("test", CircuitBreakerConfig{
		FailureThreshold: 3,
		SuccessThreshold: 2,
		Timeout:          100 * time.Millisecond,
		MaxHalfOpen:      2,
	})

	// Fail enough times to open
	for i := 0; i < 3; i++ {
		cb.Execute(func() error { return errors.New("fail") })
	}

	if cb.State() != CircuitOpen {
		t.Fatalf("expected Open, got %s", cb.State())
	}

	// Should reject
	err := cb.Execute(func() error { return nil })
	if !errors.Is(err, ErrCircuitOpen) {
		t.Fatalf("expected ErrCircuitOpen, got %v", err)
	}
}

func TestCircuitBreakerHalfOpenToClosed(t *testing.T) {
	cb := NewCircuitBreaker("test", CircuitBreakerConfig{
		FailureThreshold: 2,
		SuccessThreshold: 2,
		Timeout:          50 * time.Millisecond,
		MaxHalfOpen:      3,
	})

	// Open the circuit
	for i := 0; i < 2; i++ {
		cb.Execute(func() error { return errors.New("fail") })
	}

	// Wait for timeout → half-open
	time.Sleep(60 * time.Millisecond)

	// Succeed enough times to close
	for i := 0; i < 2; i++ {
		err := cb.Execute(func() error { return nil })
		if err != nil {
			t.Fatalf("unexpected error in half-open: %v", err)
		}
	}

	if cb.State() != CircuitClosed {
		t.Fatalf("expected Closed after recovery, got %s", cb.State())
	}
}

func TestCircuitBreakerHalfOpenToOpen(t *testing.T) {
	cb := NewCircuitBreaker("test", CircuitBreakerConfig{
		FailureThreshold: 2,
		SuccessThreshold: 2,
		Timeout:          50 * time.Millisecond,
		MaxHalfOpen:      3,
	})

	// Open
	for i := 0; i < 2; i++ {
		cb.Execute(func() error { return errors.New("fail") })
	}

	// Wait → half-open
	time.Sleep(60 * time.Millisecond)

	// Fail in half-open → back to open
	cb.Execute(func() error { return errors.New("still failing") })

	if cb.State() != CircuitOpen {
		t.Fatalf("expected Open after half-open failure, got %s", cb.State())
	}
}

func TestCircuitBreakerReset(t *testing.T) {
	cb := NewCircuitBreaker("test", DefaultCircuitBreakerConfig())

	for i := 0; i < 10; i++ {
		cb.Execute(func() error { return errors.New("fail") })
	}

	cb.Reset()
	if cb.State() != CircuitClosed {
		t.Fatalf("expected Closed after reset, got %s", cb.State())
	}
}

func TestCircuitBreakerFallback(t *testing.T) {
	cb := NewCircuitBreaker("test", DefaultCircuitBreakerConfig())

	// Open the circuit
	for i := 0; i < 10; i++ {
		cb.Execute(func() error { return errors.New("fail") })
	}

	// Should use fallback
	fallbackCalled := false
	err := cb.ExecuteWithFallback(
		func() error { return nil },
		func(fallbackErr error) error {
			fallbackCalled = true
			return errors.New("fallback response")
		},
	)

	if !fallbackCalled {
		t.Fatal("expected fallback to be called")
	}
	if err == nil || err.Error() != "fallback response" {
		t.Fatalf("expected fallback error, got %v", err)
	}
}

func TestCircuitBreakerStats(t *testing.T) {
	cb := NewCircuitBreaker("test-provider", DefaultCircuitBreakerConfig())

	cb.Execute(func() error { return nil })
	cb.Execute(func() error { return errors.New("fail") })

	stats := cb.Stats()
	if stats.Name != "test-provider" {
		t.Fatalf("expected name 'test-provider', got %s", stats.Name)
	}
	if stats.TotalRequests != 2 {
		t.Fatalf("expected 2 total requests, got %d", stats.TotalRequests)
	}
	if stats.TotalSuccesses != 1 {
		t.Fatalf("expected 1 success, got %d", stats.TotalSuccesses)
	}
	if stats.TotalFailures != 1 {
		t.Fatalf("expected 1 failure, got %d", stats.TotalFailures)
	}
}

func TestCircuitBreakerManager(t *testing.T) {
	mgr := NewCircuitBreakerManager()

	cb1 := mgr.GetOrCreate("openai")
	cb2 := mgr.GetOrCreate("openai")
	if cb1 != cb2 {
		t.Fatal("expected same circuit breaker for same provider")
	}

	cb3 := mgr.GetOrCreate("anthropic")
	if cb1 == cb3 {
		t.Fatal("expected different circuit breakers for different providers")
	}

	stats := mgr.Stats()
	if len(stats) != 2 {
		t.Fatalf("expected 2 stats, got %d", len(stats))
	}

	mgr.ResetAll()
	for _, s := range mgr.Stats() {
		if s.State != "closed" {
			t.Fatalf("expected all closed after reset, got %s", s.State)
		}
	}
}

// --- Backoff Tests ---

func TestCalculateRetryDelay(t *testing.T) {
	job := &Job{
		RetryCount: 1,
		Backoff: BackoffConfig{
			Initial: 100 * time.Millisecond,
			Max:     5 * time.Second,
			Factor:  2.0,
			Jitter:  0,
		},
	}

	delay := CalculateRetryDelay(job)
	if delay != 100*time.Millisecond {
		t.Fatalf("expected 100ms for retry 1, got %v", delay)
	}

	job.RetryCount = 2
	delay = CalculateRetryDelay(job)
	if delay != 200*time.Millisecond {
		t.Fatalf("expected 200ms for retry 2, got %v", delay)
	}

	job.RetryCount = 3
	delay = CalculateRetryDelay(job)
	if delay != 400*time.Millisecond {
		t.Fatalf("expected 400ms for retry 3, got %v", delay)
	}

	// Test max cap
	job.RetryCount = 10
	delay = CalculateRetryDelay(job)
	if delay > 5*time.Second {
		t.Fatalf("expected max 5s, got %v", delay)
	}
}

// --- Integration Test ---

func TestFullPipeline(t *testing.T) {
	q := NewQueue(QueueConfig{
		MaxSize:    100,
		MaxRetries: 2,
	})

	mgr := NewRateLimiterManager()
	mgr.Register("openai", NewRateLimiter("openai", 1000, 100, 10000, time.Minute))

	cbMgr := NewCircuitBreakerManager()
	_ = cbMgr.GetOrCreate("openai")

	wp := NewWorkerPool(WorkerPoolConfig{
		Workers: 5,
		Queue:   q,
		OnComplete: func(job *Job) {},
		OnError:    func(job *Job, err error) {},
	})

	var completed int64
	var failed int64

	// Enqueue jobs
	for i := 0; i < 20; i++ {
		q.Enqueue(context.Background(), "api-call", PriorityNormal, func(ctx context.Context) error {
			// Simulate rate limiting
			if !mgr.Allow("openai") {
				return errors.New("rate limited")
			}

			// Simulate circuit breaker
			return cbMgr.GetOrCreate("openai").Execute(func() error {
				time.Sleep(time.Millisecond)
				return nil
			})
		})
	}

	wp.Start()
	time.Sleep(500 * time.Millisecond)
	wp.Stop()

	stats := q.Stats()
	completed = stats.Completed
	failed = stats.Failed

	t.Logf("Pipeline stats: completed=%d failed=%d retries=%d",
		completed, failed, stats.Retries)

	if completed+failed != 20 {
		t.Fatalf("expected 20 total outcomes, got %d", completed+failed)
	}
}
