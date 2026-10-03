package memory_test

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/go-hypercube/go-hypercube/queue"
	memory "github.com/go-hypercube/hypercube-queue-memory"
	"github.com/stretchr/testify/require"
)

func TestQueueImplementsAllContracts(t *testing.T) {
	q := memory.New()

	var _ queue.Queue = q
	var _ queue.VisibilityExtender = q
	var _ queue.RedriveProvider = q
	var _ queue.StatsProvider = q
}

func TestPushPopOwnershipAndBatchErrors(t *testing.T) {
	q := memory.New(memory.WithPollingTimeout(0))
	ctx := context.Background()

	msg := &queue.Message{
		QueueName:  "jobs",
		Namespace:  "app",
		JobName:    "generated-id",
		Payload:    []byte("payload"),
		Extra:      []byte("extra"),
		Attempt:    99,
		DriverData: "caller-owned-data",
	}
	firstWithID := &queue.Message{
		ID:        "fixed-id",
		QueueName: "jobs",
		JobName:   "first",
		Payload:   []byte("first"),
	}
	duplicate := &queue.Message{
		ID:        "fixed-id",
		QueueName: "other-queue",
		JobName:   "duplicate",
		Payload:   []byte("duplicate"),
	}

	err := q.Push(ctx, msg, nil, firstWithID, duplicate)
	batchErr := requireBatchError(t, err, 2)
	require.Equal(t, 1, batchErr.Failed[0].Index)
	require.Nil(t, batchErr.Failed[0].Msg)
	require.ErrorIs(t, batchErr.Failed[0].Err, queue.ErrInvalidArgument)
	require.Equal(t, 3, batchErr.Failed[1].Index)
	require.Same(t, duplicate, batchErr.Failed[1].Msg)
	require.ErrorIs(t, batchErr.Failed[1].Err, queue.ErrAlreadyExists)
	require.NotEmpty(t, msg.ID)

	msg.Payload[0] = 'X'
	msg.Extra[0] = 'X'
	firstWithID.Payload[0] = 'X'

	deliveries, err := q.Pop(ctx, "jobs", 10)
	require.NoError(t, err)
	require.Len(t, deliveries, 2)

	byID := indexByID(deliveries)
	generated := byID[msg.ID]
	require.NotNil(t, generated)
	require.NotSame(t, msg, generated)
	require.Equal(t, []byte("payload"), generated.Payload)
	require.Equal(t, []byte("extra"), generated.Extra)
	require.Equal(t, 1, generated.Attempt)
	require.Zero(t, generated.Delay)
	require.NotNil(t, generated.DriverData)
	require.NotEqual(t, "caller-owned-data", generated.DriverData)

	fixed := byID[firstWithID.ID]
	require.NotNil(t, fixed)
	require.Equal(t, "first", fixed.JobName)
	require.Equal(t, []byte("first"), fixed.Payload)

	err = q.Ack(ctx, nil, generated, fixed)
	batchErr = requireBatchError(t, err, 1)
	require.Equal(t, 0, batchErr.Failed[0].Index)
	require.ErrorIs(t, batchErr.Failed[0].Err, queue.ErrInvalidArgument)

	stats, err := q.Stats(ctx, "jobs")
	require.NoError(t, err)
	require.Equal(t, queue.QueueStats{Name: "jobs"}, stats)

	names, err := q.Queues(ctx)
	require.NoError(t, err)
	require.Empty(t, names)

	// Ack removed the retained record, so reusing the ID is permitted.
	reused := &queue.Message{ID: msg.ID, QueueName: "jobs"}
	require.NoError(t, q.Push(ctx, reused))
}

func TestEmptyBatchesIgnoreCanceledContext(t *testing.T) {
	q := memory.New()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	require.NoError(t, q.Push(ctx))
	require.NoError(t, q.Ack(ctx))
	require.NoError(t, q.Retry(ctx))
	require.NoError(t, q.DeadLetter(ctx))
}

func TestRetrySnapshotsChangesAndHonorsDelay(t *testing.T) {
	q := memory.New(
		memory.WithPollingTimeout(2*time.Second),
		memory.WithDefaultVisibilityTimeout(time.Second),
	)
	ctx := context.Background()
	msg := &queue.Message{
		ID:        "retry-message",
		QueueName: "jobs",
		Payload:   []byte("before"),
		Extra:     []byte("old-extra"),
	}
	require.NoError(t, q.Push(ctx, msg))

	first := popOne(t, q, ctx, "jobs")
	firstToken := first.DriverData
	first.Payload = []byte("after")
	first.Extra = []byte("new-extra")
	first.Delay = 300 * time.Millisecond
	require.NoError(t, q.Retry(ctx, first))

	stats, err := q.Stats(ctx, "jobs")
	require.NoError(t, err)
	require.Equal(t, int64(1), stats.Delayed)
	require.Zero(t, stats.Ready)
	require.Zero(t, stats.InFlight)

	shortCtx, cancel := context.WithTimeout(ctx, 50*time.Millisecond)
	defer cancel()
	messages, err := q.Pop(shortCtx, "jobs", 1)
	require.Nil(t, messages)
	require.ErrorIs(t, err, context.DeadlineExceeded)

	second := popOne(t, q, ctx, "jobs")
	require.Equal(t, 2, second.Attempt)
	require.Zero(t, second.Delay)
	require.Equal(t, []byte("after"), second.Payload)
	require.Equal(t, []byte("new-extra"), second.Extra)
	require.NotEqual(t, firstToken, second.DriverData)

	err = q.Ack(ctx, first)
	batchErr := requireBatchError(t, err, 1)
	require.ErrorIs(t, batchErr.Failed[0].Err, queue.ErrNotFound)
	require.NoError(t, q.Ack(ctx, second))
}

func TestVisibilityExpiryRedeliveryAndExtension(t *testing.T) {
	q := memory.New(memory.WithPollingTimeout(25 * time.Millisecond))
	ctx := context.Background()
	msg := &queue.Message{
		ID:                "visibility-message",
		QueueName:         "jobs",
		Payload:           []byte("original"),
		Extra:             []byte("original-extra"),
		VisibilityTimeout: 200 * time.Millisecond,
	}
	require.NoError(t, q.Push(ctx, msg))

	first := popOne(t, q, ctx, "jobs")
	firstToken := first.DriverData
	first.Payload[0] = 'X'
	first.Extra[0] = 'X'

	require.Eventually(t, func() bool {
		stats, err := q.Stats(ctx, "jobs")
		return err == nil && stats.Ready == 1 && stats.InFlight == 0
	}, 2*time.Second, 10*time.Millisecond)

	require.ErrorIs(t, q.ExtendVisibility(ctx, time.Second, first), queue.ErrNotFound)
	err := q.Ack(ctx, first)
	batchErr := requireBatchError(t, err, 1)
	require.ErrorIs(t, batchErr.Failed[0].Err, queue.ErrNotFound)

	second := popOne(t, q, ctx, "jobs")
	// Extend immediately so assertion work cannot consume the short original lease.
	require.NoError(t, q.ExtendVisibility(ctx, 750*time.Millisecond, second))
	require.Equal(t, 2, second.Attempt)
	require.Equal(t, []byte("original"), second.Payload)
	require.Equal(t, []byte("original-extra"), second.Extra)
	require.NotEqual(t, firstToken, second.DriverData)

	require.ErrorIs(t, q.ExtendVisibility(ctx, 0, second), queue.ErrInvalidArgument)
	require.ErrorIs(t, q.ExtendVisibility(ctx, time.Second, nil), queue.ErrInvalidArgument)

	// The original 200ms lease has elapsed, but the replacement deadline has not.
	time.Sleep(300 * time.Millisecond)
	stats, err := q.Stats(ctx, "jobs")
	require.NoError(t, err)
	require.Equal(t, int64(1), stats.InFlight)
	require.Zero(t, stats.Ready)

	messages, err := q.Pop(ctx, "jobs", 1)
	require.Nil(t, messages)
	require.ErrorIs(t, err, queue.ErrEmpty)
	require.NoError(t, q.Ack(ctx, second))

	// An extension replaces the old deadline rather than adding to it.
	replacement := &queue.Message{
		ID:                "replacement-deadline",
		QueueName:         "jobs",
		VisibilityTimeout: 2 * time.Second,
	}
	require.NoError(t, q.Push(ctx, replacement))
	replacementDelivery := popOne(t, q, ctx, "jobs")
	require.NoError(t, q.ExtendVisibility(ctx, 100*time.Millisecond, replacementDelivery))
	require.Eventually(t, func() bool {
		stats, err := q.Stats(ctx, "jobs")
		return err == nil && stats.Ready == 1 && stats.InFlight == 0
	}, time.Second, 10*time.Millisecond)
}

func TestDeadLetterRedriveAndStats(t *testing.T) {
	q := memory.New(memory.WithPollingTimeout(0))
	ctx := context.Background()

	deadCandidate := &queue.Message{
		ID:                "dead-message",
		QueueName:         "alpha",
		Namespace:         "app",
		JobName:           "send",
		Payload:           []byte("before"),
		Extra:             []byte("before-extra"),
		VisibilityTimeout: time.Minute,
	}
	delayed := &queue.Message{
		ID:        "delayed-message",
		QueueName: "alpha",
		Delay:     time.Hour,
	}
	other := &queue.Message{ID: "other-message", QueueName: "beta"}
	require.NoError(t, q.Push(ctx, deadCandidate, delayed, other))

	stats, err := q.Stats(ctx, "alpha")
	require.NoError(t, err)
	require.Equal(t, queue.QueueStats{
		Name:    "alpha",
		Ready:   1,
		Delayed: 1,
	}, stats)

	names, err := q.Queues(ctx)
	require.NoError(t, err)
	require.Equal(t, []string{"alpha", "beta"}, names)

	delivery := popOne(t, q, ctx, "alpha")
	oldToken := delivery.DriverData
	delivery.Payload = []byte("after")
	delivery.Extra = []byte("failure metadata")
	delivery.Delay = time.Hour
	require.NoError(t, q.DeadLetter(ctx, delivery))

	stats, err = q.Stats(ctx, "alpha")
	require.NoError(t, err)
	require.Equal(t, int64(1), stats.Delayed)
	require.Equal(t, int64(1), stats.Dead)

	duplicate := &queue.Message{ID: deadCandidate.ID, QueueName: "gamma"}
	batchErr := requireBatchError(t, q.Push(ctx, duplicate), 1)
	require.ErrorIs(t, batchErr.Failed[0].Err, queue.ErrAlreadyExists)

	n, err := q.Redrive(ctx, "alpha", 0)
	require.Zero(t, n)
	require.ErrorIs(t, err, queue.ErrInvalidArgument)
	n, err = q.Redrive(ctx, "unknown", 10)
	require.NoError(t, err)
	require.Zero(t, n)
	n, err = q.Redrive(ctx, "alpha", 1)
	require.NoError(t, err)
	require.Equal(t, 1, n)

	stats, err = q.Stats(ctx, "alpha")
	require.NoError(t, err)
	require.Equal(t, int64(1), stats.Ready)
	require.Equal(t, int64(1), stats.Delayed)
	require.Zero(t, stats.Dead)

	redriven := popOne(t, q, ctx, "alpha")
	require.Equal(t, deadCandidate.ID, redriven.ID)
	require.Equal(t, "app", redriven.Namespace)
	require.Equal(t, "send", redriven.JobName)
	require.Equal(t, []byte("after"), redriven.Payload)
	require.Equal(t, []byte("failure metadata"), redriven.Extra)
	require.Equal(t, 1, redriven.Attempt)
	require.Zero(t, redriven.Delay)
	require.Equal(t, time.Minute, redriven.VisibilityTimeout)
	require.NotEqual(t, oldToken, redriven.DriverData)
	require.NoError(t, q.Ack(ctx, redriven))

	unknown, err := q.Stats(ctx, "does-not-exist")
	require.NoError(t, err)
	require.Equal(t, queue.QueueStats{Name: "does-not-exist"}, unknown)
}

func TestPopBlocksUntilPushDelayCancellationOrPollingTimeout(t *testing.T) {
	t.Run("push wakes waiter", func(t *testing.T) {
		q := memory.New(memory.WithPollingTimeout(time.Second))
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()

		type result struct {
			messages []*queue.Message
			err      error
		}
		resultCh := make(chan result, 1)
		go func() {
			messages, err := q.Pop(ctx, "jobs", 1)
			resultCh <- result{messages: messages, err: err}
		}()

		time.Sleep(20 * time.Millisecond)
		require.NoError(t, q.Push(ctx, &queue.Message{ID: "wake", QueueName: "jobs"}))

		select {
		case got := <-resultCh:
			require.NoError(t, got.err)
			require.Len(t, got.messages, 1)
			require.Equal(t, "wake", got.messages[0].ID)
		case <-time.After(500 * time.Millisecond):
			t.Fatal("Pop was not awakened by Push")
		}
	})

	t.Run("delay wakes waiter", func(t *testing.T) {
		q := memory.New(memory.WithPollingTimeout(time.Second))
		ctx := context.Background()
		started := time.Now()
		require.NoError(t, q.Push(ctx, &queue.Message{
			ID:        "delayed",
			QueueName: "jobs",
			Delay:     150 * time.Millisecond,
		}))

		delivery := popOne(t, q, ctx, "jobs")
		require.Equal(t, "delayed", delivery.ID)
		require.GreaterOrEqual(t, time.Since(started), 100*time.Millisecond)
	})

	t.Run("context cancellation wins", func(t *testing.T) {
		q := memory.New(memory.WithPollingTimeout(time.Second))
		ctx, cancel := context.WithCancel(context.Background())
		time.AfterFunc(20*time.Millisecond, cancel)

		messages, err := q.Pop(ctx, "jobs", 1)
		require.Nil(t, messages)
		require.ErrorIs(t, err, context.Canceled)
	})

	t.Run("polling timeout returns empty", func(t *testing.T) {
		q := memory.New(memory.WithPollingTimeout(30 * time.Millisecond))
		started := time.Now()

		messages, err := q.Pop(context.Background(), "jobs", 1)
		require.Nil(t, messages)
		require.ErrorIs(t, err, queue.ErrEmpty)
		require.GreaterOrEqual(t, time.Since(started), 20*time.Millisecond)
	})
}

func TestConcurrentPushAndPop(t *testing.T) {
	q := memory.New(memory.WithPollingTimeout(0))
	ctx := context.Background()

	const workers = 32
	start := make(chan struct{})
	results := make(chan error, workers)
	var wg sync.WaitGroup
	for range workers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			results <- q.Push(ctx, &queue.Message{ID: "shared-id", QueueName: "jobs"})
		}()
	}
	close(start)
	wg.Wait()
	close(results)

	successes := 0
	duplicates := 0
	for err := range results {
		if err == nil {
			successes++
			continue
		}
		batchErr := requireBatchError(t, err, 1)
		if errors.Is(batchErr.Failed[0].Err, queue.ErrAlreadyExists) {
			duplicates++
		}
	}
	require.Equal(t, 1, successes)
	require.Equal(t, workers-1, duplicates)

	start = make(chan struct{})
	deliveries := make(chan *queue.Message, workers)
	errorsCh := make(chan error, workers)
	for range workers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			messages, err := q.Pop(ctx, "jobs", 1)
			if err != nil {
				errorsCh <- err
				return
			}
			deliveries <- messages[0]
		}()
	}
	close(start)
	wg.Wait()
	close(deliveries)
	close(errorsCh)

	require.Len(t, deliveries, 1)
	require.Len(t, errorsCh, workers-1)
	for err := range errorsCh {
		require.ErrorIs(t, err, queue.ErrEmpty)
	}
}

func TestCanceledOperationsDoNotMutateState(t *testing.T) {
	q := memory.New(memory.WithPollingTimeout(0))
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	msg := &queue.Message{ID: "canceled", QueueName: "jobs"}
	require.ErrorIs(t, q.Push(ctx, msg), context.Canceled)

	stats, err := q.Stats(context.Background(), "jobs")
	require.NoError(t, err)
	require.Equal(t, queue.QueueStats{Name: "jobs"}, stats)

	stats, err = q.Stats(ctx, "jobs")
	require.ErrorIs(t, err, context.Canceled)
	require.Equal(t, queue.QueueStats{}, stats)

	n, err := q.Redrive(ctx, "jobs", 1)
	require.Zero(t, n)
	require.ErrorIs(t, err, context.Canceled)

	names, err := q.Queues(ctx)
	require.Nil(t, names)
	require.ErrorIs(t, err, context.Canceled)
}

func popOne(t *testing.T, q queue.Queue, ctx context.Context, queueName string) *queue.Message {
	t.Helper()
	messages, err := q.Pop(ctx, queueName, 1)
	require.NoError(t, err)
	require.Len(t, messages, 1)
	return messages[0]
}

func requireBatchError(t *testing.T, err error, failures int) *queue.BatchError {
	t.Helper()
	require.Error(t, err)
	var batchErr *queue.BatchError
	require.ErrorAs(t, err, &batchErr)
	require.Len(t, batchErr.Failed, failures)
	return batchErr
}

func indexByID(messages []*queue.Message) map[string]*queue.Message {
	indexed := make(map[string]*queue.Message, len(messages))
	for _, msg := range messages {
		indexed[msg.ID] = msg
	}
	return indexed
}
