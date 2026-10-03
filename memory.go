// Package memory provides a concurrent, process-local implementation of the
// queue contracts. It is intended for development, tests, and applications
// that do not require messages to survive a process restart.
package memory

import (
	"context"
	"sort"
	"time"

	"github.com/go-hypercube/go-hypercube/queue"
	"github.com/google/uuid"
)

const (
	// DefaultPollingTimeout is how long Pop waits for a visible message when no
	// polling timeout option is supplied.
	DefaultPollingTimeout = time.Second

	// DefaultVisibilityTimeout is the lease duration used when a message has a
	// zero VisibilityTimeout.
	DefaultVisibilityTimeout = 30 * time.Second
)

// Option configures a Queue.
type Option func(*Queue)

// WithPollingTimeout configures how long Pop waits for a visible message
// before returning queue.ErrEmpty. A non-positive timeout makes an empty Pop
// return immediately after checking for messages that are already visible.
func WithPollingTimeout(timeout time.Duration) Option {
	return func(q *Queue) {
		q.pollingTimeout = timeout
	}
}

// WithDefaultVisibilityTimeout configures the lease duration applied to
// messages whose VisibilityTimeout is zero. Non-positive values are ignored so
// a zero-value message always receives a usable lease.
func WithDefaultVisibilityTimeout(timeout time.Duration) Option {
	return func(q *Queue) {
		if timeout > 0 {
			q.defaultVisibilityTimeout = timeout
		}
	}
}

// Queue is an in-memory queue. Its messages are lost when the process exits.
// A Queue is safe for concurrent use by multiple goroutines. Instances must be
// created with New and must not be copied after first use.
type Queue struct {
	stateLock chan struct{}

	pollingTimeout           time.Duration
	defaultVisibilityTimeout time.Duration

	messages map[string]*storedMessage
	queues   map[string]*queueState
	wake     chan struct{}
	sequence uint64
}

// MemoryQueue is an alias retained for callers that prefer an explicit driver
// type name.
type MemoryQueue = Queue

type queueState struct {
	messages map[string]*storedMessage
}

type messageState uint8

const (
	statePending messageState = iota
	stateInFlight
	stateDead
)

type deliveryToken string

type storedMessage struct {
	message queue.Message
	state   messageState

	availableAt        time.Time
	visibilityDeadline time.Time
	deliveryToken      deliveryToken
	order              uint64
}

var (
	_ queue.Queue              = (*Queue)(nil)
	_ queue.VisibilityExtender = (*Queue)(nil)
	_ queue.RedriveProvider    = (*Queue)(nil)
	_ queue.StatsProvider      = (*Queue)(nil)
)

// New creates an empty in-memory queue.
func New(options ...Option) *Queue {
	stateLock := make(chan struct{}, 1)
	stateLock <- struct{}{}
	q := &Queue{
		stateLock:                stateLock,
		pollingTimeout:           DefaultPollingTimeout,
		defaultVisibilityTimeout: DefaultVisibilityTimeout,
		messages:                 make(map[string]*storedMessage),
		queues:                   make(map[string]*queueState),
		wake:                     make(chan struct{}),
	}
	for _, option := range options {
		if option != nil {
			option(q)
		}
	}
	return q
}

// Push enqueues messages in their respective named queues.
func (q *Queue) Push(ctx context.Context, msgs ...*queue.Message) error {
	if len(msgs) == 0 {
		return nil
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := q.acquire(ctx); err != nil {
		return err
	}
	defer q.release()

	failed := make([]queue.ItemError, 0)
	changed := false

	for index, msg := range msgs {
		if err := ctx.Err(); err != nil {
			if changed {
				q.signalLocked()
			}
			return err
		}
		if msg == nil {
			failed = append(failed, queue.ItemError{
				Index: index,
				Msg:   nil,
				Err:   queue.ErrInvalidArgument,
			})
			continue
		}

		if msg.ID == "" {
			msg.ID = q.newMessageIDLocked()
		}
		if _, exists := q.messages[msg.ID]; exists {
			failed = append(failed, queue.ItemError{
				Index: index,
				Msg:   msg,
				Err:   queue.ErrAlreadyExists,
			})
			continue
		}

		stored := cloneMessage(msg)
		stored.Attempt = 0
		stored.DriverData = nil

		record := &storedMessage{
			message:     stored,
			state:       statePending,
			availableAt: time.Now().Add(stored.Delay),
			order:       q.nextOrderLocked(),
		}
		q.messages[stored.ID] = record
		q.queueLocked(stored.QueueName).messages[stored.ID] = record
		changed = true
	}

	if changed {
		q.signalLocked()
	}
	return newBatchError(failed)
}

// Pop waits for and claims up to maxMessages visible messages from queueName.
func (q *Queue) Pop(ctx context.Context, queueName string, maxMessages int) ([]*queue.Message, error) {
	if maxMessages <= 0 {
		return nil, queue.ErrInvalidArgument
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	pollDeadline := time.Now().Add(q.pollingTimeout)

	for {
		if err := ctx.Err(); err != nil {
			return nil, err
		}

		if err := q.acquire(ctx); err != nil {
			return nil, err
		}

		now := time.Now()
		messages, err := q.popVisibleLocked(ctx, queueName, maxMessages, now)
		if err != nil {
			q.release()
			return nil, err
		}
		if len(messages) > 0 {
			q.signalLocked()
			q.release()
			return messages, nil
		}

		nextVisibleAt, hasNext, err := q.nextVisibleAtLocked(ctx, queueName)
		if err != nil {
			q.release()
			return nil, err
		}
		wake := q.wake
		q.release()

		now = time.Now()
		wait := pollDeadline.Sub(now)
		if wait <= 0 {
			return nil, queue.ErrEmpty
		}
		if hasNext {
			untilVisible := nextVisibleAt.Sub(now)
			if untilVisible < wait {
				wait = untilVisible
			}
		}
		if wait < 0 {
			wait = 0
		}

		timer := time.NewTimer(wait)
		select {
		case <-ctx.Done():
			stopTimer(timer)
			return nil, ctx.Err()
		case <-wake:
			stopTimer(timer)
		case <-timer.C:
		}
	}
}

// Ack permanently removes in-flight deliveries.
func (q *Queue) Ack(ctx context.Context, msgs ...*queue.Message) error {
	return q.settle(ctx, settlementAck, msgs...)
}

// Retry returns in-flight deliveries to their pending queues.
func (q *Queue) Retry(ctx context.Context, msgs ...*queue.Message) error {
	return q.settle(ctx, settlementRetry, msgs...)
}

// DeadLetter moves in-flight deliveries into the retained dead-letter store.
func (q *Queue) DeadLetter(ctx context.Context, msgs ...*queue.Message) error {
	return q.settle(ctx, settlementDeadLetter, msgs...)
}

// ExtendVisibility replaces an in-flight delivery's visibility deadline with
// a deadline measured from the time of this call.
func (q *Queue) ExtendVisibility(ctx context.Context, extension time.Duration, msg *queue.Message) error {
	if extension <= 0 || msg == nil {
		return queue.ErrInvalidArgument
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := q.acquire(ctx); err != nil {
		return err
	}
	defer q.release()

	now := time.Now()
	record, ok := q.currentDeliveryLocked(msg, now)
	if !ok {
		return queue.ErrNotFound
	}

	record.visibilityDeadline = now.Add(extension)
	q.signalLocked()
	return nil
}

// Redrive returns up to count retained dead-letter messages to the active
// queue. Redriven messages are immediately visible and begin again at attempt
// zero.
func (q *Queue) Redrive(ctx context.Context, queueName string, count int) (int, error) {
	if count <= 0 {
		return 0, queue.ErrInvalidArgument
	}
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	if err := q.acquire(ctx); err != nil {
		return 0, err
	}
	defer q.release()

	state := q.queues[queueName]
	if state == nil {
		return 0, nil
	}

	dead := make([]*storedMessage, 0)
	for _, record := range state.messages {
		if err := ctx.Err(); err != nil {
			return 0, err
		}
		if record.state == stateDead {
			dead = append(dead, record)
		}
	}
	sortStoredMessages(dead)
	if len(dead) > count {
		dead = dead[:count]
	}

	now := time.Now()
	redriven := 0
	for _, record := range dead {
		if err := ctx.Err(); err != nil {
			if redriven > 0 {
				q.signalLocked()
			}
			return redriven, err
		}

		record.message.Attempt = 0
		record.message.Delay = 0
		record.message.DriverData = nil
		record.state = statePending
		record.availableAt = now
		record.visibilityDeadline = time.Time{}
		record.deliveryToken = ""
		record.order = q.nextOrderLocked()
		redriven++
	}

	if redriven > 0 {
		q.signalLocked()
	}
	return redriven, nil
}

// Queues returns sorted names for queues that currently retain at least one
// active or dead-letter message.
func (q *Queue) Queues(ctx context.Context) ([]string, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if err := q.acquire(ctx); err != nil {
		return nil, err
	}
	defer q.release()

	names := make([]string, 0, len(q.queues))
	for name, state := range q.queues {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if len(state.messages) > 0 {
			names = append(names, name)
		}
	}
	sort.Strings(names)
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return names, nil
}

// Stats returns an exact point-in-time snapshot of the named in-memory queue.
func (q *Queue) Stats(ctx context.Context, queueName string) (queue.QueueStats, error) {
	if err := ctx.Err(); err != nil {
		return queue.QueueStats{}, err
	}
	if err := q.acquire(ctx); err != nil {
		return queue.QueueStats{}, err
	}
	defer q.release()

	stats := queue.QueueStats{Name: queueName}
	state := q.queues[queueName]
	if state == nil {
		return stats, nil
	}

	now := time.Now()
	for _, record := range state.messages {
		if err := ctx.Err(); err != nil {
			return queue.QueueStats{}, err
		}
		switch record.state {
		case statePending:
			if now.Before(record.availableAt) {
				stats.Delayed++
			} else {
				stats.Ready++
			}
		case stateInFlight:
			if now.Before(record.visibilityDeadline) {
				stats.InFlight++
			} else {
				stats.Ready++
			}
		case stateDead:
			stats.Dead++
		}
	}
	return stats, nil
}

type settlement uint8

const (
	settlementAck settlement = iota
	settlementRetry
	settlementDeadLetter
)

func (q *Queue) settle(ctx context.Context, operation settlement, msgs ...*queue.Message) error {
	if len(msgs) == 0 {
		return nil
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := q.acquire(ctx); err != nil {
		return err
	}
	defer q.release()

	failed := make([]queue.ItemError, 0)
	changed := false

	for index, msg := range msgs {
		if err := ctx.Err(); err != nil {
			if changed {
				q.signalLocked()
			}
			return err
		}
		if msg == nil {
			failed = append(failed, queue.ItemError{
				Index: index,
				Msg:   nil,
				Err:   queue.ErrInvalidArgument,
			})
			continue
		}

		itemNow := time.Now()
		record, ok := q.currentDeliveryLocked(msg, itemNow)
		if !ok {
			failed = append(failed, queue.ItemError{
				Index: index,
				Msg:   msg,
				Err:   queue.ErrNotFound,
			})
			continue
		}

		switch operation {
		case settlementAck:
			q.removeLocked(record)
		case settlementRetry:
			q.snapshotMutableFieldsLocked(record, msg)
			record.state = statePending
			record.availableAt = itemNow.Add(record.message.Delay)
			record.visibilityDeadline = time.Time{}
			record.deliveryToken = ""
			record.order = q.nextOrderLocked()
		case settlementDeadLetter:
			q.snapshotMutableFieldsLocked(record, msg)
			record.state = stateDead
			record.availableAt = time.Time{}
			record.visibilityDeadline = time.Time{}
			record.deliveryToken = ""
			record.order = q.nextOrderLocked()
		}
		changed = true
	}

	if changed {
		q.signalLocked()
	}
	return newBatchError(failed)
}

func (q *Queue) popVisibleLocked(
	ctx context.Context,
	queueName string,
	maxMessages int,
	now time.Time,
) ([]*queue.Message, error) {
	state := q.queues[queueName]
	if state == nil {
		return nil, nil
	}

	visible := make([]*storedMessage, 0)
	for _, record := range state.messages {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		switch record.state {
		case statePending:
			if !now.Before(record.availableAt) {
				visible = append(visible, record)
			}
		case stateInFlight:
			if !now.Before(record.visibilityDeadline) {
				visible = append(visible, record)
			}
		}
	}
	if len(visible) == 0 {
		return nil, nil
	}

	sort.Slice(visible, func(i, j int) bool {
		leftTime := visibleAt(visible[i])
		rightTime := visibleAt(visible[j])
		if leftTime.Equal(rightTime) {
			if visible[i].order == visible[j].order {
				return visible[i].message.ID < visible[j].message.ID
			}
			return visible[i].order < visible[j].order
		}
		return leftTime.Before(rightTime)
	})
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if len(visible) > maxMessages {
		visible = visible[:maxMessages]
	}

	messages := make([]*queue.Message, 0, len(visible))
	for _, record := range visible {
		record.message.Attempt++
		record.message.Delay = 0
		record.message.DriverData = nil
		record.state = stateInFlight
		record.availableAt = time.Time{}
		record.deliveryToken = deliveryToken(uuid.NewString())
		record.order = q.nextOrderLocked()

		visibilityTimeout := record.message.VisibilityTimeout
		if visibilityTimeout == 0 {
			visibilityTimeout = q.defaultVisibilityTimeout
		}
		record.visibilityDeadline = now.Add(visibilityTimeout)

		delivery := cloneMessage(&record.message)
		delivery.DriverData = record.deliveryToken
		messages = append(messages, &delivery)
	}
	return messages, nil
}

func (q *Queue) nextVisibleAtLocked(ctx context.Context, queueName string) (time.Time, bool, error) {
	state := q.queues[queueName]
	if state == nil {
		return time.Time{}, false, nil
	}

	var next time.Time
	for _, record := range state.messages {
		if err := ctx.Err(); err != nil {
			return time.Time{}, false, err
		}
		var candidate time.Time
		switch record.state {
		case statePending:
			candidate = record.availableAt
		case stateInFlight:
			candidate = record.visibilityDeadline
		default:
			continue
		}
		if next.IsZero() || candidate.Before(next) {
			next = candidate
		}
	}
	return next, !next.IsZero(), nil
}

func (q *Queue) currentDeliveryLocked(msg *queue.Message, now time.Time) (*storedMessage, bool) {
	token, ok := msg.DriverData.(deliveryToken)
	if !ok || token == "" {
		return nil, false
	}

	record := q.messages[msg.ID]
	if record == nil || record.state != stateInFlight {
		return nil, false
	}
	if record.deliveryToken != token || !now.Before(record.visibilityDeadline) {
		return nil, false
	}
	return record, true
}

func (q *Queue) snapshotMutableFieldsLocked(record *storedMessage, msg *queue.Message) {
	record.message.Delay = msg.Delay
	record.message.Payload = cloneBytes(msg.Payload)
	record.message.Extra = cloneBytes(msg.Extra)
	record.message.DriverData = nil
}

func (q *Queue) removeLocked(record *storedMessage) {
	delete(q.messages, record.message.ID)
	state := q.queues[record.message.QueueName]
	if state == nil {
		return
	}
	delete(state.messages, record.message.ID)
	if len(state.messages) == 0 {
		delete(q.queues, record.message.QueueName)
	}
}

func (q *Queue) queueLocked(name string) *queueState {
	state := q.queues[name]
	if state == nil {
		state = &queueState{messages: make(map[string]*storedMessage)}
		q.queues[name] = state
	}
	return state
}

func (q *Queue) newMessageIDLocked() string {
	for {
		id := uuid.NewString()
		if _, exists := q.messages[id]; !exists {
			return id
		}
	}
}

func (q *Queue) nextOrderLocked() uint64 {
	q.sequence++
	return q.sequence
}

func (q *Queue) acquire(ctx context.Context) error {
	if q.stateLock == nil {
		panic("memory queue must be created with memory.New")
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-q.stateLock:
	}
	if err := ctx.Err(); err != nil {
		q.release()
		return err
	}
	return nil
}

func (q *Queue) release() {
	q.stateLock <- struct{}{}
}

func (q *Queue) signalLocked() {
	close(q.wake)
	q.wake = make(chan struct{})
}

func cloneMessage(msg *queue.Message) queue.Message {
	cloned := *msg
	cloned.Payload = cloneBytes(msg.Payload)
	cloned.Extra = cloneBytes(msg.Extra)
	return cloned
}

func cloneBytes(value []byte) []byte {
	if value == nil {
		return nil
	}
	cloned := make([]byte, len(value))
	copy(cloned, value)
	return cloned
}

func visibleAt(record *storedMessage) time.Time {
	if record.state == statePending {
		return record.availableAt
	}
	return record.visibilityDeadline
}

func sortStoredMessages(messages []*storedMessage) {
	sort.Slice(messages, func(i, j int) bool {
		if messages[i].order == messages[j].order {
			return messages[i].message.ID < messages[j].message.ID
		}
		return messages[i].order < messages[j].order
	})
}

func newBatchError(failed []queue.ItemError) error {
	if len(failed) == 0 {
		return nil
	}
	return &queue.BatchError{Failed: failed}
}

func stopTimer(timer *time.Timer) {
	if !timer.Stop() {
		select {
		case <-timer.C:
		default:
		}
	}
}
