# Hypercube In-Memory Queue

A concurrent, process-local implementation of the [go-hypercube queue contract](https://pkg.go.dev/github.com/go-hypercube/go-hypercube/queue).

It implements the core `queue.Queue` interface and every optional queue capability:

- `queue.VisibilityExtender`
- `queue.RedriveProvider`
- `queue.StatsProvider`

This driver is intended for tests, local development, command-line applications, and single-process deployments that do not require durable or distributed queue storage.

> The installation examples below assume the standalone repository will be named `github.com/go-hypercube/hypercube-queue-memory`. Replace that path if you choose a different repository name.

## Features

- Safe for concurrent use by multiple goroutines
- Delayed message delivery
- Configurable long-polling timeout
- Per-message or default visibility timeouts
- At-least-once delivery with visibility-based redelivery
- Opaque, delivery-specific settlement tokens
- Batch push and settlement operations with partial-error reporting
- Deep copies of message payload and metadata across ownership boundaries
- Duplicate message ID detection across all retained queues
- Explicit dead-letter storage
- Programmatic dead-letter redrive
- Queue discovery and queue statistics
- Prompt context cancellation, including while waiting for internal state access
- No background goroutines and no resources that need to be closed

## Installation

```bash
go get github.com/go-hypercube/hypercube-queue-memory
```

The driver depends on the queue contracts from:

```text
github.com/go-hypercube/go-hypercube/queue
```

## Quick start

```go
package main

import (
    "context"
    "errors"
    "fmt"
    "time"

    "github.com/go-hypercube/go-hypercube/queue"
    memoryqueue "github.com/go-hypercube/hypercube-queue-memory"
)

func main() {
    ctx := context.Background()

    q := memoryqueue.New(
        memoryqueue.WithPollingTimeout(2*time.Second),
        memoryqueue.WithDefaultVisibilityTimeout(30*time.Second),
    )

    msg := &queue.Message{
        QueueName: "emails",
        Namespace: "app",
        JobName:   "send-welcome-email",
        Payload:   []byte(`{"user_id":42}`),
    }

    if err := q.Push(ctx, msg); err != nil {
        panic(err)
    }

    // Push assigns a collision-resistant ID when ID is empty.
    fmt.Println("queued message", msg.ID)

    messages, err := q.Pop(ctx, "emails", 10)
    if errors.Is(err, queue.ErrEmpty) {
        return
    }
    if err != nil {
        panic(err)
    }

    for _, delivery := range messages {
        // Process delivery.Payload here.

        if err := q.Ack(ctx, delivery); err != nil {
            panic(err)
        }
    }
}
```

## Configuration

Create a queue with `New` and zero or more options:

```go
q := memoryqueue.New(
    memoryqueue.WithPollingTimeout(5*time.Second),
    memoryqueue.WithDefaultVisibilityTimeout(time.Minute),
)
```

| Option | Default | Description |
|---|---:|---|
| `WithPollingTimeout` | `1s` | Maximum time an empty `Pop` waits for a visible message before returning `queue.ErrEmpty`. A non-positive value makes an empty `Pop` return immediately after checking for already-visible messages. |
| `WithDefaultVisibilityTimeout` | `30s` | Lease duration used when `Message.VisibilityTimeout` is zero. Non-positive option values are ignored. |

A `Queue` must be created with `New`. Do not copy a queue instance after first use.

## Message lifecycle

```text
Push             -> pending
Pop              -> pending -> in-flight
Ack              -> in-flight -> removed
Retry            -> in-flight -> pending
DeadLetter       -> in-flight -> dead
ExtendVisibility -> in-flight -> in-flight
Redrive          -> dead -> pending
```

### Push

`Push` accepts messages for different queue names in one batch:

```go
err := q.Push(ctx,
    &queue.Message{QueueName: "emails", Payload: []byte("one")},
    &queue.Message{QueueName: "reports", Payload: []byte("two")},
)
```

For each successful message, the driver:

- generates an ID when `Message.ID` is empty;
- resets the driver-owned attempt count to zero;
- ignores caller-provided `DriverData`;
- deep-copies `Payload` and `Extra`;
- applies `Delay` from the time the message is enqueued.

Message IDs are globally unique within a queue instance while the original message is retained, including while it is ready, delayed, in flight, or dead-lettered. A duplicate item fails with `queue.ErrAlreadyExists`.

Once a message is acknowledged and removed, its ID may be reused.

### Pop and visibility

`Pop` waits until one of the following happens:

1. at least one message becomes visible;
2. the context ends; or
3. the configured polling timeout expires.

```go
messages, err := q.Pop(ctx, "emails", 10)
switch {
case err == nil:
    // Process one or more messages.
case errors.Is(err, queue.ErrEmpty):
    // Normal empty-queue condition after the polling timeout.
case errors.Is(err, context.Canceled):
    // Worker is shutting down.
default:
    // Handle an unexpected failure.
}
```

Every delivery receives fresh opaque `DriverData`. `Ack`, `Retry`, `DeadLetter`, and `ExtendVisibility` validate this delivery token rather than relying on the message ID alone.

If a visibility lease expires before settlement:

- the old delivery becomes stale;
- the message becomes visible again;
- the next `Pop` increments `Attempt` and returns a fresh delivery token;
- settlement with the stale delivery returns `queue.ErrNotFound`.

The queue provides at-least-once delivery. Handlers must be idempotent.

## Retrying a message

Set `Delay` before calling `Retry` to schedule a delayed retry. Leaving it at zero makes the message immediately eligible.

```go
delivery.Payload = updatedPayload
delivery.Extra = updatedMetadata
delivery.Delay = 5 * time.Second

if err := q.Retry(ctx, delivery); err != nil {
    // Inspect batch errors as shown below.
}
```

`Retry` snapshots the delivery's current `Payload`, `Extra`, and `Delay`. The attempt count is incremented only when the message is popped again.

The driver never enforces an attempt limit and never decides when a message should be dead-lettered. Retry policy belongs to the caller.

## Extending visibility

Long-running workers can reset a delivery's visibility deadline:

```go
if err := q.ExtendVisibility(ctx, 45*time.Second, delivery); err != nil {
    switch {
    case errors.Is(err, queue.ErrNotFound):
        // The delivery expired, was redelivered, or was already settled.
    case errors.Is(err, queue.ErrInvalidArgument):
        // The extension was not positive.
    }
}
```

The extension is measured from the time `ExtendVisibility` is called. It replaces the previous deadline rather than being added to it.

## Dead-lettering and redrive

`DeadLetter` retains the latest caller-controlled message state for inspection or redrive:

```go
delivery.Extra = []byte(`{"error":"provider rejected request"}`)

if err := q.DeadLetter(ctx, delivery); err != nil {
    // Handle the batch error.
}
```

Move dead-lettered messages back to their active queue with `Redrive`:

```go
moved, err := q.Redrive(ctx, "emails", 100)
if err != nil {
    panic(err)
}
fmt.Printf("redrove %d messages\n", moved)
```

Redrive preserves:

- `ID`
- `QueueName`
- `Namespace`
- `JobName`
- `Payload`
- `Extra`
- `VisibilityTimeout`

It resets `Attempt` and `Delay` to zero, clears the old delivery state, and makes the message immediately visible. The next delivery starts with `Attempt == 1` and fresh `DriverData`.

## Batch errors

`Push`, `Ack`, `Retry`, and `DeadLetter` are batch operations. Valid items continue to be processed when another item fails.

Inspect item-level failures with `errors.As`:

```go
if err := q.Push(ctx, messages...); err != nil {
    var batchErr *queue.BatchError
    if errors.As(err, &batchErr) {
        for _, failure := range batchErr.Failed {
            fmt.Printf(
                "message at index %d failed: %v\n",
                failure.Index,
                failure.Err,
            )
        }
    }
}
```

Common item errors include:

- `queue.ErrInvalidArgument` for a nil message
- `queue.ErrAlreadyExists` for a duplicate pushed ID
- `queue.ErrNotFound` for a stale or already-settled delivery

An empty batch is a successful no-op, even when its context is already canceled.

## Statistics

The driver implements `queue.StatsProvider`:

```go
names, err := q.Queues(ctx)
if err != nil {
    panic(err)
}

for _, name := range names {
    stats, err := q.Stats(ctx, name)
    if err != nil {
        panic(err)
    }

    fmt.Printf(
        "%s: ready=%d delayed=%d in_flight=%d dead=%d\n",
        stats.Name,
        stats.Ready,
        stats.Delayed,
        stats.InFlight,
        stats.Dead,
    )
}
```

The categories are:

- `Ready`: immediately deliverable pending messages and expired leases
- `Delayed`: pending messages whose delay has not elapsed
- `InFlight`: deliveries with an active visibility lease
- `Dead`: retained dead-letter messages

An unknown or empty queue returns `queue.QueueStats{Name: queueName}`.

Although the in-memory driver takes a synchronized point-in-time snapshot, callers should follow the portable queue contract and use statistics only for observability—not correctness decisions.

## Interface checks

The concrete queue implements all queue interfaces directly:

```go
q := memoryqueue.New()

var _ queue.Queue = q
var _ queue.VisibilityExtender = q
var _ queue.RedriveProvider = q
var _ queue.StatsProvider = q
```

## Durability and operational limitations

This driver is intentionally process-local:

- Messages are lost when the process exits or restarts.
- Queue state is not shared between processes or machines.
- It does not provide replication or persistent storage.
- It is unsuitable for independently deployed distributed workers.
- Retained dead-letter messages consume memory until they are redriven and later acknowledged.
- Pending and in-flight messages also consume process memory without a configured capacity limit.
- Message ordering is an implementation detail and must not be relied upon.

Use a durable broker-backed driver for production workloads that need persistence, horizontal worker scaling, failover, or cross-process delivery.

## Testing

Run the package tests:

```bash
make test
```

Run them with the race detector:

```bash
go test -race ./...
```

Run static analysis:

```bash
make lint
```