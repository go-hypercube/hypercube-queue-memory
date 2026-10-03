package memory

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestCanceledOperationDoesNotWaitForStateLock(t *testing.T) {
	q := New()
	require.NoError(t, q.acquire(context.Background()))
	defer q.release()

	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Millisecond)
	defer cancel()

	started := time.Now()
	stats, err := q.Stats(ctx, "jobs")
	require.ErrorIs(t, err, context.DeadlineExceeded)
	require.Empty(t, stats)
	require.Less(t, time.Since(started), 500*time.Millisecond)
}
