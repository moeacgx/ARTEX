package agent

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Autumn-27/norma/harness"
)

func TestPlannerRoundTimeoutDefaultsAndTaskClamp(t *testing.T) {
	t.Setenv("ARTEX_PLANNER_ROUND_TIMEOUT", "")
	if got, _ := plannerRoundDuration(0); got != 10*time.Minute {
		t.Fatalf("default planner round timeout=%s", got)
	}
	deadline := time.Now().Add(30 * time.Second).Unix()
	got, clamped := plannerRoundDuration(deadline)
	if !clamped || got < 28*time.Second || got > 31*time.Second {
		t.Fatalf("task deadline clamp=%s clamped=%v", got, clamped)
	}
	if got, clamped = plannerRoundDuration(time.Now().Add(-time.Second).Unix()); !clamped || got != time.Nanosecond {
		t.Fatalf("expired task budget=%s clamped=%v", got, clamped)
	}
	t.Setenv("ARTEX_PLANNER_ROUND_TIMEOUT", "50ms")
	if got, _ = plannerRoundDuration(0); got != 50*time.Millisecond {
		t.Fatalf("configured planner budget=%s", got)
	}
}

func TestPlannerCleanupFailureRetainsOperation(t *testing.T) {
	var retained atomic.Bool
	ctx := WithBrowserCleanupFailure(context.Background(), func() { retained.Store(true) })
	reportPlannerCleanupFailure(ctx, errors.Join(harness.ErrHardDeadlineExceeded, harness.ErrHardDeadlineCleanup))
	if !retained.Load() {
		t.Fatal("cleanup failure did not retain operation barrier")
	}
}

func TestPlannerNonCooperativeRoundRetainsBarrier(t *testing.T) {
	var retained atomic.Bool
	ctx := WithBrowserCleanupFailure(context.Background(), func() { retained.Store(true) })
	ctx, cancel := context.WithTimeoutCause(ctx, 20*time.Millisecond, harness.ErrHardDeadlineExceeded)
	defer cancel()
	release, exited := make(chan struct{}), make(chan struct{})
	var released atomic.Bool
	defer func() {
		if !released.Swap(true) {
			close(release)
		}
	}()
	_, _, err := waitPlannerRound(ctx, 20*time.Millisecond, func() (bool, string, error) {
		<-release
		close(exited)
		return true, "late", nil
	})
	if !errors.Is(err, ErrPlannerCleanup) || retained.Load() {
		t.Fatalf("err=%v retained=%v", err, retained.Load())
	}
	select {
	case <-exited:
		t.Fatal("reported exit before actual execution ended")
	default:
	}
	if !released.Swap(true) {
		close(release)
	}
	if finalErr := AwaitPlannerExit(context.Background(), err); !errors.Is(finalErr, harness.ErrHardDeadlineExceeded) {
		t.Fatalf("resolved round lost original deadline: %v", finalErr)
	}
	select {
	case <-exited:
	case <-time.After(time.Second):
		t.Fatal("late execution did not exit")
	}
}

func TestPlannerBackgroundSharesDeadlineAndRequiresExit(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	ctx, work := withPlannerBackground(ctx)
	background, exit := compactionBackground(ctx)
	waited := make(chan struct{})
	go func() { work.wg.Wait(); close(waited) }()
	cancel()
	if background.Err() == nil {
		t.Fatal("background compaction escaped planner cancellation")
	}
	select {
	case <-waited:
		t.Fatal("background canceled but not actually exited")
	default:
	}
	exit()
	select {
	case <-waited:
	case <-time.After(time.Second):
		t.Fatal("background exit not joined")
	}
}

func TestAwaitPlannerExitWaitsEveryJoinedPending(t *testing.T) {
	for _, first := range []int{0, 1} {
		t.Run(map[int]string{0: "first_finishes_first", 1: "second_finishes_first"}[first], func(t *testing.T) {
			waits := []chan error{make(chan error, 1), make(chan error, 1)}
			err := errors.Join(context.DeadlineExceeded,
				&plannerExitPending{done: waits[0]},
				&plannerExitPending{done: waits[1]})
			resolved := make(chan error, 1)
			go func() { resolved <- AwaitPlannerExit(context.Background(), err) }()
			waits[first] <- nil
			select {
			case got := <-resolved:
				t.Fatalf("returned while another cleanup was live: %v", got)
			case <-time.After(30 * time.Millisecond):
			}
			waits[1-first] <- nil
			select {
			case got := <-resolved:
				if !errors.Is(got, context.DeadlineExceeded) {
					t.Fatalf("resolved error lost round deadline: %v", got)
				}
			case <-time.After(time.Second):
				t.Fatal("joined planner cleanups did not resolve")
			}
		})
	}
}

func TestAwaitPlannerExitWaitsAllBeforeReturningFailure(t *testing.T) {
	first, second := make(chan error, 1), make(chan error, 1)
	err := errors.Join(&plannerExitPending{done: first}, &plannerExitPending{done: second})
	resolved := make(chan error, 1)
	go func() { resolved <- AwaitPlannerExit(context.Background(), err) }()
	first <- errors.Join(ErrPlannerCleanup, errors.New("MCP cleanup failed"))
	select {
	case got := <-resolved:
		t.Fatalf("failure returned before the other cleanup exited: %v", got)
	case <-time.After(30 * time.Millisecond):
	}
	second <- nil
	select {
	case got := <-resolved:
		if !errors.Is(got, ErrPlannerCleanup) {
			t.Fatalf("final cleanup failure was lost: %v", got)
		}
	case <-time.After(time.Second):
		t.Fatal("joined cleanup failure did not resolve")
	}
}
