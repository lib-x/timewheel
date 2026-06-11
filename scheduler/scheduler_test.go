package scheduler

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"
)

func TestNewSchedulerValidatesCallbacksAndWheel(t *testing.T) {
	_, err := NewScheduler[string, int](Options[string, int]{
		Run: func(context.Context, string, int) error { return nil },
	})
	if !errors.Is(err, ErrNilNext) {
		t.Fatalf("missing Next: got %v, want ErrNilNext", err)
	}

	_, err = NewScheduler[string, int](Options[string, int]{
		Next: func(time.Time, string, int) (time.Time, bool, error) {
			return time.Time{}, false, nil
		},
	})
	if !errors.Is(err, ErrNilRun) {
		t.Fatalf("missing Run: got %v, want ErrNilRun", err)
	}

	_, err = NewScheduler[string, int](validOptions[string, int](time.Millisecond), WithWheel(0, 10))
	if !errors.Is(err, ErrInvalidWheel) {
		t.Fatalf("invalid wheel: got %v, want ErrInvalidWheel", err)
	}

	_, err = NewScheduler[string, int](validOptions[string, int](time.Millisecond), nil)
	if !errors.Is(err, ErrInvalidOption) {
		t.Fatalf("nil option: got %v, want ErrInvalidOption", err)
	}

	_, err = NewScheduler[string, int](validOptions[string, int](time.Millisecond), WithWheelOptions(nil))
	if !errors.Is(err, ErrInvalidOption) {
		t.Fatalf("nil wheel option: got %v, want ErrInvalidOption", err)
	}

	_, err = NewScheduler[string, int](validOptions[string, int](time.Millisecond), WithRunTimeout(-time.Millisecond))
	if !errors.Is(err, ErrInvalidOption) {
		t.Fatalf("negative timeout: got %v, want ErrInvalidOption", err)
	}

	_, err = NewScheduler[string, int](validOptions[string, int](time.Millisecond), WithReschedulePolicy(ReschedulePolicy(99)))
	if !errors.Is(err, ErrInvalidOption) {
		t.Fatalf("invalid reschedule policy: got %v, want ErrInvalidOption", err)
	}
}

func TestUpsertBeforeStartRunsAfterStart(t *testing.T) {
	ran := make(chan string, 1)
	s, err := NewScheduler[string, int](Options[string, int]{
		Next: onceAfter[string, int](time.Millisecond),
		Run: func(ctx context.Context, key string, data int) error {
			ran <- key
			return nil
		},
		ReschedulePolicy: NoAutoReschedule,
	}, WithWheel(time.Millisecond, 20))
	if err != nil {
		t.Fatal(err)
	}

	if err := s.Upsert(Item[string, int]{Key: "a", Data: 1}); err != nil {
		t.Fatal(err)
	}
	if got := s.Snapshot()["a"].State; got != StatePending {
		t.Fatalf("state before Start: got %s, want pending", got)
	}
	if err := s.Start(t.Context()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s.Close() })

	select {
	case key := <-ran:
		if key != "a" {
			t.Fatalf("ran key: got %q, want a", key)
		}
	case <-time.After(300 * time.Millisecond):
		t.Fatal("scheduled item did not run")
	}
}

func TestRemoveCancelsPending(t *testing.T) {
	var ran atomic.Bool
	s, err := NewScheduler[string, int](Options[string, int]{
		Next: onceAfter[string, int](50 * time.Millisecond),
		Run: func(ctx context.Context, key string, data int) error {
			ran.Store(true)
			return nil
		},
	}, WithWheel(time.Millisecond, 20))
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Start(t.Context()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s.Close() })

	if err := s.Upsert(Item[string, int]{Key: "a", Data: 1}); err != nil {
		t.Fatal(err)
	}
	if err := s.Remove("a"); err != nil {
		t.Fatal(err)
	}

	time.Sleep(100 * time.Millisecond)
	if ran.Load() {
		t.Fatal("removed pending item ran")
	}
	if _, ok := s.Snapshot()["a"]; ok {
		t.Fatal("removed key still visible in snapshot")
	}
}

func TestRemoveCancelsRunningWhenConfigured(t *testing.T) {
	started := make(chan struct{})
	done := make(chan error, 1)
	s, err := NewScheduler[string, int](Options[string, int]{
		Next: onceAfter[string, int](time.Millisecond),
		Run: func(ctx context.Context, key string, data int) error {
			close(started)
			<-ctx.Done()
			done <- ctx.Err()
			return ctx.Err()
		},
		CancelRunningOnRemove: true,
		ReschedulePolicy:      NoAutoReschedule,
	}, WithWheel(time.Millisecond, 20))
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Start(t.Context()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s.Close() })

	if err := s.Upsert(Item[string, int]{Key: "a", Data: 1}); err != nil {
		t.Fatal(err)
	}
	<-started
	if err := s.Remove("a"); err != nil {
		t.Fatal(err)
	}

	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("running ctx: got %v, want context.Canceled", err)
		}
	case <-time.After(300 * time.Millisecond):
		t.Fatal("running job was not canceled")
	}
}

func TestReplaceInvalidatesStaleCompletion(t *testing.T) {
	firstStarted := make(chan struct{})
	releaseFirst := make(chan struct{})
	finished := make(chan int, 2)
	runCount := atomic.Int32{}

	s, err := NewScheduler[string, int](Options[string, int]{
		Next: func(now time.Time, key string, data int) (time.Time, bool, error) {
			return now.Add(time.Millisecond), true, nil
		},
		Run: func(ctx context.Context, key string, data int) error {
			if runCount.Add(1) == 1 {
				close(firstStarted)
				<-releaseFirst
			}
			finished <- data
			return nil
		},
		ReschedulePolicy: RescheduleAfterFinish,
	}, WithWheel(time.Millisecond, 20))
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Start(t.Context()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s.Close() })

	if err := s.Upsert(Item[string, int]{Key: "a", Data: 1}); err != nil {
		t.Fatal(err)
	}
	<-firstStarted
	if err := s.Upsert(Item[string, int]{Key: "a", Data: 2}); err != nil {
		t.Fatal(err)
	}
	close(releaseFirst)

	deadline := time.After(300 * time.Millisecond)
	for {
		select {
		case data := <-finished:
			if data == 2 {
				return
			}
		case <-deadline:
			t.Fatal("replacement generation did not run")
		}
	}
}

func TestReplaceAllRemovesMissingKeys(t *testing.T) {
	var ranA atomic.Bool
	s, err := NewScheduler[string, int](Options[string, int]{
		Next: onceAfter[string, int](50 * time.Millisecond),
		Run: func(ctx context.Context, key string, data int) error {
			if key == "a" {
				ranA.Store(true)
			}
			return nil
		},
	}, WithWheel(time.Millisecond, 20))
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Start(t.Context()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s.Close() })

	if err := s.ReplaceAll([]Item[string, int]{
		{Key: "a", Data: 1},
		{Key: "b", Data: 2},
	}); err != nil {
		t.Fatal(err)
	}
	if err := s.ReplaceAll([]Item[string, int]{
		{Key: "b", Data: 3},
	}); err != nil {
		t.Fatal(err)
	}

	time.Sleep(100 * time.Millisecond)
	if ranA.Load() {
		t.Fatal("removed key ran after ReplaceAll")
	}
	if _, ok := s.Snapshot()["a"]; ok {
		t.Fatal("removed key still visible after ReplaceAll")
	}
}

func TestRescheduleAfterFinishDoesNotOverlap(t *testing.T) {
	started := make(chan struct{}, 2)
	release := make(chan struct{})

	s, err := NewScheduler[string, int](Options[string, int]{
		Next: onceAfter[string, int](time.Millisecond),
		Run: func(ctx context.Context, key string, data int) error {
			started <- struct{}{}
			<-release
			return nil
		},
		ReschedulePolicy: RescheduleAfterFinish,
	}, WithWheel(time.Millisecond, 20))
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Start(t.Context()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s.Close() })

	if err := s.Upsert(Item[string, int]{Key: "a", Data: 1}); err != nil {
		t.Fatal(err)
	}
	<-started
	time.Sleep(30 * time.Millisecond)
	select {
	case <-started:
		t.Fatal("RescheduleAfterFinish overlapped")
	default:
	}
	close(release)
}

func TestRescheduleBeforeRunSchedulesWhileRunning(t *testing.T) {
	started := make(chan struct{}, 2)
	release := make(chan struct{})

	s, err := NewScheduler[string, int](Options[string, int]{
		Next: onceAfter[string, int](time.Millisecond),
		Run: func(ctx context.Context, key string, data int) error {
			started <- struct{}{}
			<-release
			return nil
		},
		ReschedulePolicy: RescheduleBeforeRun,
	}, WithWheel(time.Millisecond, 20))
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Start(t.Context()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s.Close() })

	if err := s.Upsert(Item[string, int]{Key: "a", Data: 1}); err != nil {
		t.Fatal(err)
	}
	<-started
	deadline := time.After(300 * time.Millisecond)
	for {
		runtime := s.Snapshot()["a"]
		if runtime.State == StateRunning && runtime.NextRunAt != nil {
			close(release)
			return
		}
		select {
		case <-deadline:
			t.Fatal("expected next run to be scheduled while first run was running")
		default:
			time.Sleep(time.Millisecond)
		}
	}
}

func TestNoAutoRescheduleDisablesAfterRun(t *testing.T) {
	ran := make(chan struct{})
	s, err := NewScheduler[string, int](Options[string, int]{
		Next: onceAfter[string, int](time.Millisecond),
		Run: func(ctx context.Context, key string, data int) error {
			close(ran)
			return nil
		},
		ReschedulePolicy: NoAutoReschedule,
	}, WithWheel(time.Millisecond, 20))
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Start(t.Context()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s.Close() })

	if err := s.Upsert(Item[string, int]{Key: "a", Data: 1}); err != nil {
		t.Fatal(err)
	}
	<-ran

	deadline := time.After(300 * time.Millisecond)
	for {
		if got := s.Snapshot()["a"].State; got == StateDisabled {
			return
		}
		select {
		case <-deadline:
			t.Fatalf("state after run: got %s, want disabled", s.Snapshot()["a"].State)
		default:
			time.Sleep(time.Millisecond)
		}
	}
}

func TestInvalidAndDisabledSnapshot(t *testing.T) {
	onInvalid := make(chan error, 1)
	s, err := NewScheduler[string, int](Options[string, int]{
		Next: func(now time.Time, key string, data int) (time.Time, bool, error) {
			switch key {
			case "invalid":
				return time.Time{}, false, errors.New("bad schedule")
			case "disabled":
				return time.Time{}, false, nil
			default:
				return now.Add(time.Hour), true, nil
			}
		},
		Run: func(ctx context.Context, key string, data int) error { return nil },
		OnInvalid: func(key string, data int, err error) {
			onInvalid <- err
		},
	}, WithWheel(time.Millisecond, 20))
	if err != nil {
		t.Fatal(err)
	}

	if err := s.Upsert(Item[string, int]{Key: "invalid", Data: 1}); err != nil {
		t.Fatal(err)
	}
	if err := s.Upsert(Item[string, int]{Key: "disabled", Data: 2}); err != nil {
		t.Fatal(err)
	}

	snapshot := s.Snapshot()
	if got := snapshot["invalid"].State; got != StateInvalid {
		t.Fatalf("invalid state: got %s, want invalid", got)
	}
	if snapshot["invalid"].LastError == nil {
		t.Fatal("invalid snapshot missing LastError")
	}
	if got := snapshot["disabled"].State; got != StateDisabled {
		t.Fatalf("disabled state: got %s, want disabled", got)
	}
	select {
	case err := <-onInvalid:
		if err == nil {
			t.Fatal("OnInvalid got nil error")
		}
	default:
		t.Fatal("OnInvalid was not called")
	}
}

func TestCloseWaitsForRunningWhenConfigured(t *testing.T) {
	started := make(chan struct{})
	release := make(chan struct{})
	finished := make(chan struct{})

	s, err := NewScheduler[string, int](Options[string, int]{
		Next: onceAfter[string, int](time.Millisecond),
		Run: func(ctx context.Context, key string, data int) error {
			close(started)
			<-release
			close(finished)
			return nil
		},
	}, WithWheel(time.Millisecond, 20), WithWaitRunningOnClose(true), WithReschedulePolicy(NoAutoReschedule))
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Start(t.Context()); err != nil {
		t.Fatal(err)
	}
	if err := s.Upsert(Item[string, int]{Key: "a", Data: 1}); err != nil {
		t.Fatal(err)
	}
	<-started

	closeDone := make(chan error, 1)
	go func() {
		closeDone <- s.Close()
	}()

	select {
	case <-closeDone:
		t.Fatal("Close returned before running job completed")
	case <-time.After(30 * time.Millisecond):
	}

	close(release)
	select {
	case err := <-closeDone:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(300 * time.Millisecond):
		t.Fatal("Close did not return after running job completed")
	}
	<-finished
}

func TestCloseClearsPendingSnapshot(t *testing.T) {
	for _, started := range []bool{false, true} {
		t.Run(map[bool]string{false: "before start", true: "after start"}[started], func(t *testing.T) {
			s, err := NewScheduler[string, int](Options[string, int]{
				Next: onceAfter[string, int](time.Hour),
				Run:  func(ctx context.Context, key string, data int) error { return nil },
			}, WithWheel(time.Millisecond, 20))
			if err != nil {
				t.Fatal(err)
			}
			if started {
				if err := s.Start(t.Context()); err != nil {
					t.Fatal(err)
				}
			}
			if err := s.Upsert(Item[string, int]{Key: "a", Data: 1}); err != nil {
				t.Fatal(err)
			}
			if err := s.Close(); err != nil {
				t.Fatal(err)
			}

			runtime := s.Snapshot()["a"]
			if runtime.State != StateDisabled {
				t.Fatalf("state after Close: got %s, want disabled", runtime.State)
			}
			if runtime.NextRunAt != nil {
				t.Fatalf("NextRunAt after Close: got %v, want nil", *runtime.NextRunAt)
			}
		})
	}
}

func TestRunTimeoutCancelsJobContext(t *testing.T) {
	done := make(chan error, 1)
	s, err := NewScheduler[string, int](Options[string, int]{
		Next: onceAfter[string, int](time.Millisecond),
		Run: func(ctx context.Context, key string, data int) error {
			<-ctx.Done()
			done <- ctx.Err()
			return ctx.Err()
		},
	}, WithWheel(time.Millisecond, 20), WithRunTimeout(10*time.Millisecond), WithReschedulePolicy(NoAutoReschedule))
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Start(t.Context()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s.Close() })

	if err := s.Upsert(Item[string, int]{Key: "a", Data: 1}); err != nil {
		t.Fatal(err)
	}

	select {
	case err := <-done:
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("run timeout: got %v, want deadline exceeded", err)
		}
	case <-time.After(300 * time.Millisecond):
		t.Fatal("run context was not timed out")
	}
}

func validOptions[K comparable, T any](delay time.Duration) Options[K, T] {
	return Options[K, T]{
		Next: onceAfter[K, T](delay),
		Run:  func(context.Context, K, T) error { return nil },
	}
}

func onceAfter[K comparable, T any](delay time.Duration) NextFunc[K, T] {
	return func(now time.Time, key K, data T) (time.Time, bool, error) {
		return now.Add(delay), true, nil
	}
}
