package timewheel

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type logEntry struct {
	level string
	msg   string
	args  []any
}

type recordingLogger struct {
	entries chan logEntry
}

func newRecordingLogger(size int) *recordingLogger {
	return &recordingLogger{entries: make(chan logEntry, size)}
}

func (l *recordingLogger) Info(msg string, args ...any) {
	l.entries <- logEntry{level: "info", msg: msg, args: append([]any(nil), args...)}
}

func (l *recordingLogger) Warn(msg string, args ...any) {
	l.entries <- logEntry{level: "warn", msg: msg, args: append([]any(nil), args...)}
}

func startWheel[T any](t *testing.T, tw *TimeWheel[T]) {
	t.Helper()
	if err := tw.Start(t.Context()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = tw.Close() })
}

func waitLog(t *testing.T, logger *recordingLogger, level, msg string) logEntry {
	t.Helper()

	timer := time.NewTimer(500 * time.Millisecond)
	defer timer.Stop()

	for {
		select {
		case entry := <-logger.entries:
			if entry.level == level && entry.msg == msg {
				return entry
			}
		case <-timer.C:
			t.Fatalf("timed out waiting for %s log %q", level, msg)
		}
	}
}

func waitEvent[T any](t *testing.T, events <-chan JobEvent[T]) JobEvent[T] {
	t.Helper()
	select {
	case event := <-events:
		return event
	case <-time.After(500 * time.Millisecond):
		t.Fatal("timed out waiting for job event")
		var zero JobEvent[T]
		return zero
	}
}

func logArg(args []any, key string) (any, bool) {
	for i := 0; i+1 < len(args); i += 2 {
		if args[i] == key {
			return args[i+1], true
		}
	}
	return nil, false
}

func TestStartLifecycleErrors(t *testing.T) {
	tw, err := New[struct{}](10*time.Millisecond, 10, func(struct{}) {})
	if err != nil {
		t.Fatal(err)
	}
	if err := tw.Start(nil); !errors.Is(err, ErrNilContext) {
		t.Fatalf("Start(nil): got %v, want ErrNilContext", err)
	}
	if err := tw.Stop(); !errors.Is(err, ErrNotStarted) {
		t.Fatalf("Stop before Start: got %v, want ErrNotStarted", err)
	}
	if err := tw.Start(t.Context()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	if err := tw.Start(t.Context()); !errors.Is(err, ErrRunning) {
		t.Fatalf("double Start: got %v, want ErrRunning", err)
	}
	if err := tw.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if err := tw.Close(); err != nil {
		t.Fatalf("second Close: %v", err)
	}
	if err := tw.Start(t.Context()); !errors.Is(err, ErrClosed) {
		t.Fatalf("Start after Close: got %v, want ErrClosed", err)
	}
}

func TestAddAndRemoveRequireRunning(t *testing.T) {
	tw, err := New[struct{}](10*time.Millisecond, 10, func(struct{}) {})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := tw.AddTimer(time.Millisecond, struct{}{}); !errors.Is(err, ErrNotStarted) {
		t.Fatalf("Add before Start: got %v, want ErrNotStarted", err)
	}
	if err := tw.RemoveTimer(TimerID(1)); !errors.Is(err, ErrNotStarted) {
		t.Fatalf("Remove before Start: got %v, want ErrNotStarted", err)
	}
	if err := tw.Start(t.Context()); err != nil {
		t.Fatal(err)
	}
	if err := tw.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := tw.AddTimer(time.Millisecond, struct{}{}); !errors.Is(err, ErrClosed) {
		t.Fatalf("Add after Close: got %v, want ErrClosed", err)
	}
	if err := tw.RemoveTimer(TimerID(1)); !errors.Is(err, ErrClosed) {
		t.Fatalf("Remove after Close: got %v, want ErrClosed", err)
	}
}

func TestContextCancelClosesWheel(t *testing.T) {
	tw, err := New[struct{}](10*time.Millisecond, 10, func(struct{}) {})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	if err := tw.Start(ctx); err != nil {
		t.Fatal(err)
	}
	cancel()
	tw.Wait()

	if _, err := tw.AddTimer(time.Millisecond, struct{}{}); !errors.Is(err, ErrClosed) {
		t.Fatalf("Add after context cancel: got %v, want ErrClosed", err)
	}
	if err := tw.Start(t.Context()); !errors.Is(err, ErrClosed) {
		t.Fatalf("Start after context cancel: got %v, want ErrClosed", err)
	}
}

func TestAddTimerFires(t *testing.T) {
	var fired atomic.Bool
	tw, err := New[struct{}](10*time.Millisecond, 100, func(struct{}) {
		fired.Store(true)
	})
	if err != nil {
		t.Fatal(err)
	}
	startWheel(t, tw)

	if _, err := tw.AddTimer(50*time.Millisecond, struct{}{}); err != nil {
		t.Fatal(err)
	}

	time.Sleep(200 * time.Millisecond)
	if !fired.Load() {
		t.Fatal("timer did not fire")
	}
}

func TestAddTimerFiresOnce(t *testing.T) {
	var count atomic.Int32
	tw, err := New[struct{}](10*time.Millisecond, 100, func(struct{}) {
		count.Add(1)
	})
	if err != nil {
		t.Fatal(err)
	}
	startWheel(t, tw)

	if _, err := tw.AddTimer(50*time.Millisecond, struct{}{}); err != nil {
		t.Fatal(err)
	}
	time.Sleep(300 * time.Millisecond)

	if n := count.Load(); n != 1 {
		t.Fatalf("expected 1 execution, got %d", n)
	}
}

func TestRemoveTimerAcceptedBeforeFire(t *testing.T) {
	fired := make(chan struct{}, 1)
	tw, err := New[struct{}](10*time.Millisecond, 10, func(struct{}) { fired <- struct{}{} })
	if err != nil {
		t.Fatal(err)
	}
	startWheel(t, tw)

	id, err := tw.AddTimer(50*time.Millisecond, struct{}{})
	if err != nil {
		t.Fatal(err)
	}
	if err := tw.RemoveTimer(id); err != nil {
		t.Fatal(err)
	}
	if _, ok := tw.NextFireTime(id); ok {
		t.Fatal("removed timer still visible")
	}
	time.Sleep(120 * time.Millisecond)
	select {
	case <-fired:
		t.Fatal("timer fired after RemoveTimer returned")
	default:
	}
}

func TestRemoveTimerUnknownIsNoop(t *testing.T) {
	tw, err := New[struct{}](10*time.Millisecond, 100, func(struct{}) {})
	if err != nil {
		t.Fatal(err)
	}
	startWheel(t, tw)

	if err := tw.RemoveTimer(TimerID(999)); err != nil {
		t.Fatal(err)
	}
}

func TestDeleteIndexUpdatesSwappedTimer(t *testing.T) {
	tw, err := New[int](time.Hour, 4, func(int) {})
	if err != nil {
		t.Fatal(err)
	}
	startWheel(t, tw)

	first, err := tw.AddTimer(time.Hour, 1)
	if err != nil {
		t.Fatal(err)
	}
	second, err := tw.AddTimer(time.Hour, 2)
	if err != nil {
		t.Fatal(err)
	}
	if err := tw.RemoveTimer(first); err != nil {
		t.Fatal(err)
	}
	if err := tw.RemoveTimer(second); err != nil {
		t.Fatal(err)
	}
	if pending := tw.Stats().Pending; pending != 0 {
		t.Fatalf("Pending after removing swapped timers: got %d, want 0", pending)
	}
}

func TestAddTimerFunc(t *testing.T) {
	done := make(chan struct{})
	tw, err := New[struct{}](10*time.Millisecond, 100, nil)
	if err != nil {
		t.Fatal(err)
	}
	startWheel(t, tw)

	if _, err := tw.AddTimerFunc(50*time.Millisecond, func() { close(done) }); err != nil {
		t.Fatal(err)
	}

	select {
	case <-done:
	case <-time.After(300 * time.Millisecond):
		t.Fatal("AddTimerFunc did not fire")
	}
}

func TestAddTimerWithJob(t *testing.T) {
	done := make(chan int, 1)
	tw, err := New[int](10*time.Millisecond, 100, func(int) {
		t.Error("default job should not be called")
	})
	if err != nil {
		t.Fatal(err)
	}
	startWheel(t, tw)

	if _, err := tw.AddTimerWithJob(50*time.Millisecond, 42, func(n int) { done <- n }); err != nil {
		t.Fatal(err)
	}

	select {
	case v := <-done:
		if v != 42 {
			t.Fatalf("expected 42, got %d", v)
		}
	case <-time.After(300 * time.Millisecond):
		t.Fatal("per-task job did not fire")
	}
}

func TestContextJobReportsError(t *testing.T) {
	events := make(chan JobEvent[int], 1)
	want := errors.New("job failed")
	tw, err := New[int](time.Millisecond, 10, nil, WithJobObserver[int](func(e JobEvent[int]) {
		events <- e
	}))
	if err != nil {
		t.Fatal(err)
	}
	startWheel(t, tw)

	_, err = tw.AddTimerWithContextJob(time.Millisecond, 7, func(context.Context, int) error {
		return want
	})
	if err != nil {
		t.Fatal(err)
	}
	e := waitEvent(t, events)
	if !errors.Is(e.Err, want) || e.TimerID == 0 || e.Duration < 0 {
		t.Fatalf("unexpected event: %+v", e)
	}
}

func TestFixedRateMayOverlap(t *testing.T) {
	started := make(chan struct{}, 4)
	release := make(chan struct{})
	tw, err := New[struct{}](5*time.Millisecond, 20, nil)
	if err != nil {
		t.Fatal(err)
	}
	startWheel(t, tw)

	id, err := tw.AddRepeatingTimerWithJob(5*time.Millisecond, struct{}{}, func(struct{}) {
		started <- struct{}{}
		<-release
	}, RepeatOptions{Mode: FixedRate})
	if err != nil {
		t.Fatal(err)
	}
	defer close(release)
	defer func() { _ = tw.RemoveTimer(id) }()

	<-started
	select {
	case <-started:
	case <-time.After(80 * time.Millisecond):
		t.Fatal("FixedRate did not overlap while previous job was running")
	}
}

func TestFixedDelayDoesNotOverlap(t *testing.T) {
	started := make(chan struct{}, 4)
	release := make(chan struct{})
	tw, err := New[struct{}](5*time.Millisecond, 20, nil)
	if err != nil {
		t.Fatal(err)
	}
	startWheel(t, tw)

	id, err := tw.AddRepeatingTimerWithJob(5*time.Millisecond, struct{}{}, func(struct{}) {
		started <- struct{}{}
		<-release
	}, RepeatOptions{Mode: FixedDelay})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tw.RemoveTimer(id) }()

	<-started
	time.Sleep(30 * time.Millisecond)
	select {
	case <-started:
		t.Fatal("FixedDelay overlapped")
	default:
	}
	close(release)
}

func TestSkipIfRunningSkips(t *testing.T) {
	events := make(chan JobEvent[struct{}], 8)
	started := make(chan struct{}, 1)
	release := make(chan struct{})
	tw, err := New[struct{}](
		5*time.Millisecond,
		20,
		nil,
		WithJobObserver[struct{}](func(e JobEvent[struct{}]) { events <- e }),
	)
	if err != nil {
		t.Fatal(err)
	}
	startWheel(t, tw)

	id, err := tw.AddRepeatingTimerWithJob(5*time.Millisecond, struct{}{}, func(struct{}) {
		started <- struct{}{}
		<-release
	}, RepeatOptions{Mode: SkipIfRunning})
	if err != nil {
		t.Fatal(err)
	}
	defer close(release)
	defer func() { _ = tw.RemoveTimer(id) }()

	<-started
	deadline := time.After(120 * time.Millisecond)
	for tw.Stats().Skipped == 0 {
		select {
		case <-deadline:
			t.Fatal("expected skipped executions")
		default:
			time.Sleep(time.Millisecond)
		}
	}
}

func TestStats(t *testing.T) {
	tw, err := New[struct{}](10*time.Millisecond, 100, func(struct{}) {})
	if err != nil {
		t.Fatal(err)
	}
	startWheel(t, tw)

	const n = 5
	for range n {
		if _, err := tw.AddTimer(50*time.Millisecond, struct{}{}); err != nil {
			t.Fatal(err)
		}
	}

	time.Sleep(200 * time.Millisecond)

	s := tw.Stats()
	if s.Executed != n {
		t.Fatalf("Stats.Executed: want %d, got %d", n, s.Executed)
	}
	if s.Pending != 0 {
		t.Fatalf("Stats.Pending: want 0, got %d", s.Pending)
	}
}

func TestStatsPendingDoesNotWaitForJobCompletion(t *testing.T) {
	started := make(chan struct{})
	release := make(chan struct{})

	tw, err := New[struct{}](5*time.Millisecond, 10, func(struct{}) {
		close(started)
		<-release
	})
	if err != nil {
		t.Fatal(err)
	}
	startWheel(t, tw)

	if _, err := tw.AddTimer(time.Millisecond, struct{}{}); err != nil {
		t.Fatal(err)
	}

	select {
	case <-started:
	case <-time.After(300 * time.Millisecond):
		t.Fatal("timer job did not start")
	}
	defer close(release)

	s := tw.Stats()
	if s.Pending != 0 {
		t.Fatalf("Stats.Pending while job is running: want 0, got %d", s.Pending)
	}
	if s.Executed != 1 {
		t.Fatalf("Stats.Executed while job is running: want 1, got %d", s.Executed)
	}
	if s.Running != 1 {
		t.Fatalf("Stats.Running while job is running: want 1, got %d", s.Running)
	}
}

func TestErrorHandler(t *testing.T) {
	recovered := make(chan any, 1)
	tw, err := New[struct{}](
		10*time.Millisecond, 100,
		func(struct{}) { panic("boom") },
		WithErrorHandler[struct{}](func(r any) { recovered <- r }),
	)
	if err != nil {
		t.Fatal(err)
	}
	startWheel(t, tw)

	if _, err := tw.AddTimer(30*time.Millisecond, struct{}{}); err != nil {
		t.Fatal(err)
	}

	select {
	case r := <-recovered:
		if r != "boom" {
			t.Fatalf("unexpected recovered value: %v", r)
		}
	case <-time.After(300 * time.Millisecond):
		t.Fatal("error handler was not called")
	}
}

func TestNewInvalidArgs(t *testing.T) {
	if _, err := New[int](0, 10, nil); err == nil {
		t.Error("expected error for zero interval")
	}
	if _, err := New[int](time.Second, 0, nil); err == nil {
		t.Error("expected error for zero slotNum")
	}
	if _, err := New[int](time.Second, 1, nil, WithWorkerPool[int](1, -1, Block)); err == nil {
		t.Error("expected error for negative queue size")
	}
	if _, err := New[int](time.Second, 1, nil, WithWorkerPool[int](0, -1, Block)); err == nil {
		t.Error("expected error for negative queue size with disabled workers")
	}
}

func TestWorkerPoolDropPolicy(t *testing.T) {
	events := make(chan JobEvent[struct{}], 8)
	block := make(chan struct{})
	tw, err := New[struct{}](
		time.Millisecond,
		10,
		func(struct{}) { <-block },
		WithWorkerPool[struct{}](1, 0, Drop),
		WithJobObserver[struct{}](func(e JobEvent[struct{}]) { events <- e }),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer close(block)
	startWheel(t, tw)

	for range 3 {
		if _, err := tw.AddTimer(time.Millisecond, struct{}{}); err != nil {
			t.Fatal(err)
		}
	}
	deadline := time.After(300 * time.Millisecond)
	for tw.Stats().Dropped == 0 {
		select {
		case <-deadline:
			t.Fatal("expected dropped jobs")
		default:
			time.Sleep(time.Millisecond)
		}
	}
}

func TestDropFixedDelayRepeatingRecovers(t *testing.T) {
	blockStarted := make(chan struct{})
	releaseBlocker := make(chan struct{})
	var count atomic.Int32

	tw, err := New[struct{}](
		time.Millisecond,
		10,
		nil,
		WithWorkerPool[struct{}](1, 0, Drop),
	)
	if err != nil {
		t.Fatal(err)
	}
	startWheel(t, tw)

	if _, err := tw.AddTimerWithJob(time.Millisecond, struct{}{}, func(struct{}) {
		close(blockStarted)
		<-releaseBlocker
	}); err != nil {
		t.Fatal(err)
	}
	<-blockStarted

	id, err := tw.AddRepeatingTimerWithJob(time.Millisecond, struct{}{}, func(struct{}) {
		count.Add(1)
	}, RepeatOptions{Mode: FixedDelay})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tw.RemoveTimer(id) }()

	deadline := time.After(300 * time.Millisecond)
	for tw.Stats().Dropped == 0 {
		select {
		case <-deadline:
			t.Fatal("expected repeating execution to be dropped while worker is busy")
		default:
			time.Sleep(time.Millisecond)
		}
	}

	close(releaseBlocker)

	deadline = time.After(300 * time.Millisecond)
	for count.Load() == 0 {
		select {
		case <-deadline:
			t.Fatal("fixed-delay repeating timer never recovered after dropped execution")
		default:
			time.Sleep(time.Millisecond)
		}
	}
}

func TestDropSkipIfRunningRepeatingRecovers(t *testing.T) {
	blockStarted := make(chan struct{})
	releaseBlocker := make(chan struct{})
	var count atomic.Int32

	tw, err := New[struct{}](
		time.Millisecond,
		10,
		nil,
		WithWorkerPool[struct{}](1, 0, Drop),
	)
	if err != nil {
		t.Fatal(err)
	}
	startWheel(t, tw)

	if _, err := tw.AddTimerWithJob(time.Millisecond, struct{}{}, func(struct{}) {
		close(blockStarted)
		<-releaseBlocker
	}); err != nil {
		t.Fatal(err)
	}
	<-blockStarted

	id, err := tw.AddRepeatingTimerWithJob(time.Millisecond, struct{}{}, func(struct{}) {
		count.Add(1)
	}, RepeatOptions{Mode: SkipIfRunning})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tw.RemoveTimer(id) }()

	deadline := time.After(300 * time.Millisecond)
	for tw.Stats().Dropped == 0 {
		select {
		case <-deadline:
			t.Fatal("expected repeating execution to be dropped while worker is busy")
		default:
			time.Sleep(time.Millisecond)
		}
	}

	close(releaseBlocker)

	deadline = time.After(300 * time.Millisecond)
	for count.Load() == 0 {
		select {
		case <-deadline:
			t.Fatal("skip-if-running repeating timer never recovered after dropped execution")
		default:
			time.Sleep(time.Millisecond)
		}
	}
}

func TestWorkerPoolRunInlinePolicy(t *testing.T) {
	var count atomic.Int32
	block := make(chan struct{})
	tw, err := New[struct{}](
		time.Millisecond,
		10,
		func(struct{}) {
			count.Add(1)
			if count.Load() == 1 {
				<-block
			}
		},
		WithWorkerPool[struct{}](1, 0, RunInline),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer close(block)
	startWheel(t, tw)

	for range 2 {
		if _, err := tw.AddTimer(time.Millisecond, struct{}{}); err != nil {
			t.Fatal(err)
		}
	}
	deadline := time.After(300 * time.Millisecond)
	for count.Load() < 2 {
		select {
		case <-deadline:
			t.Fatal("expected inline execution")
		default:
			time.Sleep(time.Millisecond)
		}
	}
}

func TestRunInlineFixedDelayDoesNotDeadlockWhenCommandChannelIsFull(t *testing.T) {
	workerStarted := make(chan struct{})
	releaseWorker := make(chan struct{})
	inlineStarted := make(chan struct{})
	releaseInline := make(chan struct{})
	var inlineOnce sync.Once

	tw, err := New[struct{}](
		time.Millisecond,
		10,
		nil,
		WithWorkerPool[struct{}](1, 0, RunInline),
	)
	if err != nil {
		t.Fatal(err)
	}
	if err := tw.Start(t.Context()); err != nil {
		t.Fatal(err)
	}

	if _, err := tw.AddTimerWithJob(time.Millisecond, struct{}{}, func(struct{}) {
		close(workerStarted)
		<-releaseWorker
	}); err != nil {
		t.Fatal(err)
	}
	<-workerStarted

	if _, err := tw.AddRepeatingTimerWithJob(time.Millisecond, struct{}{}, func(struct{}) {
		inlineOnce.Do(func() { close(inlineStarted) })
		<-releaseInline
	}, RepeatOptions{Mode: FixedDelay}); err != nil {
		t.Fatal(err)
	}
	<-inlineStarted

	addDone := make(chan struct{}, cap(tw.commandCh))
	for range cap(tw.commandCh) {
		go func() {
			_, _ = tw.AddTimer(time.Hour, struct{}{})
			addDone <- struct{}{}
		}()
	}

	time.Sleep(20 * time.Millisecond)
	close(releaseInline)
	close(releaseWorker)

	closeDone := make(chan error, 1)
	go func() {
		closeDone <- tw.Close()
	}()

	select {
	case err := <-closeDone:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(300 * time.Millisecond):
		t.Fatal("Close did not return after inline fixed-delay job completed")
	}

	if pending := tw.PendingTimers(); len(pending) != 0 {
		t.Fatalf("PendingTimers after Close: got %d, want 0", len(pending))
	}
	if pending := tw.Stats().Pending; pending != 0 {
		t.Fatalf("Stats.Pending after Close: got %d, want 0", pending)
	}
}

func TestWithLoggerRecordsStopReason(t *testing.T) {
	logger := newRecordingLogger(2)
	tw, err := New[struct{}](
		10*time.Millisecond, 100,
		func(struct{}) {},
		WithLogger[struct{}](logger),
	)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	if err := tw.Start(ctx); err != nil {
		t.Fatal(err)
	}

	cancel()
	tw.Wait()

	entry := waitLog(t, logger, "info", "timewheel: stopped")
	value, ok := logArg(entry.args, "reason")
	if !ok {
		t.Fatal("stop log missing reason")
	}
	errValue, ok := value.(error)
	if !ok || !errors.Is(errValue, context.Canceled) {
		t.Fatalf("stop reason: got %v, want context.Canceled", value)
	}
}

func TestWithLoggerRecordsMissingJobWarning(t *testing.T) {
	logger := newRecordingLogger(4)
	tw, err := New[struct{}](
		5*time.Millisecond, 10,
		nil,
		WithLogger[struct{}](logger),
	)
	if err != nil {
		t.Fatal(err)
	}
	startWheel(t, tw)

	id, err := tw.AddTimer(time.Millisecond, struct{}{})
	if err != nil {
		t.Fatal(err)
	}

	entry := waitLog(t, logger, "warn", "timewheel: task has no job and wheel has no default job")
	value, ok := logArg(entry.args, "key")
	if !ok {
		t.Fatal("missing-job warning missing key")
	}
	if value != id {
		t.Fatalf("missing-job warning key: got %v, want %d", value, id)
	}
}

func TestCalcPosCircleCeilTicksFromNextScan(t *testing.T) {
	tw, err := New[struct{}](10*time.Millisecond, 4, nil)
	if err != nil {
		t.Fatal(err)
	}

	tests := []struct {
		name       string
		delay      time.Duration
		wantPos    int
		wantCircle int
	}{
		{name: "partial tick", delay: time.Nanosecond, wantPos: 0, wantCircle: 0},
		{name: "one tick", delay: 10 * time.Millisecond, wantPos: 0, wantCircle: 0},
		{name: "two ticks", delay: 20 * time.Millisecond, wantPos: 1, wantCircle: 0},
		{name: "one rotation", delay: 40 * time.Millisecond, wantPos: 3, wantCircle: 0},
		{name: "one rotation plus one tick", delay: 40*time.Millisecond + time.Nanosecond, wantPos: 0, wantCircle: 1},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotPos, gotCircle := tw.calcPosCircle(tt.delay)
			if gotPos != tt.wantPos || gotCircle != tt.wantCircle {
				t.Fatalf("calcPosCircle(%s): got pos=%d circle=%d, want pos=%d circle=%d",
					tt.delay, gotPos, gotCircle, tt.wantPos, tt.wantCircle)
			}
		})
	}
}

func TestNextFireTimeKnownKey(t *testing.T) {
	tw, err := New[struct{}](10*time.Millisecond, 100, func(struct{}) {})
	if err != nil {
		t.Fatal(err)
	}
	startWheel(t, tw)

	delay := 200 * time.Millisecond
	before := time.Now()
	id, err := tw.AddTimer(delay, struct{}{})
	if err != nil {
		t.Fatal(err)
	}

	ft, ok := tw.NextFireTime(id)
	if !ok {
		t.Fatal("NextFireTime: key not found immediately after AddTimer")
	}

	lo := before.Add(delay - 10*time.Millisecond)
	hi := before.Add(delay + 30*time.Millisecond)
	if ft.Before(lo) || ft.After(hi) {
		t.Fatalf("NextFireTime out of expected range: got %v, want [%v, %v]", ft, lo, hi)
	}
}

func TestNextFireTimeClearedAfterFire(t *testing.T) {
	fired := make(chan struct{})
	tw, err := New[struct{}](10*time.Millisecond, 100, func(struct{}) { close(fired) })
	if err != nil {
		t.Fatal(err)
	}
	startWheel(t, tw)

	id, err := tw.AddTimer(50*time.Millisecond, struct{}{})
	if err != nil {
		t.Fatal(err)
	}

	<-fired
	time.Sleep(30 * time.Millisecond)

	_, ok := tw.NextFireTime(id)
	if ok {
		t.Fatal("NextFireTime: key should be gone after one-shot timer fires")
	}
}

func TestNextFireTimeRepeatingAdvances(t *testing.T) {
	tw, err := New[struct{}](10*time.Millisecond, 100, func(struct{}) {})
	if err != nil {
		t.Fatal(err)
	}
	startWheel(t, tw)

	id, err := tw.AddRepeatingTimer(60*time.Millisecond, struct{}{}, RepeatOptions{})
	if err != nil {
		t.Fatal(err)
	}

	ft1, ok := tw.NextFireTime(id)
	if !ok {
		t.Fatal("NextFireTime: key not found after AddRepeatingTimer")
	}

	time.Sleep(160 * time.Millisecond)

	ft2, ok := tw.NextFireTime(id)
	if !ok {
		t.Fatal("NextFireTime: key gone after repeating fire")
	}
	if !ft2.After(ft1) {
		t.Fatalf("NextFireTime did not advance after repeat: ft1=%v ft2=%v", ft1, ft2)
	}

	if err := tw.RemoveTimer(id); err != nil {
		t.Fatal(err)
	}
}

func TestPendingTimers(t *testing.T) {
	tw, err := New[int](10*time.Millisecond, 100, func(int) {})
	if err != nil {
		t.Fatal(err)
	}
	startWheel(t, tw)

	ids := make([]TimerID, 5)
	for i := range ids {
		id, err := tw.AddTimer(time.Duration(i+1)*100*time.Millisecond, i)
		if err != nil {
			t.Fatal(err)
		}
		ids[i] = id
	}

	pending := tw.PendingTimers()
	if len(pending) != 5 {
		t.Fatalf("PendingTimers: want 5 entries, got %d", len(pending))
	}

	for i := range len(pending) - 1 {
		if pending[i+1].NextFireAt.Before(pending[i].NextFireAt) {
			t.Fatalf("PendingTimers not sorted at index %d", i+1)
		}
	}

	if err := tw.RemoveTimer(ids[0]); err != nil {
		t.Fatal(err)
	}

	pending2 := tw.PendingTimers()
	if len(pending2) != 4 {
		t.Fatalf("PendingTimers after remove: want 4, got %d", len(pending2))
	}
}

func TestPendingTimersRepeatingMarked(t *testing.T) {
	tw, err := New[struct{}](10*time.Millisecond, 100, func(struct{}) {})
	if err != nil {
		t.Fatal(err)
	}
	startWheel(t, tw)

	if _, err := tw.AddTimer(500*time.Millisecond, struct{}{}); err != nil {
		t.Fatal(err)
	}
	id, err := tw.AddRepeatingTimer(500*time.Millisecond, struct{}{}, RepeatOptions{Mode: SkipIfRunning})
	if err != nil {
		t.Fatal(err)
	}

	pending := tw.PendingTimers()
	counts := map[bool]int{}
	var repeatMode RepeatMode
	for _, p := range pending {
		counts[p.Repeating]++
		if p.Repeating {
			repeatMode = p.RepeatMode
		}
	}
	if counts[false] != 1 || counts[true] != 1 {
		t.Fatalf("expected 1 one-shot and 1 repeating, got %v", counts)
	}
	if repeatMode != SkipIfRunning {
		t.Fatalf("repeat mode: got %v, want SkipIfRunning", repeatMode)
	}
	if err := tw.RemoveTimer(id); err != nil {
		t.Fatal(err)
	}
}

func BenchmarkAddTimer(b *testing.B) {
	tw, err := New[int](time.Millisecond, 1000, func(int) {})
	if err != nil {
		b.Fatal(err)
	}
	if err := tw.Start(b.Context()); err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { _ = tw.Close() })

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			if _, err := tw.AddTimer(500*time.Millisecond, 0); err != nil {
				b.Fatal(err)
			}
		}
	})
}

func BenchmarkTick(b *testing.B) {
	for _, n := range []int{100, 1_000, 10_000} {
		b.Run(fmt.Sprintf("tasks=%d", n), func(b *testing.B) {
			tw, err := New[int](time.Hour, 3600, func(int) {})
			if err != nil {
				b.Fatal(err)
			}
			if err := tw.Start(b.Context()); err != nil {
				b.Fatal(err)
			}
			b.Cleanup(func() { _ = tw.Close() })

			for i := range n {
				if _, err := tw.AddTimer(time.Hour, i); err != nil {
					b.Fatal(err)
				}
			}
			time.Sleep(50 * time.Millisecond)

			b.ResetTimer()
			for b.Loop() {
				tw.tick()
			}
		})
	}
}
