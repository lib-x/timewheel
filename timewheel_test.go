package timewheel

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"testing"
	"time"
)

// ─── helpers ──────────────────────────────────────────────────────────────────

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
	tw.Start(t.Context())
	t.Cleanup(tw.Wait)
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

func logArg(args []any, key string) (any, bool) {
	for i := 0; i+1 < len(args); i += 2 {
		if args[i] == key {
			return args[i+1], true
		}
	}
	return nil, false
}

// ─── correctness ──────────────────────────────────────────────────────────────

func TestAddTimer_fires(t *testing.T) {
	var fired atomic.Bool
	tw, err := New[struct{}](10*time.Millisecond, 100, func(struct{}) {
		fired.Store(true)
	})
	if err != nil {
		t.Fatal(err)
	}
	startWheel(t, tw)

	tw.AddTimer(50*time.Millisecond, struct{}{})

	time.Sleep(200 * time.Millisecond)
	if !fired.Load() {
		t.Fatal("timer did not fire")
	}
}

func TestAddTimer_firesOnce(t *testing.T) {
	var count atomic.Int32
	tw, _ := New[struct{}](10*time.Millisecond, 100, func(struct{}) {
		count.Add(1)
	})
	startWheel(t, tw)

	tw.AddTimer(50*time.Millisecond, struct{}{})
	time.Sleep(300 * time.Millisecond)

	if n := count.Load(); n != 1 {
		t.Fatalf("expected 1 execution, got %d", n)
	}
}

func TestRemoveTimer(t *testing.T) {
	var fired atomic.Bool
	tw, _ := New[struct{}](10*time.Millisecond, 100, func(struct{}) {
		fired.Store(true)
	})
	startWheel(t, tw)

	key := tw.AddTimer(200*time.Millisecond, struct{}{})
	time.Sleep(50 * time.Millisecond)
	tw.RemoveTimer(key)
	time.Sleep(300 * time.Millisecond)

	if fired.Load() {
		t.Fatal("timer fired after removal")
	}
}

func TestAddRepeating(t *testing.T) {
	var count atomic.Int32
	tw, _ := New[struct{}](10*time.Millisecond, 100, func(struct{}) {
		count.Add(1)
	})
	startWheel(t, tw)

	key := tw.AddRepeating(50*time.Millisecond, struct{}{})
	time.Sleep(280 * time.Millisecond)
	tw.RemoveTimer(key)
	time.Sleep(100 * time.Millisecond)

	n := count.Load()
	// expect ~5 ticks; allow generous window for scheduling jitter
	if n < 3 || n > 7 {
		t.Fatalf("expected ~5 executions, got %d", n)
	}
}

func TestAddTimerFunc(t *testing.T) {
	done := make(chan struct{})
	tw, _ := New[struct{}](10*time.Millisecond, 100, nil)
	startWheel(t, tw)

	tw.AddTimerFunc(50*time.Millisecond, func() { close(done) })

	select {
	case <-done:
	case <-time.After(300 * time.Millisecond):
		t.Fatal("AddTimerFunc did not fire")
	}
}

func TestAddTimerWithJob(t *testing.T) {
	done := make(chan int, 1)
	tw, _ := New[int](10*time.Millisecond, 100, func(int) {
		t.Error("default job should not be called")
	})
	startWheel(t, tw)

	tw.AddTimerWithJob(50*time.Millisecond, 42, func(n int) { done <- n })

	select {
	case v := <-done:
		if v != 42 {
			t.Fatalf("expected 42, got %d", v)
		}
	case <-time.After(300 * time.Millisecond):
		t.Fatal("per-task job did not fire")
	}
}

func TestStats(t *testing.T) {
	tw, _ := New[struct{}](10*time.Millisecond, 100, func(struct{}) {})
	startWheel(t, tw)

	const n = 5
	for range n {
		tw.AddTimer(50*time.Millisecond, struct{}{})
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

func TestStats_pendingDoesNotWaitForJobCompletion(t *testing.T) {
	started := make(chan struct{})
	release := make(chan struct{})

	tw, _ := New[struct{}](5*time.Millisecond, 10, func(struct{}) {
		close(started)
		<-release
	})
	startWheel(t, tw)

	tw.AddTimer(time.Millisecond, struct{}{})

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
}

func TestErrorHandler(t *testing.T) {
	recovered := make(chan any, 1)
	tw, _ := New[struct{}](
		10*time.Millisecond, 100,
		func(struct{}) { panic("boom") },
		WithErrorHandler[struct{}](func(r any) { recovered <- r }),
	)
	startWheel(t, tw)

	tw.AddTimer(30*time.Millisecond, struct{}{})

	select {
	case r := <-recovered:
		if r != "boom" {
			t.Fatalf("unexpected recovered value: %v", r)
		}
	case <-time.After(300 * time.Millisecond):
		t.Fatal("error handler was not called")
	}

	s := tw.Stats()
	if s.Pending != 0 {
		t.Fatalf("Stats.Pending after recovered panic: want 0, got %d", s.Pending)
	}
	if s.Executed != 1 {
		t.Fatalf("Stats.Executed after recovered panic: want 1, got %d", s.Executed)
	}
}

func TestNew_invalidArgs(t *testing.T) {
	if _, err := New[int](0, 10, nil); err == nil {
		t.Error("expected error for zero interval")
	}
	if _, err := New[int](time.Second, 0, nil); err == nil {
		t.Error("expected error for zero slotNum")
	}
}

func TestWithWorkerPool(t *testing.T) {
	var concurrent atomic.Int32
	var peak atomic.Int32

	tw, _ := New[struct{}](
		10*time.Millisecond, 100,
		func(struct{}) {
			c := concurrent.Add(1)
			for {
				old := peak.Load()
				if c <= old || peak.CompareAndSwap(old, c) {
					break
				}
			}
			time.Sleep(30 * time.Millisecond)
			concurrent.Add(-1)
		},
		WithWorkerPool[struct{}](3),
	)
	startWheel(t, tw)

	for range 10 {
		tw.AddTimer(20*time.Millisecond, struct{}{})
	}

	time.Sleep(500 * time.Millisecond)

	if p := peak.Load(); p > 3 {
		t.Fatalf("worker pool exceeded: peak concurrency = %d, want ≤ 3", p)
	}
}

func TestWithLogger_recordsStopReason(t *testing.T) {
	logger := newRecordingLogger(2)
	tw, _ := New[struct{}](
		10*time.Millisecond, 100,
		func(struct{}) {},
		WithLogger[struct{}](logger),
	)
	ctx, cancel := context.WithCancel(t.Context())
	tw.Start(ctx)

	cancel()
	tw.Wait()

	entry := waitLog(t, logger, "info", "timewheel: stopped")
	value, ok := logArg(entry.args, "reason")
	if !ok {
		t.Fatal("stop log missing reason")
	}
	err, ok := value.(error)
	if !ok || !errors.Is(err, context.Canceled) {
		t.Fatalf("stop reason: got %v, want context.Canceled", value)
	}
}

func TestWithLogger_recordsMissingJobWarning(t *testing.T) {
	logger := newRecordingLogger(4)
	tw, _ := New[struct{}](
		5*time.Millisecond, 10,
		nil,
		WithLogger[struct{}](logger),
	)
	startWheel(t, tw)

	key := tw.AddTimer(time.Millisecond, struct{}{})

	entry := waitLog(t, logger, "warn", "timewheel: task has no job and wheel has no default job")
	value, ok := logArg(entry.args, "key")
	if !ok {
		t.Fatal("missing-job warning missing key")
	}
	if value != key {
		t.Fatalf("missing-job warning key: got %v, want %d", value, key)
	}
}

func TestWithLogger_nilDisablesInternalLogs(t *testing.T) {
	tw, _ := New[struct{}](
		5*time.Millisecond, 10,
		nil,
		WithLogger[struct{}](nil),
	)
	startWheel(t, tw)

	key := tw.AddTimer(time.Millisecond, struct{}{})
	timer := time.NewTimer(200 * time.Millisecond)
	defer timer.Stop()

	for {
		if _, ok := tw.NextFireTime(key); !ok {
			return
		}
		select {
		case <-time.After(time.Millisecond):
		case <-timer.C:
			t.Fatal("timer with nil logger was not cleared")
		}
	}
}

func TestRemoveTimer_beforePlacement(t *testing.T) {
	tw, _ := New[struct{}](10*time.Millisecond, 100, func(struct{}) {})

	key := tw.AddTimer(time.Second, struct{}{})
	tw.deleteTask(key)
	tw.placeTask(<-tw.addCh)

	if _, ok := tw.NextFireTime(key); ok {
		t.Fatal("NextFireTime: key should be gone after removing before placement")
	}

	for i := range tw.slots {
		if len(tw.slots[i].tasks) != 0 {
			t.Fatalf("slot %d has %d tasks after pre-placement removal", i, len(tw.slots[i].tasks))
		}
	}

	s := tw.Stats()
	if s.Pending != 0 {
		t.Fatalf("Stats.Pending after pre-placement removal: want 0, got %d", s.Pending)
	}
	if s.Removed != 1 {
		t.Fatalf("Stats.Removed after pre-placement removal: want 1, got %d", s.Removed)
	}
}

func TestCalcPosCircle_ceilTicksFromNextScan(t *testing.T) {
	tw, _ := New[struct{}](10*time.Millisecond, 4, nil)

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

func TestDispatch_repeatingDoesNotSendToAddChannel(t *testing.T) {
	tw, _ := New[struct{}](10*time.Millisecond, 100, func(struct{}) {})
	for range cap(tw.addCh) {
		tw.addCh <- &task[struct{}]{}
	}

	tw.timerIndex[1] = timerMeta{
		nextFireAt: time.Now(),
		delay:      time.Second,
		mode:       modeRepeat,
	}
	tw.stats.pending.Store(1)

	done := make(chan struct{})
	go func() {
		tw.dispatch(&task[struct{}]{
			key:   1,
			delay: time.Second,
			mode:  modeRepeat,
		})
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(300 * time.Millisecond):
		t.Fatal("dispatch blocked while re-enqueuing repeating timer")
	}
}

// ─── next-fire-time ───────────────────────────────────────────────────────────

func TestNextFireTime_knownKey(t *testing.T) {
	tw, _ := New[struct{}](10*time.Millisecond, 100, func(struct{}) {})
	startWheel(t, tw)

	delay := 200 * time.Millisecond
	before := time.Now()
	key := tw.AddTimer(delay, struct{}{})

	ft, ok := tw.NextFireTime(key)
	if !ok {
		t.Fatal("NextFireTime: key not found immediately after AddTimer")
	}

	// The returned time must be roughly now+delay (within one interval of slack).
	lo := before.Add(delay - 10*time.Millisecond)
	hi := before.Add(delay + 20*time.Millisecond)
	if ft.Before(lo) || ft.After(hi) {
		t.Fatalf("NextFireTime out of expected range: got %v, want [%v, %v]", ft, lo, hi)
	}
}

func TestNextFireTime_unknownKey(t *testing.T) {
	tw, _ := New[struct{}](10*time.Millisecond, 100, func(struct{}) {})
	startWheel(t, tw)

	_, ok := tw.NextFireTime(99999)
	if ok {
		t.Fatal("NextFireTime: expected false for unknown key")
	}
}

func TestNextFireTime_clearedAfterFire(t *testing.T) {
	fired := make(chan struct{})
	tw, _ := New[struct{}](10*time.Millisecond, 100, func(struct{}) { close(fired) })
	startWheel(t, tw)

	key := tw.AddTimer(50*time.Millisecond, struct{}{})

	<-fired
	time.Sleep(30 * time.Millisecond) // let index deletion propagate

	_, ok := tw.NextFireTime(key)
	if ok {
		t.Fatal("NextFireTime: key should be gone after one-shot timer fires")
	}
}

func TestNextFireTime_clearedAfterRemove(t *testing.T) {
	tw, _ := New[struct{}](10*time.Millisecond, 100, func(struct{}) {})
	startWheel(t, tw)

	key := tw.AddTimer(500*time.Millisecond, struct{}{})
	time.Sleep(20 * time.Millisecond)
	tw.RemoveTimer(key)
	time.Sleep(30 * time.Millisecond) // let event loop process removal

	_, ok := tw.NextFireTime(key)
	if ok {
		t.Fatal("NextFireTime: key should be gone after RemoveTimer")
	}
}

func TestNextFireTime_repeatingAdvances(t *testing.T) {
	tw, _ := New[struct{}](10*time.Millisecond, 100, func(struct{}) {})
	startWheel(t, tw)

	key := tw.AddRepeating(60*time.Millisecond, struct{}{})

	ft1, ok := tw.NextFireTime(key)
	if !ok {
		t.Fatal("NextFireTime: key not found after AddRepeating")
	}

	// Wait for two firings.
	time.Sleep(160 * time.Millisecond)

	ft2, ok := tw.NextFireTime(key)
	if !ok {
		t.Fatal("NextFireTime: key gone after repeating fire — should still exist")
	}
	if !ft2.After(ft1) {
		t.Fatalf("NextFireTime did not advance after repeat: ft1=%v ft2=%v", ft1, ft2)
	}

	tw.RemoveTimer(key)
}

func TestPendingTimers(t *testing.T) {
	tw, _ := New[int](10*time.Millisecond, 100, func(int) {})
	startWheel(t, tw)

	keys := make([]uint64, 5)
	for i := range keys {
		keys[i] = tw.AddTimer(time.Duration(i+1)*100*time.Millisecond, i)
	}
	// Give the event loop a moment to place all tasks.
	time.Sleep(30 * time.Millisecond)

	pending := tw.PendingTimers()
	if len(pending) != 5 {
		t.Fatalf("PendingTimers: want 5 entries, got %d", len(pending))
	}

	// Verify ascending order.
	for i := range len(pending) - 1 {
		if pending[i+1].NextFireAt.Before(pending[i].NextFireAt) {
			t.Fatalf("PendingTimers not sorted at index %d", i+1)
		}
	}

	// Remove one and re-check count.
	tw.RemoveTimer(keys[0])
	time.Sleep(20 * time.Millisecond)

	pending2 := tw.PendingTimers()
	if len(pending2) != 4 {
		t.Fatalf("PendingTimers after remove: want 4, got %d", len(pending2))
	}
}

func TestPendingTimers_repeatingMarked(t *testing.T) {
	tw, _ := New[struct{}](10*time.Millisecond, 100, func(struct{}) {})
	startWheel(t, tw)

	tw.AddTimer(500*time.Millisecond, struct{}{})
	key := tw.AddRepeating(500*time.Millisecond, struct{}{})
	time.Sleep(20 * time.Millisecond)

	pending := tw.PendingTimers()
	counts := map[bool]int{}
	for _, p := range pending {
		counts[p.Repeating]++
	}
	if counts[false] != 1 || counts[true] != 1 {
		t.Fatalf("expected 1 one-shot and 1 repeating, got %v", counts)
	}
	tw.RemoveTimer(key)
}

// ─── benchmarks ───────────────────────────────────────────────────────────────

// BenchmarkAddTimer measures the throughput of enqueuing timers.
func BenchmarkAddTimer(b *testing.B) {
	tw, _ := New[int](time.Millisecond, 1000, func(int) {})
	tw.Start(b.Context())
	b.Cleanup(tw.Wait)

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			tw.AddTimer(500*time.Millisecond, 0)
		}
	})
}

// BenchmarkTick measures the overhead of a single wheel tick with N pending tasks.
func BenchmarkTick(b *testing.B) {
	for _, n := range []int{100, 1_000, 10_000} {
		b.Run(fmt.Sprintf("tasks=%d", n), func(b *testing.B) {
			tw, _ := New[int](time.Hour, 3600, func(int) {})
			// pre-load all tasks into slot 1 (1 tick away) so they fire each tick
			tw.Start(b.Context())
			b.Cleanup(tw.Wait)

			for i := range n {
				tw.AddTimer(time.Hour, i) // circle=0, slot depends on current pos
			}
			time.Sleep(50 * time.Millisecond) // let addCh drain

			b.ResetTimer()
			for b.Loop() {
				tw.tick()
			}
		})
	}
}

// suppress "declared and not used" for the fmt import used only in benchmarks
var _ = fmt.Sprintf
