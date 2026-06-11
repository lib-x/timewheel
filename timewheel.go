// Package timewheel provides a generic, high-performance timer wheel implementation.
//
// A time wheel is a data structure used to efficiently manage a large number of
// timers. It works by dividing time into fixed-size slots arranged in a circular
// buffer. Each tick advances the pointer by one slot and executes any tasks
// scheduled for that slot.
//
// # Basic usage
//
//	tw, err := timewheel.New[string](
//	    100*time.Millisecond, // tick interval (resolution)
//	    60,                   // number of slots
//	    func(data string) {   // default job
//	        fmt.Println("fired:", data)
//	    },
//	)
//	if err != nil {
//	    log.Fatal(err)
//	}
//
//	ctx, cancel := context.WithCancel(context.Background())
//	defer cancel()
//	tw.Start(ctx)
//
//	key := tw.AddTimer(500*time.Millisecond, "hello")
//	if t, ok := tw.NextFireTime(key); ok {
//	    fmt.Println("fires at", t)
//	}
//
// # Generics
//
// TimeWheel is parameterised over the task-data type T. This eliminates the
// need for type assertions and makes the API type-safe at compile time.
//
// # Concurrency
//
// All public methods are safe for concurrent use. Internally each slot owns
// its own mutex so that concurrent add / remove operations on different slots
// do not block one another. The event loop itself runs in a single goroutine;
// actual job execution is dispatched to separate goroutines (optionally bounded
// by a worker-pool semaphore).
package timewheel

import (
	"cmp"
	"context"
	"errors"
	"slices"
	"sync"
	"sync/atomic"
	"time"
)

// ─── Job & Task ───────────────────────────────────────────────────────────────

// Job is the callback signature invoked when a timer fires.
// T is the type of the payload that was registered with the timer.
type Job[T any] func(data T)

// task is the internal representation of a pending timer.
// Instances are recycled via a sync.Pool to reduce allocations.
type task[T any] struct {
	// circle is the number of full wheel rotations remaining before the task
	// fires. Decremented by one on every tick that lands on this slot.
	circle int

	// key is the unique identifier assigned at enqueue time.
	key uint64

	data  T
	job   Job[T] // per-task override; nil means use the wheel's default job
	delay time.Duration
	mode  taskMode
}

type taskMode uint8

const (
	modeOnce   taskMode = iota // fire once, then discard
	modeRepeat                 // re-enqueue after each execution
)

// ─── timerMeta ────────────────────────────────────────────────────────────────

// timerMeta stores scheduling metadata for a live timer.
// It is kept in a separate index (timerIndex) rather than in the task struct
// so that NextFireTime and PendingTimers can be served without acquiring any
// slot lock.
type timerMeta struct {
	// nextFireAt is the wall-clock time at which this timer is expected to fire.
	// For repeating timers it is updated each time the task is re-enqueued.
	nextFireAt time.Time

	// delay is the original registration delay; used to recompute nextFireAt
	// when a repeating timer re-enqueues itself.
	delay time.Duration

	mode taskMode
}

// ─── Stats & TimerInfo ────────────────────────────────────────────────────────

// Stats is a snapshot of runtime counters. All fields are read atomically.
type Stats struct {
	// Pending is the number of tasks currently queued in the wheel.
	Pending int64

	// Executed is the total number of tasks that have been dispatched.
	Executed int64

	// Removed is the total number of tasks explicitly cancelled via [TimeWheel.RemoveTimer].
	Removed int64
}

// TimerInfo describes a single live timer as returned by [TimeWheel.PendingTimers].
type TimerInfo struct {
	// Key is the unique timer identifier returned by the Add* methods.
	Key uint64

	// NextFireAt is the wall-clock time at which the timer is next expected to fire.
	// For repeating timers this reflects the start of the current period.
	NextFireAt time.Time

	// Delay is the original delay passed to the Add* method.
	Delay time.Duration

	// Repeating is true if the timer was registered via AddRepeating or
	// AddRepeatingWithJob.
	Repeating bool
}

// ─── slot ─────────────────────────────────────────────────────────────────────

// slot holds the tasks assigned to one position of the wheel.
// Each slot owns its own mutex to allow concurrent access from multiple
// goroutines without a global lock.
type slot[T any] struct {
	mu    sync.Mutex
	tasks []*task[T]
}

// ─── TimeWheel ────────────────────────────────────────────────────────────────

// TimeWheel is a generic timer wheel.
//
// T is the type of the data payload stored with each timer.
// Use [New] to create a TimeWheel; the zero value is not usable.
type TimeWheel[T any] struct {
	interval   time.Duration
	slotNum    int
	defaultJob Job[T]

	slots []slot[T]

	// currentPos is the index of the slot that will be scanned on the next tick.
	// Updated exclusively by the event loop goroutine; stored as int64 for
	// atomic reads from calcPosCircle without requiring a lock.
	currentPos atomic.Int64

	pool sync.Pool // recycles *task[T] objects

	// addCh / removeCh are the channels through which callers communicate with
	// the event loop. Buffered to avoid blocking short bursts of registrations.
	addCh    chan *task[T]
	removeCh chan uint64

	// keyGen is a monotonically increasing counter used to generate unique keys.
	keyGen atomic.Uint64

	// timerIndex is a read-mostly map from key → timerMeta. It is maintained
	// in parallel with the slot lists and enables O(1) NextFireTime lookups
	// without touching the slot mutexes.
	//
	// Writes happen only in the event loop (placeTask, deleteTask, dispatch).
	// Reads happen concurrently from NextFireTime / PendingTimers.
	timerIndex   map[uint64]timerMeta
	timerIndexMu sync.RWMutex

	stats struct {
		pending  atomic.Int64
		executed atomic.Int64
		removed  atomic.Int64
	}

	// workerSem is a counting semaphore that bounds the worker pool.
	// nil means unlimited.
	workerSem chan struct{}

	cfg config[T]
	wg  sync.WaitGroup
}

// New creates and initialises a new TimeWheel.
//
// Parameters:
//   - interval: tick resolution; the minimum timer precision equals interval.
//   - slotNum: number of slots in the wheel. Together with interval this
//     determines the maximum delay before circle counting kicks in:
//     maxDirect = interval × slotNum.
//   - defaultJob: callback invoked for tasks that have no per-task job.
//     May be nil if every timer is registered via [TimeWheel.AddTimerWithJob].
//   - opts: zero or more [Option] values.
//
// New returns an error if interval or slotNum are not positive.
func New[T any](interval time.Duration, slotNum int, defaultJob Job[T], opts ...Option[T]) (*TimeWheel[T], error) {
	if interval <= 0 {
		return nil, errors.New("timewheel: interval must be positive")
	}
	if slotNum <= 0 {
		return nil, errors.New("timewheel: slotNum must be positive")
	}

	var cfg config[T]
	for _, o := range opts {
		o(&cfg)
	}

	tw := &TimeWheel[T]{
		interval:   interval,
		slotNum:    slotNum,
		defaultJob: defaultJob,
		slots:      make([]slot[T], slotNum),
		addCh:      make(chan *task[T], 64),
		removeCh:   make(chan uint64, 64),
		timerIndex: make(map[uint64]timerMeta),
		cfg:        cfg,
	}
	tw.pool = sync.Pool{New: func() any { return new(task[T]) }}

	if cfg.workerNum > 0 {
		tw.workerSem = make(chan struct{}, cfg.workerNum)
	}

	return tw, nil
}

// ─── Lifecycle ────────────────────────────────────────────────────────────────

// Start launches the event loop in a background goroutine.
// The wheel runs until ctx is cancelled. Call [TimeWheel.Wait] to block until
// the event loop has fully exited.
//
// Start must be called exactly once.
func (tw *TimeWheel[T]) Start(ctx context.Context) {
	tw.wg.Add(1)
	go tw.run(ctx)
}

// Wait blocks until the event loop goroutine started by [TimeWheel.Start] exits.
func (tw *TimeWheel[T]) Wait() { tw.wg.Wait() }

func (tw *TimeWheel[T]) run(ctx context.Context) {
	defer tw.wg.Done()

	ticker := time.Tick(tw.interval)

	for {
		select {
		case <-ctx.Done():
			tw.logInfo("timewheel: stopped", "reason", ctx.Err())
			return
		case <-ticker:
			tw.tick()
		case t := <-tw.addCh:
			tw.placeTask(t)
		case key := <-tw.removeCh:
			tw.deleteTask(key)
		}
	}
}

// ─── Public API ───────────────────────────────────────────────────────────────

// AddTimer enqueues a one-shot timer that fires after delay using the wheel's
// default job. It returns a key that can be passed to [TimeWheel.RemoveTimer]
// or [TimeWheel.NextFireTime].
//
// If delay is shorter than the wheel's tick interval it is rounded up to one tick.
func (tw *TimeWheel[T]) AddTimer(delay time.Duration, data T) uint64 {
	return tw.enqueue(delay, data, nil, modeOnce)
}

// AddTimerWithJob is like [TimeWheel.AddTimer] but uses job instead of the
// wheel's default job. job must not be nil.
func (tw *TimeWheel[T]) AddTimerWithJob(delay time.Duration, data T, job Job[T]) uint64 {
	if job == nil {
		panic("timewheel: AddTimerWithJob called with nil job")
	}
	return tw.enqueue(delay, data, job, modeOnce)
}

// AddTimerFunc enqueues a one-shot timer that calls fn after delay.
// No payload is required; fn captures any needed state via closure.
// It returns a key that can be passed to [TimeWheel.RemoveTimer] or
// [TimeWheel.NextFireTime].
func (tw *TimeWheel[T]) AddTimerFunc(delay time.Duration, fn func()) uint64 {
	var zero T
	return tw.enqueue(delay, zero, func(T) { fn() }, modeOnce)
}

// AddRepeating enqueues a recurring timer. After firing, the task is
// automatically re-enqueued with the same delay using the wheel's default job.
// It returns a key that can be passed to [TimeWheel.RemoveTimer] to stop the
// repetition, or to [TimeWheel.NextFireTime] to inspect the next scheduled
// fire time.
func (tw *TimeWheel[T]) AddRepeating(delay time.Duration, data T) uint64 {
	return tw.enqueue(delay, data, nil, modeRepeat)
}

// AddRepeatingWithJob is like [TimeWheel.AddRepeating] but uses a per-task job.
func (tw *TimeWheel[T]) AddRepeatingWithJob(delay time.Duration, data T, job Job[T]) uint64 {
	if job == nil {
		panic("timewheel: AddRepeatingWithJob called with nil job")
	}
	return tw.enqueue(delay, data, job, modeRepeat)
}

// RemoveTimer cancels the timer identified by key.
// RemoveTimer is a no-op if the key is unknown or the timer has already fired.
func (tw *TimeWheel[T]) RemoveTimer(key uint64) {
	tw.removeCh <- key
}

// NextFireTime returns the wall-clock time at which the timer identified by key
// is next expected to fire, and whether the key is known to the wheel.
//
// For repeating timers the returned time reflects the beginning of the current
// period (i.e. it advances after every execution).
//
// The returned time is an estimate: it is computed when the task is enqueued
// and is not adjusted for scheduling jitter. The actual fire time may be up to
// one tick interval later than the returned value.
//
// NextFireTime returns (zero, false) if the key does not exist — either because
// it was never registered, has already fired (one-shot), or was removed via
// [TimeWheel.RemoveTimer].
func (tw *TimeWheel[T]) NextFireTime(key uint64) (time.Time, bool) {
	tw.timerIndexMu.RLock()
	meta, ok := tw.timerIndex[key]
	tw.timerIndexMu.RUnlock()
	if !ok {
		return time.Time{}, false
	}
	return meta.nextFireAt, true
}

// PendingTimers returns a snapshot of all timers currently queued in the wheel,
// sorted by ascending NextFireAt time.
//
// The snapshot is taken under a read lock on the internal index, so it reflects
// a consistent moment in time. The returned slice is freshly allocated on every
// call; callers are free to modify it.
func (tw *TimeWheel[T]) PendingTimers() []TimerInfo {
	tw.timerIndexMu.RLock()
	out := make([]TimerInfo, 0, len(tw.timerIndex))
	for key, meta := range tw.timerIndex {
		out = append(out, TimerInfo{
			Key:        key,
			NextFireAt: meta.nextFireAt,
			Delay:      meta.delay,
			Repeating:  meta.mode == modeRepeat,
		})
	}
	tw.timerIndexMu.RUnlock()

	// Sort by ascending fire time so callers get a useful ordering by default.
	slices.SortFunc(out, func(a, b TimerInfo) int {
		return cmp.Or(a.NextFireAt.Compare(b.NextFireAt), cmp.Compare(a.Key, b.Key))
	})
	return out
}

// Stats returns a snapshot of the wheel's runtime counters.
func (tw *TimeWheel[T]) Stats() Stats {
	return Stats{
		Pending:  tw.stats.pending.Load(),
		Executed: tw.stats.executed.Load(),
		Removed:  tw.stats.removed.Load(),
	}
}

// ─── Internal helpers ─────────────────────────────────────────────────────────

// enqueue allocates (or recycles) a task, registers it in the timer index, and
// sends it to the event loop.
func (tw *TimeWheel[T]) enqueue(delay time.Duration, data T, job Job[T], mode taskMode) uint64 {
	delay = max(delay, tw.interval)

	key := tw.keyGen.Add(1)
	now := time.Now()

	// Register in the index before sending to the channel so that a caller
	// who immediately calls NextFireTime after AddTimer always finds the entry.
	tw.timerIndexMu.Lock()
	tw.timerIndex[key] = timerMeta{
		nextFireAt: now.Add(delay),
		delay:      delay,
		mode:       mode,
	}
	tw.timerIndexMu.Unlock()

	t := tw.pool.Get().(*task[T])
	*t = task[T]{
		key:   key,
		data:  data,
		job:   job,
		delay: delay,
		mode:  mode,
	}

	tw.stats.pending.Add(1)
	tw.addCh <- t
	return key
}

// placeTask computes the target slot and appends the task to it.
// Must only be called from the event loop goroutine.
func (tw *TimeWheel[T]) placeTask(t *task[T]) {
	if !tw.hasTimer(t.key) {
		tw.releaseTask(t)
		return
	}

	pos, circle := tw.calcPosCircle(t.delay)
	t.circle = circle

	s := &tw.slots[pos]
	s.mu.Lock()
	s.tasks = append(s.tasks, t)
	s.mu.Unlock()
}

// tick scans the current slot, dispatches due tasks, then advances the pointer.
// Must only be called from the event loop goroutine.
func (tw *TimeWheel[T]) tick() {
	cur := int(tw.currentPos.Load())
	s := &tw.slots[cur]

	s.mu.Lock()

	// Partition tasks: decrement circle for those not yet due; collect due ones.
	remaining := s.tasks[:0]
	var due []*task[T]

	for _, t := range s.tasks {
		if t.circle > 0 {
			t.circle--
			remaining = append(remaining, t)
		} else {
			due = append(due, t)
		}
	}

	// Zero out trailing pointers to allow GC of evicted tasks.
	clear(s.tasks[len(remaining):])
	s.tasks = remaining

	s.mu.Unlock()

	for _, t := range due {
		tw.dispatch(t)
	}

	// Advance the wheel pointer.
	tw.currentPos.Store(int64((cur + 1) % tw.slotNum))
}

// dispatch executes a task's job in a separate goroutine, removes the timer
// from the index (one-shot) or updates its nextFireAt (repeating), then
// recycles the task object.
func (tw *TimeWheel[T]) dispatch(t *task[T]) {
	tw.stats.pending.Add(-1)

	job := t.job
	if job == nil {
		job = tw.defaultJob
	}
	if job == nil {
		tw.logWarn("timewheel: task has no job and wheel has no default job",
			"key", t.key)
		tw.removeFromIndex(t.key)
		tw.releaseTask(t)
		return
	}

	data := t.data
	mode := t.mode
	delay := t.delay
	key := t.key
	capturedJob := t.job // preserve per-task job across re-enqueue

	run := func() {
		tw.stats.executed.Add(1)
		if tw.cfg.errorHandler != nil {
			defer func() {
				if r := recover(); r != nil {
					tw.cfg.errorHandler(r)
				}
			}()
		}
		job(data)
	}

	if tw.workerSem != nil {
		go func() {
			tw.workerSem <- struct{}{}
			defer func() { <-tw.workerSem }()
			run()
		}()
	} else {
		go run()
	}

	if mode == modeRepeat {
		// Re-enqueue with the same key so callers can keep using it.
		// enqueue generates a new key, so we patch it back after the fact
		// via a dedicated helper that reuses the existing key.
		tw.reenqueue(key, delay, data, capturedJob)
	} else {
		// One-shot: remove the index entry now that the task has fired.
		tw.removeFromIndex(key)
	}

	tw.releaseTask(t)
}

// reenqueue places a repeating task back into the wheel under its original key,
// updating the timer index with the new expected fire time.
//
// Unlike enqueue, reenqueue does not allocate a new key; it reuses the one
// passed in. This keeps the key stable across all repetitions of a timer.
func (tw *TimeWheel[T]) reenqueue(key uint64, delay time.Duration, data T, job Job[T]) {
	now := time.Now()

	tw.timerIndexMu.Lock()
	tw.timerIndex[key] = timerMeta{
		nextFireAt: now.Add(delay),
		delay:      delay,
		mode:       modeRepeat,
	}
	tw.timerIndexMu.Unlock()

	t := tw.pool.Get().(*task[T])
	*t = task[T]{
		key:   key,
		data:  data,
		job:   job,
		delay: delay,
		mode:  modeRepeat,
	}

	tw.stats.pending.Add(1)
	tw.placeTask(t)
}

// deleteTask removes the first task with the given key from its slot and from
// the timer index. Must only be called from the event loop goroutine.
func (tw *TimeWheel[T]) deleteTask(key uint64) {
	if !tw.deleteFromIndex(key) {
		return
	}

	for i := range tw.slots {
		s := &tw.slots[i]
		s.mu.Lock()

		for j, t := range s.tasks {
			if t.key != key {
				continue
			}

			// O(1) swap-and-shrink deletion.
			last := len(s.tasks) - 1
			s.tasks[j] = s.tasks[last]
			s.tasks[last] = nil
			s.tasks = s.tasks[:last]

			tw.stats.pending.Add(-1)
			tw.stats.removed.Add(1)
			tw.releaseTask(t)

			s.mu.Unlock()
			return
		}

		s.mu.Unlock()
	}

	tw.stats.pending.Add(-1)
	tw.stats.removed.Add(1)
}

// removeFromIndex deletes the key from the timer index.
func (tw *TimeWheel[T]) removeFromIndex(key uint64) {
	tw.timerIndexMu.Lock()
	delete(tw.timerIndex, key)
	tw.timerIndexMu.Unlock()
}

func (tw *TimeWheel[T]) deleteFromIndex(key uint64) bool {
	tw.timerIndexMu.Lock()
	_, ok := tw.timerIndex[key]
	delete(tw.timerIndex, key)
	tw.timerIndexMu.Unlock()
	return ok
}

func (tw *TimeWheel[T]) hasTimer(key uint64) bool {
	tw.timerIndexMu.RLock()
	_, ok := tw.timerIndex[key]
	tw.timerIndexMu.RUnlock()
	return ok
}

func (tw *TimeWheel[T]) releaseTask(t *task[T]) {
	*t = task[T]{}
	tw.pool.Put(t)
}

// calcPosCircle returns the slot index and the number of additional full
// rotations required for a task with the given delay.
func (tw *TimeWheel[T]) calcPosCircle(d time.Duration) (pos, circle int) {
	ticks := int((d-1)/tw.interval) + 1
	offset := ticks - 1
	cur := int(tw.currentPos.Load())
	circle = offset / tw.slotNum
	pos = (cur + offset) % tw.slotNum
	return
}

// ─── logging helpers ─────────────────────────────────────────────────────────

func (tw *TimeWheel[T]) logInfo(msg string, args ...any) {
	if tw.cfg.logger != nil {
		tw.cfg.logger.Info(msg, args...)
	}
}

func (tw *TimeWheel[T]) logWarn(msg string, args ...any) {
	if tw.cfg.logger != nil {
		tw.cfg.logger.Warn(msg, args...)
	}
}
