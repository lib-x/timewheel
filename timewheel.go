// Package timewheel provides a generic timer wheel implementation.
//
// TimeWheel is safe for concurrent use. Timer placement and deletion are
// serialized through the wheel event loop; job execution is dispatched outside
// the event loop. Wheel slots are owned exclusively by the event loop
// goroutine and are accessed without locks.
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

// TimerID uniquely identifies a timer within one TimeWheel instance.
type TimerID uint64

// Job is the callback signature invoked when a timer fires.
type Job[T any] func(data T)

// JobContext is the context-aware callback signature invoked when a timer fires.
type JobContext[T any] func(context.Context, T) error

var (
	ErrNotStarted   = errors.New("timewheel: not started")
	ErrRunning      = errors.New("timewheel: already running")
	ErrClosed       = errors.New("timewheel: closed")
	ErrNilContext   = errors.New("timewheel: nil context")
	ErrNilJob       = errors.New("timewheel: nil job")
	ErrUnknownTimer = errors.New("timewheel: unknown timer")
	ErrQueueFull    = errors.New("timewheel: worker queue full")
)

// RepeatMode controls how a repeating timer schedules its next execution.
type RepeatMode uint8

const (
	// FixedRate schedules the next fire when the current fire is dispatched.
	// Jobs may overlap.
	FixedRate RepeatMode = iota

	// FixedDelay schedules the next fire after the previous job returns.
	// Jobs do not overlap.
	FixedDelay

	// SkipIfRunning keeps a fixed-rate cadence but skips a fire when the
	// previous job for the same timer is still running.
	SkipIfRunning
)

// RepeatOptions configures repeating timer behavior.
type RepeatOptions struct {
	// Mode defaults to FixedRate.
	Mode RepeatMode
}

// JobEvent describes the result of one job scheduling or execution attempt.
type JobEvent[T any] struct {
	TimerID      TimerID
	Data         T
	StartedAt    time.Time
	FinishedAt   time.Time
	ScheduledFor time.Time
	Lateness     time.Duration
	Duration     time.Duration
	Err          error
	Panic        any
	Dropped      bool
	Skipped      bool
}

// JobObserver receives job execution, drop, and skip events.
type JobObserver[T any] func(JobEvent[T])

type taskMode uint8

const (
	modeOnce taskMode = iota
	modeRepeat
)

type wheelState uint8

const (
	stateNew wheelState = iota
	stateRunning
	stateClosed
)

type commandKind uint8

const (
	commandAdd commandKind = iota
	commandRemove
	commandJobDone
)

type timerLocation struct {
	slot  int
	index int
}

func noLocation() timerLocation {
	return timerLocation{slot: -1, index: -1}
}

func (l timerLocation) valid() bool {
	return l.slot >= 0 && l.index >= 0
}

type task[T any] struct {
	circle       int
	key          TimerID
	data         T
	job          Job[T]
	contextJob   JobContext[T]
	delay        time.Duration
	mode         taskMode
	repeatMode   RepeatMode
	scheduledFor time.Time
}

type timerMeta struct {
	nextFireAt time.Time
	delay      time.Duration
	mode       taskMode
	repeatMode RepeatMode
	location   timerLocation
	running    bool
}

// Stats is a snapshot of runtime counters. All fields are read atomically.
type Stats struct {
	Pending  int64
	Executed int64
	Removed  int64
	Queued   int64
	Running  int64
	Dropped  int64
	Skipped  int64
}

// TimerInfo describes a pending timer returned by PendingTimers.
type TimerInfo struct {
	Key        TimerID
	NextFireAt time.Time
	Delay      time.Duration
	Repeating  bool
	RepeatMode RepeatMode
}

type wheelCommand[T any] struct {
	kind commandKind
	task *task[T]
	id   TimerID
	done jobDone[T]
	ack  chan error
}

type jobDone[T any] struct {
	id         TimerID
	data       T
	job        Job[T]
	contextJob JobContext[T]
	delay      time.Duration
	repeatMode RepeatMode
}

type workItem[T any] struct {
	id           TimerID
	data         T
	job          Job[T]
	contextJob   JobContext[T]
	delay        time.Duration
	mode         taskMode
	repeatMode   RepeatMode
	scheduledFor time.Time
}

type executeResult uint8

const (
	executeAccepted executeResult = iota
	executeDropped
	executeCanceled
)

// TimeWheel is a generic timer wheel.
//
// Use New to construct a wheel, Start to run it, and Close to stop and wait.
// The zero value is not usable.
type TimeWheel[T any] struct {
	interval   time.Duration
	slotNum    int
	defaultJob Job[T]

	// slots, currentPos, and due are owned by the event loop goroutine and
	// accessed without locks: every mutation happens on the loop started by
	// Start, either while handling a tick or while handling a command.
	slots      [][]*task[T]
	currentPos int
	due        []*task[T]

	pool   sync.Pool
	keyGen atomic.Uint64

	commandCh chan wheelCommand[T]
	done      chan struct{}
	closeDone sync.Once

	stateMu sync.Mutex
	state   wheelState
	ctx     context.Context
	cancel  context.CancelFunc

	timerIndex   map[TimerID]timerMeta
	timerIndexMu sync.RWMutex

	stats struct {
		pending  atomic.Int64
		executed atomic.Int64
		removed  atomic.Int64
		queued   atomic.Int64
		running  atomic.Int64
		dropped  atomic.Int64
		skipped  atomic.Int64
	}

	workCh chan workItem[T]

	cfg config[T]
	wg  sync.WaitGroup
}

// New creates and initialises a new TimeWheel.
func New[T any](interval time.Duration, slotNum int, defaultJob Job[T], opts ...Option[T]) (*TimeWheel[T], error) {
	if interval <= 0 {
		return nil, errors.New("timewheel: interval must be positive")
	}
	if slotNum <= 0 {
		return nil, errors.New("timewheel: slotNum must be positive")
	}

	cfg := config[T]{
		clock: realClock{},
	}
	for _, o := range opts {
		o(&cfg)
	}
	if cfg.queueSize < 0 {
		return nil, errors.New("timewheel: worker queue size must be non-negative")
	}
	if cfg.clock == nil {
		cfg.clock = realClock{}
	}

	tw := &TimeWheel[T]{
		interval:   interval,
		slotNum:    slotNum,
		defaultJob: defaultJob,
		slots:      make([][]*task[T], slotNum),
		commandCh:  make(chan wheelCommand[T], 64),
		done:       make(chan struct{}),
		timerIndex: make(map[TimerID]timerMeta),
		cfg:        cfg,
	}
	tw.pool = sync.Pool{New: func() any { return new(task[T]) }}
	if cfg.workerNum > 0 {
		tw.workCh = make(chan workItem[T], cfg.queueSize)
	}
	return tw, nil
}

// Start launches the event loop. It may be called once successfully.
func (tw *TimeWheel[T]) Start(ctx context.Context) error {
	if ctx == nil {
		return ErrNilContext
	}

	tw.stateMu.Lock()
	defer tw.stateMu.Unlock()

	switch tw.state {
	case stateRunning:
		return ErrRunning
	case stateClosed:
		return ErrClosed
	}

	tw.ctx, tw.cancel = context.WithCancel(ctx)
	tw.state = stateRunning

	for range tw.cfg.workerNum {
		tw.wg.Add(1)
		go tw.worker()
	}

	tw.wg.Add(1)
	go tw.run()
	return nil
}

// Stop begins shutdown. It does not wait for the event loop or workers to exit.
func (tw *TimeWheel[T]) Stop() error {
	tw.stateMu.Lock()
	defer tw.stateMu.Unlock()

	switch tw.state {
	case stateNew:
		return ErrNotStarted
	case stateClosed:
		return nil
	}

	tw.state = stateClosed
	if tw.cancel != nil {
		tw.cancel()
	}
	return nil
}

// Close stops the wheel and waits for the event loop and worker pool to exit.
func (tw *TimeWheel[T]) Close() error {
	tw.stateMu.Lock()
	switch tw.state {
	case stateNew:
		tw.state = stateClosed
		tw.closeDone.Do(func() { close(tw.done) })
		tw.stateMu.Unlock()
		return nil
	case stateRunning:
		tw.state = stateClosed
		if tw.cancel != nil {
			tw.cancel()
		}
	}
	tw.stateMu.Unlock()

	tw.Wait()
	return nil
}

// Wait blocks until the event loop and worker pool have exited.
func (tw *TimeWheel[T]) Wait() {
	tw.wg.Wait()
}

func (tw *TimeWheel[T]) run() {
	defer tw.wg.Done()
	defer func() {
		tw.stateMu.Lock()
		tw.state = stateClosed
		tw.stateMu.Unlock()
		tw.clearTimers()
		tw.closeDone.Do(func() { close(tw.done) })
	}()

	ticker := tw.cfg.clock.NewTicker(tw.interval)
	defer ticker.Stop()

	for {
		select {
		case <-tw.ctx.Done():
			tw.logInfo("timewheel: stopped", "reason", tw.ctx.Err())
			return
		case <-ticker.C():
			tw.tick()
		case cmd := <-tw.commandCh:
			tw.handleCommand(cmd)
		}
	}
}

func (tw *TimeWheel[T]) handleCommand(cmd wheelCommand[T]) {
	var err error
	switch cmd.kind {
	case commandAdd:
		tw.placeNewTask(cmd.task)
	case commandRemove:
		tw.deleteTask(cmd.id)
	case commandJobDone:
		tw.finishRepeatingJob(cmd.done)
	}
	if cmd.ack != nil {
		cmd.ack <- err
	}
}

// AddTimer enqueues a one-shot timer that uses the wheel's default job.
func (tw *TimeWheel[T]) AddTimer(delay time.Duration, data T) (TimerID, error) {
	return tw.enqueue(delay, data, nil, nil, modeOnce, FixedRate)
}

// AddTimerWithJob enqueues a one-shot timer with a per-timer job.
func (tw *TimeWheel[T]) AddTimerWithJob(delay time.Duration, data T, job Job[T]) (TimerID, error) {
	if job == nil {
		return 0, ErrNilJob
	}
	return tw.enqueue(delay, data, job, nil, modeOnce, FixedRate)
}

// AddTimerWithContextJob enqueues a one-shot timer with a context-aware job.
func (tw *TimeWheel[T]) AddTimerWithContextJob(delay time.Duration, data T, job JobContext[T]) (TimerID, error) {
	if job == nil {
		return 0, ErrNilJob
	}
	return tw.enqueue(delay, data, nil, job, modeOnce, FixedRate)
}

// AddTimerFunc enqueues a one-shot timer that calls fn after delay.
func (tw *TimeWheel[T]) AddTimerFunc(delay time.Duration, fn func()) (TimerID, error) {
	if fn == nil {
		return 0, ErrNilJob
	}
	var zero T
	return tw.enqueue(delay, zero, func(T) { fn() }, nil, modeOnce, FixedRate)
}

// AddRepeatingTimer enqueues a repeating timer that uses the default job.
func (tw *TimeWheel[T]) AddRepeatingTimer(delay time.Duration, data T, opts RepeatOptions) (TimerID, error) {
	return tw.enqueue(delay, data, nil, nil, modeRepeat, normalizeRepeatMode(opts.Mode))
}

// AddRepeatingTimerWithJob enqueues a repeating timer with a per-timer job.
func (tw *TimeWheel[T]) AddRepeatingTimerWithJob(delay time.Duration, data T, job Job[T], opts RepeatOptions) (TimerID, error) {
	if job == nil {
		return 0, ErrNilJob
	}
	return tw.enqueue(delay, data, job, nil, modeRepeat, normalizeRepeatMode(opts.Mode))
}

// AddRepeatingTimerWithContextJob enqueues a repeating timer with a context-aware job.
func (tw *TimeWheel[T]) AddRepeatingTimerWithContextJob(delay time.Duration, data T, job JobContext[T], opts RepeatOptions) (TimerID, error) {
	if job == nil {
		return 0, ErrNilJob
	}
	return tw.enqueue(delay, data, nil, job, modeRepeat, normalizeRepeatMode(opts.Mode))
}

// RemoveTimer cancels future not-yet-started executions for id.
func (tw *TimeWheel[T]) RemoveTimer(id TimerID) error {
	return tw.sendCommand(wheelCommand[T]{
		kind: commandRemove,
		id:   id,
	})
}

// NextFireTime returns the estimated next fire time for a pending timer.
func (tw *TimeWheel[T]) NextFireTime(id TimerID) (time.Time, bool) {
	tw.timerIndexMu.RLock()
	meta, ok := tw.timerIndex[id]
	tw.timerIndexMu.RUnlock()
	if !ok || !meta.location.valid() || meta.nextFireAt.IsZero() {
		return time.Time{}, false
	}
	return meta.nextFireAt, true
}

// PendingTimers returns a sorted snapshot of pending timers.
func (tw *TimeWheel[T]) PendingTimers() []TimerInfo {
	tw.timerIndexMu.RLock()
	out := make([]TimerInfo, 0, len(tw.timerIndex))
	for key, meta := range tw.timerIndex {
		if !meta.location.valid() || meta.nextFireAt.IsZero() {
			continue
		}
		out = append(out, TimerInfo{
			Key:        key,
			NextFireAt: meta.nextFireAt,
			Delay:      meta.delay,
			Repeating:  meta.mode == modeRepeat,
			RepeatMode: meta.repeatMode,
		})
	}
	tw.timerIndexMu.RUnlock()

	slices.SortFunc(out, func(a, b TimerInfo) int {
		return cmp.Or(a.NextFireAt.Compare(b.NextFireAt), cmp.Compare(a.Key, b.Key))
	})
	return out
}

// Stats returns a snapshot of runtime counters.
func (tw *TimeWheel[T]) Stats() Stats {
	return Stats{
		Pending:  tw.stats.pending.Load(),
		Executed: tw.stats.executed.Load(),
		Removed:  tw.stats.removed.Load(),
		Queued:   tw.stats.queued.Load(),
		Running:  tw.stats.running.Load(),
		Dropped:  tw.stats.dropped.Load(),
		Skipped:  tw.stats.skipped.Load(),
	}
}

func (tw *TimeWheel[T]) enqueue(delay time.Duration, data T, job Job[T], contextJob JobContext[T], mode taskMode, repeatMode RepeatMode) (TimerID, error) {
	delay = max(delay, tw.interval)
	id := TimerID(tw.keyGen.Add(1))

	t := tw.pool.Get().(*task[T])
	*t = task[T]{
		key:        id,
		data:       data,
		job:        job,
		contextJob: contextJob,
		delay:      delay,
		mode:       mode,
		repeatMode: repeatMode,
	}

	err := tw.sendCommand(wheelCommand[T]{
		kind: commandAdd,
		task: t,
	})
	if err != nil {
		tw.releaseTask(t)
		return 0, err
	}
	return id, nil
}

// ackPool recycles the per-command acknowledgement channels. A channel is
// returned to the pool only once no event-loop write to it can happen: after
// its value was consumed, or once it is known the command was never processed.
var ackPool = sync.Pool{New: func() any { return make(chan error, 1) }}

// sendCommand submits cmd to the event loop and waits for the acknowledgement
// that the command was applied.
func (tw *TimeWheel[T]) sendCommand(cmd wheelCommand[T]) error {
	tw.stateMu.Lock()
	switch tw.state {
	case stateNew:
		tw.stateMu.Unlock()
		return ErrNotStarted
	case stateClosed:
		tw.stateMu.Unlock()
		return ErrClosed
	}
	tw.stateMu.Unlock()

	ack := ackPool.Get().(chan error)
	cmd.ack = ack

	select {
	case tw.commandCh <- cmd:
	case <-tw.done:
		ackPool.Put(ack)
		return ErrClosed
	}

	select {
	case err := <-ack:
		ackPool.Put(ack)
		return err
	case <-tw.done:
		// The event loop acknowledges every command it handles before it
		// exits, and tw.done is closed only after the loop has returned, so
		// a buffered acknowledgement is guaranteed to be visible here. An
		// empty channel means the command was never processed.
		select {
		case err := <-ack:
			ackPool.Put(ack)
			return err
		default:
			ackPool.Put(ack)
			return ErrClosed
		}
	}
}

// placeNewTask inserts a newly added timer. It runs on the event loop.
func (tw *TimeWheel[T]) placeNewTask(t *task[T]) {
	nextFireAt := tw.cfg.clock.Now().Add(t.delay)
	pos, circle := tw.calcPosCircle(t.delay)
	t.circle = circle
	t.scheduledFor = nextFireAt

	tw.slots[pos] = append(tw.slots[pos], t)

	tw.timerIndexMu.Lock()
	tw.timerIndex[t.key] = timerMeta{
		nextFireAt: nextFireAt,
		delay:      t.delay,
		mode:       t.mode,
		repeatMode: t.repeatMode,
		location:   timerLocation{slot: pos, index: len(tw.slots[pos]) - 1},
	}
	tw.timerIndexMu.Unlock()
	tw.stats.pending.Add(1)
}

// placeTask re-places a repeating timer so that it fires at nextFireAt, which
// is wait from now. It runs on the event loop. If the timer was removed in
// the meantime the task is discarded.
func (tw *TimeWheel[T]) placeTask(t *task[T], nextFireAt time.Time, wait time.Duration) {
	pos, circle := tw.calcPosCircle(wait)
	t.circle = circle
	t.scheduledFor = nextFireAt

	tw.timerIndexMu.Lock()
	meta, ok := tw.timerIndex[t.key]
	if !ok {
		tw.timerIndexMu.Unlock()
		tw.releaseTask(t)
		return
	}
	tw.slots[pos] = append(tw.slots[pos], t)
	meta.nextFireAt = nextFireAt
	meta.location = timerLocation{slot: pos, index: len(tw.slots[pos]) - 1}
	tw.timerIndex[t.key] = meta
	tw.timerIndexMu.Unlock()
	tw.stats.pending.Add(1)
}

// tick advances the wheel by one slot. It runs on the event loop.
func (tw *TimeWheel[T]) tick() {
	cur := tw.currentPos
	tasks := tw.slots[cur]

	due := tw.due[:0]
	remaining := tasks[:0]
	firstDue := -1

	for i, t := range tasks {
		if t.circle > 0 {
			t.circle--
			remaining = append(remaining, t)
			continue
		}
		if firstDue < 0 {
			firstDue = i
		}
		due = append(due, t)
	}

	clear(tasks[len(remaining):])
	tw.slots[cur] = remaining

	// Slot indexes only change when a due task was extracted: tasks before
	// the first extraction keep their index, the rest shift left. Batch all
	// index updates under one lock acquisition.
	if len(due) > 0 {
		tw.timerIndexMu.Lock()
		for i := firstDue; i < len(remaining); i++ {
			if meta, ok := tw.timerIndex[remaining[i].key]; ok {
				meta.location = timerLocation{slot: cur, index: i}
				tw.timerIndex[remaining[i].key] = meta
			}
		}
		for _, t := range due {
			if meta, ok := tw.timerIndex[t.key]; ok {
				meta.location = noLocation()
				tw.timerIndex[t.key] = meta
			}
		}
		tw.timerIndexMu.Unlock()
	}

	tw.currentPos = (cur + 1) % tw.slotNum

	for _, t := range due {
		tw.dispatch(t)
	}
	clear(due)
	tw.due = due[:0]
}

func (tw *TimeWheel[T]) dispatch(t *task[T]) {
	tw.stats.pending.Add(-1)

	job := t.job
	contextJob := t.contextJob
	if job == nil && contextJob == nil {
		job = tw.defaultJob
	}
	if job == nil && contextJob == nil {
		tw.logWarn("timewheel: task has no job and wheel has no default job", "key", t.key)
		tw.observe(JobEvent[T]{
			TimerID:      t.key,
			Data:         t.data,
			ScheduledFor: t.scheduledFor,
			Err:          ErrNilJob,
			Skipped:      true,
		})
		tw.removeFromIndex(t.key)
		tw.releaseTask(t)
		return
	}

	item := workItem[T]{
		id:           t.key,
		data:         t.data,
		job:          job,
		contextJob:   contextJob,
		delay:        t.delay,
		mode:         t.mode,
		repeatMode:   t.repeatMode,
		scheduledFor: t.scheduledFor,
	}

	switch t.mode {
	case modeOnce:
		tw.removeFromIndex(t.key)
		tw.execute(item)
	case modeRepeat:
		tw.dispatchRepeating(t, item)
	}

	tw.releaseTask(t)
}

func (tw *TimeWheel[T]) dispatchRepeating(t *task[T], item workItem[T]) {
	switch t.repeatMode {
	case FixedDelay:
		tw.markRunning(t.key, true)
		result := tw.execute(item)
		if result != executeAccepted {
			tw.markRunning(t.key, false)
			if result == executeDropped {
				tw.reenqueue(t.key, t.delay, t.data, t.job, t.contextJob, t.repeatMode, t.scheduledFor)
			}
		}
	case SkipIfRunning:
		if tw.isRunning(t.key) {
			tw.stats.skipped.Add(1)
			tw.observe(JobEvent[T]{
				TimerID:      t.key,
				Data:         t.data,
				ScheduledFor: t.scheduledFor,
				Skipped:      true,
			})
			tw.reenqueue(t.key, t.delay, t.data, t.job, t.contextJob, t.repeatMode, t.scheduledFor)
			return
		}
		tw.markRunning(t.key, true)
		result := tw.execute(item)
		if result == executeAccepted {
			tw.reenqueue(t.key, t.delay, t.data, t.job, t.contextJob, t.repeatMode, t.scheduledFor)
			return
		}
		tw.markRunning(t.key, false)
		if result == executeDropped {
			tw.reenqueue(t.key, t.delay, t.data, t.job, t.contextJob, t.repeatMode, t.scheduledFor)
		}
	default:
		result := tw.execute(item)
		if result != executeCanceled {
			tw.reenqueue(t.key, t.delay, t.data, t.job, t.contextJob, FixedRate, t.scheduledFor)
		}
	}
}

func (tw *TimeWheel[T]) execute(item workItem[T]) executeResult {
	if tw.workCh == nil {
		go tw.runJob(item, false)
		return executeAccepted
	}

	switch tw.cfg.backpressure {
	case Drop:
		tw.stats.queued.Add(1)
		select {
		case tw.workCh <- item:
			return executeAccepted
		default:
			tw.stats.queued.Add(-1)
			tw.stats.dropped.Add(1)
			tw.observe(JobEvent[T]{
				TimerID:      item.id,
				Data:         item.data,
				ScheduledFor: item.scheduledFor,
				Dropped:      true,
				Err:          ErrQueueFull,
			})
			return executeDropped
		}
	case RunInline:
		tw.stats.queued.Add(1)
		select {
		case tw.workCh <- item:
			return executeAccepted
		default:
			tw.stats.queued.Add(-1)
			tw.runJob(item, true)
			return executeAccepted
		}
	default:
		tw.stats.queued.Add(1)
		select {
		case tw.workCh <- item:
			return executeAccepted
		case <-tw.ctx.Done():
			tw.stats.queued.Add(-1)
			return executeCanceled
		}
	}
}

func (tw *TimeWheel[T]) worker() {
	defer tw.wg.Done()

	for {
		select {
		case <-tw.ctx.Done():
			return
		case item := <-tw.workCh:
			tw.stats.queued.Add(-1)
			tw.runJob(item, false)
		}
	}
}

func (tw *TimeWheel[T]) runJob(item workItem[T], inlineCompletion bool) {
	startedAt := tw.cfg.clock.Now()
	event := JobEvent[T]{
		TimerID:      item.id,
		Data:         item.data,
		StartedAt:    startedAt,
		ScheduledFor: item.scheduledFor,
		Lateness:     startedAt.Sub(item.scheduledFor),
	}
	if event.Lateness < 0 {
		event.Lateness = 0
	}

	tw.stats.running.Add(1)
	tw.stats.executed.Add(1)

	shouldRecover := tw.cfg.errorHandler != nil || tw.cfg.observer != nil
	if shouldRecover {
		defer func() {
			if r := recover(); r != nil {
				event.Panic = r
				if tw.cfg.errorHandler != nil {
					tw.cfg.errorHandler(r)
				}
			}
			tw.finishJob(item, event, inlineCompletion)
		}()
	} else {
		defer func() {
			tw.finishJob(item, event, inlineCompletion)
		}()
	}

	if item.contextJob != nil {
		event.Err = item.contextJob(tw.ctx, item.data)
		return
	}
	item.job(item.data)
}

func (tw *TimeWheel[T]) finishJob(item workItem[T], event JobEvent[T], inlineCompletion bool) {
	finishedAt := tw.cfg.clock.Now()
	event.FinishedAt = finishedAt
	event.Duration = finishedAt.Sub(event.StartedAt)
	tw.stats.running.Add(-1)
	tw.observe(event)

	if item.mode != modeRepeat {
		return
	}
	if item.repeatMode != FixedDelay && item.repeatMode != SkipIfRunning {
		return
	}

	done := jobDone[T]{
		id:         item.id,
		data:       item.data,
		job:        item.job,
		contextJob: item.contextJob,
		delay:      item.delay,
		repeatMode: item.repeatMode,
	}
	if inlineCompletion {
		tw.finishRepeatingJob(done)
		return
	}

	select {
	case tw.commandCh <- wheelCommand[T]{
		kind: commandJobDone,
		done: done,
	}:
	case <-tw.ctx.Done():
	case <-tw.done:
	}
}

func (tw *TimeWheel[T]) finishRepeatingJob(done jobDone[T]) {
	tw.timerIndexMu.Lock()
	meta, ok := tw.timerIndex[done.id]
	if !ok {
		tw.timerIndexMu.Unlock()
		return
	}
	meta.running = false
	tw.timerIndex[done.id] = meta
	tw.timerIndexMu.Unlock()

	if done.repeatMode == FixedDelay {
		tw.reenqueue(done.id, done.delay, done.data, done.job, done.contextJob, done.repeatMode, time.Time{})
	}
}

// reenqueue places the next occurrence of a repeating timer. FixedRate and
// SkipIfRunning stay anchored to the schedule grid (prev + n*delay), skipping
// past periods that were missed entirely; FixedDelay waits delay from now.
func (tw *TimeWheel[T]) reenqueue(id TimerID, delay time.Duration, data T, job Job[T], contextJob JobContext[T], repeatMode RepeatMode, prev time.Time) {
	select {
	case <-tw.ctx.Done():
		return
	default:
	}

	t := tw.pool.Get().(*task[T])
	*t = task[T]{
		key:        id,
		data:       data,
		job:        job,
		contextJob: contextJob,
		delay:      delay,
		mode:       modeRepeat,
		repeatMode: repeatMode,
	}

	now := tw.cfg.clock.Now()
	next := now.Add(delay)
	wait := delay
	if repeatMode != FixedDelay && !prev.IsZero() {
		next = nextGridTime(prev, delay, now)
		wait = next.Sub(now)
	}
	tw.placeTask(t, next, wait)
}

// nextGridTime returns the first instant after now on the prev + n*period
// grid. Missed periods are skipped rather than fired in a burst.
func nextGridTime(prev time.Time, period time.Duration, now time.Time) time.Time {
	next := prev.Add(period)
	if next.After(now) {
		return next
	}
	behind := now.Sub(next)
	return next.Add((behind/period + 1) * period)
}

func (tw *TimeWheel[T]) clearTimers() {
	for i := range tw.slots {
		for _, t := range tw.slots[i] {
			tw.releaseTask(t)
		}
		tw.slots[i] = nil
	}

	tw.timerIndexMu.Lock()
	clear(tw.timerIndex)
	tw.timerIndexMu.Unlock()
	tw.stats.pending.Store(0)
}

func (tw *TimeWheel[T]) deleteTask(id TimerID) {
	tw.timerIndexMu.Lock()
	meta, ok := tw.timerIndex[id]
	if !ok {
		tw.timerIndexMu.Unlock()
		return
	}
	delete(tw.timerIndex, id)
	tw.timerIndexMu.Unlock()

	if meta.location.valid() {
		tw.deleteFromSlot(meta.location)
		tw.stats.pending.Add(-1)
	}
	tw.stats.removed.Add(1)
}

// deleteFromSlot removes the task at loc using swap-and-shrink. It runs on
// the event loop.
func (tw *TimeWheel[T]) deleteFromSlot(loc timerLocation) {
	tasks := tw.slots[loc.slot]
	if loc.index < 0 || loc.index >= len(tasks) {
		return
	}

	t := tasks[loc.index]
	last := len(tasks) - 1
	tasks[loc.index] = tasks[last]
	tasks[last] = nil
	tw.slots[loc.slot] = tasks[:last]

	if loc.index < last {
		tw.updateLocation(tasks[loc.index].key, loc)
	}
	tw.releaseTask(t)
}

func (tw *TimeWheel[T]) removeFromIndex(id TimerID) {
	tw.timerIndexMu.Lock()
	delete(tw.timerIndex, id)
	tw.timerIndexMu.Unlock()
}

func (tw *TimeWheel[T]) updateLocation(id TimerID, loc timerLocation) {
	tw.timerIndexMu.Lock()
	meta, ok := tw.timerIndex[id]
	if ok {
		meta.location = loc
		tw.timerIndex[id] = meta
	}
	tw.timerIndexMu.Unlock()
}

func (tw *TimeWheel[T]) markRunning(id TimerID, running bool) {
	tw.timerIndexMu.Lock()
	meta, ok := tw.timerIndex[id]
	if ok {
		meta.running = running
		tw.timerIndex[id] = meta
	}
	tw.timerIndexMu.Unlock()
}

func (tw *TimeWheel[T]) isRunning(id TimerID) bool {
	tw.timerIndexMu.RLock()
	meta, ok := tw.timerIndex[id]
	tw.timerIndexMu.RUnlock()
	return ok && meta.running
}

func (tw *TimeWheel[T]) observe(event JobEvent[T]) {
	if tw.cfg.observer != nil {
		tw.cfg.observer(event)
	}
}

func (tw *TimeWheel[T]) releaseTask(t *task[T]) {
	*t = task[T]{}
	tw.pool.Put(t)
}

func (tw *TimeWheel[T]) calcPosCircle(delay time.Duration) (pos int, circle int) {
	ticks := int((delay + tw.interval - 1) / tw.interval)
	if ticks <= 0 {
		ticks = 1
	}

	offset := ticks - 1
	pos = (tw.currentPos + offset) % tw.slotNum
	circle = offset / tw.slotNum
	return pos, circle
}

func normalizeRepeatMode(mode RepeatMode) RepeatMode {
	switch mode {
	case FixedRate, FixedDelay, SkipIfRunning:
		return mode
	default:
		return FixedRate
	}
}

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
