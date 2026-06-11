// Package timewheel provides a generic timer wheel implementation.
//
// TimeWheel is safe for concurrent use. Timer placement and deletion are
// serialized through the wheel event loop; job execution is dispatched outside
// the slot locks.
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

type slot[T any] struct {
	mu    sync.Mutex
	tasks []*task[T]
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

// TimeWheel is a generic timer wheel.
//
// Use New to construct a wheel, Start to run it, and Close to stop and wait.
// The zero value is not usable.
type TimeWheel[T any] struct {
	interval   time.Duration
	slotNum    int
	defaultJob Job[T]

	slots []slot[T]

	currentPos atomic.Int64
	pool       sync.Pool
	keyGen     atomic.Uint64

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
		slots:      make([]slot[T], slotNum),
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
		ack:  make(chan error, 1),
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
		ack:  make(chan error, 1),
	})
	if err != nil {
		tw.releaseTask(t)
		return 0, err
	}
	return id, nil
}

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

	select {
	case tw.commandCh <- cmd:
	case <-tw.done:
		return ErrClosed
	}

	if cmd.ack == nil {
		return nil
	}
	select {
	case err := <-cmd.ack:
		return err
	case <-tw.done:
		return ErrClosed
	}
}

func (tw *TimeWheel[T]) placeNewTask(t *task[T]) {
	nextFireAt := tw.cfg.clock.Now().Add(t.delay)

	tw.timerIndexMu.Lock()
	tw.timerIndex[t.key] = timerMeta{
		nextFireAt: nextFireAt,
		delay:      t.delay,
		mode:       t.mode,
		repeatMode: t.repeatMode,
		location:   noLocation(),
	}
	tw.timerIndexMu.Unlock()

	tw.placeTask(t, nextFireAt)
}

func (tw *TimeWheel[T]) placeTask(t *task[T], nextFireAt time.Time) {
	pos, circle := tw.calcPosCircle(t.delay)
	t.circle = circle
	t.scheduledFor = nextFireAt

	s := &tw.slots[pos]
	s.mu.Lock()
	idx := len(s.tasks)
	s.tasks = append(s.tasks, t)
	s.mu.Unlock()

	tw.timerIndexMu.Lock()
	meta, ok := tw.timerIndex[t.key]
	if ok {
		meta.nextFireAt = nextFireAt
		meta.location = timerLocation{slot: pos, index: idx}
		tw.timerIndex[t.key] = meta
		tw.stats.pending.Add(1)
	} else {
		tw.removeTaskFromSlot(pos, idx)
		tw.releaseTask(t)
	}
	tw.timerIndexMu.Unlock()
}

func (tw *TimeWheel[T]) tick() {
	cur := int(tw.currentPos.Load())
	s := &tw.slots[cur]

	s.mu.Lock()
	remaining := s.tasks[:0]
	var due []*task[T]

	for _, t := range s.tasks {
		if t.circle > 0 {
			t.circle--
			remaining = append(remaining, t)
			continue
		}
		due = append(due, t)
	}

	clear(s.tasks[len(remaining):])
	s.tasks = remaining
	for i, t := range s.tasks {
		tw.updateLocation(t.key, timerLocation{slot: cur, index: i})
	}
	s.mu.Unlock()

	for _, t := range due {
		tw.clearLocation(t.key)
	}

	tw.currentPos.Store(int64((cur + 1) % tw.slotNum))

	for _, t := range due {
		tw.dispatch(t)
	}
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
		tw.execute(item)
	case SkipIfRunning:
		if tw.isRunning(t.key) {
			tw.stats.skipped.Add(1)
			tw.observe(JobEvent[T]{
				TimerID:      t.key,
				Data:         t.data,
				ScheduledFor: t.scheduledFor,
				Skipped:      true,
			})
			tw.reenqueue(t.key, t.delay, t.data, t.job, t.contextJob, t.repeatMode)
			return
		}
		tw.markRunning(t.key, true)
		tw.execute(item)
		tw.reenqueue(t.key, t.delay, t.data, t.job, t.contextJob, t.repeatMode)
	default:
		tw.execute(item)
		tw.reenqueue(t.key, t.delay, t.data, t.job, t.contextJob, FixedRate)
	}
}

func (tw *TimeWheel[T]) execute(item workItem[T]) {
	if tw.workCh == nil {
		go tw.runJob(item)
		return
	}

	switch tw.cfg.backpressure {
	case Drop:
		tw.stats.queued.Add(1)
		select {
		case tw.workCh <- item:
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
		}
	case RunInline:
		tw.stats.queued.Add(1)
		select {
		case tw.workCh <- item:
		default:
			tw.stats.queued.Add(-1)
			tw.runJob(item)
		}
	default:
		tw.stats.queued.Add(1)
		select {
		case tw.workCh <- item:
		case <-tw.ctx.Done():
			tw.stats.queued.Add(-1)
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
			tw.runJob(item)
		}
	}
}

func (tw *TimeWheel[T]) runJob(item workItem[T]) {
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
			tw.finishJob(item, event)
		}()
	} else {
		defer func() {
			tw.finishJob(item, event)
		}()
	}

	if item.contextJob != nil {
		event.Err = item.contextJob(tw.ctx, item.data)
		return
	}
	item.job(item.data)
}

func (tw *TimeWheel[T]) finishJob(item workItem[T], event JobEvent[T]) {
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

	select {
	case tw.commandCh <- wheelCommand[T]{
		kind: commandJobDone,
		done: jobDone[T]{
			id:         item.id,
			data:       item.data,
			job:        item.job,
			contextJob: item.contextJob,
			delay:      item.delay,
			repeatMode: item.repeatMode,
		},
	}:
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
		tw.reenqueue(done.id, done.delay, done.data, done.job, done.contextJob, done.repeatMode)
	}
}

func (tw *TimeWheel[T]) reenqueue(id TimerID, delay time.Duration, data T, job Job[T], contextJob JobContext[T], repeatMode RepeatMode) {
	tw.timerIndexMu.RLock()
	_, ok := tw.timerIndex[id]
	tw.timerIndexMu.RUnlock()
	if !ok {
		return
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
	tw.placeTask(t, tw.cfg.clock.Now().Add(delay))
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

func (tw *TimeWheel[T]) deleteFromSlot(loc timerLocation) {
	s := &tw.slots[loc.slot]
	s.mu.Lock()
	defer s.mu.Unlock()

	if loc.index < 0 || loc.index >= len(s.tasks) {
		return
	}

	t := s.tasks[loc.index]
	last := len(s.tasks) - 1
	s.tasks[loc.index] = s.tasks[last]
	s.tasks[last] = nil
	s.tasks = s.tasks[:last]

	if loc.index < last {
		moved := s.tasks[loc.index]
		tw.updateLocation(moved.key, loc)
	}
	tw.releaseTask(t)
}

func (tw *TimeWheel[T]) removeTaskFromSlot(slotIndex, taskIndex int) {
	s := &tw.slots[slotIndex]
	s.mu.Lock()
	defer s.mu.Unlock()

	if taskIndex < 0 || taskIndex >= len(s.tasks) {
		return
	}
	last := len(s.tasks) - 1
	s.tasks[taskIndex] = s.tasks[last]
	s.tasks[last] = nil
	s.tasks = s.tasks[:last]
	if taskIndex < last {
		moved := s.tasks[taskIndex]
		tw.updateLocation(moved.key, timerLocation{slot: slotIndex, index: taskIndex})
	}
}

func (tw *TimeWheel[T]) removeFromIndex(id TimerID) {
	tw.timerIndexMu.Lock()
	delete(tw.timerIndex, id)
	tw.timerIndexMu.Unlock()
}

func (tw *TimeWheel[T]) clearLocation(id TimerID) {
	tw.updateLocation(id, noLocation())
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
	cur := int(tw.currentPos.Load())
	pos = (cur + offset) % tw.slotNum
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
