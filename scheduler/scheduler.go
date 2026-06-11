// Package scheduler provides a keyed dynamic scheduling layer on top of
// github.com/lib-x/timewheel.
package scheduler

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/lib-x/timewheel"
)

// NextFunc calculates the next execution time for a keyed item.
type NextFunc[K comparable, T any] func(now time.Time, key K, data T) (next time.Time, ok bool, err error)

// RunFunc executes a keyed item.
type RunFunc[K comparable, T any] func(ctx context.Context, key K, data T) error

// Options configures scheduler behavior and callbacks.
type Options[K comparable, T any] struct {
	Next NextFunc[K, T]
	Run  RunFunc[K, T]

	CancelRunningOnRemove  bool
	CancelRunningOnReplace bool
	WaitRunningOnClose     bool

	RunTimeout time.Duration

	ReschedulePolicy ReschedulePolicy

	OnFinish      func(key K, data T, err error)
	OnInvalid     func(key K, data T, err error)
	OnStateChange func(key K, state State)
}

// Item is a keyed scheduler item.
type Item[K comparable, T any] struct {
	Key  K
	Data T
}

// Runtime is a point-in-time snapshot of a keyed item.
type Runtime struct {
	State        State
	NextRunAt    *time.Time
	RunningSince *time.Time
	LastError    error
}

// State describes the runtime state of an item.
type State string

const (
	StatePending  State = "pending"
	StateRunning  State = "running"
	StateDisabled State = "disabled"
	StateInvalid  State = "invalid"
)

// ReschedulePolicy controls when the next execution is calculated.
type ReschedulePolicy int

const (
	// RescheduleAfterFinish schedules the next run after the current run completes.
	RescheduleAfterFinish ReschedulePolicy = iota

	// RescheduleBeforeRun schedules the next run before starting the current run.
	RescheduleBeforeRun

	// NoAutoReschedule disables automatic rescheduling after a run fires.
	NoAutoReschedule
)

var (
	ErrNilNext       = errors.New("scheduler: nil Next")
	ErrNilRun        = errors.New("scheduler: nil Run")
	ErrNilContext    = errors.New("scheduler: nil context")
	ErrRunning       = errors.New("scheduler: already running")
	ErrClosed        = errors.New("scheduler: closed")
	ErrInvalidWheel  = errors.New("scheduler: invalid wheel configuration")
	ErrInvalidOption = errors.New("scheduler: invalid option")
)

const (
	defaultWheelInterval = time.Second
	defaultWheelSlots    = 3600
)

type config struct {
	wheelInterval time.Duration
	wheelSlots    int
	wheelOptions  []timewheel.Option[any]

	cancelRunningOnRemove  bool
	cancelRunningOnReplace bool
	waitRunningOnClose     bool
	runTimeout             time.Duration
	reschedulePolicy       ReschedulePolicy
}

// Option configures Scheduler runtime behavior.
type Option func(*config)

// WithWheel configures the underlying time wheel.
func WithWheel(interval time.Duration, slots int) Option {
	return func(c *config) {
		c.wheelInterval = interval
		c.wheelSlots = slots
	}
}

// WithWheelOptions appends options for the underlying time wheel.
func WithWheelOptions(opts ...timewheel.Option[any]) Option {
	return func(c *config) {
		c.wheelOptions = append(c.wheelOptions, opts...)
	}
}

// WithCancelRunningOnRemove controls whether Remove cancels the running job context.
func WithCancelRunningOnRemove(enabled bool) Option {
	return func(c *config) {
		c.cancelRunningOnRemove = enabled
	}
}

// WithCancelRunningOnReplace controls whether Upsert and ReplaceAll cancel replaced job contexts.
func WithCancelRunningOnReplace(enabled bool) Option {
	return func(c *config) {
		c.cancelRunningOnReplace = enabled
	}
}

// WithWaitRunningOnClose controls whether Close waits for running jobs to finish.
func WithWaitRunningOnClose(enabled bool) Option {
	return func(c *config) {
		c.waitRunningOnClose = enabled
	}
}

// WithRunTimeout configures a per-run context timeout. Zero disables the timeout.
func WithRunTimeout(timeout time.Duration) Option {
	return func(c *config) {
		c.runTimeout = timeout
	}
}

// WithReschedulePolicy configures when automatic rescheduling happens.
func WithReschedulePolicy(policy ReschedulePolicy) Option {
	return func(c *config) {
		c.reschedulePolicy = policy
	}
}

type Scheduler[K comparable, T any] struct {
	opts Options[K, T]
	cfg  config

	mu      sync.Mutex
	items   map[K]*entry[T]
	started bool
	closed  bool

	ctx    context.Context
	cancel context.CancelFunc
	wheel  *timewheel.TimeWheel[any]

	runSeq uint64
	wg     sync.WaitGroup
}

type entry[T any] struct {
	data       T
	generation uint64

	baseState State
	lastState State
	lastError error

	timerID   timewheel.TimerID
	hasTimer  bool
	nextRunAt *time.Time

	running map[uint64]runningJob
}

type runningJob struct {
	generation uint64
	startedAt  time.Time
	cancel     context.CancelFunc
}

type runRequest[K comparable, T any] struct {
	key        K
	data       T
	generation uint64
}

type stateChange[K comparable] struct {
	key   K
	state State
}

// NewScheduler creates a keyed Scheduler.
func NewScheduler[K comparable, T any](opts Options[K, T], options ...Option) (*Scheduler[K, T], error) {
	if opts.Next == nil {
		return nil, ErrNilNext
	}
	if opts.Run == nil {
		return nil, ErrNilRun
	}

	cfg := config{
		wheelInterval:          defaultWheelInterval,
		wheelSlots:             defaultWheelSlots,
		cancelRunningOnRemove:  opts.CancelRunningOnRemove,
		cancelRunningOnReplace: opts.CancelRunningOnReplace,
		waitRunningOnClose:     opts.WaitRunningOnClose,
		runTimeout:             opts.RunTimeout,
		reschedulePolicy:       opts.ReschedulePolicy,
	}
	for _, option := range options {
		if option == nil {
			return nil, ErrInvalidOption
		}
		option(&cfg)
	}
	if cfg.wheelInterval <= 0 || cfg.wheelSlots <= 0 {
		return nil, ErrInvalidWheel
	}
	for _, option := range cfg.wheelOptions {
		if option == nil {
			return nil, ErrInvalidOption
		}
	}
	if cfg.runTimeout < 0 {
		return nil, ErrInvalidOption
	}
	switch cfg.reschedulePolicy {
	case RescheduleAfterFinish, RescheduleBeforeRun, NoAutoReschedule:
	default:
		return nil, ErrInvalidOption
	}

	opts.CancelRunningOnRemove = cfg.cancelRunningOnRemove
	opts.CancelRunningOnReplace = cfg.cancelRunningOnReplace
	opts.WaitRunningOnClose = cfg.waitRunningOnClose
	opts.RunTimeout = cfg.runTimeout
	opts.ReschedulePolicy = cfg.reschedulePolicy

	return &Scheduler[K, T]{
		opts:  opts,
		cfg:   cfg,
		items: make(map[K]*entry[T]),
	}, nil
}

// Start starts the scheduler and its underlying wheel.
func (s *Scheduler[K, T]) Start(ctx context.Context) error {
	if ctx == nil {
		return ErrNilContext
	}

	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return ErrClosed
	}
	if s.started {
		s.mu.Unlock()
		return ErrRunning
	}

	wheel, err := timewheel.New[any](
		s.cfg.wheelInterval,
		s.cfg.wheelSlots,
		func(v any) {
			req, ok := v.(runRequest[K, T])
			if !ok {
				return
			}
			s.fire(req)
		},
		s.cfg.wheelOptions...,
	)
	if err != nil {
		s.mu.Unlock()
		return fmt.Errorf("%w: %v", ErrInvalidWheel, err)
	}

	s.ctx, s.cancel = context.WithCancel(ctx)
	s.wheel = wheel
	s.started = true

	keys := make([]K, 0, len(s.items))
	generations := make(map[K]uint64, len(s.items))
	data := make(map[K]T, len(s.items))
	for key, item := range s.items {
		if item.hasTimer {
			item.hasTimer = false
			item.timerID = 0
			item.nextRunAt = nil
		}
		keys = append(keys, key)
		generations[key] = item.generation
		data[key] = item.data
	}
	s.mu.Unlock()

	if err := wheel.Start(s.ctx); err != nil {
		s.mu.Lock()
		s.started = false
		s.closed = true
		s.mu.Unlock()
		return err
	}

	for _, key := range keys {
		s.schedule(key, generations[key], data[key])
	}
	return nil
}

// Close stops the scheduler. It is idempotent.
func (s *Scheduler[K, T]) Close() error {
	var wheel *timewheel.TimeWheel[any]
	var cancel context.CancelFunc
	var timerIDs []timewheel.TimerID
	var stateChanges []stateChange[K]

	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		if s.opts.WaitRunningOnClose {
			s.wg.Wait()
		}
		return nil
	}
	s.closed = true
	wheel = s.wheel
	cancel = s.cancel
	if cancel != nil {
		cancel()
	}
	for key, item := range s.items {
		item.generation++
		if item.hasTimer {
			timerIDs = append(timerIDs, item.timerID)
			item.hasTimer = false
			item.timerID = 0
		}
		item.nextRunAt = nil
		if item.baseState == StatePending {
			item.baseState = StateDisabled
		}
		for _, running := range item.running {
			running.cancel()
		}
		changed, state := s.refreshStateLocked(item)
		if changed {
			stateChanges = append(stateChanges, stateChange[K]{key: key, state: state})
		}
	}
	s.mu.Unlock()

	for _, change := range stateChanges {
		s.notifyStateChange(change.key, true, change.state)
	}

	for _, id := range timerIDs {
		if wheel != nil {
			_ = wheel.RemoveTimer(id)
		}
	}
	if wheel != nil {
		if err := wheel.Close(); err != nil {
			return err
		}
	}
	if s.opts.WaitRunningOnClose {
		s.wg.Wait()
	}
	return nil
}

// ReplaceAll replaces the scheduler's item set.
func (s *Scheduler[K, T]) ReplaceAll(items []Item[K, T]) error {
	next := make(map[K]T, len(items))
	for _, item := range items {
		next[item.Key] = item.Data
	}

	var removeTimers []timewheel.TimerID
	var schedules []runRequest[K, T]

	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return ErrClosed
	}

	for key, item := range s.items {
		if _, ok := next[key]; ok {
			continue
		}
		item.generation++
		if item.hasTimer {
			removeTimers = append(removeTimers, item.timerID)
		}
		if s.opts.CancelRunningOnRemove {
			for _, running := range item.running {
				running.cancel()
			}
		}
		delete(s.items, key)
	}

	for key, data := range next {
		item := s.ensureEntryLocked(key, data)
		item.generation++
		item.data = data
		item.lastError = nil
		if item.hasTimer {
			removeTimers = append(removeTimers, item.timerID)
			item.hasTimer = false
			item.timerID = 0
			item.nextRunAt = nil
		}
		if s.opts.CancelRunningOnReplace {
			for _, running := range item.running {
				running.cancel()
			}
		}
		schedules = append(schedules, runRequest[K, T]{
			key:        key,
			data:       data,
			generation: item.generation,
		})
	}
	wheel := s.wheel
	s.mu.Unlock()

	for _, id := range removeTimers {
		if wheel != nil {
			_ = wheel.RemoveTimer(id)
		}
	}
	for _, req := range schedules {
		s.schedule(req.key, req.generation, req.data)
	}
	return nil
}

// Upsert inserts or replaces one scheduler item.
func (s *Scheduler[K, T]) Upsert(item Item[K, T]) error {
	var removeTimer *timewheel.TimerID

	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return ErrClosed
	}
	entry := s.ensureEntryLocked(item.Key, item.Data)
	entry.generation++
	entry.data = item.Data
	entry.lastError = nil
	if entry.hasTimer {
		id := entry.timerID
		removeTimer = &id
		entry.hasTimer = false
		entry.timerID = 0
		entry.nextRunAt = nil
	}
	if s.opts.CancelRunningOnReplace {
		for _, running := range entry.running {
			running.cancel()
		}
	}
	generation := entry.generation
	wheel := s.wheel
	s.mu.Unlock()

	if removeTimer != nil && wheel != nil {
		_ = wheel.RemoveTimer(*removeTimer)
	}
	s.schedule(item.Key, generation, item.Data)
	return nil
}

// Remove removes one scheduler item.
func (s *Scheduler[K, T]) Remove(key K) error {
	var removeTimer *timewheel.TimerID

	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return ErrClosed
	}
	item, ok := s.items[key]
	if !ok {
		s.mu.Unlock()
		return nil
	}
	item.generation++
	if item.hasTimer {
		id := item.timerID
		removeTimer = &id
	}
	if s.opts.CancelRunningOnRemove {
		for _, running := range item.running {
			running.cancel()
		}
	}
	delete(s.items, key)
	wheel := s.wheel
	s.mu.Unlock()

	if removeTimer != nil && wheel != nil {
		_ = wheel.RemoveTimer(*removeTimer)
	}
	return nil
}

// Snapshot returns a copy of all scheduler runtimes keyed by item key.
func (s *Scheduler[K, T]) Snapshot() map[K]Runtime {
	s.mu.Lock()
	defer s.mu.Unlock()

	out := make(map[K]Runtime, len(s.items))
	for key, item := range s.items {
		runtime := Runtime{
			State:     item.lastState,
			LastError: item.lastError,
		}
		if item.nextRunAt != nil {
			next := *item.nextRunAt
			runtime.NextRunAt = &next
		}
		if len(item.running) > 0 {
			var earliest time.Time
			for _, running := range item.running {
				if earliest.IsZero() || running.startedAt.Before(earliest) {
					earliest = running.startedAt
				}
			}
			runtime.RunningSince = &earliest
		}
		out[key] = runtime
	}
	return out
}

func (s *Scheduler[K, T]) schedule(key K, generation uint64, data T) {
	s.mu.Lock()
	item, ok := s.items[key]
	if !ok || s.closed || item.generation != generation {
		s.mu.Unlock()
		return
	}
	s.mu.Unlock()

	now := time.Now()
	next, ok, err := s.opts.Next(now, key, data)
	if err != nil {
		s.markInvalid(key, generation, data, err)
		return
	}
	if !ok {
		s.markDisabled(key, generation)
		return
	}

	var timerID timewheel.TimerID
	var addErr error

	s.mu.Lock()
	started := s.started && !s.closed && s.wheel != nil
	wheel := s.wheel
	s.mu.Unlock()

	if started {
		delay := time.Until(next)
		if delay < 0 {
			delay = 0
		}
		timerID, addErr = wheel.AddTimer(delay, runRequest[K, T]{
			key:        key,
			data:       data,
			generation: generation,
		})
		if addErr != nil {
			s.markInvalid(key, generation, data, addErr)
			return
		}
	}

	s.mu.Lock()
	item, current := s.items[key]
	if !current || s.closed || item.generation != generation {
		s.mu.Unlock()
		if started && addErr == nil {
			_ = wheel.RemoveTimer(timerID)
		}
		return
	}
	item.baseState = StatePending
	item.nextRunAt = &next
	item.lastError = nil
	if started {
		item.timerID = timerID
		item.hasTimer = true
	}
	changed, state := s.refreshStateLocked(item)
	s.mu.Unlock()
	s.notifyStateChange(key, changed, state)
}

func (s *Scheduler[K, T]) markInvalid(key K, generation uint64, data T, err error) {
	s.mu.Lock()
	item, ok := s.items[key]
	if !ok || item.generation != generation || s.closed {
		s.mu.Unlock()
		return
	}
	item.baseState = StateInvalid
	item.nextRunAt = nil
	item.hasTimer = false
	item.timerID = 0
	item.lastError = err
	changed, state := s.refreshStateLocked(item)
	s.mu.Unlock()

	if s.opts.OnInvalid != nil {
		s.opts.OnInvalid(key, data, err)
	}
	s.notifyStateChange(key, changed, state)
}

func (s *Scheduler[K, T]) markDisabled(key K, generation uint64) {
	s.mu.Lock()
	item, ok := s.items[key]
	if !ok || item.generation != generation || s.closed {
		s.mu.Unlock()
		return
	}
	item.baseState = StateDisabled
	item.nextRunAt = nil
	item.hasTimer = false
	item.timerID = 0
	changed, state := s.refreshStateLocked(item)
	s.mu.Unlock()
	s.notifyStateChange(key, changed, state)
}

func (s *Scheduler[K, T]) fire(req runRequest[K, T]) {
	s.mu.Lock()
	item, ok := s.items[req.key]
	if !ok || s.closed || item.generation != req.generation {
		s.mu.Unlock()
		return
	}
	item.hasTimer = false
	item.timerID = 0
	item.nextRunAt = nil
	item.baseState = StateDisabled
	data := item.data
	generation := item.generation
	policy := s.opts.ReschedulePolicy
	s.mu.Unlock()

	if policy == RescheduleBeforeRun {
		s.schedule(req.key, generation, data)
	}

	runID, ctx, ok := s.startRun(req.key, generation, data)
	if !ok {
		return
	}
	go func() {
		defer s.wg.Done()
		err := s.opts.Run(ctx, req.key, data)
		s.finishRun(req.key, generation, runID, data, err)
	}()
}

func (s *Scheduler[K, T]) startRun(key K, generation uint64, data T) (uint64, context.Context, bool) {
	s.mu.Lock()
	item, ok := s.items[key]
	if !ok || s.closed || item.generation != generation || s.ctx == nil {
		s.mu.Unlock()
		return 0, nil, false
	}
	s.runSeq++
	runID := s.runSeq
	ctx := s.ctx
	var cancel context.CancelFunc
	if s.opts.RunTimeout > 0 {
		ctx, cancel = context.WithTimeout(ctx, s.opts.RunTimeout)
	} else {
		ctx, cancel = context.WithCancel(ctx)
	}
	startedAt := time.Now()
	if item.running == nil {
		item.running = make(map[uint64]runningJob)
	}
	item.running[runID] = runningJob{
		generation: generation,
		startedAt:  startedAt,
		cancel:     cancel,
	}
	s.wg.Add(1)
	changed, state := s.refreshStateLocked(item)
	s.mu.Unlock()

	s.notifyStateChange(key, changed, state)
	return runID, ctx, true
}

func (s *Scheduler[K, T]) finishRun(key K, generation uint64, runID uint64, data T, runErr error) {
	var scheduleNext bool

	s.mu.Lock()
	item, ok := s.items[key]
	if ok {
		if running, exists := item.running[runID]; exists {
			running.cancel()
			delete(item.running, runID)
		}
		if item.generation == generation {
			item.lastError = runErr
			switch s.opts.ReschedulePolicy {
			case RescheduleAfterFinish:
				scheduleNext = !s.closed
			case NoAutoReschedule:
				if !item.hasTimer {
					item.baseState = StateDisabled
					item.nextRunAt = nil
				}
			case RescheduleBeforeRun:
				if !item.hasTimer && item.baseState != StateInvalid {
					item.baseState = StateDisabled
				}
			}
		}
		changed, state := s.refreshStateLocked(item)
		s.mu.Unlock()
		s.notifyStateChange(key, changed, state)
	} else {
		s.mu.Unlock()
	}

	if s.opts.OnFinish != nil {
		s.opts.OnFinish(key, data, runErr)
	}
	if scheduleNext {
		s.schedule(key, generation, data)
	}
}

func (s *Scheduler[K, T]) ensureEntryLocked(key K, data T) *entry[T] {
	item, ok := s.items[key]
	if ok {
		return item
	}
	item = &entry[T]{
		data:       data,
		baseState:  StateDisabled,
		lastState:  StateDisabled,
		running:    make(map[uint64]runningJob),
		generation: 0,
	}
	s.items[key] = item
	return item
}

func (s *Scheduler[K, T]) refreshStateLocked(item *entry[T]) (bool, State) {
	state := item.baseState
	if len(item.running) > 0 {
		state = StateRunning
	}
	if state == "" {
		state = StateDisabled
	}
	if item.lastState == state {
		return false, state
	}
	item.lastState = state
	return true, state
}

func (s *Scheduler[K, T]) notifyStateChange(key K, changed bool, state State) {
	if changed && s.opts.OnStateChange != nil {
		s.opts.OnStateChange(key, state)
	}
}
