# timewheel v2 API and Runtime Design

Date: 2026-06-11

## Goal

Ship a breaking v2 API for `github.com/lib-x/timewheel/v2` while the library is still new. The v2 API should make lifecycle, cancellation, repeating behavior, queueing, and inspection semantics explicit enough for production use.

Go version changes are out of scope for this design.

The module path must change to `github.com/lib-x/timewheel/v2` before tagging `v2.0.0`, following Go's semantic import versioning rules.

## Non-Goals

- Add cron expression parsing, persistent scheduling, task orchestration, or business-key mapping to the core package.
- Guarantee cancellation of a job that has already started running.
- Make `NextFireTime` a hard real-time guarantee. It remains an estimate based on scheduling state.

## Public API Shape

### Timer IDs

Use a named public ID type everywhere:

```go
type TimerID uint64
```

All public timer APIs accept and return `TimerID`, not `uint64`.

### Errors

Expose sentinel errors for state and input boundaries:

```go
var (
    ErrNotStarted = errors.New("timewheel: not started")
    ErrRunning    = errors.New("timewheel: already running")
    ErrClosed     = errors.New("timewheel: closed")
    ErrNilContext = errors.New("timewheel: nil context")
    ErrNilJob     = errors.New("timewheel: nil job")
    ErrUnknownTimer = errors.New("timewheel: unknown timer")
    ErrQueueFull  = errors.New("timewheel: worker queue full")
)
```

`ErrUnknownTimer` is used only when a method needs to distinguish "known but removed" from "not found". `RemoveTimer` treats unknown, already-fired, and already-removed timers as a successful no-op unless a stricter method is added later.

### Lifecycle

`TimeWheel` has an explicit state machine:

```text
new -> running -> closed
```

`Start(ctx)` launches the event loop and returns an error for invalid transitions:

```go
func (tw *TimeWheel[T]) Start(ctx context.Context) error
func (tw *TimeWheel[T]) Stop() error
func (tw *TimeWheel[T]) Close() error
func (tw *TimeWheel[T]) Wait()
```

Rules:

- `Start(nil)` returns `ErrNilContext`.
- `Start` may be called once successfully.
- Calling `Start` while running returns `ErrRunning`.
- Calling `Start` after `Stop` or `Close` returns `ErrClosed`.
- `Stop` before `Start` returns `ErrNotStarted`.
- `Stop` is idempotent after the wheel is running or closed and begins shutdown without waiting for job goroutines.
- `Close` is idempotent, stops the wheel, and waits for the event loop and worker pool to exit.
- `Wait` waits for the event loop and workers started by this wheel.
- The context passed to `Start` is used as the root context for context-aware jobs.
- Canceling the root context has the same effect as `Stop`.

Timer registration requires the wheel to be running. Adding before `Start` returns `ErrNotStarted`. Adding after shutdown starts returns `ErrClosed`.

### One-Shot Timers

The simple job API remains available:

```go
type Job[T any] func(T)
type JobContext[T any] func(context.Context, T) error

func (tw *TimeWheel[T]) AddTimer(delay time.Duration, data T) (TimerID, error)
func (tw *TimeWheel[T]) AddTimerWithJob(delay time.Duration, data T, job Job[T]) (TimerID, error)
func (tw *TimeWheel[T]) AddTimerWithContextJob(delay time.Duration, data T, job JobContext[T]) (TimerID, error)
func (tw *TimeWheel[T]) AddTimerFunc(delay time.Duration, fn func()) (TimerID, error)
```

Rules:

- Delays shorter than one interval are rounded up to one interval.
- Nil per-timer jobs return `ErrNilJob`.
- If a default job is nil and a timer fires without a per-timer job, the wheel records a skipped execution through the observer/logger and removes the timer.

### Repeating Timers

Repeating behavior is explicit. The old ambiguous `AddRepeating` API is removed.

```go
type RepeatMode uint8

const (
    FixedRate RepeatMode = iota
    FixedDelay
    SkipIfRunning
)

type RepeatOptions struct {
    Mode RepeatMode
}

func (tw *TimeWheel[T]) AddRepeatingTimer(
    delay time.Duration,
    data T,
    opts RepeatOptions,
) (TimerID, error)

func (tw *TimeWheel[T]) AddRepeatingTimerWithJob(
    delay time.Duration,
    data T,
    job Job[T],
    opts RepeatOptions,
) (TimerID, error)

func (tw *TimeWheel[T]) AddRepeatingTimerWithContextJob(
    delay time.Duration,
    data T,
    job JobContext[T],
    opts RepeatOptions,
) (TimerID, error)
```

Mode semantics:

- `FixedRate`: schedule the next fire when the current fire is dispatched. Jobs may overlap.
- `FixedDelay`: schedule the next fire only after the previous job returns. Jobs do not overlap.
- `SkipIfRunning`: schedule on a fixed-rate cadence, but skip a fire when the previous job for the same timer is still running.

The default zero-value `RepeatOptions` uses `FixedRate`.

### Removal

```go
func (tw *TimeWheel[T]) RemoveTimer(id TimerID) error
```

Rules:

- Calling `RemoveTimer` before `Start` returns `ErrNotStarted`.
- Calling it after shutdown starts returns `ErrClosed`.
- Unknown, already-fired, and already-removed timer IDs are successful no-ops.
- After `RemoveTimer` returns nil, the wheel will not dispatch any future not-yet-started execution for that timer.
- `RemoveTimer` does not cancel a job that has already started. Context-aware jobs can observe shutdown through their context, not individual timer removal.

## Internal Scheduling Model

### Timer Index

Maintain a single authoritative index:

```go
type timerLocation struct {
    slot  int
    index int
}
```

Each live timer has metadata containing next fire time, delay, mode, repeat mode, running state, and slot location. Placement writes both the slot slice and the index. Deletion uses the location to find the task in O(1), then swap-shrinks the slot slice. When a swapped task moves, update its location in the index.

This makes timer removal O(1) under the wheel mutexes and avoids scanning all slots.

### Event Loop Commands

All mutation of slots and timer metadata happens through the event loop. Public methods communicate through command messages that include an acknowledgement channel. This gives `Add*` and `RemoveTimer` precise success/failure results and lets methods return when their state transition has been accepted.

Command sends must select on the wheel's done channel so public methods cannot block forever after shutdown.

### Clock Abstraction

Add an internal clock interface used by tests:

```go
type clock interface {
    Now() time.Time
    NewTicker(time.Duration) ticker
}

type ticker interface {
    C() <-chan time.Time
    Stop()
}
```

Production uses the real clock. Tests can inject a fake clock through an unexported or test-only option. Existing sleep-based tests should be converted where the behavior depends on tick timing, removal races, repeating modes, stop behavior, jitter, and next-fire-time updates.

## Worker Pool

Replace the semaphore-based pool with fixed worker goroutines and a bounded queue.

```go
type BackpressurePolicy uint8

const (
    Block BackpressurePolicy = iota
    Drop
    RunInline
)

func WithWorkerPool[T any](workers int, queueSize int, policy BackpressurePolicy) Option[T]
```

Rules:

- `workers <= 0` means jobs run in their own goroutine without a bounded queue.
- `queueSize < 0` is invalid and should be rejected by `New`.
- `Block` waits for queue capacity unless shutdown starts.
- `Drop` records a dropped job and does not run it when the queue is full.
- `RunInline` executes on the event loop when the queue is full. This preserves execution but can delay ticks.

Stats include at least:

```go
type Stats struct {
    Pending int64
    Executed int64
    Removed int64
    Queued int64
    Running int64
    Dropped int64
}
```

`Pending` means timers waiting in the wheel, not jobs waiting in the worker queue.

## Observation and Error Handling

Keep panic recovery configurable, but prefer a richer observer:

```go
type JobEvent[T any] struct {
    TimerID    TimerID
    Data       T
    StartedAt  time.Time
    FinishedAt time.Time
    ScheduledFor time.Time
    Lateness   time.Duration
    Duration   time.Duration
    Err        error
    Panic      any
    Dropped    bool
    Skipped    bool
}

type JobObserver[T any] func(JobEvent[T])

func WithJobObserver[T any](observer JobObserver[T]) Option[T]
func WithErrorHandler[T any](h func(recovered any)) Option[T]
```

Context-aware jobs return errors through `JobEvent.Err`. Panics are recovered when either observer or error handler is configured. If neither is configured, panics keep the current Go behavior and crash the program.

## Inspection Semantics

```go
func (tw *TimeWheel[T]) NextFireTime(id TimerID) (time.Time, bool)
func (tw *TimeWheel[T]) PendingTimers() []TimerInfo
func (tw *TimeWheel[T]) Stats() Stats
```

Rules:

- Inspection methods remain safe before start, while running, and after close.
- `NextFireTime` and `PendingTimers` are snapshots, not hard guarantees.
- Actual dispatch happens in the range `[scheduled, scheduled + interval + runtime jitter]`.
- Repeating timers update `NextFireAt` according to their repeat mode.

## README Contract Updates

The README must explicitly document:

- Whether `Add*` and `RemoveTimer` are allowed before `Start`.
- Behavior after root context cancellation, `Stop`, and `Close`.
- `RemoveTimer` guarantees and non-guarantees.
- Timer precision and jitter range.
- Repeating overlap behavior for every `RepeatMode`.
- Difference between pending timers and queued/running jobs.
- `NextFireTime` and `PendingTimers` snapshot semantics.
- Actual deletion complexity after the index is implemented.

## Testing Plan

Add or update tests for:

- `Start(nil)`, double start, start after close, stop idempotency, close idempotency.
- Add before start, add after stop, remove before start, remove after stop.
- `RemoveTimer` returning after accepted removal and preventing later dispatch.
- `TimerID` public API usage.
- `FixedRate` overlap behavior.
- `FixedDelay` non-overlap behavior and delay-after-completion scheduling.
- `SkipIfRunning` skipped executions and observer/stat increments.
- O(1) delete index maintenance after swap-shrink.
- Bounded queue policies: `Block`, `Drop`, `RunInline`.
- Observer events for success, returned error, panic, dropped job, skipped repeat, and missing job.
- Fake-clock driven timing tests replacing sleep-heavy tests.
