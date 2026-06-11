# timewheel

[![Go Reference](https://pkg.go.dev/badge/github.com/lib-x/timewheel.svg)](https://pkg.go.dev/github.com/lib-x/timewheel)
[![Go Report Card](https://goreportcard.com/badge/github.com/lib-x/timewheel)](https://goreportcard.com/report/github.com/lib-x/timewheel)

A generic timer wheel for Go.

## Requirements

Go 1.25 or later.

## Installation

```bash
go get github.com/lib-x/timewheel
```

## Quick Start

```go
package main

import (
    "context"
    "fmt"
    "log"
    "time"

    "github.com/lib-x/timewheel"
)

func main() {
    tw, err := timewheel.New[string](
        100*time.Millisecond,
        60,
        func(msg string) {
            fmt.Println("fired:", msg)
        },
    )
    if err != nil {
        log.Fatal(err)
    }

    ctx, cancel := context.WithCancel(context.Background())
    defer cancel()

    if err := tw.Start(ctx); err != nil {
        log.Fatal(err)
    }
    defer tw.Close()

    id, err := tw.AddTimer(500*time.Millisecond, "hello")
    if err != nil {
        log.Fatal(err)
    }

    if fireAt, ok := tw.NextFireTime(id); ok {
        fmt.Printf("'hello' fires in %s\n", time.Until(fireAt).Round(time.Millisecond))
    }
}
```

## API

### Construction

```go
func New[T any](
    interval   time.Duration,
    slotNum    int,
    defaultJob Job[T],
    opts       ...Option[T],
) (*TimeWheel[T], error)
```

`interval` is the tick resolution. Delays shorter than one interval are rounded
up to one tick. `slotNum` controls how many buckets the wheel uses before circle
counting handles longer delays.

### Lifecycle

```go
func (tw *TimeWheel[T]) Start(ctx context.Context) error
func (tw *TimeWheel[T]) Stop() error
func (tw *TimeWheel[T]) Close() error
func (tw *TimeWheel[T]) Wait()
```

Lifecycle is explicit:

```text
new -> running -> closed
```

- `Start(nil)` returns `ErrNilContext`.
- `Start` may succeed once.
- Starting an already running wheel returns `ErrRunning`.
- Starting a closed wheel returns `ErrClosed`.
- `Stop` before `Start` returns `ErrNotStarted`.
- `Stop` is idempotent after the wheel is running or closed.
- `Close` is idempotent, stops the wheel, and waits for the event loop and worker pool.
- Canceling the context passed to `Start` stops the wheel.

Timer registration requires a running wheel. `Add*` and `RemoveTimer` return
`ErrNotStarted` before `Start` and `ErrClosed` after shutdown begins.

### Timer IDs

```go
type TimerID uint64
```

All timer APIs use `TimerID` instead of raw integers.

### One-Shot Timers

```go
type Job[T any] func(T)
type JobContext[T any] func(context.Context, T) error

func (tw *TimeWheel[T]) AddTimer(delay time.Duration, data T) (TimerID, error)
func (tw *TimeWheel[T]) AddTimerWithJob(delay time.Duration, data T, job Job[T]) (TimerID, error)
func (tw *TimeWheel[T]) AddTimerWithContextJob(delay time.Duration, data T, job JobContext[T]) (TimerID, error)
func (tw *TimeWheel[T]) AddTimerFunc(delay time.Duration, fn func()) (TimerID, error)
```

Nil per-timer jobs return `ErrNilJob`. A nil default job is allowed only when
timers provide their own job. If a timer fires without any job, the wheel logs
a warning when a logger is configured and removes the timer.

### Repeating Timers

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

func (tw *TimeWheel[T]) AddRepeatingTimer(delay time.Duration, data T, opts RepeatOptions) (TimerID, error)
func (tw *TimeWheel[T]) AddRepeatingTimerWithJob(delay time.Duration, data T, job Job[T], opts RepeatOptions) (TimerID, error)
func (tw *TimeWheel[T]) AddRepeatingTimerWithContextJob(delay time.Duration, data T, job JobContext[T], opts RepeatOptions) (TimerID, error)
```

Repeat modes:

- `FixedRate`: schedules the next fire when the current fire is dispatched. Jobs may overlap.
- `FixedDelay`: waits for the previous job to return, then waits the delay. Jobs do not overlap.
- `SkipIfRunning`: keeps a fixed-rate cadence but skips a fire if the previous job is still running.

The zero-value `RepeatOptions{}` uses `FixedRate`.

### Removing Timers

```go
func (tw *TimeWheel[T]) RemoveTimer(id TimerID) error
```

Unknown, already-fired, and already-removed timer IDs are successful no-ops.
After `RemoveTimer` returns nil, the wheel will not dispatch any future
not-yet-started execution for that timer.

`RemoveTimer` does not cancel a job that has already started. Use
`JobContext` if a job needs to observe root wheel shutdown.

### Observability

```go
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

type JobObserver[T any] func(JobEvent[T])

func WithJobObserver[T any](observer JobObserver[T]) Option[T]
func WithErrorHandler[T any](h func(recovered any)) Option[T]
func WithLogger[T any](l Logger) Option[T]
```

`JobContext` errors are reported through `JobEvent.Err`. Panics are recovered
when an error handler or observer is configured. Without either, a panic keeps
normal Go behavior and crashes the program.

### Worker Pool

```go
type BackpressurePolicy uint8

const (
    Block BackpressurePolicy = iota
    Drop
    RunInline
)

func WithWorkerPool[T any](workers int, queueSize int, policy BackpressurePolicy) Option[T]
```

`workers <= 0` disables the pool and runs jobs in independent goroutines.
`queueSize` bounds the worker queue when the pool is enabled.

Backpressure policies:

- `Block`: wait for queue capacity unless shutdown starts.
- `Drop`: record a dropped job and do not run it when the queue is full.
- `RunInline`: run the job on the event loop when the queue is full.

`RunInline` preserves execution but can delay ticks.

### Stats

```go
type Stats struct {
    Pending  int64
    Executed int64
    Removed  int64
    Queued   int64
    Running  int64
    Dropped  int64
    Skipped  int64
}
```

`Pending` counts timers currently waiting in wheel slots. It does not count
jobs waiting in the worker queue or jobs already running.

### Inspecting Timers

```go
func (tw *TimeWheel[T]) NextFireTime(id TimerID) (time.Time, bool)
func (tw *TimeWheel[T]) PendingTimers() []TimerInfo
func (tw *TimeWheel[T]) Stats() Stats
```

`NextFireTime` and `PendingTimers` are snapshots. They are estimates based on
the wheel state when queried, not hard real-time guarantees. Actual dispatch
happens no earlier than the scheduled time and can be delayed by up to one tick
plus runtime scheduling jitter.

For `FixedDelay` timers, there is no pending next fire while the previous job is
still running; the next fire is scheduled after that job returns.

## Design Notes

### Time Wheel Placement

```text
ticks  = ceil(delay / interval)
offset = ticks - 1
circle = offset / slotNum
pos    = (currentPos + offset) % slotNum
```

The event loop scans one slot on every tick, dispatches due tasks, then advances
the wheel pointer.

### Deletion Complexity

The wheel keeps a `TimerID -> slot/index` location index. `RemoveTimer` uses the
index to find the timer in O(1), then removes it from the slot with
swap-and-shrink. When another task is swapped into the removed position, its
index is updated immediately.

### Core Scope

The core package handles delay, repeat, cancel, execution, and inspection. Cron
expressions, persistent scheduling, orchestration, and business-key mapping
belong in separate packages layered on top.

## License

MIT
