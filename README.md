# timewheel

[![Go Reference](https://pkg.go.dev/badge/github.com/lib-x/timewheel.svg)](https://pkg.go.dev/github.com/lib-x/timewheel)
[![Go Report Card](https://goreportcard.com/badge/github.com/lib-x/timewheel)](https://goreportcard.com/report/github.com/lib-x/timewheel)

A generic, high-performance timer wheel for Go 1.25+.

## Features

- **Generics** — the payload type `T` is a type parameter; no `interface{}` assertions needed
- **Context-aware lifecycle** — the wheel stops cleanly when the supplied `context.Context` is cancelled
- **Multiple timer modes** — one-shot, repeating, and bare-closure (`AddTimerFunc`) variants
- **Bounded worker pool** — optionally cap concurrent job goroutines with `WithWorkerPool`
- **Panic recovery** — optionally recover job panics via `WithErrorHandler`
- **Logger abstraction** — optionally pass any logger with `Info` / `Warn` methods; no logging package is imported by timewheel
- **Runtime stats** — query pending / executed / removed counters at any time via `Stats()`
- **Low allocation hot path** — `sync.Pool` recycles task objects; slots use slice swap-deletion (O(1), no heap pressure)

## Requirements

Go 1.25 or later.

## Installation

```bash
go get github.com/lib-x/timewheel
```

## Quick start

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
    // Create a wheel that ticks every 100 ms across 60 slots.
    // Maximum single-rotation range: 100ms × 60 = 6 s.
    // Delays beyond that are handled transparently via circle counting.
    tw, err := timewheel.New[string](
        100*time.Millisecond, // tick interval (resolution)
        60,                   // number of slots
        func(msg string) {    // default job
            fmt.Println("fired:", msg)
        },
    )
    if err != nil {
        log.Fatal(err)
    }

    ctx, cancel := context.WithCancel(context.Background())
    defer cancel()

    tw.Start(ctx)

    key := tw.AddTimer(500*time.Millisecond, "hello")
    tw.AddTimer(1*time.Second, "world")

    // Inspect the scheduled fire time immediately after registration.
    if fireAt, ok := tw.NextFireTime(key); ok {
        fmt.Printf("'hello' fires in %s\n", time.Until(fireAt).Round(time.Millisecond))
    }

    time.Sleep(2 * time.Second)
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

| Parameter | Description |
|-----------|-------------|
| `interval` | Tick resolution. The minimum timer precision equals `interval`. |
| `slotNum` | Number of slots. A larger value spreads tasks across more buckets and reduces per-tick scan work. |
| `defaultJob` | Callback used when a task has no per-task job. May be `nil` if every timer is registered via `AddTimerWithJob`. |

### Options

```go
type Logger interface {
    Info(msg string, args ...any)
    Warn(msg string, args ...any)
}
```

| Option | Description |
|--------|-------------|
| `WithWorkerPool[T](n int)` | Limit concurrent job goroutines to `n`. |
| `WithErrorHandler[T](fn)` | Called with the recovered value whenever a job panics. |
| `WithLogger[T](l Logger)` | Use this logger for internal diagnostics. If omitted or nil, timewheel does not log. |

`WithLogger` accepts any concrete logger or adapter that implements the small interface above, including `*slog.Logger`. The package itself does not import `log/slog`, zap, zerolog, or any other logging implementation.

### Lifecycle

```go
tw.Start(ctx)  // launch event loop; stops when ctx is cancelled
tw.Wait()      // block until the event loop exits
```

### Timer registration

| Method | Description |
|--------|-------------|
| `AddTimer(delay, data) uint64` | One-shot timer using the default job. |
| `AddTimerWithJob(delay, data, job) uint64` | One-shot timer with a per-task job. |
| `AddTimerFunc(delay, fn) uint64` | One-shot timer from a plain closure (no payload required). |
| `AddRepeating(delay, data) uint64` | Recurring timer using the default job. |
| `AddRepeatingWithJob(delay, data, job) uint64` | Recurring timer with a per-task job. |
| `RemoveTimer(key uint64)` | Cancel a pending timer. No-op for unknown or already-fired keys. |

All registration methods return a `uint64` key that uniquely identifies the timer within the wheel's lifetime.

### Inspecting next fire time

```go
key := tw.AddTimer(5*time.Second, "payload")

// Query a single timer — O(1), no slot lock acquired.
if fireAt, ok := tw.NextFireTime(key); ok {
    fmt.Println("fires at:", fireAt)
    fmt.Println("in:      ", time.Until(fireAt).Round(time.Millisecond))
}

// List all pending timers, sorted by ascending fire time.
for _, info := range tw.PendingTimers() {
    fmt.Printf("key=%-6d  repeating=%-5v  next=%s  in=%s\n",
        info.Key,
        info.Repeating,
        info.NextFireAt.Format(time.TimeOnly),
        time.Until(info.NextFireAt).Round(time.Millisecond),
    )
}
```

`NextFireTime` returns `(zero, false)` when the key does not exist — either because it was never registered, has already fired (one-shot), or was explicitly removed. For repeating timers the returned time advances after every execution.

`PendingTimers` returns a freshly allocated `[]TimerInfo` snapshot. Each entry carries:

| Field | Type | Description |
|-------|------|-------------|
| `Key` | `uint64` | Timer identifier |
| `NextFireAt` | `time.Time` | Expected wall-clock fire time |
| `Delay` | `time.Duration` | Original registration delay |
| `Repeating` | `bool` | True for `AddRepeating` / `AddRepeatingWithJob` timers |

### Observability

```go
s := tw.Stats()
fmt.Println(s.Pending)  // tasks currently in the wheel
fmt.Println(s.Executed) // total tasks dispatched since Start
fmt.Println(s.Removed)  // total tasks cancelled via RemoveTimer
```

## Design notes

### Time wheel basics

```
slots:  [ 0 ][ 1 ][ 2 ] ... [ N-1 ]
                  ↑
            currentPos

Every interval:
  1. Scan slot[currentPos]: decrement circle for tasks not yet due;
     dispatch tasks whose circle == 0.
  2. Advance currentPos = (currentPos + 1) % slotNum.
```

A task with delay `d` is placed at:

```
ticks  = ceil(d / interval)
offset = ticks - 1             // currentPos is scanned on the next tick
circle = offset / slotNum      // full rotations to wait
pos    = (currentPos + offset) % slotNum
```

### Slot-level locking

Each slot owns its own `sync.Mutex`. The event loop acquires a slot's lock only while placing or deleting tasks. Concurrent callers operating on different slots do not block one another.

### Object pooling

`task` structs are allocated once and returned to a `sync.Pool` after use, keeping the hot path largely allocation-free and reducing GC pressure under high timer turnover.

### Deletion

`deleteTask` performs an O(1) swap-and-shrink: the target element is overwritten with the last element in the slice and the slice is shortened by one, avoiding the O(n) copy of `append(s[:i], s[i+1:]...)`.

## Example: bounded worker pool + error recovery

```go
tw, _ := timewheel.New[[]byte](
    50*time.Millisecond,
    200,
    processPayload,
    timewheel.WithWorkerPool[[]byte](16),
    timewheel.WithErrorHandler[[]byte](func(r any) {
        slog.Error("job panicked", "err", r)
    }),
    timewheel.WithLogger[[]byte](slog.Default()),
)

ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt)
defer cancel()

tw.Start(ctx)

// Enqueue work
tw.AddTimer(200*time.Millisecond, payload)

tw.Wait()
```

## Example: repeating ticker with graceful stop

```go
key := tw.AddRepeating(1*time.Second, struct{}{})

// ... later ...
tw.RemoveTimer(key) // stops the repetition
```

## License

MIT
