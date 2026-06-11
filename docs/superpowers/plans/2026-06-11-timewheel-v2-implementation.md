# timewheel v2 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [x]`) syntax for tracking.

**Goal:** Implement the approved breaking v2 API and runtime semantics, then verify, commit, push, and tag `v2.0.0`.

**Architecture:** Keep the public package small while making state and command handling explicit. The event loop remains the single owner of timer placement and removal; public methods synchronize through commands with acknowledgements. Worker execution moves from semaphore-gated goroutines to a bounded queue with explicit backpressure policies.

**Tech Stack:** Go 1.25, standard library only, `go test ./...`, Git tags using Go semantic import versioning with module path `github.com/lib-x/timewheel/v2`.

---

## File Structure

- Modify `go.mod`: change module path to `github.com/lib-x/timewheel/v2`.
- Modify `timewheel.go`: define `TimerID`, sentinel errors, lifecycle state, timer command types, timer index locations, scheduling, repeat modes, observer events, and public methods.
- Modify `options.go`: define worker queue config, backpressure policy, observer/error handler options, and test clock option.
- Create `clock.go`: real clock/ticker abstraction used by runtime and fake tests.
- Create `fake_clock_test.go`: deterministic fake clock and ticker helpers for tests.
- Modify `timewheel_test.go`: update all tests to v2 API and add lifecycle, repeat mode, queue, observer, and delete-index coverage.
- Modify `README.md`: document v2 module path, lifecycle contract, errors, repeat modes, queue semantics, inspection semantics, and deletion complexity.
- Keep `docs/superpowers/specs/2026-06-11-timewheel-v2-design.md` and this plan in the final commit.

## Task 1: Module Path, Public Types, and Lifecycle Contract

**Files:**
- Modify: `go.mod`
- Modify: `timewheel.go`
- Modify: `options.go`
- Modify: `timewheel_test.go`

- [x] **Step 1: Write failing lifecycle/API tests**

Add tests named:

```go
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
```

- [x] **Step 2: Run tests to verify the old API fails**

Run:

```bash
go test ./...
```

Expected: compile failures because `Start` does not return an error, `TimerID` does not exist, and `AddTimer` does not return `(TimerID, error)`.

- [x] **Step 3: Implement lifecycle state and public error-returning API**

Implement these public declarations:

```go
type TimerID uint64

var (
    ErrNotStarted  = errors.New("timewheel: not started")
    ErrRunning     = errors.New("timewheel: already running")
    ErrClosed      = errors.New("timewheel: closed")
    ErrNilContext  = errors.New("timewheel: nil context")
    ErrNilJob      = errors.New("timewheel: nil job")
    ErrUnknownTimer = errors.New("timewheel: unknown timer")
    ErrQueueFull   = errors.New("timewheel: worker queue full")
)
```

Add internal state:

```go
type wheelState uint8

const (
    stateNew wheelState = iota
    stateRunning
    stateClosed
)
```

Make `Start(ctx) error`, `Stop() error`, `Close() error`, and `Wait()` follow the v2 design. Add public methods returning errors:

```go
func (tw *TimeWheel[T]) AddTimer(delay time.Duration, data T) (TimerID, error)
func (tw *TimeWheel[T]) AddTimerWithJob(delay time.Duration, data T, job Job[T]) (TimerID, error)
func (tw *TimeWheel[T]) AddTimerFunc(delay time.Duration, fn func()) (TimerID, error)
func (tw *TimeWheel[T]) RemoveTimer(id TimerID) error
```

- [x] **Step 4: Run focused lifecycle tests**

Run:

```bash
go test ./... -run 'TestStartLifecycleErrors|TestAddAndRemoveRequireRunning'
```

Expected: pass.

## Task 2: Event Loop Commands and O(1) Deletion Index

**Files:**
- Modify: `timewheel.go`
- Modify: `timewheel_test.go`

- [x] **Step 1: Write failing removal/index tests**

Add tests named:

```go
func TestRemoveTimerAcceptedBeforeFire(t *testing.T) {
    fired := make(chan struct{}, 1)
    tw, err := New[struct{}](10*time.Millisecond, 10, func(struct{}) { fired <- struct{}{} })
    if err != nil {
        t.Fatal(err)
    }
    if err := tw.Start(t.Context()); err != nil {
        t.Fatal(err)
    }
    t.Cleanup(func() { _ = tw.Close() })

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

func TestDeleteIndexUpdatesSwappedTimer(t *testing.T) {
    tw, err := New[int](time.Hour, 4, func(int) {})
    if err != nil {
        t.Fatal(err)
    }
    if err := tw.Start(t.Context()); err != nil {
        t.Fatal(err)
    }
    t.Cleanup(func() { _ = tw.Close() })

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
```

- [x] **Step 2: Implement acknowledged event-loop commands**

Replace direct add/remove channel sends with command structs:

```go
type wheelCommand[T any] struct {
    add    *task[T]
    remove TimerID
    ack    chan error
}
```

Public `Add*` and `RemoveTimer` send commands and wait for `ack`, selecting on the closed/done channel.

- [x] **Step 3: Implement timer location index**

Use `TimerID -> timerMeta` where metadata includes:

```go
type timerLocation struct {
    slot  int
    index int
}
```

`placeTask` appends and stores location. `deleteTask` uses location, swap-shrinks, and updates the moved task's location.

- [x] **Step 4: Run removal tests**

Run:

```bash
go test ./... -run 'TestRemoveTimerAcceptedBeforeFire|TestDeleteIndexUpdatesSwappedTimer|TestRemoveTimer'
```

Expected: pass.

## Task 3: Context Jobs, Repeat Modes, Observer, and Worker Queue

**Files:**
- Modify: `timewheel.go`
- Modify: `options.go`
- Modify: `timewheel_test.go`

- [x] **Step 1: Write failing context job, repeat, observer, and queue tests**

Add tests named:

```go
func TestContextJobReportsError(t *testing.T) {
    events := make(chan JobEvent[int], 1)
    want := errors.New("job failed")
    tw, err := New[int](time.Millisecond, 10, nil, WithJobObserver[int](func(e JobEvent[int]) {
        events <- e
    }))
    if err != nil {
        t.Fatal(err)
    }
    if err := tw.Start(t.Context()); err != nil {
        t.Fatal(err)
    }
    t.Cleanup(func() { _ = tw.Close() })

    _, err = tw.AddTimerWithContextJob(time.Millisecond, 7, func(context.Context, int) error {
        return want
    })
    if err != nil {
        t.Fatal(err)
    }
    e := <-events
    if !errors.Is(e.Err, want) || e.TimerID == 0 || e.Duration < 0 {
        t.Fatalf("unexpected event: %+v", e)
    }
}

func TestFixedDelayDoesNotOverlap(t *testing.T) {
    started := make(chan struct{}, 4)
    release := make(chan struct{})
    tw, err := New[struct{}](5*time.Millisecond, 20, nil)
    if err != nil {
        t.Fatal(err)
    }
    if err := tw.Start(t.Context()); err != nil {
        t.Fatal(err)
    }
    t.Cleanup(func() { _ = tw.Close() })

    _, err = tw.AddRepeatingTimerWithJob(5*time.Millisecond, struct{}{}, func(struct{}) {
        started <- struct{}{}
        <-release
    }, RepeatOptions{Mode: FixedDelay})
    if err != nil {
        t.Fatal(err)
    }
    <-started
    time.Sleep(30 * time.Millisecond)
    select {
    case <-started:
        t.Fatal("FixedDelay overlapped")
    default:
    }
    close(release)
}

func TestWorkerPoolDropPolicy(t *testing.T) {
    events := make(chan JobEvent[struct{}], 4)
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
    if err := tw.Start(t.Context()); err != nil {
        t.Fatal(err)
    }
    defer close(block)
    t.Cleanup(func() { _ = tw.Close() })

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
```

- [x] **Step 2: Implement public repeat/job/observer types**

Add:

```go
type JobContext[T any] func(context.Context, T) error
type RepeatMode uint8
type RepeatOptions struct { Mode RepeatMode }
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
```

Add `AddTimerWithContextJob`, `AddRepeatingTimer`, `AddRepeatingTimerWithJob`, and `AddRepeatingTimerWithContextJob`.

- [x] **Step 3: Implement repeat semantics**

Make `FixedRate` re-enqueue at dispatch, `FixedDelay` re-enqueue after job completion, and `SkipIfRunning` skip due fires while the same timer is running.

- [x] **Step 4: Implement worker queue policies and stats**

Replace semaphore worker pool with fixed workers and a bounded job queue for `WithWorkerPool(workers, queueSize, policy)`. Update `Stats` with `Queued`, `Running`, and `Dropped`.

- [x] **Step 5: Run focused runtime tests**

Run:

```bash
go test ./... -run 'TestContextJobReportsError|TestFixedDelayDoesNotOverlap|TestWorkerPoolDropPolicy|TestAddRepeating'
```

Expected: pass.

## Task 4: Clock Abstraction, README, Full Verification, Commit, Push, Tag

**Files:**
- Create: `clock.go`
- Create: `fake_clock_test.go`
- Modify: `timewheel.go`
- Modify: `timewheel_test.go`
- Modify: `README.md`

- [x] **Step 1: Add clock abstraction**

Create `clock.go`:

```go
package timewheel

import "time"

type clock interface {
    Now() time.Time
    NewTicker(time.Duration) ticker
}

type ticker interface {
    C() <-chan time.Time
    Stop()
}

type realClock struct{}

func (realClock) Now() time.Time { return time.Now() }
func (realClock) NewTicker(d time.Duration) ticker {
    return realTicker{Ticker: time.NewTicker(d)}
}

type realTicker struct{ *time.Ticker }
func (t realTicker) C() <-chan time.Time { return t.Ticker.C }
```

- [x] **Step 2: Add fake clock tests for deterministic timing where practical**

Create `fake_clock_test.go` with a manual ticker and use `withClock` test option to verify a one-shot timer can fire after explicit ticks without real sleep.

- [x] **Step 3: Update README**

Document:

- module path `github.com/lib-x/timewheel/v2`
- `Start`, `Stop`, `Close`, `Wait` behavior
- error-returning `Add*` and `RemoveTimer`
- `TimerID`
- `FixedRate`, `FixedDelay`, `SkipIfRunning`
- context job and observer APIs
- worker queue and backpressure policies
- deletion complexity with index
- `NextFireTime` and `PendingTimers` snapshot semantics

- [x] **Step 4: Run formatting and full tests**

Run:

```bash
gofmt -w *.go
go test ./...
```

Expected: all tests pass.

- [x] **Step 5: Release gate and commit**

Run:

```bash
git status --short --branch -uall
git diff --check
git diff --stat
git rev-parse HEAD
```

Then commit intended files:

```bash
git add go.mod *.go README.md docs/superpowers/specs/2026-06-11-timewheel-v2-design.md docs/superpowers/plans/2026-06-11-timewheel-v2-implementation.md
git commit -m "feat!: redesign timewheel v2 API"
```

- [x] **Step 6: Push and tag**

After commit, re-check remote and tags:

```bash
git status --short --branch -uall
git rev-parse HEAD
git ls-remote origin refs/heads/main
git ls-remote --tags origin
```

Create and push the v2 tag:

```bash
git push origin main
git tag -a v2.0.0 -m "v2.0.0"
git push origin v2.0.0
```
