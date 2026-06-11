package timewheel

import (
	"sync"
	"testing"
	"time"
)

type fakeClock struct {
	mu      sync.Mutex
	now     time.Time
	tickers []*fakeTicker
}

func newFakeClock(now time.Time) *fakeClock {
	return &fakeClock{now: now}
}

func (c *fakeClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *fakeClock) NewTicker(time.Duration) ticker {
	t := &fakeTicker{ch: make(chan time.Time, 16)}
	c.mu.Lock()
	c.tickers = append(c.tickers, t)
	c.mu.Unlock()
	return t
}

func (c *fakeClock) Advance(d time.Duration) {
	c.mu.Lock()
	c.now = c.now.Add(d)
	now := c.now
	tickers := append([]*fakeTicker(nil), c.tickers...)
	c.mu.Unlock()

	for _, t := range tickers {
		t.tick(now)
	}
}

type fakeTicker struct {
	mu      sync.Mutex
	ch      chan time.Time
	stopped bool
}

func (t *fakeTicker) C() <-chan time.Time {
	return t.ch
}

func (t *fakeTicker) Stop() {
	t.mu.Lock()
	t.stopped = true
	t.mu.Unlock()
}

func (t *fakeTicker) tick(now time.Time) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.stopped {
		return
	}
	t.ch <- now
}

func TestFakeClockDrivesOneShotTimer(t *testing.T) {
	clk := newFakeClock(time.Date(2026, 6, 11, 12, 0, 0, 0, time.UTC))
	fired := make(chan struct{})

	tw, err := New[struct{}](
		10*time.Millisecond,
		4,
		func(struct{}) { close(fired) },
		withClock[struct{}](clk),
	)
	if err != nil {
		t.Fatal(err)
	}
	startWheel(t, tw)

	id, err := tw.AddTimer(10*time.Millisecond, struct{}{})
	if err != nil {
		t.Fatal(err)
	}
	fireAt, ok := tw.NextFireTime(id)
	if !ok {
		t.Fatal("timer missing before fake tick")
	}
	if want := clk.Now().Add(10 * time.Millisecond); !fireAt.Equal(want) {
		t.Fatalf("NextFireTime: got %s, want %s", fireAt, want)
	}

	clk.Advance(10 * time.Millisecond)

	select {
	case <-fired:
	case <-time.After(100 * time.Millisecond):
		t.Fatal("fake clock tick did not fire timer")
	}
}
