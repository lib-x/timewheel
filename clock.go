package timewheel

import "time"

// Clock abstracts the time source used by the wheel. Implementations must be
// safe for concurrent use. The default clock uses the time package.
type Clock interface {
	Now() time.Time
	NewTicker(time.Duration) Ticker
}

// Ticker is the minimal ticker surface consumed by the event loop.
type Ticker interface {
	C() <-chan time.Time
	Stop()
}

type realClock struct{}

func (realClock) Now() time.Time {
	return time.Now()
}

func (realClock) NewTicker(d time.Duration) Ticker {
	return realTicker{Ticker: time.NewTicker(d)}
}

type realTicker struct {
	*time.Ticker
}

func (t realTicker) C() <-chan time.Time {
	return t.Ticker.C
}
