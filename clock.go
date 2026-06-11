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

func (realClock) Now() time.Time {
	return time.Now()
}

func (realClock) NewTicker(d time.Duration) ticker {
	return realTicker{Ticker: time.NewTicker(d)}
}

type realTicker struct {
	*time.Ticker
}

func (t realTicker) C() <-chan time.Time {
	return t.Ticker.C
}
