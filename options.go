package timewheel

type config[T any] struct {
	workerNum    int
	queueSize    int
	backpressure BackpressurePolicy
	errorHandler func(recovered any)
	observer     JobObserver[T]
	logger       Logger
	clock        Clock
}

// Option is a functional option for New.
type Option[T any] func(*config[T])

// Logger is the minimal logging interface used by TimeWheel.
type Logger interface {
	Info(msg string, args ...any)
	Warn(msg string, args ...any)
}

// BackpressurePolicy controls behavior when the bounded worker queue is full.
type BackpressurePolicy uint8

const (
	// Block waits for queue capacity unless the wheel is shutting down.
	Block BackpressurePolicy = iota

	// Drop records a dropped job and does not run it when the queue is full.
	Drop

	// RunInline runs the job on the event loop when the queue is full.
	RunInline
)

// WithWorkerPool configures a fixed worker pool and bounded queue.
//
// workers <= 0 disables the pool and runs each job in its own goroutine.
func WithWorkerPool[T any](workers int, queueSize int, policy BackpressurePolicy) Option[T] {
	return func(c *config[T]) {
		c.queueSize = queueSize
		if workers > 0 {
			c.workerNum = workers
			c.backpressure = policy
		}
	}
}

// WithErrorHandler registers a function called with recovered job panics.
func WithErrorHandler[T any](h func(recovered any)) Option[T] {
	return func(c *config[T]) { c.errorHandler = h }
}

// WithJobObserver registers a function called for job execution, drop, and skip events.
func WithJobObserver[T any](observer JobObserver[T]) Option[T] {
	return func(c *config[T]) { c.observer = observer }
}

// WithLogger configures the logger used for internal diagnostic messages.
func WithLogger[T any](l Logger) Option[T] {
	return func(c *config[T]) { c.logger = l }
}

// WithClock overrides the wheel's time source. A nil clock keeps the default
// real-time clock. Intended for tests that need deterministic time.
func WithClock[T any](clk Clock) Option[T] {
	return func(c *config[T]) { c.clock = clk }
}
