package timewheel

type config[T any] struct {
	workerNum    int
	errorHandler func(recovered any)
	logger       Logger
}

// Option is a functional option for [New].
type Option[T any] func(*config[T])

// Logger is the minimal logging interface used by TimeWheel.
//
// The package does not bind to any concrete logging implementation. Callers
// may pass *slog.Logger directly or adapt zap, zerolog, or another logger.
// If no logger is configured, TimeWheel does not emit internal logs.
type Logger interface {
	Info(msg string, args ...any)
	Warn(msg string, args ...any)
}

// WithWorkerPool limits the number of concurrently running job goroutines to n.
// When n <= 0 the option is ignored and goroutines are spawned without bound.
func WithWorkerPool[T any](n int) Option[T] {
	return func(c *config[T]) {
		if n > 0 {
			c.workerNum = n
		}
	}
}

// WithErrorHandler registers a function that is called with the recovered value
// whenever a job panics. If not set, panics propagate and crash the program.
func WithErrorHandler[T any](h func(recovered any)) Option[T] {
	return func(c *config[T]) { c.errorHandler = h }
}

// WithLogger configures the logger used for internal diagnostic messages.
func WithLogger[T any](l Logger) Option[T] {
	return func(c *config[T]) { c.logger = l }
}
