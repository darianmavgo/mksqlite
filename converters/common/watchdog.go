package common

import (
	"log"
	"sync"
	"time"
)

// Watchdog monitors activity and closes a channel if no activity is recorded within the timeout.
type Watchdog struct {
	timeout time.Duration
	timer   *time.Timer
	doneCh  chan struct{}
	once    sync.Once
	mu      sync.Mutex
	running bool
}

// NewWatchdog creates a new Watchdog.
// If timeout is <= 0, the watchdog is inert and never times out.
func NewWatchdog(timeout time.Duration) *Watchdog {
	return &Watchdog{
		timeout: timeout,
		doneCh:  make(chan struct{}),
	}
}

// Start begins the monitoring. It returns a channel that will be closed on timeout.
func (w *Watchdog) Start() <-chan struct{} {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.running {
		return w.doneCh
	}
	w.running = true

	if w.timeout <= 0 {
		return w.doneCh
	}

	w.timer = time.AfterFunc(w.timeout, func() {
		w.close()
	})

	return w.doneCh
}

// Kick resets the timeout.
func (w *Watchdog) Kick() {
	w.mu.Lock()
	defer w.mu.Unlock()

	if !w.running || w.timer == nil {
		return
	}

	// We don't check return of Stop() because if it returns false,
	// the function might be running or about to run.
	// We just try to reset. If the channel is already closed, it's too late.
	select {
	case <-w.doneCh:
		return
	default:
	}

	w.timer.Reset(w.timeout)
}

// Stop stops the watchdog preventing the timeout from firing.
func (w *Watchdog) Stop() {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.timer != nil {
		w.timer.Stop()
	}
}

// Done returns the channel that shuts down on timeout.
func (w *Watchdog) Done() <-chan struct{} {
	return w.doneCh
}

func (w *Watchdog) close() {
	w.once.Do(func() {
		log.Printf("Watchdog timeout (%v) triggered.", w.timeout)
		close(w.doneCh)
	})
}
