package common

import (
	"testing"
	"time"
)

func TestWatchdog_Timeout(t *testing.T) {
	// Test that it fires
	w := NewWatchdog(50 * time.Millisecond)
	done := w.Start()

	select {
	case <-done:
		// Good
	case <-time.After(200 * time.Millisecond):
		t.Fatal("Watchdog did not fire")
	}
}

func TestWatchdog_Kick(t *testing.T) {
	// Test that Kick extends life
	w := NewWatchdog(50 * time.Millisecond)
	done := w.Start()

	// Kick at 25ms
	time.Sleep(25 * time.Millisecond)
	w.Kick()

	// Should not fire until 25ms + 50ms = 75ms total
	select {
	case <-done:
		t.Fatal("Watchdog fired too early")
	case <-time.After(35 * time.Millisecond):
		// This makes total wait > 60ms. Original timeout was 50ms.
		// If it hasn't fired yet, Kick worked.
	}

	// Should verify it eventually fires
	select {
	case <-done:
		// Good
	case <-time.After(100 * time.Millisecond):
		t.Fatal("Watchdog did not fire eventually")
	}
}

func TestWatchdog_Stop(t *testing.T) {
	w := NewWatchdog(50 * time.Millisecond)
	done := w.Start()

	w.Stop()

	select {
	case <-done:
		t.Fatal("Watchdog fired after stop")
	case <-time.After(100 * time.Millisecond):
		// Good
	}
}

func TestWatchdog_Zero(t *testing.T) {
	w := NewWatchdog(0)
	done := w.Start()

	select {
	case <-done:
		t.Fatal("Zero timeout fired")
	case <-time.After(50 * time.Millisecond):
		// Good
	}
}
