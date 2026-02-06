package filesystem

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestCancellation(t *testing.T) {
	// Create a large directory structure to ensure valid scan time
	tmpDir, err := os.MkdirTemp("", "cancel_test")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tmpDir)

	for i := 0; i < 50; i++ {
		path := filepath.Join(tmpDir, fmt.Sprintf("file%d.txt", i))
		os.WriteFile(path, []byte("test"), 0644)
	}

	converter, err := NewFilesystemConverter(tmpDir)
	if err != nil {
		t.Fatal(err)
	}

	// Create a context that cancels quickly
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()

	start := time.Now()
	err = converter.ScanRows(ctx, FSTB, func(row []interface{}, err error) error {
		// Simulate slow processing
		time.Sleep(100 * time.Millisecond)
		return nil
	})

	duration := time.Since(start)

	if err == nil {
		t.Fatal("Expected cancellation error, got nil")
	}

	// It should handle matching either ErrInterrupted (if we defined it) or just stop
	// The key is that it shouldn't take forever.
	if duration > 1*time.Second {
		t.Errorf("Cancellation took too long: %v", duration)
	}
}
