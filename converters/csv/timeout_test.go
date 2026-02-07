package csv

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/darianmavgo/mksqlite/converters"
	"github.com/darianmavgo/mksqlite/converters/common"
)

type blockingReader struct {
	delay      time.Duration
	headerSent bool
}

func (r *blockingReader) Read(p []byte) (n int, err error) {
	if !r.headerSent {
		n = copy(p, []byte("col1,col2\n"))
		r.headerSent = true
		return n, nil
	}
	time.Sleep(r.delay)
	return 0, nil
}

func TestCSVTimeout(t *testing.T) {
	// Reader that blocks for 500ms
	r := &blockingReader{delay: 500 * time.Millisecond}

	config := &common.ConversionConfig{
		TableName:   "timeout_test",
		ScanTimeout: "100ms", // Should timeout before reader returns
		Delimiter:   ',',     // Avoid Peek which blocks on incomplete input
	}

	c, err := NewCSVConverterWithConfig(r, config)
	if err != nil {
		t.Fatalf("Failed to create converter: %v", err)
	}

	err = c.ScanRows(context.Background(), "timeout_test", func(row []interface{}, err error) error {
		return nil
	})

	if err == nil {
		t.Fatal("Expected timeout error, got nil")
	}

	if !errors.Is(err, converters.ErrScanTimeout) {
		t.Errorf("Expected ErrScanTimeout, got %v", err)
	}
}
