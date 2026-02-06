package json

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/darianmavgo/mksqlite/converters"
	"github.com/darianmavgo/mksqlite/converters/common"
)

type blockingJSONReader struct {
	delay time.Duration
	data  []byte
	ptr   int
}

func (r *blockingJSONReader) Read(p []byte) (n int, err error) {
	// If we have data, return it
	if r.ptr < len(r.data) {
		n = copy(p, r.data[r.ptr:])
		r.ptr += n
		return n, nil
	}
	// After data, block
	time.Sleep(r.delay)
	return 0, nil
}

func TestJSONTimeout(t *testing.T) {
	// A valid JSON array start, then blocks
	jsonData := []byte("[{\"id\":1},")

	r := &blockingJSONReader{
		delay: 500 * time.Millisecond,
		data:  jsonData,
	}

	config := &common.ConversionConfig{
		TableName:   "timeout_test",
		ScanTimeout: "100ms", // Should timeout
	}

	c, err := NewJSONConverterWithConfig(r, config)
	if err != nil {
		t.Fatalf("Failed to create converter: %v", err)
	}

	// Should fail during ScanRows
	err = c.ScanRows(context.Background(), "jsontb0", func(row []interface{}, err error) error {
		return nil
	})

	if err == nil {
		t.Fatal("Expected timeout error, got nil")
	}

	if !errors.Is(err, converters.ErrScanTimeout) {
		t.Errorf("Expected ErrScanTimeout, got %v", err)
	}
}
