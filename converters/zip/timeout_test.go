package zip

import (
	"errors"
	"testing"
	"time"

	"github.com/darianmavgo/mksqlite/converters"
	"github.com/darianmavgo/mksqlite/converters/common"
)

type blockingReader struct {
	delay time.Duration
}

func (r *blockingReader) Read(p []byte) (n int, err error) {
	time.Sleep(r.delay)
	return 0, nil
}

func TestZipTimeout(t *testing.T) {
	// Reader blocks
	r := &blockingReader{delay: 500 * time.Millisecond}

	config := &common.ConversionConfig{
		TableName:   "timeout_test",
		ScanTimeout: "100ms",
	}

	// Should fail in constructor
	_, err := NewZipConverterWithConfig(r, config)
	if err == nil {
		t.Fatal("Expected timeout error, got nil")
	}

	if !errors.Is(err, converters.ErrScanTimeout) {
		t.Errorf("Expected ErrScanTimeout, got %v", err)
	}
}
