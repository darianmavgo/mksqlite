package txt

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

func TestTxtTimeout(t *testing.T) {
	r := &blockingReader{delay: 500 * time.Millisecond}

	config := &common.ConversionConfig{
		TableName:   "timeout_test",
		ScanTimeout: "100ms",
	}

	c, err := NewTxtConverterWithConfig(r, config)
	if err != nil {
		t.Fatalf("Failed to create converter: %v", err)
	}

	err = c.ScanRows("timeout_test", func(row []interface{}, err error) error {
		return nil
	})

	if err == nil {
		t.Fatal("Expected timeout error, got nil")
	}

	if !errors.Is(err, converters.ErrScanTimeout) {
		t.Errorf("Expected ErrScanTimeout, got %v", err)
	}
}
