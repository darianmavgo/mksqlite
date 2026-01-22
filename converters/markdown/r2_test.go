package markdown_test

import (
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"testing"

	"github.com/darianmavgo/mksqlite/converters"
	"github.com/darianmavgo/mksqlite/converters/markdown"
)

// R2FaultyReader simulates a stream interruption
type R2FaultyReader struct {
	Body        io.ReadCloser
	ReadCount   int64
	FailAtBytes int64
}

func (r *R2FaultyReader) Read(p []byte) (n int, err error) {
	n, err = r.Body.Read(p)
	r.ReadCount += int64(n)
	if r.ReadCount > r.FailAtBytes {
		r.Body.Close()
		return n, fmt.Errorf("simulated stream interruption at %d bytes", r.ReadCount)
	}
	return n, err
}

func (r *R2FaultyReader) Close() error {
	return r.Body.Close()
}

func TestMarkdownStreamingFromR2(t *testing.T) {
	url := "https://pub-a1c6b68deb9d48e1b5783f84723c93ec.r2.dev/sample_data/demo_mavgo_flight/Expenses.csv" // using any file for stream interrupt test
	resp, err := http.Get(url)
	if err != nil {
		t.Fatalf("Failed to fetch from R2: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("Failed to fetch from R2: status %d", resp.StatusCode)
	}

	// Trigger failure at ~50% of content length
	failAt := int64(1024)
	if resp.ContentLength > 0 {
		failAt = resp.ContentLength / 2
	}

	faultyReader := &R2FaultyReader{
		Body:        resp.Body,
		FailAtBytes: failAt,
	}

	converter, err := markdown.NewMarkdownConverter(faultyReader, nil)
	if err != nil {
		t.Logf("NewMarkdownConverter interrupt handled (likely failed during parse): %v", err)
		return
	}

	outputDir := "../../test_output"
	if err := os.MkdirAll(outputDir, 0755); err != nil {
		t.Fatalf("Failed to create output directory: %v", err)
	}
	dbPath := filepath.Join(outputDir, "markdown_r2_test.db")
	dbFile, err := os.Create(dbPath)
	if err != nil {
		t.Fatalf("Failed to create db file: %v", err)
	}
	defer dbFile.Close()

	// Disable batching for test to ensure frequent commits
	originalBatchSize := converters.BatchSize
	converters.BatchSize = 1
	defer func() { converters.BatchSize = originalBatchSize }()

	err = converters.ImportToSQLite(converter, dbFile, nil)
	if err == nil {
		t.Log("ImportToSQLite succeeded, maybe file was too small to interrupt?")
	} else {
		t.Logf("ImportToSQLite interrupted as expected: %v", err)
	}
}
