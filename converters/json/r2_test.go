package json_test

import (
	"fmt"
	"io"
	"github.com/darianmavgo/mksqlite/converters"
	"github.com/darianmavgo/mksqlite/converters/json"
	"net/http"
	"os"
	"path/filepath"
	"testing"
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

func TestJSONStreamingFromR2(t *testing.T) {
	url := "https://pub-a1c6b68deb9d48e1b5783f84723c93ec.r2.dev/sample_data/20mb.json"
	resp, err := http.Get(url)
	if err != nil {
		t.Fatalf("Failed to fetch from R2: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("Failed to fetch from R2: status %d", resp.StatusCode)
	}

	// Trigger failure near end of file to ensure partial parsing attempt?
	// Or early failure.
	failAt := int64(100)
	if resp.ContentLength > 0 {
		failAt = resp.ContentLength * 4 / 5
	}

	faultyReader := &R2FaultyReader{
		Body:        resp.Body,
		FailAtBytes: failAt,
	}

	converter, err := json.NewJSONConverter(faultyReader)
	if err != nil {
		t.Logf("NewJSONConverter interrupt handled: %v", err)
		return
	}

	outputDir := "../../test_output"
	if err := os.MkdirAll(outputDir, 0755); err != nil {
		t.Fatalf("Failed to create output directory: %v", err)
	}
	dbPath := filepath.Join(outputDir, "r2_json_test.db")
	dbFile, err := os.Create(dbPath)
	if err != nil {
		t.Fatalf("Failed to create db file: %v", err)
	}
	defer dbFile.Close()

	err = converters.ImportToSQLite(converter, dbFile, nil)
	if err == nil {
		t.Log("ImportToSQLite succeeded")
	} else {
		t.Logf("ImportToSQLite interrupted: %v", err)
	}
}
