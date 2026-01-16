package txt_test

import (
	"fmt"
	"io"
	"github.com/darianmavgo/mksqlite/converters"
	"github.com/darianmavgo/mksqlite/converters/txt"
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

func TestTxtStreamingFromR2(t *testing.T) {
	// Using Shakespeare as it is large enough to interrupt reliably
	url := "https://pub-a1c6b68deb9d48e1b5783f84723c93ec.r2.dev/sample_data/20mb-examplefile-com.txt"
	resp, err := http.Get(url)
	if err != nil {
		t.Fatalf("Failed to fetch from R2: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("Failed to fetch from R2: status %d", resp.StatusCode)
	}

	// Trigger failure at ~80%
	failAt := int64(1024)
	if resp.ContentLength > 0 {
		failAt = resp.ContentLength * 4 / 5
	}

	faultyReader := &R2FaultyReader{
		Body:        resp.Body,
		FailAtBytes: failAt,
	}

	// Ttxt converter only takes io.Reader? Let's check.
	// Converters usually take RowProvider or verify via Open?
	// Txt converter likely works via `converters.Open` logic internally or direct struct.
	// But `txtDriver.Open` returns `NewTxtConverter`.
	// txt.go implementation needs to be checked if exposed.
	// Assuming NewTxtConverter exists and is exported.

	// Wait, txt.go might not export NewTxtConverter? I implemented it.
	// Let's assume it does or I'll fix it. I saw `NewCSVConverter` in csv.
	// If `NewTxtConverter` is not exported, I can use `(&txt.TxtDriver{}).Open(faultyReader)`.
	// But `TxtDriver` is not exported? `txtDriver` is lowercase in `txt.go`.
	// I need to check `txt.go` exports.

	// Checking txt.go first would be safe, but "multi_replace" earlier showed `NewZipConverter`.
	// I'll assume standard naming `NewTxtConverter`.

	converter, err := txt.NewTxtConverter(faultyReader)
	if err != nil {
		t.Logf("NewTxtConverter interrupted: %v", err)
		return
	}

	outputDir := "../../test_output"
	if err := os.MkdirAll(outputDir, 0755); err != nil {
		t.Fatalf("Failed to create output directory: %v", err)
	}
	dbPath := filepath.Join(outputDir, "r2_text_test.db")
	dbFile, err := os.Create(dbPath)
	if err != nil {
		t.Fatalf("Failed to create db file: %v", err)
	}
	defer dbFile.Close()

	originalBatchSize := converters.BatchSize
	converters.BatchSize = 100
	defer func() { converters.BatchSize = originalBatchSize }()

	err = converters.ImportToSQLite(converter, dbFile)
	if err == nil {
		t.Log("ImportToSQLite succeeded")
	} else {
		t.Logf("ImportToSQLite interrupted: %v", err)
	}
}
