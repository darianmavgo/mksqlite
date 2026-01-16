package csv_test

import (
	"fmt"
	"io"
	"github.com/darianmavgo/mksqlite/converters"
	"github.com/darianmavgo/mksqlite/converters/csv"
	"net/http"
	"os"
	"path/filepath"
	"testing"
)

// LargeFaultyReader simulates interruption for large streams
type LargeFaultyReader struct {
	Body        io.ReadCloser
	ReadCount   int64
	FailAtBytes int64
}

func (r *LargeFaultyReader) Read(p []byte) (n int, err error) {
	n, err = r.Body.Read(p)
	r.ReadCount += int64(n)
	if r.ReadCount > r.FailAtBytes {
		r.Body.Close()
		return n, fmt.Errorf("simulated stream interruption at %d bytes", r.ReadCount)
	}
	return n, err
}

func (r *LargeFaultyReader) Close() error {
	return r.Body.Close()
}

func TestLargeShakespeareCSV(t *testing.T) {
	url := "https://huggingface.co/datasets/flwrlabs/shakespeare/resolve/main/shakespeare.csv"
	resp, err := http.Get(url)
	if err != nil {
		t.Fatalf("Failed to fetch from HuggingFace: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("Failed to fetch from HuggingFace: status %d", resp.StatusCode)
	}

	// Interrupt after 5MB (sufficient to test streaming without downloading 500MB)
	failAt := int64(5 * 1024 * 1024)

	faultyReader := &LargeFaultyReader{
		Body:        resp.Body,
		FailAtBytes: failAt,
	}

	converter, err := csv.NewCSVConverter(faultyReader)
	if err != nil {
		t.Logf("NewCSVConverter interrupt: %v", err)
		return
	}

	outputDir := "../../test_output/large_tests"
	if err := os.MkdirAll(outputDir, 0755); err != nil {
		t.Fatalf("Failed to create output directory: %v", err)
	}
	dbPath := filepath.Join(outputDir, "shakespeare.db")
	dbFile, err := os.Create(dbPath)
	if err != nil {
		t.Fatalf("Failed to create db file: %v", err)
	}
	defer dbFile.Close()

	// Ensure batch size is standard
	// converters.BatchSize = 1000 // default

	err = converters.ImportToSQLite(converter, dbFile)
	if err == nil {
		t.Log("ImportToSQLite succeeded (surprisingly, if we expected interruption)")
	} else {
		t.Logf("ImportToSQLite interrupted as expected: %v", err)
	}
}
