package converters

import (
	"bytes"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	_ "github.com/mattn/go-sqlite3"
)

// FaultyReader simulates a stream that fails after reading a certain amount of data.
type FaultyReader struct {
	data        []byte
	readIndex   int
	failAtBytes int
}

func (r *FaultyReader) Read(p []byte) (n int, err error) {
	if r.readIndex >= r.failAtBytes {
		return 0, fmt.Errorf("simulated stream interruption")
	}
	remaining := len(r.data) - r.readIndex
	toRead := len(p)
	if toRead > remaining {
		toRead = remaining
	}
	// Cap at failAtBytes
	if r.readIndex + toRead > r.failAtBytes {
		toRead = r.failAtBytes - r.readIndex
	}

	copy(p, r.data[r.readIndex:r.readIndex+toRead])
	r.readIndex += toRead

	return toRead, nil
}

func TestStreamingInterruption(t *testing.T) {
	// 1. Generate CSV Data
	var buffer bytes.Buffer
	buffer.WriteString("id,value\n")
	for i := 0; i < 2000; i++ {
		buffer.WriteString(fmt.Sprintf("%d,value_%d\n", i, i))
	}
	data := buffer.Bytes()

	// 2. Setup FaultyReader to fail at ~75%
	// This ensures we get past the header and into the rows.
	failAt := len(data) * 3 / 4
	reader := &FaultyReader{
		data:        data,
		failAtBytes: failAt,
	}

	// 3. Create Converter
	// Note: NewCSVConverterFromReader reads the header immediately.
	// Make sure failAtBytes is large enough (it is).
	converter, err := NewCSVConverterFromReader(reader)
	if err != nil {
		t.Fatalf("Failed to create converter: %v", err)
	}

	// 4. Run ImportToSQLite
	tmpDir := "../sample_out/streaming_test"
	if err := os.RemoveAll(tmpDir); err != nil {
		t.Fatalf("Failed to clean tmp dir: %v", err)
	}
	if err := os.MkdirAll(tmpDir, 0755); err != nil {
		t.Fatalf("Failed to create tmp dir: %v", err)
	}
	dbPath := filepath.Join(tmpDir, "interrupted.db")

	// Set batch size to 100 for testing
	originalBatchSize := BatchSize
	BatchSize = 100
	defer func() { BatchSize = originalBatchSize }()

	err = ImportToSQLiteFile(converter, dbPath)
	if err == nil {
		t.Log("ImportToSQLite succeeded unexpectedly (stream interruption didn't occur or was handled?)")
	} else {
		t.Logf("ImportToSQLite failed as expected: %v", err)
	}

	// 5. Check DB Rows
	db, err := sql.Open("sqlite3", dbPath)
	if err != nil {
		t.Fatalf("Failed to open DB: %v", err)
	}
	defer db.Close()

	// Check if table exists and count rows
	var count int
	// Use CSVTB constant if exported, otherwise "tb0"
	tableName := "tb0"

	// Verify table exists first (it should, as creation is outside transaction)
	query := fmt.Sprintf("SELECT name FROM sqlite_master WHERE type='table' AND name='%s'", tableName)
	var name string
	err = db.QueryRow(query).Scan(&name)
	if err != nil {
		if err == sql.ErrNoRows {
			t.Fatalf("Table %s was not created!", tableName)
		}
		t.Fatalf("Failed to check table existence: %v", err)
	}

	err = db.QueryRow(fmt.Sprintf("SELECT COUNT(*) FROM %s", tableName)).Scan(&count)
	if err != nil {
		t.Fatalf("Failed to count rows: %v", err)
	}

	t.Logf("Rows in DB: %d", count)

	// We expect significant number of rows (e.g. > 1000) because batching commits periodically.
	if count < 1000 {
		t.Errorf("Expected > 1000 rows with batching, got %d", count)
	} else {
		t.Logf("Verified: %d rows inserted despite stream interruption.", count)
	}
}
