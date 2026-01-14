package converters

import (
	"database/sql"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"

	_ "github.com/mattn/go-sqlite3"
)

func TestCSVStreamingFromURL(t *testing.T) {
	// 1. Setup Mock HTTP Server serving a large CSV
	rowCount := 10000
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Write header
		fmt.Fprintln(w, "id,name,value,timestamp")
		// Write rows
		for i := 0; i < rowCount; i++ {
			// Simulate some delay to test streaming nature? No, fast is fine.
			fmt.Fprintf(w, "%d,row_%d,%d,%s\n", i, i, i*10, time.Now().Format(time.RFC3339))
			if i%1000 == 0 {
				// Flush occasionally if possible to simulate streaming chunks
				if f, ok := w.(http.Flusher); ok {
					f.Flush()
				}
			}
		}
	}))
	defer ts.Close()

	// 2. Perform Request
	resp, err := http.Get(ts.URL)
	if err != nil {
		t.Fatalf("Failed to perform GET request: %v", err)
	}
	defer resp.Body.Close()

	// 3. Initialize CSVConverter with the response body
	converter, err := NewCSVConverterFromReader(resp.Body)
	if err != nil {
		t.Fatalf("NewCSVConverterFromReader failed: %v", err)
	}

	// 4. Convert to SQLite (using ImportToSQLite via ConvertFile-like logic but manual ImportToSQLite call)
	// We need to call ImportToSQLite(converter, outputPath)
	// Since converter implements RowProvider.

	tempDir := t.TempDir()
	outputPath := filepath.Join(tempDir, "streamed.db")

	err = ImportToSQLite(converter, outputPath)
	if err != nil {
		t.Fatalf("ImportToSQLite failed: %v", err)
	}

	// 5. Verify Data
	db, err := sql.Open("sqlite3", outputPath)
	if err != nil {
		t.Fatalf("Failed to open output database: %v", err)
	}
	defer db.Close()

	var count int
	err = db.QueryRow("SELECT COUNT(*) FROM tb0").Scan(&count)
	if err != nil {
		t.Fatalf("Failed to query database: %v", err)
	}

	if count != rowCount {
		t.Errorf("Expected %d rows, got %d", rowCount, count)
	}

	// Verify a sample row
	var name string
	err = db.QueryRow("SELECT name FROM tb0 WHERE id = '0'").Scan(&name) // IDs are text because headers aren't typed yet
	if err != nil {
		t.Fatalf("Failed to query row 0: %v", err)
	}
	if name != "row_0" {
		t.Errorf("Expected name 'row_0', got '%s'", name)
	}
}

func TestCSVStreamingSQLGeneration(t *testing.T) {
	// Test ConvertToSQL with reader
	rowCount := 100
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprintln(w, "col1,col2")
		for i := 0; i < rowCount; i++ {
			fmt.Fprintf(w, "val1_%d,val2_%d\n", i, i)
		}
	}))
	defer ts.Close()

	resp, err := http.Get(ts.URL)
	if err != nil {
		t.Fatalf("Failed to perform GET request: %v", err)
	}
	defer resp.Body.Close()

	// We use ConvertToSQL directly on a Converter.
	// We can use NewCSVConverterFromReader or just a dummy one since ConvertToSQL takes reader.
	// But ConvertToSQL is a method on *CSVConverter.
	converter := &CSVConverter{}
	// Note: ConvertToSQL method (as I implemented) uses the passed reader, not the struct state.

	tempDir := t.TempDir()
	outputPath := filepath.Join(tempDir, "streamed.sql")
	outFile, err := os.Create(outputPath)
	if err != nil {
		t.Fatalf("Failed to create output file: %v", err)
	}
	defer outFile.Close()

	err = converter.ConvertToSQL(resp.Body, outFile)
	if err != nil {
		t.Fatalf("ConvertToSQL failed: %v", err)
	}

	// Verify file content size/existence
	info, err := os.Stat(outputPath)
	if err != nil {
		t.Fatalf("Failed to stat output file: %v", err)
	}
	if info.Size() == 0 {
		t.Error("Output SQL file is empty")
	}
}
