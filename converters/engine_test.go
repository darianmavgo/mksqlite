package converters

import (
	"bytes"
	"database/sql"
	"fmt"
	"io"
	"github.com/darianmavgo/mksqlite/converters/common"
	"os"
	"testing"

	_ "github.com/mattn/go-sqlite3"
)

// MockProvider implements common.RowProvider for testing
type MockProvider struct {
	tableNames []string
	headers    map[string][]string
	rows       map[string][][]interface{}
}

// Ensure MockProvider implements common.RowProvider
var _ common.RowProvider = (*MockProvider)(nil)

func (m *MockProvider) GetTableNames() []string {
	return m.tableNames
}

func (m *MockProvider) GetHeaders(tableName string) []string {
	return m.headers[tableName]
}

func (m *MockProvider) ScanRows(tableName string, yield func([]interface{}, error) error) error {
	rows := m.rows[tableName]
	for _, row := range rows {
		if err := yield(row, nil); err != nil {
			return err
		}
	}
	return nil
}

func TestImportToSQLiteWriter(t *testing.T) {
	// Setup mock provider
	provider := &MockProvider{
		tableNames: []string{"tb0"},
		headers: map[string][]string{
			"tb0": {"col1", "col2"},
		},
		rows: map[string][][]interface{}{
			"tb0": {
				{"val1", "val2"},
				{"val3", "val4"},
			},
		},
	}

	// Create a buffer to write to
	var buf bytes.Buffer

	// Call ImportToSQLite
	err := ImportToSQLite(provider, &buf, nil)
	if err != nil {
		t.Fatalf("ImportToSQLite failed: %v", err)
	}

	// Verify buffer is not empty
	if buf.Len() == 0 {
		t.Fatal("Buffer is empty")
	}

	// Verify it looks like a SQLite file
	// SQLite database header string is "SQLite format 3\000"
	header := buf.Bytes()
	if len(header) < 16 {
		t.Fatal("Buffer too short to be SQLite file")
	}
	expectedHeader := []byte("SQLite format 3\000")
	if !bytes.Equal(header[:16], expectedHeader) {
		t.Errorf("Invalid SQLite header: got %q, want prefix %q", header[:16], expectedHeader)
	}

	// Verify content by writing to a persistent file and opening it
	outputPath := "../sample_out/writer_verify.db"
	tmpFile, err := os.Create(outputPath)
	if err != nil {
		t.Fatalf("Failed to create verification file: %v", err)
	}

	if _, err := io.Copy(tmpFile, bytes.NewReader(buf.Bytes())); err != nil {
		t.Fatalf("Failed to write buffer to file: %v", err)
	}
	tmpFile.Close()

	// Open the verification DB
	db, err := sql.Open("sqlite3", outputPath)
	if err != nil {
		t.Fatalf("Failed to open verification DB: %v", err)
	}
	defer db.Close()

	// Query data
	var val1, val2 string
	err = db.QueryRow("SELECT col1, col2 FROM tb0 WHERE rowid = 1").Scan(&val1, &val2)
	if err != nil {
		t.Fatalf("Failed to query row: %v", err)
	}

	if val1 != "val1" || val2 != "val2" {
		t.Errorf("Unexpected values: got %s, %s; want val1, val2", val1, val2)
	}
}

// ErrorMockProvider simulates errors during scanning
type ErrorMockProvider struct {
	MockProvider
	rowErrors map[string]map[int]error
}

func (m *ErrorMockProvider) ScanRows(tableName string, yield func([]interface{}, error) error) error {
	rows := m.rows[tableName]
	for i, row := range rows {
		var rowErr error
		if errs, ok := m.rowErrors[tableName]; ok {
			if err, ok := errs[i]; ok {
				rowErr = err
			}
		}
		if err := yield(row, rowErr); err != nil {
			return err
		}
	}
	return nil
}

func TestImportToSQLiteWithLogging(t *testing.T) {
	// Setup mock provider with some errors
	provider := &ErrorMockProvider{
		MockProvider: MockProvider{
			tableNames: []string{"tb0"},
			headers: map[string][]string{
				"tb0": {"col1"},
			},
			rows: map[string][][]interface{}{
				"tb0": {
					{"val1"},
					{"val2"}, // This one will have error
					{"val3"},
				},
			},
		},
		rowErrors: map[string]map[int]error{
			"tb0": {
				1: fmt.Errorf("mock error for row 2"),
			},
		},
	}

	// Temp output file
	tmpFile, err := os.CreateTemp("", "logging_test_*.db")
	if err != nil {
		t.Fatalf("Failed to create temp file: %v", err)
	}
	dbPath := tmpFile.Name()
	tmpFile.Close()
	defer os.Remove(dbPath)

	// Re-open as *os.File to pass to ImportToSQLite (so it writes to disk)
	f, err := os.OpenFile(dbPath, os.O_RDWR, 0666)
	if err != nil {
		t.Fatalf("Failed to open temp file: %v", err)
	}

	// Call ImportToSQLite with logging enabled
	err = ImportToSQLite(provider, f, &ImportOptions{LogErrors: true})
	f.Close()
	if err != nil {
		t.Fatalf("ImportToSQLite failed: %v", err)
	}

	// Verify DB content
	db, err := sql.Open("sqlite3", dbPath)
	if err != nil {
		t.Fatalf("Failed to open DB: %v", err)
	}
	defer db.Close()

	// Check valid rows (val1 and val3 should be there)
	var count int
	err = db.QueryRow("SELECT count(*) FROM tb0").Scan(&count)
	if err != nil {
		t.Fatalf("Failed to query count: %v", err)
	}
	if count != 2 {
		t.Errorf("Expected 2 valid rows, got %d", count)
	}

	// Check error log
	err = db.QueryRow("SELECT count(*) FROM _mksqlite_errors").Scan(&count)
	if err != nil {
		// Table might not exist if no errors were logged (but we expect one)
		t.Fatalf("Failed to query error log count: %v", err)
	}
	if count != 1 {
		t.Errorf("Expected 1 error log, got %d", count)
	}

	var msg, tbl string
	err = db.QueryRow("SELECT message, table_name FROM _mksqlite_errors LIMIT 1").Scan(&msg, &tbl)
	if err != nil {
		t.Fatalf("Failed to query error log: %v", err)
	}

	if msg != "mock error for row 2" {
		t.Errorf("Unexpected error message: %s", msg)
	}
	if tbl != "tb0" {
		t.Errorf("Unexpected table name: %s", tbl)
	}
}
