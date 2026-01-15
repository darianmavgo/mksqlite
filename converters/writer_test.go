package converters

import (
	"bytes"
	"database/sql"
	"io"
	"os"
	"testing"

	_ "github.com/mattn/go-sqlite3"
)

// MockProvider implements RowProvider for testing
type MockProvider struct {
	tableNames []string
	headers    map[string][]string
	rows       map[string][][]interface{}
}

func (m *MockProvider) GetTableNames() []string {
	return m.tableNames
}

func (m *MockProvider) GetHeaders(tableName string) []string {
	return m.headers[tableName]
}

func (m *MockProvider) ScanRows(tableName string, yield func([]interface{}) error) error {
	rows := m.rows[tableName]
	for _, row := range rows {
		if err := yield(row); err != nil {
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
	err := ImportToSQLite(provider, &buf)
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
