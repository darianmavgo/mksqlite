package txt

import (
	"database/sql"
	"mksqlite/converters"
	"os"
	"path/filepath"
	"strings"
	"testing"

	_ "github.com/mattn/go-sqlite3"
)

func TestTxtConverter_RequestHeadersSampleCurl(t *testing.T) {
	// We are now in mksqlite/converters/txt (presumably when running tests here)
	// Sample data is at ../../sample_data/...
	// NOTE: This path adjustment depends on where `go test` is run from.
	// We'll try to find the project root.

	sampleRelPath := "../../sample_data/demo_chrome/request_headers_sample_curl.txt"
	// Check if this exists, if not, try from root
	if _, err := os.Stat(sampleRelPath); os.IsNotExist(err) {
		sampleRelPath = "sample_data/demo_chrome/request_headers_sample_curl.txt"
	}

	file, err := os.Open(sampleRelPath)
	if err != nil {
		// Try one more common path for "go test ./..." from root
		sampleRelPath = "sample_data/demo_chrome/request_headers_sample_curl.txt"
		file, err = os.Open(sampleRelPath)
		if err != nil {
			t.Fatalf("failed to open sample file: %v", err)
		}
	}
	defer file.Close()

	converter, err := NewTxtConverter(file)
	if err != nil {
		t.Fatalf("failed to create TxtConverter: %v", err)
	}

	dbPath := filepath.Join(t.TempDir(), "test.db")
	dbFile, err := os.Create(dbPath)
	if err != nil {
		t.Fatalf("failed to create db file: %v", err)
	}
	defer dbFile.Close()

	if err := converters.ImportToSQLite(converter, dbFile); err != nil {
		t.Fatalf("ImportToSQLite failed: %v", err)
	}

	db, err := sql.Open("sqlite3", dbPath)
	if err != nil {
		t.Fatalf("failed to open sqlite db: %v", err)
	}
	defer db.Close()

	rows, err := db.Query("SELECT name FROM sqlite_master WHERE type='table' AND name=?;", "tb0")
	if err != nil {
		t.Fatalf("failed to query tables: %v", err)
	}
	if !rows.Next() {
		t.Fatal("table 'tb0' not found")
	}
	rows.Close()

	var count int
	err = db.QueryRow("SELECT COUNT(*) FROM tb0").Scan(&count)
	if err != nil {
		t.Fatalf("failed to count rows: %v", err)
	}

	expectedLine1 := "User-Agent: curl/7.85.0"
	var content string
	err = db.QueryRow("SELECT content FROM tb0 WHERE rowid = 1").Scan(&content)
	if err != nil {
		t.Fatalf("failed to query row 1: %v", err)
	}

	if content != expectedLine1 {
		t.Errorf("Row 1 content mismatch.\nGot: %q\nWant: %q", content, expectedLine1)
	}

	if count != 20 {
		t.Errorf("Unexpected row count. Got: %d, Want: 20", count)
	}
}

func TestTxtConverter_StreamSQL(t *testing.T) {
	input := `Line 1
Line '2'
Line 3`
	reader := strings.NewReader(input)

	converter, err := NewTxtConverter(reader)
	if err != nil {
		t.Fatalf("failed to create converter: %v", err)
	}

	var builder strings.Builder
	if err := converter.ConvertToSQL(&builder); err != nil {
		t.Fatalf("ConvertToSQL failed: %v", err)
	}

	output := builder.String()
	expectedContains := []string{
		"CREATE TABLE tb0 (content TEXT);",
		"INSERT INTO tb0 (content) VALUES ('Line 1');",
		"INSERT INTO tb0 (content) VALUES ('Line ''2''');",
		"INSERT INTO tb0 (content) VALUES ('Line 3');",
	}

	for _, exp := range expectedContains {
		if !strings.Contains(output, exp) {
			t.Errorf("Output missing expected string: %q", exp)
		}
	}
}
