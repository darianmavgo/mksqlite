package txt

import (
	"database/sql"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/darianmavgo/mksqlite/converters"

	_ "modernc.org/sqlite"
)

func TestTxtConverter_LargeFile(t *testing.T) {
	// We are now in mksqlite/converters/txt (presumably when running tests here)
	// Sample data is at ../../sample_data/...
	// NOTE: This path adjustment depends on where `go test` is run from.
	// We'll try to find the project root.
	// We verify that we can read the large file and get a significant number of rows.

	inputPath := "../../sample_data/20mb-examplefile-com.txt"
	// Check if this exists, if not, try from root
	if _, err := os.Stat(inputPath); os.IsNotExist(err) {
		inputPath = "sample_data/20mb-examplefile-com.txt"
	}

	file, err := os.Open(inputPath)
	if err != nil {
		// Try one more common path for "go test ./..." from root
		inputPath = "sample_data/20mb-examplefile-com.txt"
		file, err = os.Open(inputPath)
		if err != nil {
			t.Fatalf("failed to open sample file: %v", err)
		}
	}
	defer file.Close()

	conv, err := NewTxtConverter(file)
	if err != nil {
		t.Fatalf("failed to create TxtConverter: %v", err)
	}

	outputDir := "../../test_output"
	if err := os.MkdirAll(outputDir, 0755); err != nil {
		t.Fatalf("Failed to create output directory: %v", err)
	}
	dbPath := filepath.Join(outputDir, "test.db")
	dbFile, err := os.Create(dbPath)
	if err != nil {
		t.Fatalf("failed to create db file: %v", err)
	}
	defer dbFile.Close()

	if err := converters.ImportToSQLite(conv, dbFile, nil); err != nil {
		t.Fatalf("ImportToSQLite failed: %v", err)
	}

	// Verify DB content
	db, err := sql.Open("sqlite", dbPath)
	if err != nil {
		t.Fatalf("failed to open db: %v", err)
	}
	defer db.Close()

	// Check row count
	var count int
	err = db.QueryRow("SELECT COUNT(*) FROM tb0").Scan(&count)
	if err != nil {
		t.Fatalf("failed to count rows: %v", err)
	}
	if count < 1000 {
		t.Errorf("Unexpected row count. Got: %d, Want: > 1000", count)
	}

	// Check first row
	// Check first row
	var content string
	err = db.QueryRow("SELECT content FROM tb0 LIMIT 1").Scan(&content)
	if err != nil {
		t.Fatalf("failed to query first row: %v", err)
	}
	// "examplefile.com | Your Example Files." seemed to be the content
	if !strings.Contains(content, "examplefile") {
		t.Logf("First row content: %q", content)
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
