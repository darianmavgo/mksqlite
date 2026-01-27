package zip

import (
	"database/sql"
	"os"
	"path/filepath"
	"testing"

	"github.com/darianmavgo/mksqlite/converters"

	_ "modernc.org/sqlite"
)

func TestZipConvertFile(t *testing.T) {
	inputPath := "../../sample_data/history.db.zip"
	outputDir := "../../test_output"
	if err := os.MkdirAll(outputDir, 0755); err != nil {
		t.Fatalf("Failed to create output directory: %v", err)
	}
	outputPath := filepath.Join(outputDir, "zip_convert.db")

	// Clean up potential old output
	os.Remove(outputPath)

	// We do NOT remove inputPath as it is a real underlying file now.
	// We also do not create a test zip on the fly.

	file, err := os.Open(inputPath)
	if err != nil {
		t.Fatalf("Failed to open input file: %v", err)
	}
	defer file.Close()

	converter, err := NewZipConverter(file)
	if err != nil {
		t.Fatalf("Failed to create Zip converter: %v", err)
	}

	// Ensure output directory exists (redundant but safe)
	if err := os.MkdirAll(filepath.Dir(outputPath), 0755); err != nil {
		t.Fatalf("Failed to create output directory: %v", err)
	}

	outFile, err := os.Create(outputPath)
	if err != nil {
		t.Fatalf("Failed to create output file: %v", err)
	}
	defer outFile.Close()

	err = converters.ImportToSQLite(converter, outFile, nil)
	if err != nil {
		t.Fatalf("ImportToSQLite failed: %v", err)
	}
	t.Logf("Zip ConvertFile output: %s", outputPath)

	// Verify the database was created and contains data
	db, err := sql.Open("sqlite", outputPath)
	if err != nil {
		t.Fatalf("Failed to open output database: %v", err)
	}
	defer db.Close()

	var count int
	err = db.QueryRow("SELECT COUNT(*) FROM file_list").Scan(&count)
	if err != nil {
		t.Fatalf("Failed to query database: %v", err)
	}

	// We expect at least one file (history.db presumably)
	if count < 1 {
		t.Errorf("Expected at least 1 row in database, but found %d", count)
	}

	// Check if history.db is present (assuming simple zip of the db file)
	var found int
	err = db.QueryRow("SELECT COUNT(*) FROM file_list WHERE name LIKE '%history.db%'").Scan(&found)
	if err != nil {
		t.Fatalf("Failed to query for history.db: %v", err)
	}
	if found == 0 {
		t.Log("Warning: 'history.db' not found in file_list. Content might be named differently.")
		// Print first few names
		rows, _ := db.Query("SELECT name FROM file_list LIMIT 5")
		defer rows.Close()
		for rows.Next() {
			var name string
			rows.Scan(&name)
			t.Logf("Found file: %s", name)
		}
	}
}
