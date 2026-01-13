package converters

import (
	"database/sql"
	"os"
	"testing"

	_ "github.com/mattn/go-sqlite3"
)

func TestZipConvertFile(t *testing.T) {
	converter := &ZipConverter{}

	// Use sample.xlsx as a zip file source, as xlsx is a zip file.
	inputPath := "../sample_data/sample.xlsx"
	if _, err := os.Stat(inputPath); os.IsNotExist(err) {
		t.Skipf("Sample file not found: %s", inputPath)
	}

	f, err := os.CreateTemp("", "zip_convert_*.db")
	if err != nil {
		t.Fatalf("failed to create temp file: %v", err)
	}
	outputPath := f.Name()
	f.Close()
	defer os.Remove(outputPath)

	err = converter.ConvertFile(inputPath, outputPath)
	if err != nil {
		t.Fatalf("ConvertFile failed: %v", err)
	}
	t.Logf("Zip ConvertFile output: %s", outputPath)

	// Verify the database was created and contains data
	db, err := sql.Open("sqlite3", outputPath)
	if err != nil {
		t.Fatalf("Failed to open output database: %v", err)
	}
	defer db.Close()

	var count int
	err = db.QueryRow("SELECT COUNT(*) FROM file_list").Scan(&count)
	if err != nil {
		t.Fatalf("Failed to query database: %v", err)
	}

	if count == 0 {
		t.Error("Expected rows in database, but found none")
	}

	// Check for standard Excel xml files inside the zip
	// usually [Content_Types].xml or xl/workbook.xml exist.
	rows, err := db.Query("SELECT name FROM file_list WHERE name LIKE '%workbook.xml%' OR name LIKE '%[Content_Types].xml%'")
	if err != nil {
		t.Fatalf("Failed to query specific row: %v", err)
	}
	defer rows.Close()

	found := false
	for rows.Next() {
		found = true
		break
	}

	if !found {
		t.Error("Expected to find workbook.xml or [Content_Types].xml inside the xlsx/zip file")
	}
}
