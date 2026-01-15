package converters

import (
	"archive/zip"
	"database/sql"
	"os"
	"path/filepath"
	"testing"
	"time"

	_ "github.com/mattn/go-sqlite3"
)

func createTestZip(t *testing.T, path string) {
	// Ensure directory exists
	if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
		t.Fatalf("Failed to create directory: %v", err)
	}

	f, err := os.Create(path)
	if err != nil {
		t.Fatalf("Failed to create zip file: %v", err)
	}
	defer f.Close()

	w := zip.NewWriter(f)
	defer w.Close()

	var files = []struct {
		Name, Body string
		IsDir      bool
	}{
		{"readme.txt", "This is a readme file", false},
		{"data.csv", "name,age\nalice,30", false},
		{"images/", "", true},
		{"images/logo.png", "fake png content", false},
	}

	for _, file := range files {
		header := &zip.FileHeader{
			Name:     file.Name,
			Method:   zip.Deflate,
			Modified: time.Now(),
		}
		if file.IsDir {
			header.Name += "/"
		}

		f, err := w.CreateHeader(header)
		if err != nil {
			t.Fatalf("Failed to create entry in zip: %v", err)
		}
		if !file.IsDir {
			_, err = f.Write([]byte(file.Body))
			if err != nil {
				t.Fatalf("Failed to write to zip entry: %v", err)
			}
		}
	}
}

func TestZipConvertFile(t *testing.T) {
	inputPath := "../sample_data/test_archive.zip"
	outputPath := "../sample_out/zip_convert.db"

	// Clean up potential old files
	os.Remove(inputPath)
	os.Remove(outputPath)

	createTestZip(t, inputPath)
	defer os.Remove(inputPath)

	file, err := os.Open(inputPath)
	if err != nil {
		t.Fatalf("Failed to open input file: %v", err)
	}
	defer file.Close()

	converter, err := NewZipConverter(file)
	if err != nil {
		t.Fatalf("Failed to create Zip converter: %v", err)
	}

	// Ensure output directory exists
	if err := os.MkdirAll(filepath.Dir(outputPath), 0755); err != nil {
		t.Fatalf("Failed to create output directory: %v", err)
	}

	outFile, err := os.Create(outputPath)
	if err != nil {
		t.Fatalf("Failed to create output file: %v", err)
	}
	defer outFile.Close()

	err = ImportToSQLite(converter, outFile)
	if err != nil {
		t.Fatalf("ImportToSQLite failed: %v", err)
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

	expectedCount := 4 // readme.txt, data.csv, images/, images/logo.png
	// Note: Standard zip library might handle directories differently depending on how they are added.
	// But our createTestZip adds 4 entries explicitly.

	if count != expectedCount {
		t.Errorf("Expected %d rows in database, but found %d", expectedCount, count)
	}

	// Verify columns exist
	rows, err := db.Query("SELECT name, uncompressed_size, is_dir FROM file_list WHERE name LIKE '%readme.txt%'")
	if err != nil {
		t.Fatalf("Failed to query specific row: %v", err)
	}
	defer rows.Close()

	if rows.Next() {
		var name string
		var size int
		var isDir string
		err = rows.Scan(&name, &size, &isDir)
		if err != nil {
			t.Fatalf("Failed to scan row: %v", err)
		}
		if name != "readme.txt" {
			t.Errorf("Expected name 'readme.txt', got '%s'", name)
		}
		if size != len("This is a readme file") {
			t.Errorf("Expected size %d, got %d", len("This is a readme file"), size)
		}
		if isDir != "false" {
			t.Errorf("Expected is_dir 'false', got '%s'", isDir)
		}
	} else {
		t.Error("Row not found")
	}
}
