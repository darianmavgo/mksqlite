package html

import (
	"database/sql"
	"github.com/darianmavgo/mksqlite/converters"
	"os"
	"path/filepath"
	"strings"
	"testing"

	_ "github.com/mattn/go-sqlite3"
)

func TestHTMLConvertFile(t *testing.T) {
	inputPath := "../../sample_data/demo_mavgo_flight/Expenses.html" // Using real sample data
	outputDir := "../../test_output"
	if err := os.MkdirAll(outputDir, 0755); err != nil {
		t.Fatalf("Failed to create output directory: %v", err)
	}
	outputPath := filepath.Join(outputDir, "html_convert.db")

	// Ensure output directory exists
	if err := os.MkdirAll(filepath.Dir(outputPath), 0755); err != nil {
		t.Fatalf("Failed to create output directory: %v", err)
	}

	// Create a dummy HTML file for testing if sample data doesn't exist or is not suitable
	// But let's check if sample data exists first.
	_, err := os.Stat(inputPath)
	if os.IsNotExist(err) {
		// Create a dummy file
		content := `
<html>
<body>
<table id="test_table">
<tr><th>Name</th><th>Age</th></tr>
<tr><td>Alice</td><td>30</td></tr>
<tr><td>Bob</td><td>25</td></tr>
</table>
</body>
</html>
`
		tmpDir := t.TempDir()
		inputPath = filepath.Join(tmpDir, "test.html")
		if err := os.WriteFile(inputPath, []byte(content), 0644); err != nil {
			t.Fatalf("Failed to create dummy HTML file: %v", err)
		}
	}

	file, err := os.Open(inputPath)
	if err != nil {
		t.Fatalf("Failed to open input file: %v", err)
	}
	defer file.Close()

	converter, err := NewHTMLConverter(file)
	if err != nil {
		t.Fatalf("Failed to create HTML converter: %v", err)
	}

	outFile, err := os.Create(outputPath)
	if err != nil {
		t.Fatalf("Failed to create output file: %v", err)
	}
	defer outFile.Close()

	err = converters.ImportToSQLite(converter, outFile)
	if err != nil {
		t.Fatalf("ImportToSQLite failed: %v", err)
	}
	t.Logf("HTML ConvertFile output: %s", outputPath)

	// Verify the database was created and contains data
	db, err := sql.Open("sqlite3", outputPath)
	if err != nil {
		t.Fatalf("Failed to open output database: %v", err)
	}
	defer db.Close()

	// List tables
	rows, err := db.Query("SELECT name FROM sqlite_master WHERE type='table'")
	if err != nil {
		t.Fatalf("Failed to query tables: %v", err)
	}
	defer rows.Close()

	found := false
	for rows.Next() {
		var name string
		rows.Scan(&name)
		t.Logf("Found table: %s", name)
		found = true
	}

	if !found {
		t.Error("No tables found in database")
	}
}

func TestHTMLConvertToSQL(t *testing.T) {
	inputPath := "../../sample_data/demo_mavgo_flight/Expenses.html"
	outputDir := "../../test_output"
	if err := os.MkdirAll(outputDir, 0755); err != nil {
		t.Fatalf("Failed to create output directory: %v", err)
	}
	outputPath := filepath.Join(outputDir, "html_convert.sql")

	// Ensure output directory exists
	if err := os.MkdirAll(filepath.Dir(outputPath), 0755); err != nil {
		t.Fatalf("Failed to create output directory: %v", err)
	}

	// Use dummy content if file missing
	_, err := os.Stat(inputPath)
	if os.IsNotExist(err) {
		content := `
<html>
<body>
<table id="test_table">
<tr><th>Name</th><th>Age</th></tr>
<tr><td>Alice</td><td>30</td></tr>
<tr><td>Bob</td><td>25</td></tr>
</table>
</body>
</html>
`
		tmpDir := t.TempDir()
		inputPath = filepath.Join(tmpDir, "test.html")
		if err := os.WriteFile(inputPath, []byte(content), 0644); err != nil {
			t.Fatalf("Failed to create dummy HTML file: %v", err)
		}
	}

	file, err := os.Open(inputPath)
	if err != nil {
		t.Fatalf("Failed to open input file: %v", err)
	}
	defer file.Close()

	converter, err := NewHTMLConverter(file)
	if err != nil {
		t.Fatalf("Failed to create converter: %v", err)
	}

	outFile, err := os.Create(outputPath)
	if err != nil {
		t.Fatalf("Failed to create output file: %v", err)
	}
	defer outFile.Close()

	err = converter.ConvertToSQL(outFile)
	if err != nil {
		t.Fatalf("ConvertToSQL failed: %v", err)
	}

	content, err := os.ReadFile(outputPath)
	if err != nil {
		t.Fatalf("Failed to read output file: %v", err)
	}
	sqlStr := string(content)

	if len(sqlStr) == 0 {
		t.Error("Output SQL file is empty")
	}

	if !strings.Contains(sqlStr, "CREATE TABLE") {
		t.Error("Expected CREATE TABLE in SQL output")
	}
	if !strings.Contains(sqlStr, "INSERT INTO") {
		t.Error("Expected INSERT INTO in SQL output")
	}
}
