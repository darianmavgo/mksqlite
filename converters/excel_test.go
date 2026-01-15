package converters

import (
	"database/sql"
	"os"
	"path/filepath"
	"strings"
	"testing"

	_ "github.com/mattn/go-sqlite3"
)

func TestExcelConvertFile(t *testing.T) {
	inputPath := "../sample_data/sample.xlsx"
	outputPath := "../sample_out/excel_convert.db"

	// Check if file exists, if not, skip
	if _, err := os.Stat(inputPath); os.IsNotExist(err) {
		t.Skipf("Sample file not found: %s", inputPath)
	}

	// Ensure output directory exists
	if err := os.MkdirAll(filepath.Dir(outputPath), 0755); err != nil {
		t.Fatalf("Failed to create output directory: %v", err)
	}

	file, err := os.Open(inputPath)
	if err != nil {
		t.Fatalf("Failed to open input file: %v", err)
	}
	defer file.Close()

	converter, err := NewExcelConverter(file)
	if err != nil {
		t.Fatalf("Failed to create Excel converter: %v", err)
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
	t.Logf("Excel ConvertFile output: %s", outputPath)

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

	var tables []string
	for rows.Next() {
		var name string
		rows.Scan(&name)
		tables = append(tables, name)
	}
	t.Logf("Tables in DB: %v", tables)

	if len(tables) == 0 {
		t.Error("No tables found in database")
	}

	// Assume first table if any
	if len(tables) > 0 {
		var count int
		err = db.QueryRow("SELECT COUNT(*) FROM " + tables[0]).Scan(&count)
		if err != nil {
			t.Fatalf("Failed to query database: %v", err)
		}
		if count == 0 {
			t.Logf("Table %s is empty", tables[0])
		}
	}
}

func TestExcelConvertToSQL(t *testing.T) {
	inputPath := "../sample_data/sample.xlsx"
	outputPath := "../sample_out/excel_convert.sql"

	// Check if file exists
	if _, err := os.Stat(inputPath); os.IsNotExist(err) {
		t.Skipf("Sample file not found: %s", inputPath)
	}

	// Ensure output directory
	if err := os.MkdirAll(filepath.Dir(outputPath), 0755); err != nil {
		t.Fatalf("Failed to create output directory: %v", err)
	}

	file, err := os.Open(inputPath)
	if err != nil {
		t.Fatalf("Failed to open input file: %v", err)
	}
	defer file.Close()

	converter := &ExcelConverter{}

	outFile, err := os.Create(outputPath)
	if err != nil {
		t.Fatalf("Failed to create output file: %v", err)
	}
	defer outFile.Close()

	err = converter.ConvertToSQL(file, outFile)
	if err != nil {
		t.Fatalf("ConvertToSQL failed: %v", err)
	}

	// Verify content
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
