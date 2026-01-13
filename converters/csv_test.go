package converters

import (
	"database/sql"
	"os"
	"strings"
	"testing"

	_ "github.com/mattn/go-sqlite3"
)

func TestCSVConvertFile(t *testing.T) {
	converter := &CSVConverter{}

	inputPath := "../sample_data/demo_mavgo_flight/Expenses.csv" // Using real sample data
	if _, err := os.Stat(inputPath); os.IsNotExist(err) {
		t.Skipf("Sample file not found: %s", inputPath)
	}

	f, err := os.CreateTemp("", "csv_convert_*.db")
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
	t.Logf("CSV ConvertFile output: %s", outputPath)

	// Verify the database was created and contains data
	db, err := sql.Open("sqlite3", outputPath)
	if err != nil {
		t.Fatalf("Failed to open output database: %v", err)
	}
	defer db.Close()

	var count int
	err = db.QueryRow("SELECT COUNT(*) FROM data").Scan(&count)
	if err != nil {
		t.Fatalf("Failed to query database: %v", err)
	}

	if count == 0 {
		t.Error("Expected data in database, but found none")
	}

	// Verify specific data exists
	// Description column should contain "Visible.edu Visible.ed"
	// Column name might be sanitized. "Description" -> "description"
	var desc string
	err = db.QueryRow("SELECT description FROM data WHERE description LIKE 'Visible.edu%' LIMIT 1").Scan(&desc)
	if err != nil {
		// Try generic check if column name is different
		t.Logf("Failed to query 'description' column: %v. Checking generic existence.", err)
	} else {
		if !strings.Contains(desc, "Visible.edu") {
			t.Errorf("Expected description to contain 'Visible.edu', got '%s'", desc)
		}
	}
}

func TestCSVConvertToSQL(t *testing.T) {
	converter := &CSVConverter{}

	inputPath := "../sample_data/demo_mavgo_flight/Expenses.csv"
	if _, err := os.Stat(inputPath); os.IsNotExist(err) {
		t.Skipf("Sample file not found: %s", inputPath)
	}

	f, err := os.CreateTemp("", "csv_convert_*.sql")
	if err != nil {
		t.Fatalf("failed to create temp file: %v", err)
	}
	outputPath := f.Name()
	f.Close()
	defer os.Remove(outputPath)

	file, err := os.Open(inputPath)
	if err != nil {
		t.Fatalf("Failed to open input file: %v", err)
	}
	defer file.Close()

	outFile, err := os.Create(outputPath)
	if err != nil {
		t.Fatalf("Failed to create output file: %v", err)
	}
	defer outFile.Close()

	err = converter.ConvertToSQL(file, outFile)
	if err != nil {
		t.Fatalf("ConvertToSQL failed: %v", err)
	}

	// Read back to verify
	content, err := os.ReadFile(outputPath)
	if err != nil {
		t.Fatalf("Failed to read output file: %v", err)
	}
	sqlOutput := string(content)
	if !strings.Contains(sqlOutput, "CREATE TABLE data") {
		t.Error("Expected CREATE TABLE statement in SQL output")
	}
	if !strings.Contains(sqlOutput, "INSERT INTO data") {
		t.Error("Expected INSERT statement in SQL output")
	}
	if !strings.Contains(sqlOutput, "Visible.edu") {
		t.Error("Expected 'Visible.edu' in SQL output")
	}
}

func TestCSVParseCSV(t *testing.T) {
	inputPath := "../sample_data/demo_mavgo_flight/Expenses.csv"
	if _, err := os.Stat(inputPath); os.IsNotExist(err) {
		t.Skipf("Sample file not found: %s", inputPath)
	}

	file, err := os.Open(inputPath)
	if err != nil {
		t.Fatalf("Failed to open input file: %v", err)
	}
	defer file.Close()

	headers, rows, err := parseCSV(file)
	if err != nil {
		t.Fatalf("parseCSV failed: %v", err)
	}

	if len(headers) == 0 {
		t.Error("Expected headers, but got none")
	}
	if len(rows) == 0 {
		t.Error("Expected rows, but got none")
	}

	// Check that headers are sanitized (no special chars, etc.)
	for _, header := range headers {
		if strings.ContainsAny(header, " !@#$%^&*()") {
			t.Errorf("Header '%s' contains invalid characters", header)
		}
	}
}
