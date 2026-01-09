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
	outputPath := "../sample_out/csv_convert.db"

	err := converter.ConvertFile(inputPath, outputPath)
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
}

func TestCSVConvertToSQL(t *testing.T) {
	converter := &CSVConverter{}

	inputPath := "../sample_data/demo_mavgo_flight/Expenses.csv"
	outputPath := "../sample_out/csv_convert.sql"

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
	t.Logf("CSV ConvertToSQL output: %s", outputPath)

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
}

func TestCSVParseCSV(t *testing.T) {
	inputPath := "../sample_data/demo_mavgo_flight/Expenses.csv"
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