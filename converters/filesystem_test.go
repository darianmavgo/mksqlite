package converters

import (
	"database/sql"
	"os"
	"strings"
	"testing"

	_ "github.com/mattn/go-sqlite3"
)

func TestFilesystemConvertFile(t *testing.T) {
	inputPath := "../sample_data/demo_mavgo_flight/"
	// Check if directory exists
	if _, err := os.Stat(inputPath); os.IsNotExist(err) {
		t.Skipf("Sample data directory not found: %s", inputPath)
	}

	f, err := os.CreateTemp("", "fs_convert_*.db")
	if err != nil {
		t.Fatalf("failed to create temp file: %v", err)
	}
	outputPath := f.Name()
	f.Close()
	defer os.Remove(outputPath)

	converter := &FilesystemConverter{}

	err = converter.ConvertFile(inputPath, outputPath)
	if err != nil {
		t.Fatalf("ConvertFile failed: %v", err)
	}

	// Verify database content
	db, err := sql.Open("sqlite3", outputPath)
	if err != nil {
		t.Fatalf("failed to open db: %v", err)
	}
	defer db.Close()

	rows, err := db.Query("SELECT name FROM data")
	if err != nil {
		t.Fatalf("failed to query db: %v", err)
	}
	defer rows.Close()

	foundExpenses := false
	foundHistory := false

	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			t.Fatalf("failed to scan row: %v", err)
		}
		if name == "Expenses.csv" {
			foundExpenses = true
		}
		if name == "History.xlsx" {
			foundHistory = true
		}
	}

	if !foundExpenses {
		t.Errorf("Expected to find 'Expenses.csv' in database")
	}
	if !foundHistory {
		t.Errorf("Expected to find 'History.xlsx' in database")
	}
}

func TestFilesystemConvertToSQL(t *testing.T) {
	inputPath := "../sample_data/demo_mavgo_flight/"
	if _, err := os.Stat(inputPath); os.IsNotExist(err) {
		t.Skipf("Sample data directory not found: %s", inputPath)
	}

	f, err := os.CreateTemp("", "fs_convert_*.sql")
	if err != nil {
		t.Fatalf("failed to create temp file: %v", err)
	}
	outputPath := f.Name()
	f.Close()
	defer os.Remove(outputPath)

	converter := &FilesystemConverter{}

	// Open directory as file
	dirFile, err := os.Open(inputPath)
	if err != nil {
		t.Fatalf("failed to open dir: %v", err)
	}
	defer dirFile.Close()

	outputFile, err := os.Create(outputPath)
	if err != nil {
		t.Fatalf("failed to create output file: %v", err)
	}
	defer outputFile.Close()

	err = converter.ConvertToSQL(dirFile, outputFile)
	if err != nil {
		t.Fatalf("ConvertToSQL failed: %v", err)
	}

	// Verify SQL content
	content, err := os.ReadFile(outputPath)
	if err != nil {
		t.Fatalf("failed to read sql file: %v", err)
	}
	sqlStr := string(content)

	if !strings.Contains(sqlStr, "CREATE TABLE data") {
		t.Error("Expected CREATE TABLE in output")
	}
	if !strings.Contains(sqlStr, "INSERT INTO data") {
		t.Error("Expected INSERT INTO in output")
	}
	if !strings.Contains(sqlStr, "Expenses.csv") {
		t.Error("Expected 'Expenses.csv' in output")
	}
}
