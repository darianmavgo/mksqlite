package converters

import (
	"database/sql"
	"io"
	"os"
	"strings"
	"testing"

	_ "github.com/mattn/go-sqlite3"
)

func TestExcelConvertFile(t *testing.T) {
	converter := &ExcelConverter{}

	inputPath := "../sample_data/sample.xlsx" // Assuming sample_data is at project root
	outputPath := "../test_output/excel_convert.db"

	err := converter.ConvertFile(inputPath, outputPath)
	if err != nil {
		t.Fatalf("ConvertFile failed: %v", err)
	}
	t.Logf("Excel ConvertFile output: %s", outputPath)

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

func TestExcelConvertToSQL(t *testing.T) {
	converter := &ExcelConverter{}

	inputPath := "../sample_data/sample.xlsx"

	file, err := os.Open(inputPath)
	if err != nil {
		t.Fatalf("Failed to open input file: %v", err)
	}
	defer file.Close()

	err = converter.ConvertToSQL(file, io.Discard)
	if err == nil {
		t.Fatal("Expected ConvertToSQL to fail for Excel (not implemented)")
	}
	if !strings.Contains(err.Error(), "not yet implemented") {
		t.Errorf("Unexpected error: %v", err)
	}
}

func TestExcelParseAndConvert(t *testing.T) {
	// Test that Excel sheets are properly parsed and converted
	converter := &ExcelConverter{}

	inputPath := "../sample_data/sample.xlsx"
	outputPath := "../test_output/excel_parse.db"

	err := converter.ConvertFile(inputPath, outputPath)
	if err != nil {
		t.Fatalf("ConvertFile failed: %v", err)
	}
	t.Logf("Excel ParseAndConvert output: %s", outputPath)

	// Check table structure
	db, err := sql.Open("sqlite3", outputPath)
	if err != nil {
		t.Fatalf("Failed to open output database: %v", err)
	}
	defer db.Close()

	rows, err := db.Query("PRAGMA table_info(data)")
	if err != nil {
		t.Fatalf("Failed to get table info: %v", err)
	}
	defer rows.Close()

	columns := 0
	for rows.Next() {
		columns++
	}

	if columns == 0 {
		t.Error("Expected columns in table, but found none")
	}

	// Verify data types are TEXT
	rows, err = db.Query("PRAGMA table_info(data)")
	if err != nil {
		t.Fatalf("Failed to get table info: %v", err)
	}
	defer rows.Close()

	for rows.Next() {
		var cid int
		var name, ctype string
		var notnull, dflt_value interface{}
		var pk int
		err = rows.Scan(&cid, &name, &ctype, &notnull, &dflt_value, &pk)
		if err != nil {
			t.Fatalf("Failed to scan column info: %v", err)
		}
		if ctype != "TEXT" {
			t.Errorf("Expected column type TEXT, got %s", ctype)
		}
	}
}