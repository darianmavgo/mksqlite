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

	inputPath := "../sample_data/demo_mavgo_flight/History.xlsx" // Using real sample data
	outputPath := "../sample_out/excel_convert.db"

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

	// Get the first table name
	var tableName string
	err = db.QueryRow("SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%' LIMIT 1").Scan(&tableName)
	if err != nil {
		t.Fatalf("Failed to get table name: %v", err)
	}

	var count int
	err = db.QueryRow("SELECT COUNT(*) FROM " + tableName).Scan(&count)
	if err != nil {
		t.Fatalf("Failed to query database: %v", err)
	}

	if count == 0 {
		t.Error("Expected data in database, but found none")
	}
}

func TestExcelConvertToSQL(t *testing.T) {
	converter := &ExcelConverter{}

	inputPath := "../sample_data/demo_mavgo_flight/History.xlsx"

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

	inputPath := "../sample_data/demo_mavgo_flight/History.xlsx"
	outputPath := "../sample_out/excel_parse.db"

	err := converter.ConvertFile(inputPath, outputPath)
	if err != nil {
		t.Fatalf("ConvertFile failed: %v", err)
	}
	t.Logf("Excel ParseAndConvert output: %s", outputPath)

	// Check that tables were created
	db, err := sql.Open("sqlite3", outputPath)
	if err != nil {
		t.Fatalf("Failed to open output database: %v", err)
	}
	defer db.Close()

	// Get list of tables
	tableRows, err := db.Query("SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'")
	if err != nil {
		t.Fatalf("Failed to query tables: %v", err)
	}
	defer tableRows.Close()

	tables := []string{}
	for tableRows.Next() {
		var name string
		err = tableRows.Scan(&name)
		if err != nil {
			t.Fatalf("Failed to scan table name: %v", err)
		}
		tables = append(tables, name)
	}

	expectedTables := map[string]bool{
		"organized":       true,
		"timeline":        true,
		"raw_content":     true,
		"example_for_chatgpt": true,
	}

	if len(tables) < 4 {
		t.Errorf("Expected at least 4 tables, got %d: %v", len(tables), tables)
	}

	for _, table := range tables {
		if expectedTables[table] {
			delete(expectedTables, table)
		}
	}

	if len(expectedTables) > 0 {
		missing := []string{}
		for t := range expectedTables {
			missing = append(missing, t)
		}
		t.Errorf("Missing expected tables: %v", missing)
	}

	// Check one table's structure
	if len(tables) > 0 {
		tableName := tables[0]
		rows, err := db.Query("PRAGMA table_info(" + tableName + ")")
		if err != nil {
			t.Fatalf("Failed to get table info for %s: %v", tableName, err)
		}
		defer rows.Close()

		columns := 0
		for rows.Next() {
			columns++
		}

		if columns == 0 {
			t.Errorf("Expected columns in table %s, but found none", tableName)
		}
	}
}