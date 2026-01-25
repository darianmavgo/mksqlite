package csv

import (
	"bytes"
	"database/sql"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/darianmavgo/mksqlite/converters"

	_ "github.com/mattn/go-sqlite3"
)

func TestCSVConvertFromURL(t *testing.T) {
	url := "https://pub-a1c6b68deb9d48e1b5783f84723c93ec.r2.dev/Apps_GoogleDownload_Darian.Device_takeout-20251014T200156Z-1-007_Takeout_Drive_trading_crisis-winners_TZA_6_years_data.csv"

	resp, err := http.Get(url)
	if err != nil {
		t.Fatalf("Failed to fetch URL: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("Failed to fetch URL, status code: %d", resp.StatusCode)
	}

	converter, err := NewCSVConverter(resp.Body)
	if err != nil {
		t.Fatalf("Failed to create converter from reader: %v", err)
	}

	outputDir := "../../test_output"
	if err := os.MkdirAll(outputDir, 0755); err != nil {
		t.Fatalf("Failed to create output directory: %v", err)
	}
	outputPath := filepath.Join(outputDir, "url_test.db")
	if err := os.Remove(outputPath); err != nil && !os.IsNotExist(err) {
		t.Logf("Failed to remove existing output: %v", err)
	}

	// Ensure output directory exists (since we changed path to ../../sample_out)
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
		t.Logf("ImportToSQLite finished with error (possibly network interruption): %v", err)
	} else {
		t.Log("ImportToSQLite finished successfully")
	}

	// Verify database content
	db, err := sql.Open("sqlite3", outputPath)
	if err != nil {
		t.Fatalf("Failed to open output database: %v", err)
	}
	defer db.Close()

	var count int
	// Using CSVTB default "tb0"
	query := fmt.Sprintf("SELECT COUNT(*) FROM %s", CSVTB)
	err = db.QueryRow(query).Scan(&count)
	if err != nil {
		// If the table doesn't exist, it means nothing was committed (or creation failed)
		t.Fatalf("Failed to query database (table might be missing): %v", err)
	}

	t.Logf("Rows in DB: %d", count)

	if count == 0 {
		t.Error("Expected data in database, but found none")
	}
}

func TestCSVConvertFile(t *testing.T) {
	inputPath := "../../sample_data/21mb.csv" // Using real large sample data
	outputDir := "../../test_output"
	if err := os.MkdirAll(outputDir, 0755); err != nil {
		t.Fatalf("Failed to create output directory: %v", err)
	}
	outputPath := filepath.Join(outputDir, "csv_convert.db")

	// Ensure output directory exists
	if err := os.MkdirAll(filepath.Dir(outputPath), 0755); err != nil {
		t.Fatalf("Failed to create output directory: %v", err)
	}

	file, err := os.Open(inputPath)
	if err != nil {
		t.Fatalf("Failed to open input file: %v", err)
	}
	defer file.Close()

	converter, err := NewCSVConverter(file)
	if err != nil {
		t.Fatalf("Failed to create CSV converter: %v", err)
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
	t.Logf("CSV ConvertFile output: %s", outputPath)

	// Verify the database was created and contains data
	db, err := sql.Open("sqlite3", outputPath)
	if err != nil {
		t.Fatalf("Failed to open output database: %v", err)
	}
	defer db.Close()

	var count int
	err = db.QueryRow("SELECT COUNT(*) FROM tb0").Scan(&count)
	if err != nil {
		t.Fatalf("Failed to query database: %v", err)
	}

	if count == 0 {
		t.Error("Expected data in database, but found none")
	}
}

func TestCSVConvertToSQL(t *testing.T) {
	inputPath := "../../sample_data/21mb.csv"
	outputDir := "../../test_output"
	if err := os.MkdirAll(outputDir, 0755); err != nil {
		t.Fatalf("Failed to create output directory: %v", err)
	}
	outputPath := filepath.Join(outputDir, "csv_convert.sql")

	// Ensure output directory exists
	if err := os.MkdirAll(filepath.Dir(outputPath), 0755); err != nil {
		t.Fatalf("Failed to create output directory: %v", err)
	}

	file, err := os.Open(inputPath)
	if err != nil {
		t.Fatalf("Failed to open input file: %v", err)
	}
	defer file.Close()

	converter, err := NewCSVConverter(file)
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
	t.Logf("CSV ConvertToSQL output: %s", outputPath)

	// Read back to verify
	content, err := os.ReadFile(outputPath)
	if err != nil {
		t.Fatalf("Failed to read output file: %v", err)
	}
	sqlOutput := string(content)
	if !strings.Contains(sqlOutput, "CREATE TABLE tb0") {
		t.Error("Expected CREATE TABLE statement in SQL output")
	}
	if !strings.Contains(sqlOutput, "INSERT INTO tb0") {
		t.Error("Expected INSERT statement in SQL output")
	}
}

func TestCSVParseCSV(t *testing.T) {
	inputPath := "../../sample_data/demo_mavgo_flight/Expenses.csv"
	file, err := os.Open(inputPath)
	if err != nil {
		t.Fatalf("Failed to open input file: %v", err)
	}
	defer file.Close()

	converter, err := NewCSVConverter(file)
	if err != nil {
		t.Fatalf("NewCSVConverter failed: %v", err)
	}

	headers := converter.GetHeaders(CSVTB)
	if len(headers) == 0 {
		t.Error("Expected headers, but got none")
	}

	var rows [][]interface{}
	err = converter.ScanRows(CSVTB, func(row []interface{}, err error) error {
		if err != nil {
			return err
		}
		rows = append(rows, row)
		return nil
	})
	if err != nil {
		t.Fatalf("ScanRows failed: %v", err)
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

func TestEscapingLogic(t *testing.T) {
	// Input CSV content with single quotes
	csvContent := `id,name
1,O'Connor
2,'Quote at start
3,Quote at end'
4,'Quote in 'middle'`

	reader := strings.NewReader(csvContent)
	converter, err := NewCSVConverter(reader)
	if err != nil {
		t.Fatalf("Failed to create converter: %v", err)
	}

	var output bytes.Buffer // bytes is not imported in csv_test.go yet, need to check
	err = converter.ConvertToSQL(&output)
	if err != nil {
		t.Fatalf("ConvertToSQL failed: %v", err)
	}

	sqlOutput := output.String()

	expectedStrings := []string{
		"'O''Connor'",
		"'''Quote at start'",
		"'Quote at end'''",
		"'''Quote in ''middle'''",
	}

	for _, expected := range expectedStrings {
		if !strings.Contains(sqlOutput, expected) {
			t.Errorf("Output missing expected escaped string: %s. \nGot: %s", expected, sqlOutput)
		}
	}
}
