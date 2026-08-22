package pdf

import (
	"bytes"
	"context"
	"database/sql"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/darianmavgo/mksqlite/converters"
	"github.com/darianmavgo/mksqlite/converters/common"

	_ "modernc.org/sqlite"
)

const samplePDFPath = "/Users/darianhickman/Documents/Income/IB Garwood Omnifunds Transaction History.pdf"

func TestPDFConverterWithSample(t *testing.T) {
	if _, err := os.Stat(samplePDFPath); os.IsNotExist(err) {
		t.Skip("Sample PDF not found, skipping test")
	}

	file, err := os.Open(samplePDFPath)
	if err != nil {
		t.Fatalf("Failed to open sample PDF: %v", err)
	}
	defer file.Close()

	converter, err := NewPDFConverterWithConfig(file, &common.ConversionConfig{
		TableName: "transactions",
	})
	if err != nil {
		t.Fatalf("Failed to create PDF converter: %v", err)
	}
	defer converter.Close()

	tableNames := converter.GetTableNames()
	if len(tableNames) == 0 {
		t.Fatal("Expected at least one table name")
	}
	t.Logf("Table names: %v", tableNames)

	headers := converter.GetHeaders(tableNames[0])
	if len(headers) == 0 {
		t.Fatal("Expected headers for table")
	}
	t.Logf("Headers: %v", headers)

	tmpDir := t.TempDir()
	outDBPath := filepath.Join(tmpDir, "output.db")
	outFile, err := os.Create(outDBPath)
	if err != nil {
		t.Fatalf("Failed to create temp db file: %v", err)
	}
	defer outFile.Close()

	if err := converters.ImportToSQLite(converter, outFile, nil); err != nil {
		t.Fatalf("ImportToSQLite failed: %v", err)
	}

	// Verify database content
	db, err := sql.Open("sqlite", outDBPath)
	if err != nil {
		t.Fatalf("Failed to open output database: %v", err)
	}
	defer db.Close()

	var rowCount int
	err = db.QueryRow("SELECT COUNT(*) FROM " + tableNames[0]).Scan(&rowCount)
	if err != nil {
		t.Fatalf("Failed to query row count: %v", err)
	}
	t.Logf("Imported %d rows into %s", rowCount, tableNames[0])
	if rowCount < 100 {
		t.Errorf("Expected at least 100 rows, got %d", rowCount)
	}
}

func TestPDFConvertToSQL(t *testing.T) {
	if _, err := os.Stat(samplePDFPath); os.IsNotExist(err) {
		t.Skip("Sample PDF not found, skipping test")
	}

	file, err := os.Open(samplePDFPath)
	if err != nil {
		t.Fatalf("Failed to open sample PDF: %v", err)
	}
	defer file.Close()

	converter, err := NewPDFConverter(file)
	if err != nil {
		t.Fatalf("Failed to create PDF converter: %v", err)
	}
	defer converter.Close()

	var sqlBuf bytes.Buffer
	if err := converter.ConvertToSQL(context.Background(), &sqlBuf); err != nil {
		t.Fatalf("ConvertToSQL failed: %v", err)
	}

	sqlStr := sqlBuf.String()
	if !strings.Contains(sqlStr, "CREATE TABLE") {
		t.Error("Expected CREATE TABLE in SQL output")
	}
	if !strings.Contains(sqlStr, "INSERT INTO") {
		t.Error("Expected INSERT INTO in SQL output")
	}
}
