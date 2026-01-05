package converters

import (
	"database/sql"
	"fmt"
	"io"
	"os"
	"path/filepath"

	_ "github.com/mattn/go-sqlite3"
	"github.com/xuri/excelize/v2"
)

// ExcelConverter converts Excel files to SQLite tables
type ExcelConverter struct{}

// ConvertFile implements FileConverter for Excel files (creates SQLite database)
func (e *ExcelConverter) ConvertFile(inputPath, outputPath string) error {
	// Ensure output directory exists
	dir := filepath.Dir(outputPath)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return fmt.Errorf("failed to create output directory: %w", err)
	}

	// Remove existing database file if it exists
	if _, err := os.Stat(outputPath); err == nil {
		if err := os.Remove(outputPath); err != nil {
			return fmt.Errorf("failed to remove existing database: %w", err)
		}
	}

	// Open Excel file
	f, err := excelize.OpenFile(inputPath)
	if err != nil {
		return fmt.Errorf("failed to open Excel file: %w", err)
	}
	defer f.Close()

	// Get first sheet
	sheets := f.GetSheetList()
	if len(sheets) == 0 {
		return fmt.Errorf("no sheets found in Excel file")
	}
	sheetName := sheets[0]

	// Get all rows from the first sheet
	rows, err := f.GetRows(sheetName)
	if err != nil {
		return fmt.Errorf("failed to read Excel rows: %w", err)
	}

	if len(rows) == 0 {
		return fmt.Errorf("no data found in Excel sheet")
	}

	// First row is headers
	headers := rows[0]
	dataRows := rows[1:]

	// Sanitize headers for SQL column names
	sanitizedHeaders := make([]string, len(headers))
	for i, header := range headers {
		sanitizedHeaders[i] = sanitizeColumnName(header)
	}

	// Connect to SQLite database
	db, err := sql.Open("sqlite3", outputPath)
	if err != nil {
		return fmt.Errorf("failed to open database: %w", err)
	}
	defer db.Close()

	// Create table
	createTableSQL := buildCreateTableSQL(sanitizedHeaders)
	_, err = db.Exec(createTableSQL)
	if err != nil {
		return fmt.Errorf("failed to create table: %w", err)
	}

	// Prepare insert statement
	insertSQL := buildInsertSQL(sanitizedHeaders)
	stmt, err := db.Prepare(insertSQL)
	if err != nil {
		return fmt.Errorf("failed to prepare insert statement: %w", err)
	}
	defer stmt.Close()

	// Insert data rows
	for _, row := range dataRows {
		// Ensure row has the same number of columns as headers
		if len(row) < len(sanitizedHeaders) {
			// Pad with empty strings
			for len(row) < len(sanitizedHeaders) {
				row = append(row, "")
			}
		} else if len(row) > len(sanitizedHeaders) {
			// Truncate to match header count
			row = row[:len(sanitizedHeaders)]
		}

		// Convert row to interface{} slice for insertion
		values := make([]interface{}, len(row))
		for i, val := range row {
			values[i] = val
		}

		_, err = stmt.Exec(values...)
		if err != nil {
			return fmt.Errorf("failed to insert row: %w", err)
		}
	}

	return nil
}

// ConvertToSQL implements StreamConverter for Excel files (outputs SQL to writer)
// Note: This currently requires reading from a file path since Excel format parsing needs random access
func (e *ExcelConverter) ConvertToSQL(reader io.Reader, writer io.Writer) error {
	// For now, Excel stream conversion is not implemented
	// Excel files require random access reading which io.Reader doesn't provide
	// To implement this, we'd need io.ReaderAt or to buffer the entire content
	return fmt.Errorf("Excel stream conversion not yet implemented - use file-based conversion")
}
