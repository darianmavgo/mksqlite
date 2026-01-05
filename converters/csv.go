package converters

import (
	"database/sql"
	"encoding/csv"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	_ "github.com/mattn/go-sqlite3"
)

// CSVConverter converts CSV files to SQLite tables
type CSVConverter struct{}

// ConvertFile implements FileConverter for CSV files (creates SQLite database)
func (c *CSVConverter) ConvertFile(inputPath, outputPath string) error {
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

	// Open the CSV file
	file, err := os.Open(inputPath)
	if err != nil {
		return fmt.Errorf("failed to open input file: %w", err)
	}
	defer file.Close()

	return c.convertCSVToSQLite(file, outputPath)
}

// convertCSVToSQLite converts CSV data from reader to SQLite database
func (c *CSVConverter) convertCSVToSQLite(reader io.Reader, dbPath string) error {
	headers, rows, err := parseCSV(reader)
	if err != nil {
		return err
	}

	// Connect to SQLite database
	db, err := sql.Open("sqlite3", dbPath)
	if err != nil {
		return fmt.Errorf("failed to open database: %w", err)
	}
	defer db.Close()

	// Create table
	createTableSQL := buildCreateTableSQL(headers)
	_, err = db.Exec(createTableSQL)
	if err != nil {
		return fmt.Errorf("failed to create table: %w", err)
	}

	// Prepare insert statement
	insertSQL := buildInsertSQL(headers)
	stmt, err := db.Prepare(insertSQL)
	if err != nil {
		return fmt.Errorf("failed to prepare insert statement: %w", err)
	}
	defer stmt.Close()

	// Insert rows
	for _, row := range rows {
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

// sanitizeColumnName converts a header to a valid SQL column name
func sanitizeColumnName(header string) string {
	// Replace spaces and special characters with underscores
	result := strings.ReplaceAll(header, " ", "_")
	result = strings.ReplaceAll(result, "-", "_")
	result = strings.ReplaceAll(result, ".", "_")
	// Remove other special characters
	for _, char := range []string{"(", ")", "[", "]", "{", "}", "!", "@", "#", "$", "%", "^", "&", "*", "+", "=", "|", "\\", "/", "?", "<", ">", ",", ":", ";", "'", "\""} {
		result = strings.ReplaceAll(result, char, "")
	}

	// Ensure column name doesn't start with a digit (invalid in SQL)
	if len(result) > 0 && result[0] >= '0' && result[0] <= '9' {
		result = "col_" + result
	}

	// Ensure it's not empty
	if result == "" {
		result = "column"
	}

	return result
}

// buildCreateTableSQL builds the CREATE TABLE statement
func buildCreateTableSQL(columns []string) string {
	sql := "CREATE TABLE data ("
	for i, col := range columns {
		sql += fmt.Sprintf("%s TEXT", col)
		if i < len(columns)-1 {
			sql += ", "
		}
	}
	sql += ")"
	return sql
}

// parseCSV reads CSV data from reader and returns sanitized headers and rows
func parseCSV(reader io.Reader) ([]string, [][]string, error) {
	csvReader := csv.NewReader(reader)
	headers, err := csvReader.Read()
	if err != nil {
		return nil, nil, fmt.Errorf("failed to read CSV headers: %w", err)
	}

	// Filter out empty headers and sanitize
	var filteredHeaders []string
	for _, header := range headers {
		if strings.TrimSpace(header) != "" {
			filteredHeaders = append(filteredHeaders, strings.TrimSpace(header))
		}
	}

	// Sanitize headers for SQL column names
	sanitizedHeaders := make([]string, len(filteredHeaders))
	for i, header := range filteredHeaders {
		sanitizedHeaders[i] = sanitizeColumnName(header)
	}

	// Read all rows
	var rows [][]string
	for {
		row, err := csvReader.Read()
		if err != nil {
			if err.Error() == "EOF" {
				break
			}
			return nil, nil, fmt.Errorf("failed to read CSV row: %w", err)
		}

		// Ensure row has the same number of columns as filtered headers
		if len(row) < len(sanitizedHeaders) {
			// Pad with empty strings
			for len(row) < len(sanitizedHeaders) {
				row = append(row, "")
			}
		} else if len(row) > len(sanitizedHeaders) {
			// Truncate to match header count
			row = row[:len(sanitizedHeaders)]
		}

		rows = append(rows, row)
	}

	return sanitizedHeaders, rows, nil
}

// writeSQL writes SQL DDL and DML statements to writer
func writeSQL(headers []string, rows [][]string, writer io.Writer) error {
	// Write CREATE TABLE statement
	createTableSQL := buildCreateTableSQL(headers)
	if _, err := fmt.Fprintf(writer, "%s;\n\n", createTableSQL); err != nil {
		return fmt.Errorf("failed to write CREATE TABLE: %w", err)
	}

	// Write INSERT statements
	for _, row := range rows {
		if _, err := fmt.Fprintf(writer, "INSERT INTO data ("); err != nil {
			return fmt.Errorf("failed to write INSERT start: %w", err)
		}

		// Write column names
		for i, header := range headers {
			if i > 0 {
				if _, err := writer.Write([]byte(", ")); err != nil {
					return fmt.Errorf("failed to write column separator: %w", err)
				}
			}
			if _, err := fmt.Fprintf(writer, "%s", header); err != nil {
				return fmt.Errorf("failed to write column name: %w", err)
			}
		}

		if _, err := fmt.Fprintf(writer, ") VALUES ("); err != nil {
			return fmt.Errorf("failed to write VALUES start: %w", err)
		}

		// Write values
		for i, val := range row {
			if i > 0 {
				if _, err := writer.Write([]byte(", ")); err != nil {
					return fmt.Errorf("failed to write value separator: %w", err)
				}
			}
			// Escape single quotes by doubling them
			escapedVal := strings.ReplaceAll(val, "'", "''")
			if _, err := fmt.Fprintf(writer, "'%s'", escapedVal); err != nil {
				return fmt.Errorf("failed to write value: %w", err)
			}
		}

		if _, err := writer.Write([]byte(");\n")); err != nil {
			return fmt.Errorf("failed to write statement end: %w", err)
		}
	}

	return nil
}

// ConvertToSQL implements StreamConverter for CSV files (outputs SQL to writer)
func (c *CSVConverter) ConvertToSQL(reader io.Reader, writer io.Writer) error {
	headers, rows, err := parseCSV(reader)
	if err != nil {
		return err
	}

	return writeSQL(headers, rows, writer)
}

// buildInsertSQL builds the INSERT statement
func buildInsertSQL(columns []string) string {
	sql := "INSERT INTO data ("
	sql += strings.Join(columns, ", ")
	sql += ") VALUES ("
	placeholders := make([]string, len(columns))
	for i := range placeholders {
		placeholders[i] = "?"
	}
	sql += strings.Join(placeholders, ", ")
	sql += ")"
	return sql
}
