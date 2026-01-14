package converters

import (
	"encoding/csv"
	"fmt"
	"io"
	"os"
	"strings"
)

// CSVConverter converts CSV files to SQLite tables
type CSVConverter struct {
	headers []string
	rows    [][]string
}

// Ensure CSVConverter implements RowProvider
var _ RowProvider = (*CSVConverter)(nil)

// NewCSVConverter creates a new CSVConverter and parses the file
func NewCSVConverter(inputPath string) (*CSVConverter, error) {
	file, err := os.Open(inputPath)
	if err != nil {
		return nil, fmt.Errorf("failed to open input file: %w", err)
	}
	defer file.Close()

	headers, rows, err := parseCSV(file)
	if err != nil {
		return nil, err
	}

	return &CSVConverter{
		headers: headers,
		rows:    rows,
	}, nil
}

// ConvertFile implements FileConverter for CSV files (creates SQLite database)
func (c *CSVConverter) ConvertFile(inputPath, outputPath string) error {
	// Re-initialize with the input path (in case c is reused or uninitialized)
	// Actually, the main.go creates a new instance. But main.go does `&converters.CSVConverter{}`.
	// So we need to parse the file inside ConvertFile.

	// Since we are changing the architecture, let's just do it here.
	file, err := os.Open(inputPath)
	if err != nil {
		return fmt.Errorf("failed to open input file: %w", err)
	}
	defer file.Close()

	headers, rows, err := parseCSV(file)
	if err != nil {
		return err
	}

	c.headers = headers
	c.rows = rows

	return ImportToSQLite(c, outputPath)
}

// GetTableNames implements RowProvider
func (c *CSVConverter) GetTableNames() []string {
	return []string{"data"}
}

// GetHeaders implements RowProvider
func (c *CSVConverter) GetHeaders(tableName string) []string {
	if tableName == "data" {
		return c.headers
	}
	return nil
}

// GetRows implements RowProvider
func (c *CSVConverter) GetRows(tableName string) [][]interface{} {
	if tableName == "data" {
		// Convert string rows to interface rows
		interfaceRows := make([][]interface{}, len(c.rows))
		for i, row := range c.rows {
			interfaceRow := make([]interface{}, len(row))
			for j, val := range row {
				interfaceRow[j] = val
			}
			interfaceRows[i] = interfaceRow
		}
		return interfaceRows
	}
	return nil
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
	sanitizedHeaders := GenColumnNames(filteredHeaders)

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
	createTableSQL := GenCreateTableSQL("data", headers)
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
