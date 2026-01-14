package converters

import (
	"encoding/csv"
	"fmt"
	"io"
	"os"
	"strings"
)

const (
	CSVTB = "tb0"
)

// CSVConverter converts CSV files to SQLite tables
type CSVConverter struct {
	headers   []string
	inputPath string
	csvReader *csv.Reader // Used for streaming from an io.Reader
}

// Ensure CSVConverter implements RowProvider
var _ RowProvider = (*CSVConverter)(nil)

// NewCSVConverter creates a new CSVConverter
func NewCSVConverter(inputPath string) (*CSVConverter, error) {
	file, err := os.Open(inputPath)
	if err != nil {
		return nil, fmt.Errorf("failed to open input file: %w", err)
	}
	defer file.Close()

	headers, err := parseCSVHeaders(file)
	if err != nil {
		return nil, err
	}

	return &CSVConverter{
		headers:   headers,
		inputPath: inputPath,
	}, nil
}

// NewCSVConverterFromReader creates a new CSVConverter from an io.Reader.
// This allows streaming data from a source (e.g. HTTP response) without a local file.
// Note: scanRows can only be called once in this mode.
func NewCSVConverterFromReader(r io.Reader) (*CSVConverter, error) {
	reader := csv.NewReader(r)
	headers, err := reader.Read()
	if err != nil {
		return nil, fmt.Errorf("failed to read CSV headers: %w", err)
	}

	// Filter out empty headers and sanitize
	var filteredHeaders []string
	for _, header := range headers {
		if strings.TrimSpace(header) != "" {
			filteredHeaders = append(filteredHeaders, strings.TrimSpace(header))
		}
	}

	sanitizedHeaders := GenColumnNames(filteredHeaders)

	return &CSVConverter{
		headers:   sanitizedHeaders,
		csvReader: reader,
	}, nil
}

// ConvertFile implements FileConverter for CSV files (creates SQLite database)
func (c *CSVConverter) ConvertFile(inputPath, outputPath string) error {
	file, err := os.Open(inputPath)
	if err != nil {
		return fmt.Errorf("failed to open input file: %w", err)
	}
	defer file.Close()

	headers, err := parseCSVHeaders(file)
	if err != nil {
		return err
	}

	c.headers = headers
	c.inputPath = inputPath

	return ImportToSQLite(c, outputPath)
}

// GetTableNames implements RowProvider
func (c *CSVConverter) GetTableNames() []string {
	return []string{CSVTB}
}

// GetHeaders implements RowProvider
func (c *CSVConverter) GetHeaders(tableName string) []string {
	if tableName == CSVTB {
		return c.headers
	}
	return nil
}

// ScanRows implements RowProvider using a worker pattern (pipelining) to improve streaming performance.
func (c *CSVConverter) ScanRows(tableName string, yield func([]interface{}) error) error {
	if tableName != CSVTB {
		return nil
	}

	var reader *csv.Reader
	var file *os.File
	var err error

	if c.csvReader != nil {
		// Use the existing reader (streaming mode)
		reader = c.csvReader
	} else {
		// File mode
		file, err = os.Open(c.inputPath)
		if err != nil {
			return fmt.Errorf("failed to open input file: %w", err)
		}
		defer file.Close()

		reader = csv.NewReader(file)

		// Read and discard headers
		_, err = reader.Read()
		if err != nil {
			return fmt.Errorf("failed to read CSV headers: %w", err)
		}
	}

	// Channel to pipeline reading and processing
	rowsCh := make(chan []interface{}, 100)
	errCh := make(chan error, 1)

	// Producer goroutine
	go func() {
		defer close(rowsCh)
		for {
			row, err := reader.Read()
			if err != nil {
				if err == io.EOF {
					break
				}
				errCh <- fmt.Errorf("failed to read CSV row: %w", err)
				return
			}

			// Ensure row has the same number of columns as headers
			if len(row) < len(c.headers) {
				// Pad with empty strings
				for len(row) < len(c.headers) {
					row = append(row, "")
				}
			} else if len(row) > len(c.headers) {
				// Truncate to match header count
				row = row[:len(c.headers)]
			}

			// Convert to interface{}
			interfaceRow := make([]interface{}, len(row))
			for i, val := range row {
				interfaceRow[i] = val
			}

			rowsCh <- interfaceRow
		}
	}()

	// Consumer (Main Thread)
	for row := range rowsCh {
		if err := yield(row); err != nil {
			return err
		}
	}

	// Check for producer error
	select {
	case err := <-errCh:
		return err
	default:
		return nil
	}
}

func parseCSVHeaders(reader io.Reader) ([]string, error) {
	csvReader := csv.NewReader(reader)
	headers, err := csvReader.Read()
	if err != nil {
		return nil, fmt.Errorf("failed to read CSV headers: %w", err)
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
	return sanitizedHeaders, nil
}

// ConvertToSQL implements StreamConverter for CSV files (outputs SQL to writer).
// It uses concurrency to pipeline reading and writing.
func (c *CSVConverter) ConvertToSQL(reader io.Reader, writer io.Writer) error {
	csvReader := csv.NewReader(reader)
	headers, err := csvReader.Read()
	if err != nil {
		return fmt.Errorf("failed to read CSV headers: %w", err)
	}

	// Filter out empty headers and sanitize
	var filteredHeaders []string
	for _, header := range headers {
		if strings.TrimSpace(header) != "" {
			filteredHeaders = append(filteredHeaders, strings.TrimSpace(header))
		}
	}

	sanitizedHeaders := GenColumnNames(filteredHeaders)

	// Write CREATE TABLE statement
	createTableSQL := GenCreateTableSQL(CSVTB, sanitizedHeaders)
	if _, err := fmt.Fprintf(writer, "%s;\n\n", createTableSQL); err != nil {
		return fmt.Errorf("failed to write CREATE TABLE: %w", err)
	}

	// Channel to pipeline reading and writing
	rowsCh := make(chan []string, 100)
	errCh := make(chan error, 1)

	// Producer goroutine
	go func() {
		defer close(rowsCh)
		for {
			row, err := csvReader.Read()
			if err != nil {
				if err == io.EOF {
					break
				}
				errCh <- fmt.Errorf("failed to read CSV row: %w", err)
				return
			}

			// Ensure row has the same number of columns as sanitized headers
			if len(row) < len(sanitizedHeaders) {
				for len(row) < len(sanitizedHeaders) {
					row = append(row, "")
				}
			} else if len(row) > len(sanitizedHeaders) {
				row = row[:len(sanitizedHeaders)]
			}

			rowsCh <- row
		}
	}()

	// Consumer (Main Thread)
	for row := range rowsCh {
		if _, err := fmt.Fprintf(writer, "INSERT INTO %s (", CSVTB); err != nil {
			return fmt.Errorf("failed to write INSERT start: %w", err)
		}

		// Write column names
		for i, header := range sanitizedHeaders {
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

	// Check for producer error
	select {
	case err := <-errCh:
		return err
	default:
		return nil
	}
}

// parseCSV reads CSV data from reader and returns sanitized headers and rows.
// This is a helper function primarily used for testing or small files.
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
			if err == io.EOF {
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
