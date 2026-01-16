package converters

import (
	"encoding/csv"
	"fmt"
	"io"
	"strings"
)

const (
	CSVTB = "tb0"
)

// CalcColumnCount calculates the maximum number of columns based on sampled lines.
// It attempts to detect the delimiter if not provided by checking consistency across lines.
// This where I should eventually document detected/assumed options as some kind of config object.
func ColumnCount(rawlines []string, delimiter string) int {
	if len(rawlines) == 0 {
		return 0
	}

	// Detect delimiter if not provided
	if delimiter == "" {
		commonDelimiters := []string{",", "\t", ";", "|"}
		bestDelim := ""
		bestScore := -1.0

		for _, candidate := range commonDelimiters {
			counts := make([]int, len(rawlines))
			nonZero := 0
			total := 0

			for i, line := range rawlines {
				c := strings.Count(line, candidate)
				counts[i] = c
				total += c
				if c > 0 {
					nonZero++
				}
			}

			// If delimiter never appears, skip it
			if nonZero == 0 {
				continue
			}

			// Calculate consistency
			isConsistent := true
			first := counts[0]
			for _, c := range counts {
				if c != first {
					isConsistent = false
					break
				}
			}

			avg := float64(total) / float64(len(rawlines))
			currentScore := avg

			// Boost consistent delimiters significantly
			// We prioritize consistency over raw count to avoid false positives from text fields
			if isConsistent {
				currentScore += 1000.0
			}

			if currentScore > bestScore {
				bestScore = currentScore
				bestDelim = candidate
			}
		}
		delimiter = bestDelim
	}

	// If no delimiter found or lines empty of delimiters, return 1 column
	if delimiter == "" {
		return 1
	}

	// Calculate max columns using the chosen delimiter
	maxCols := 0
	for _, line := range rawlines {
		// Column count is separator count + 1
		cols := strings.Count(line, delimiter) + 1
		if cols > maxCols {
			maxCols = cols
		}
	}
	return maxCols
}

// CSVConverter converts CSV files to SQLite tables
type CSVConverter struct {
	headers   []string
	csvReader *csv.Reader // Used for streaming from an io.Reader
}

// Ensure CSVConverter implements RowProvider
var _ RowProvider = (*CSVConverter)(nil)

// NewCSVConverter creates a new CSVConverter from an io.Reader.
// This allows streaming data from a source (e.g. HTTP response) without a local file.
// Note: scanRows can only be called once in this mode.
func NewCSVConverter(r io.Reader) (*CSVConverter, error) {
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

	if c.csvReader == nil {
		return fmt.Errorf("CSV reader is not initialized")
	}

	reader := c.csvReader

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
