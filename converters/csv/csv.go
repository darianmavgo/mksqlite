package csv

import (
	"bufio"
	"encoding/csv"
	"fmt"
	"io"
	"github.com/darianmavgo/mksqlite/converters"
	"github.com/darianmavgo/mksqlite/converters/common"
	"strings"
)

const (
	CSVTB = "tb0"
)

func init() {
	converters.Register("csv", &csvDriver{})
}

type csvDriver struct{}

func (d *csvDriver) Open(source io.Reader) (common.RowProvider, error) {
	return NewCSVConverter(source)
}

var emptyPadding = make([]string, 1024)

// CSVConverter converts CSV files to SQLite tables
type CSVConverter struct {
	headers   []string
	csvReader *csv.Reader // Used for streaming from an io.Reader
	Config    common.ConversionConfig
}

// Ensure CSVConverter implements RowProvider
var _ common.RowProvider = (*CSVConverter)(nil)

// Ensure CSVConverter implements StreamConverter
var _ common.StreamConverter = (*CSVConverter)(nil)

// NewCSVConverter creates a new CSVConverter from an io.Reader.
// This allows streaming data from a source (e.g. HTTP response) without a local file.
// Note: scanRows can only be called once in this mode.
func NewCSVConverter(r io.Reader) (*CSVConverter, error) {
	br := bufio.NewReader(r)

	// Peek at first 2KB to detect delimiter
	peekBytes, _ := br.Peek(2048)
	sample := string(peekBytes)
	if idx := strings.IndexAny(sample, "\r\n"); idx != -1 {
		sample = sample[:idx]
	}

	delim := common.DetectDelimiter(sample)

	reader := csv.NewReader(br)
	reader.Comma = delim
	reader.FieldsPerRecord = -1 // Allow variable number of fields
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

	sanitizedHeaders := common.GenColumnNames(filteredHeaders)

	return &CSVConverter{
		headers:   sanitizedHeaders,
		csvReader: reader,
		Config: common.ConversionConfig{
			Delimiter: delim,
			TableName: CSVTB,
		},
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

// padRow pads or truncates the row to match the target length.
func padRow(row []string, targetLen int) []string {
	if len(row) < targetLen {
		needed := targetLen - len(row)
		if needed <= len(emptyPadding) {
			row = append(row, emptyPadding[:needed]...)
		} else {
			row = append(row, make([]string, needed)...)
		}
	} else if len(row) > targetLen {
		row = row[:targetLen]
	}
	return row
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
			row = padRow(row, len(c.headers))

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
func (c *CSVConverter) ConvertToSQL(writer io.Writer) error {
	if c.csvReader == nil {
		return fmt.Errorf("CSV reader is not initialized")
	}

	// Write CREATE TABLE statement
	createTableSQL := common.GenCreateTableSQL(CSVTB, c.headers)
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
			row, err := c.csvReader.Read()
			if err != nil {
				if err == io.EOF {
					break
				}
				errCh <- fmt.Errorf("failed to read CSV row: %w", err)
				return
			}

			// Ensure row has the same number of columns as sanitized headers
			row = padRow(row, len(c.headers))

			rowsCh <- row
		}
	}()

	// Consumer (Main Thread)
	for row := range rowsCh {
		if _, err := fmt.Fprintf(writer, "INSERT INTO %s (", CSVTB); err != nil {
			return fmt.Errorf("failed to write INSERT start: %w", err)
		}

		// Write column names
		for i, header := range c.headers {
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
	br := bufio.NewReader(reader)

	// Peek at first 2KB to detect delimiter
	peekBytes, _ := br.Peek(2048)
	sample := string(peekBytes)
	if idx := strings.IndexAny(sample, "\r\n"); idx != -1 {
		sample = sample[:idx]
	}

	delim := common.DetectDelimiter(sample)

	csvReader := csv.NewReader(br)
	csvReader.Comma = delim
	csvReader.FieldsPerRecord = -1 // Allow variable number of fields
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
	sanitizedHeaders := common.GenColumnNames(filteredHeaders)

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
		row = padRow(row, len(sanitizedHeaders))

		rows = append(rows, row)
	}

	return sanitizedHeaders, rows, nil
}
