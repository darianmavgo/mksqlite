package csv

import (
	"bufio"
	"encoding/csv"
	"fmt"
	"io"
	"strings"
	"sync"

	"github.com/darianmavgo/mksqlite/converters"
	"github.com/darianmavgo/mksqlite/converters/common"
)

const (
	CSVTB = "tb0"
)

func init() {
	converters.Register("csv", &csvDriver{})
}

type csvDriver struct{}

func (d *csvDriver) Open(source io.Reader, config *common.ConversionConfig) (common.RowProvider, error) {
	return NewCSVConverterWithConfig(source, config)
}

var emptyPadding = make([]string, 1024)

type rowWrapper struct {
	values []interface{}
}

// Pool for reusing rowWrapper structs to reduce allocations
var rowWrapperPool = sync.Pool{
	New: func() interface{} {
		return &rowWrapper{
			values: make([]interface{}, 0, 16),
		}
	},
}

// CSVConverter converts CSV files to SQLite tables
type CSVConverter struct {
	headers      []string
	bufferedRows [][]string
	csvReader    *csv.Reader
	Config       common.ConversionConfig
}

// Ensure CSVConverter implements RowProvider
var _ common.RowProvider = (*CSVConverter)(nil)

// Ensure CSVConverter implements StreamConverter
var _ common.StreamConverter = (*CSVConverter)(nil)

// NewCSVConverter creates a new CSVConverter from an io.Reader.
// This allows streaming data from a source (e.g. HTTP response) without a local file.
// Note: scanRows can only be called once in this mode.
func NewCSVConverter(r io.Reader) (*CSVConverter, error) {
	return NewCSVConverterWithConfig(r, nil)
}

// NewCSVConverterWithConfig creates a new CSVConverter from an io.Reader with optional config.
func NewCSVConverterWithConfig(r io.Reader, config *common.ConversionConfig) (*CSVConverter, error) {
	if config == nil {
		config = &common.ConversionConfig{
			TableName: CSVTB,
		}
	}

	if config.TableName == "" {
		config.TableName = CSVTB
	}

	br := bufio.NewReaderSize(r, 65536)

	// Detect delimiter if not set
	if config.Delimiter == 0 {
		peekBytes, _ := br.Peek(2048)
		sample := string(peekBytes)
		if idx := strings.IndexAny(sample, "\r\n"); idx != -1 {
			sample = sample[:idx]
		}
		config.Delimiter = common.DetectDelimiter(sample)
	}

	reader := csv.NewReader(br)
	reader.Comma = config.Delimiter
	reader.FieldsPerRecord = -1 // Allow variable number of fields

	var headers []string
	var bufferedRows [][]string

	if config.AdvancedHeaderDetection {
		var scanRows [][]string
		// Read up to 10 rows for assessment
		for i := 0; i < 10; i++ {
			row, err := reader.Read()
			if err != nil {
				if err == io.EOF {
					break
				}
				return nil, fmt.Errorf("failed to read CSV row for assessment: %w", err)
			}
			scanRows = append(scanRows, row)
		}

		if len(scanRows) > 0 {
			idx := common.AssessHeaderRow(scanRows, 10)
			if idx >= 0 && idx < len(scanRows) {
				headers = scanRows[idx]
				if idx+1 < len(scanRows) {
					bufferedRows = scanRows[idx+1:]
				}
			} else {
				headers = scanRows[0]
				if len(scanRows) > 1 {
					bufferedRows = scanRows[1:]
				}
			}
		}
	} else {
		// Default behavior: First row is header
		h, err := reader.Read()
		if err != nil {
			if err == io.EOF {
				return nil, fmt.Errorf("CSV file is empty")
			}
			return nil, fmt.Errorf("failed to read CSV headers: %w", err)
		}
		headers = h
	}

	// Sanitize headers
	sanitizedHeaders := common.GenColumnNames(headers)

	return &CSVConverter{
		headers:      sanitizedHeaders,
		bufferedRows: bufferedRows,
		csvReader:    reader,
		Config:       *config,
	}, nil
}

// GetTableNames implements RowProvider
func (c *CSVConverter) GetTableNames() []string {
	return []string{c.Config.TableName}
}

// GetHeaders implements RowProvider
func (c *CSVConverter) GetHeaders(tableName string) []string {
	if tableName == c.Config.TableName {
		return c.headers
	}
	return nil
}

// GetColumnTypes implements RowProvider
func (c *CSVConverter) GetColumnTypes(tableName string) []string {
	if tableName != c.Config.TableName {
		return nil
	}
	// Use buffered rows for inference
	return common.InferColumnTypes(c.bufferedRows, len(c.headers))
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
func (c *CSVConverter) ScanRows(tableName string, yield func([]interface{}, error) error) error {
	if tableName != c.Config.TableName {
		return nil
	}

	if c.csvReader == nil {
		return fmt.Errorf("CSV reader is not initialized")
	}

	reader := c.csvReader

	type rowOrError struct {
		row     []interface{}
		wrapper *rowWrapper
		err     error
	}

	// Channel to pipeline reading and processing
	rowsCh := make(chan rowOrError, 100)

	// Producer goroutine
	go func() {
		defer close(rowsCh)

		// Send buffered rows first
		for _, row := range c.bufferedRows {
			row = padRow(row, len(c.headers))

			wrapper := rowWrapperPool.Get().(*rowWrapper)
			if cap(wrapper.values) < len(row) {
				wrapper.values = make([]interface{}, len(row))
			} else {
				wrapper.values = wrapper.values[:len(row)]
			}

			for i, val := range row {
				wrapper.values[i] = val
			}
			rowsCh <- rowOrError{row: wrapper.values, wrapper: wrapper}
		}

		for {
			row, err := reader.Read()
			if err != nil {
				if err == io.EOF {
					break
				}
				// Send error to consumer
				rowsCh <- rowOrError{err: fmt.Errorf("failed to read CSV row: %w", err)}
				// Continue reading next row
				continue
			}

			// Ensure row has the same number of columns as headers
			row = padRow(row, len(c.headers))

			wrapper := rowWrapperPool.Get().(*rowWrapper)
			if cap(wrapper.values) < len(row) {
				wrapper.values = make([]interface{}, len(row))
			} else {
				wrapper.values = wrapper.values[:len(row)]
			}

			for i, val := range row {
				wrapper.values[i] = val
			}

			rowsCh <- rowOrError{row: wrapper.values, wrapper: wrapper}
		}
	}()

	// Consumer (Main Thread)
	for item := range rowsCh {
		err := yield(item.row, item.err)
		if item.wrapper != nil {
			rowWrapperPool.Put(item.wrapper)
		}
		if err != nil {
			return err
		}
	}

	return nil
}

// ConvertToSQL implements StreamConverter for CSV files (outputs SQL to writer).
// It uses concurrency to pipeline reading and writing.
func (c *CSVConverter) ConvertToSQL(writer io.Writer) error {
	if c.csvReader == nil {
		return fmt.Errorf("CSV reader is not initialized")
	}

	// Get column types
	colTypes := c.GetColumnTypes(c.Config.TableName)

	// Write CREATE TABLE statement
	createTableSQL := common.GenCreateTableSQLWithTypes(c.Config.TableName, c.headers, colTypes)
	if _, err := fmt.Fprintf(writer, "%s;\n\n", createTableSQL); err != nil {
		return fmt.Errorf("failed to write CREATE TABLE: %w", err)
	}

	// Channel to pipeline reading and writing
	rowsCh := make(chan []string, 100)
	errCh := make(chan error, 1)

	// Producer goroutine
	go func() {
		defer close(rowsCh)

		// Send buffered rows
		for _, row := range c.bufferedRows {
			row = padRow(row, len(c.headers))
			rowsCh <- row
		}

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
		if _, err := fmt.Fprintf(writer, "INSERT INTO %s (", c.Config.TableName); err != nil {
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
				if _, err := io.WriteString(writer, ", "); err != nil {
					return fmt.Errorf("failed to write value separator: %w", err)
				}
			}

			if _, err := io.WriteString(writer, "'"); err != nil {
				return fmt.Errorf("failed to write value start: %w", err)
			}

			// Escape single quotes by doubling them
			last := 0
			for j := 0; j < len(val); j++ {
				if val[j] == '\'' {
					if _, err := io.WriteString(writer, val[last:j+1]); err != nil {
						return fmt.Errorf("failed to write value chunk: %w", err)
					}
					if _, err := io.WriteString(writer, "'"); err != nil {
						return fmt.Errorf("failed to write escape quote: %w", err)
					}
					last = j + 1
				}
			}
			if _, err := io.WriteString(writer, val[last:]); err != nil {
				return fmt.Errorf("failed to write value end: %w", err)
			}

			if _, err := io.WriteString(writer, "'"); err != nil {
				return fmt.Errorf("failed to write value end quote: %w", err)
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
