package csv

import (
	"bufio"
	"context"
	"encoding/csv"
	"fmt"
	"io"
	"strings"
	"sync"
	"time"

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
	timeout      time.Duration
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

	// We simply read the first row to act as headers.
	h, err := reader.Read()
	if err != nil {
		if err == io.EOF {
			return nil, fmt.Errorf("CSV file is empty")
		}
		return nil, fmt.Errorf("failed to read CSV headers: %w", err)
	}
	headers = h

	// Sanitize headers
	sanitizedHeaders := common.GenColumnNames(headers)

	var timeout time.Duration
	if config.ScanTimeout != "" {
		if d, err := time.ParseDuration(config.ScanTimeout); err == nil {
			timeout = d
		}
	}

	return &CSVConverter{
		headers:      sanitizedHeaders,
		csvReader:    reader,
		Config:       *config,
		timeout:      timeout,
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
	// For RowProvider compatibility we default to strings.
	return common.GenColumnTypes(c.headers)
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
func (c *CSVConverter) ScanRows(ctx context.Context, tableName string, yield func([]interface{}, error) error) error {
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
	// Channel to signal cancellation to producer
	cancelCh := make(chan struct{})

	// Producer goroutine
	go func() {
		defer close(rowsCh)

		// Helper to safely send
		send := func(item rowOrError) bool {
			select {
			case rowsCh <- item:
				return true
			case <-cancelCh:
				return false
			}
		}

		for {
			row, err := reader.Read()
			if err != nil {
				if err == io.EOF {
					break
				}
				// Send error to consumer
				send(rowOrError{err: fmt.Errorf("failed to read CSV row: %w", err)})
				return
			}

			// If scanning via RowProvider (e.g. for fallback or tests), pad row to match the first row's headers
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

			if !send(rowOrError{row: wrapper.values, wrapper: wrapper}) {
				return
			}
		}
	}()

	// Consumer (Main Thread)
	// Signal cancellation when we exit
	defer close(cancelCh)

	wd := common.NewWatchdog(c.timeout)
	wdDone := wd.Start()
	defer wd.Stop()

	for {
		select {
		case item, ok := <-rowsCh:
			if !ok {
				return nil
			}

			wd.Kick()

			err := yield(item.row, item.err)
			if item.wrapper != nil {
				rowWrapperPool.Put(item.wrapper)
			}
			if err != nil {
				return err
			}
		case <-wdDone:
			return converters.ErrScanTimeout
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

// ConvertToSQL implements StreamConverter for CSV files (outputs SQL to writer).
// It processes the stream sequentially, discovering tables dynamically when row lengths change,
// without needing any lookaheads. Caps at 10 tables to prevent runaway table creation on bad data.
func (c *CSVConverter) ConvertToSQL(ctx context.Context, writer io.Writer) error {
	if c.csvReader == nil {
		return fmt.Errorf("CSV reader is not initialized")
	}

	bw := bufio.NewWriter(writer)
	defer bw.Flush()

	type tableState struct {
		TableName string
		Headers   []string
		ColLen    int
	}

	var currentTable *tableState
	tableCount := 0

	// Initialize the first table with the headers already read in Open
	currentTable = &tableState{
		TableName: c.Config.TableName,
		Headers:   c.headers,
		ColLen:    len(c.headers), // Note: len(c.headers) might be sanitized, but it should match original length
	}

	// Create the first table
	colTypes := common.GenColumnTypes(currentTable.Headers)
	createTableSQL := common.GenCreateTableSQLWithTypes(currentTable.TableName, currentTable.Headers, colTypes)
	if _, err := fmt.Fprintf(bw, "%s;\n\nBEGIN TRANSACTION;\n", createTableSQL); err != nil {
		return fmt.Errorf("failed to write CREATE TABLE: %w", err)
	}
	tableCount++

	type rowOrError struct {
		row []string
		err error
	}

	rowsCh := make(chan rowOrError, 100)

	go func() {
		defer close(rowsCh)

		for {
			row, err := c.csvReader.Read()
			if err != nil {
				if err == io.EOF {
					break
				}
				rowsCh <- rowOrError{err: err}
				return
			}
			rowsCh <- rowOrError{row: row}
		}
	}()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case item, ok := <-rowsCh:
			if !ok {
				if currentTable != nil {
					if _, err := io.WriteString(bw, "COMMIT;\n\n"); err != nil {
						return err
					}
				}
				return bw.Flush()
			}
			if item.err != nil {
				return fmt.Errorf("failed to read CSV row: %w", item.err)
			}

			row := item.row

			// Ignore empty rows completely
			isEmpty := true
			for _, col := range row {
				if strings.TrimSpace(col) != "" {
					isEmpty = false
					break
				}
			}
			if isEmpty {
				continue
			}

			rowLen := len(row)
			if rowLen != currentTable.ColLen {
				if tableCount < 10 {
					// We encountered a new length and haven't hit the cap, so we start a new table.
					// This row becomes the headers for the new table.
					tableName := fmt.Sprintf("%s_%d", c.Config.TableName, tableCount)
					tableCount++

					sanitizedHeaders := common.GenColumnNames(row)
					newColTypes := common.GenColumnTypes(sanitizedHeaders)

					newCreateTableSQL := common.GenCreateTableSQLWithTypes(tableName, sanitizedHeaders, newColTypes)

					// Write transaction commit for previous table
					if _, err := io.WriteString(bw, "COMMIT;\n\n"); err != nil {
						return err
					}

					if _, err := fmt.Fprintf(bw, "%s;\n\nBEGIN TRANSACTION;\n", newCreateTableSQL); err != nil {
						return fmt.Errorf("failed to write CREATE TABLE: %w", err)
					}

					currentTable = &tableState{
						TableName: tableName,
						Headers:   sanitizedHeaders,
						ColLen:    rowLen,
					}
					continue // The header row is not inserted as data
				} else {
					// Cap reached. Pad or truncate to fit the current table (the 10th table)
					row = padRow(row, currentTable.ColLen)
				}
			}

			// Generate and insert the data row
			if _, err := fmt.Fprintf(bw, "INSERT INTO %s (", currentTable.TableName); err != nil {
				return fmt.Errorf("failed to write INSERT start: %w", err)
			}

			// Write column names
			for i, header := range currentTable.Headers {
				if i > 0 {
					if _, err := bw.Write([]byte(", ")); err != nil {
						return fmt.Errorf("failed to write column separator: %w", err)
					}
				}
				if _, err := bw.WriteString(header); err != nil {
					return fmt.Errorf("failed to write column name: %w", err)
				}
			}

			if _, err := bw.WriteString(") VALUES ("); err != nil {
				return fmt.Errorf("failed to write VALUES start: %w", err)
			}

			// Write values
			for i, val := range row {
				if i > 0 {
					if _, err := io.WriteString(bw, ", "); err != nil {
						return fmt.Errorf("failed to write value separator: %w", err)
					}
				}

				if _, err := io.WriteString(bw, "'"); err != nil {
					return fmt.Errorf("failed to write value start: %w", err)
				}

				// Escape single quotes by doubling them
				last := 0
				for j := 0; j < len(val); j++ {
					if val[j] == '\'' {
						if _, err := io.WriteString(bw, val[last:j+1]); err != nil {
							return fmt.Errorf("failed to write value chunk: %w", err)
						}
						if _, err := io.WriteString(bw, "'"); err != nil {
							return fmt.Errorf("failed to write escape quote: %w", err)
						}
						last = j + 1
					}
				}
				if _, err := io.WriteString(bw, val[last:]); err != nil {
					return fmt.Errorf("failed to write value end: %w", err)
				}

				if _, err := io.WriteString(bw, "'"); err != nil {
					return fmt.Errorf("failed to write value end quote: %w", err)
				}
			}

			if _, err := bw.Write([]byte(");\n")); err != nil {
				return fmt.Errorf("failed to write statement end: %w", err)
			}
		}
	}
}
