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

type tableData struct {
	headers      []string
	bufferedRows [][]string
}

// CSVConverter converts CSV files to SQLite tables
type CSVConverter struct {
	headers      []string
	bufferedRows [][]string

	// MultiSchema support
	tables       map[string]*tableData
	tableNames   []string

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
	var bufferedRows [][]string
	tables := make(map[string]*tableData)
	var tableNames []string

	if config.MultiSchema {
		// Read entire CSV into memory to group by table
		allRows, err := reader.ReadAll()
		if err != nil && err != io.EOF {
			return nil, fmt.Errorf("failed to read CSV for multischema: %w", err)
		}

		for _, row := range allRows {
			if len(row) < 3 {
				continue // Skip rows that don't have Table,Type,Data...
			}
			rawTableName := strings.TrimSpace(row[0])
			rowType := strings.TrimSpace(row[1])

			// Sanitize table name
			compliantNames := common.GenTableNames([]string{rawTableName})
			if len(compliantNames) == 0 {
				continue
			}
			tableName := compliantNames[0]

			tData, exists := tables[tableName]
			if !exists {
				tData = &tableData{
					headers:      nil,
					bufferedRows: make([][]string, 0),
				}
				tables[tableName] = tData
				tableNames = append(tableNames, tableName)
			}

			if strings.EqualFold(rowType, "Header") {
				// The rest of the row is headers
				tData.headers = common.GenColumnNames(row[2:])
			} else if strings.EqualFold(rowType, "Data") {
				// The rest of the row is data
				tData.bufferedRows = append(tData.bufferedRows, row[2:])
			}
		}

		// If no tables found, fallback or empty
		// For tables with no explicit headers, fallback to generating them
		for _, name := range tableNames {
			tData := tables[name]
			if len(tData.headers) == 0 {
				if len(tData.bufferedRows) > 0 {
					// Use first data row as headers if advanced is off, or just generate
					// Here we just make cl0, cl1 etc
					colCount := len(tData.bufferedRows[0])
					dummy := make([]string, colCount)
					for i := range dummy {
						dummy[i] = fmt.Sprintf("cl%d", i)
					}
					tData.headers = dummy
				}
			}
		}

		// Unset reader so ScanRows doesn't try to read more
		reader = nil
	} else if config.AdvancedHeaderDetection {
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

	var timeout time.Duration
	if config.ScanTimeout != "" {
		if d, err := time.ParseDuration(config.ScanTimeout); err == nil {
			timeout = d
		}
	}

	return &CSVConverter{
		headers:      sanitizedHeaders,
		bufferedRows: bufferedRows,
		tables:       tables,
		tableNames:   tableNames,
		csvReader:    reader,
		Config:       *config,
		timeout:      timeout,
	}, nil
}

// GetTableNames implements RowProvider
func (c *CSVConverter) GetTableNames() []string {
	if c.Config.MultiSchema {
		return c.tableNames
	}
	return []string{c.Config.TableName}
}

// GetHeaders implements RowProvider
func (c *CSVConverter) GetHeaders(tableName string) []string {
	if c.Config.MultiSchema {
		if tData, ok := c.tables[tableName]; ok {
			return tData.headers
		}
		return nil
	}

	if tableName == c.Config.TableName {
		return c.headers
	}
	return nil
}

// GetColumnTypes implements RowProvider
func (c *CSVConverter) GetColumnTypes(tableName string) []string {
	if c.Config.MultiSchema {
		if tData, ok := c.tables[tableName]; ok {
			return common.InferColumnTypes(tData.bufferedRows, len(tData.headers))
		}
		return nil
	}

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
func (c *CSVConverter) ScanRows(ctx context.Context, tableName string, yield func([]interface{}, error) error) error {
	var headers []string
	var bufferedRows [][]string
	var reader *csv.Reader

	if c.Config.MultiSchema {
		tData, ok := c.tables[tableName]
		if !ok {
			return nil
		}
		headers = tData.headers
		bufferedRows = tData.bufferedRows
		reader = nil // no more reading, everything is buffered
	} else {
		if tableName != c.Config.TableName {
			return nil
		}
		if c.csvReader == nil {
			return fmt.Errorf("CSV reader is not initialized")
		}
		headers = c.headers
		bufferedRows = c.bufferedRows
		reader = c.csvReader
	}

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

		// Send buffered rows first
		for _, row := range bufferedRows {
			row = padRow(row, len(headers))

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

		if reader != nil {
			for {
				row, err := reader.Read()
				if err != nil {
					if err == io.EOF {
						break
					}
					// Send error to consumer
					send(rowOrError{err: fmt.Errorf("failed to read CSV row: %w", err)})
					// Continue reading next row ?? Usually error is fatal or we stop.
					// For CSV, Read() error usually stops.
					return
				}

				// Ensure row has the same number of columns as headers
				row = padRow(row, len(headers))

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
// It uses concurrency to pipeline reading and writing.
func (c *CSVConverter) ConvertToSQL(ctx context.Context, writer io.Writer) error {
	if c.csvReader == nil && !c.Config.MultiSchema {
		return fmt.Errorf("CSV reader is not initialized")
	}

	bw := bufio.NewWriter(writer)
	defer bw.Flush()
	writer = bw

	tableNames := c.GetTableNames()

	for _, tableName := range tableNames {
		colTypes := c.GetColumnTypes(tableName)
		headers := c.GetHeaders(tableName)

		if len(headers) == 0 {
			continue // Skip tables without headers
		}

		// Write CREATE TABLE statement
		createTableSQL := common.GenCreateTableSQLWithTypes(tableName, headers, colTypes)
		if _, err := fmt.Fprintf(writer, "%s;\n\n", createTableSQL); err != nil {
			return fmt.Errorf("failed to write CREATE TABLE: %w", err)
		}

		// If multischema, all rows are buffered.
		// If streaming (not multischema), we use channels.
		// Since ConvertToSQL can only stream if it's the single table mode, we branch here.
		if c.Config.MultiSchema {
			tData, ok := c.tables[tableName]
			if !ok {
				continue
			}

			for _, row := range tData.bufferedRows {
				if err := ctx.Err(); err != nil {
					return err
				}

				row = padRow(row, len(headers))
				if err := c.writeRowSQL(writer, tableName, headers, row); err != nil {
					return err
				}
			}
		} else {
			// Single table streaming mode
			// Channel to pipeline reading and writing
			rowsCh := make(chan []string, 100)
			errCh := make(chan error, 1)

			// Producer goroutine
			go func() {
				defer close(rowsCh)

				// Send buffered rows
				for _, row := range c.bufferedRows {
					row = padRow(row, len(headers))
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
					row = padRow(row, len(headers))

					rowsCh <- row
				}
			}()

			// Consumer (Main Thread)
			for {
				select {
				case <-ctx.Done():
					return ctx.Err()
				case row, ok := <-rowsCh:
					if !ok {
						goto StreamDone
					}

					if err := c.writeRowSQL(writer, tableName, headers, row); err != nil {
						return err
					}
				}
			}
		StreamDone:
			// Check for producer error
			select {
			case err := <-errCh:
				return err
			default:
				// continue
			}
		}
	}

	return bw.Flush()
}

func (c *CSVConverter) writeRowSQL(writer io.Writer, tableName string, headers []string, row []string) error {
	if _, err := fmt.Fprintf(writer, "INSERT INTO %s (", tableName); err != nil {
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
	return nil
}
