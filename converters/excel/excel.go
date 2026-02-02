package excel

import (
	"fmt"
	"io"
	"strings"

	"github.com/darianmavgo/mksqlite/converters"
	"github.com/darianmavgo/mksqlite/converters/common"

	"github.com/xuri/excelize/v2"
)

func init() {
	converters.Register("excel", &excelDriver{})
}

type excelDriver struct{}

func (d *excelDriver) Open(source io.Reader, config *common.ConversionConfig) (common.RowProvider, error) {
	return NewExcelConverterWithConfig(source, config)
}

// ExcelConverter converts Excel files to SQLite tables
type ExcelConverter struct {
	tableNames     []string
	headers        map[string][]string // map tableName to headers
	sheetMap       map[string]string   // map tableName to sheetName
	file           *excelize.File
	headerRowIndex map[string]int // map tableName to header row index (0-based)
}

// Ensure ExcelConverter implements RowProvider
var _ common.RowProvider = (*ExcelConverter)(nil)

// Ensure ExcelConverter implements StreamConverter
var _ common.StreamConverter = (*ExcelConverter)(nil)

// Ensure ExcelConverter implements io.Closer
var _ io.Closer = (*ExcelConverter)(nil)

// NewExcelConverter creates a new ExcelConverter from an io.Reader
func NewExcelConverter(r io.Reader) (*ExcelConverter, error) {
	return NewExcelConverterWithConfig(r, nil)
}

// NewExcelConverterWithConfig creates a new ExcelConverter from an io.Reader with optional config
func NewExcelConverterWithConfig(r io.Reader, config *common.ConversionConfig) (*ExcelConverter, error) {
	// Open Excel stream
	f, err := excelize.OpenReader(r)
	if err != nil {
		return nil, fmt.Errorf("failed to open Excel stream: %w", err)
	}

	// Get all sheets
	sheets := f.GetSheetList()
	if len(sheets) == 0 {
		f.Close()
		return nil, fmt.Errorf("no sheets found in Excel file")
	}

	tableNames := common.GenTableNames(sheets)
	headersMap := make(map[string][]string)
	sheetMap := make(map[string]string)
	headerRowIndex := make(map[string]int)

	for idx, sheetName := range sheets {
		tableName := tableNames[idx]
		sheetMap[tableName] = sheetName
		headerRowIndex[tableName] = 0 // Default

		// Use iterator to get rows
		rows, err := f.Rows(sheetName)
		if err != nil {
			f.Close()
			return nil, fmt.Errorf("failed to get rows iterator for sheet %s: %w", sheetName, err)
		}

		var scannedRows [][]string
		// Scan up to 10 rows or finding header if no advanced detection
		limit := 1
		if config != nil && config.AdvancedHeaderDetection {
			limit = 10
		}

		for i := 0; i < limit && rows.Next(); i++ {
			cols, err := rows.Columns()
			if err != nil {
				rows.Close()
				f.Close()
				return nil, fmt.Errorf("failed to read row for sheet %s: %w", sheetName, err)
			}
			scannedRows = append(scannedRows, cols)
		}
		rows.Close()

		var headerRow []string
		if config != nil && config.AdvancedHeaderDetection {
			idx := common.AssessHeaderRow(scannedRows, 10)
			if idx >= 0 && idx < len(scannedRows) {
				headerRow = scannedRows[idx]
				headerRowIndex[tableName] = idx
			} else if len(scannedRows) > 0 {
				headerRow = scannedRows[0]
			}
		} else {
			if len(scannedRows) > 0 {
				headerRow = scannedRows[0]
			}
		}

		if len(headerRow) > 0 {
			headersMap[tableName] = common.GenColumnNames(headerRow)
		}
	}

	return &ExcelConverter{
		tableNames:     tableNames,
		headers:        headersMap,
		sheetMap:       sheetMap,
		file:           f,
		headerRowIndex: headerRowIndex,
	}, nil
}

// GetTableNames implements RowProvider
func (e *ExcelConverter) GetTableNames() []string {
	return e.tableNames
}

// GetHeaders implements RowProvider
func (e *ExcelConverter) GetHeaders(tableName string) []string {
	return e.headers[tableName]
}

// GetColumnTypes implements RowProvider
func (e *ExcelConverter) GetColumnTypes(tableName string) []string {
	sheetName, ok := e.sheetMap[tableName]
	if !ok {
		return nil
	}
	headers, ok := e.headers[tableName]
	if !ok {
		return nil
	}

	rows, err := e.file.Rows(sheetName)
	if err != nil {
		// Fallback to TEXT if we can't read
		return common.GenColumnTypes(headers)
	}
	defer rows.Close()

	// Skip to header row
	headerIdx := e.headerRowIndex[tableName]
	// We want rows 5-15 from data start. Data starts after header.
	// So skip headerIdx + 1 (header row itself)
	// Then skip 5 more? Or does user mean absolute 5-15?
	// "rows 5 through 15". Usually implies data rows.
	// Let's assume data rows 5-15 (0-indexed data).
	// So we skip headerIdx + 1 + 5.
	skipCount := headerIdx + 1

	// Read a batch of rows for inference
	// We'll read up to 20 rows to capture the 5-15 range mentioned, or just read the first few batches
	// common.InferColumnTypes now handles the 5-15 logic internally if we pass it enough rows.
	// So let's just pass it the first 20 rows of DATA.

	for i := 0; i < skipCount; i++ {
		if !rows.Next() {
			return common.GenColumnTypes(headers)
		}
		if _, err := rows.Columns(); err != nil {
			return common.GenColumnTypes(headers)
		}
	}

	var scannedRows [][]string
	for i := 0; i < 20 && rows.Next(); i++ {
		cols, err := rows.Columns()
		if err != nil {
			break
		}
		// Pad cols if necessary
		if len(cols) < len(headers) {
			padded := make([]string, len(headers))
			copy(padded, cols)
			cols = padded
		}
		scannedRows = append(scannedRows, cols)
	}

	return common.InferColumnTypes(scannedRows, len(headers))
}

// ScanRows implements RowProvider
func (e *ExcelConverter) ScanRows(tableName string, yield func([]interface{}, error) error) error {
	sheetName, ok := e.sheetMap[tableName]
	if !ok {
		return nil // Should not happen if GetTableNames is correct
	}

	rows, err := e.file.Rows(sheetName)
	if err != nil {
		return fmt.Errorf("failed to get rows iterator for sheet %s: %w", sheetName, err)
	}
	defer rows.Close()

	// Skip rows up to header
	skipCount := e.headerRowIndex[tableName] + 1
	for i := 0; i < skipCount; i++ {
		if rows.Next() {
			_, err := rows.Columns()
			if err != nil {
				return err
			}
		}
	}

	for rows.Next() {
		row, err := rows.Columns()
		if err != nil {
			return fmt.Errorf("failed to read row: %w", err)
		}

		// Convert to interface{}
		interfaceRow := make([]interface{}, len(row))
		for i, val := range row {
			interfaceRow[i] = val
		}

		if err := yield(interfaceRow, nil); err != nil {
			return err
		}
	}

	return nil
}

// Close closes the underlying Excel file
func (e *ExcelConverter) Close() error {
	if e.file != nil {
		return e.file.Close()
	}
	return nil
}

// ConvertToSQL implements StreamConverter for Excel files (outputs SQL to writer)
func (e *ExcelConverter) ConvertToSQL(writer io.Writer) error {
	if e.file == nil {
		return fmt.Errorf("ExcelConverter not initialized")
	}

	for _, tableName := range e.tableNames {
		headers := e.headers[tableName]
		if len(headers) == 0 {
			continue // Skip empty tables
		}

		// Get column types
		colTypes := e.GetColumnTypes(tableName)

		// Write CREATE TABLE statement
		createTableSQL := common.GenCreateTableSQLWithTypes(tableName, headers, colTypes)
		if _, err := fmt.Fprintf(writer, "%s;\n\n", createTableSQL); err != nil {
			return fmt.Errorf("failed to write CREATE TABLE: %w", err)
		}

		err := e.ScanRows(tableName, func(row []interface{}, err error) error {
			if err != nil {
				return err
			}
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
					if _, err := writer.Write([]byte(", ")); err != nil {
						return fmt.Errorf("failed to write value separator: %w", err)
					}
				}

				// Handle value types. Excelize returns strings for everything usually, but ScanRows returns interface{}.
				strVal := ""
				switch v := val.(type) {
				case string:
					strVal = v
				default:
					strVal = fmt.Sprintf("%v", v)
				}

				// Escape single quotes by doubling them
				escapedVal := strings.ReplaceAll(strVal, "'", "''")
				if _, err := fmt.Fprintf(writer, "'%s'", escapedVal); err != nil {
					return fmt.Errorf("failed to write value: %w", err)
				}
			}

			if _, err := writer.Write([]byte(");\n")); err != nil {
				return fmt.Errorf("failed to write statement end: %w", err)
			}
			return nil
		})

		if err != nil {
			return err
		}

		if _, err := writer.Write([]byte("\n")); err != nil {
			return fmt.Errorf("failed to write table separator: %w", err)
		}
	}

	return nil
}
