package excel

import (
	"fmt"
	"io"
	"github.com/darianmavgo/mksqlite/converters"
	"github.com/darianmavgo/mksqlite/converters/common"
	"strings"

	"github.com/xuri/excelize/v2"
)

func init() {
	converters.Register("excel", &excelDriver{})
}

type excelDriver struct{}

func (d *excelDriver) Open(source io.Reader) (common.RowProvider, error) {
	return NewExcelConverter(source)
}

// ExcelConverter converts Excel files to SQLite tables
type ExcelConverter struct {
	tableNames []string
	headers    map[string][]string // map tableName to headers
	sheetMap   map[string]string   // map tableName to sheetName
	file       *excelize.File
}

// Ensure ExcelConverter implements RowProvider
var _ common.RowProvider = (*ExcelConverter)(nil)

// Ensure ExcelConverter implements StreamConverter
var _ common.StreamConverter = (*ExcelConverter)(nil)

// Ensure ExcelConverter implements io.Closer
var _ io.Closer = (*ExcelConverter)(nil)

// NewExcelConverter creates a new ExcelConverter from an io.Reader
func NewExcelConverter(r io.Reader) (*ExcelConverter, error) {
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

	for idx, sheetName := range sheets {
		tableName := tableNames[idx]
		sheetMap[tableName] = sheetName

		// Use iterator to get just the first row for headers
		rows, err := f.Rows(sheetName)
		if err != nil {
			f.Close()
			return nil, fmt.Errorf("failed to get rows iterator for sheet %s: %w", sheetName, err)
		}

		if rows.Next() {
			headerRow, err := rows.Columns()
			if err != nil {
				rows.Close()
				f.Close()
				return nil, fmt.Errorf("failed to read header row for sheet %s: %w", sheetName, err)
			}
			headersMap[tableName] = common.GenColumnNames(headerRow)
		}
		rows.Close()
	}

	return &ExcelConverter{
		tableNames: tableNames,
		headers:    headersMap,
		sheetMap:   sheetMap,
		file:       f,
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

	// Skip header row
	if rows.Next() {
		// Just consume the first row
		_, err := rows.Columns()
		if err != nil {
			return err
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

		// Write CREATE TABLE statement
		createTableSQL := common.GenCreateTableSQL(tableName, headers)
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
