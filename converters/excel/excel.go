package excel

import (
	"fmt"
	"io"
	"mksqlite/converters"
	"strings"

	"github.com/xuri/excelize/v2"
)

func init() {
	converters.Register("excel", &Driver{})
}

type Driver struct{}

func (d *Driver) Open(r io.Reader) (converters.RowProvider, error) {
	return NewExcelConverter(r)
}

func (d *Driver) ConvertToSQL(r io.Reader, w io.Writer) error {
	c := &ExcelConverter{}
	return c.ConvertToSQL(r, w)
}

// ExcelConverter converts Excel files to SQLite tables
type ExcelConverter struct {
	tableNames []string
	headers    map[string][]string // map tableName to headers
	sheetMap   map[string]string   // map tableName to sheetName
	file       *excelize.File
}

// Ensure ExcelConverter implements RowProvider
var _ converters.RowProvider = (*ExcelConverter)(nil)

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

	tableNames := converters.GenTableNames(sheets)
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
			headersMap[tableName] = converters.GenColumnNames(headerRow)
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
func (e *ExcelConverter) ScanRows(tableName string, yield func([]interface{}) error) error {
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

		if err := yield(interfaceRow); err != nil {
			return err
		}
	}

	return nil
}

// ConvertToSQL implements StreamConverter for Excel files (outputs SQL to writer)
func (e *ExcelConverter) ConvertToSQL(reader io.Reader, writer io.Writer) error {
	// Note: e.file is already open if created via NewExcelConverter.
	// However, StreamConverter interface passes a reader.
	// If this method is called directly without NewExcelConverter (e.g. from main's exportToSQL),
	// we need to open the reader.

	// If we are using the instance method on an empty struct, we need to open the reader.
	// But StreamConverter interface ConvertToSQL(reader, writer) implies we might create a new converter or use the existing one?
	// The interface is:
	// type StreamConverter interface { ConvertToSQL(reader io.Reader, writer io.Writer) error }
	// So `exportToSQL` creates `&ExcelConverter{}` and calls `ConvertToSQL`.
	// In that case `e.file` is nil.

	f, err := excelize.OpenReader(reader)
	if err != nil {
		return fmt.Errorf("failed to open Excel stream: %w", err)
	}
	defer f.Close()

	sheets := f.GetSheetList()
	if len(sheets) == 0 {
		return fmt.Errorf("no sheets found in Excel stream")
	}

	tableNames := converters.GenTableNames(sheets)

	for idx, sheetName := range sheets {
		tableName := tableNames[idx]

		rows, err := f.Rows(sheetName)
		if err != nil {
			return fmt.Errorf("failed to get rows for sheet %s: %w", sheetName, err)
		}

		var headers []string
		if rows.Next() {
			headerRow, err := rows.Columns()
			if err != nil {
				rows.Close()
				return fmt.Errorf("failed to read header row for sheet %s: %w", sheetName, err)
			}
			headers = converters.GenColumnNames(headerRow)
		} else {
			rows.Close()
			continue // Skip empty sheet
		}

		// Write CREATE TABLE statement
		createTableSQL := converters.GenCreateTableSQL(tableName, headers)
		if _, err := fmt.Fprintf(writer, "%s;\n\n", createTableSQL); err != nil {
			rows.Close()
			return fmt.Errorf("failed to write CREATE TABLE: %w", err)
		}

		for rows.Next() {
			row, err := rows.Columns()
			if err != nil {
				rows.Close()
				return fmt.Errorf("failed to read row: %w", err)
			}

			// Ensure row matches headers length
			if len(row) < len(headers) {
				for len(row) < len(headers) {
					row = append(row, "")
				}
			} else if len(row) > len(headers) {
				row = row[:len(headers)]
			}

			if _, err := fmt.Fprintf(writer, "INSERT INTO %s (", tableName); err != nil {
				rows.Close()
				return fmt.Errorf("failed to write INSERT start: %w", err)
			}

			// Write column names
			for i, header := range headers {
				if i > 0 {
					if _, err := writer.Write([]byte(", ")); err != nil {
						rows.Close()
						return fmt.Errorf("failed to write column separator: %w", err)
					}
				}
				if _, err := fmt.Fprintf(writer, "%s", header); err != nil {
					rows.Close()
					return fmt.Errorf("failed to write column name: %w", err)
				}
			}

			if _, err := fmt.Fprintf(writer, ") VALUES ("); err != nil {
				rows.Close()
				return fmt.Errorf("failed to write VALUES start: %w", err)
			}

			// Write values
			for i, val := range row {
				if i > 0 {
					if _, err := writer.Write([]byte(", ")); err != nil {
						rows.Close()
						return fmt.Errorf("failed to write value separator: %w", err)
					}
				}
				// Escape single quotes by doubling them
				escapedVal := strings.ReplaceAll(val, "'", "''")
				if _, err := fmt.Fprintf(writer, "'%s'", escapedVal); err != nil {
					rows.Close()
					return fmt.Errorf("failed to write value: %w", err)
				}
			}

			if _, err := writer.Write([]byte(");\n")); err != nil {
				rows.Close()
				return fmt.Errorf("failed to write statement end: %w", err)
			}
		}
		rows.Close()

		if _, err := writer.Write([]byte("\n")); err != nil {
			return fmt.Errorf("failed to write table separator: %w", err)
		}
	}

	return nil
}
