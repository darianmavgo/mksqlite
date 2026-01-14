package converters

import (
	"fmt"
	"io"
	"strings"

	"github.com/xuri/excelize/v2"
)

// ExcelConverter converts Excel files to SQLite tables
type ExcelConverter struct {
	tableNames []string
	headers    map[string][]string // map tableName to headers
	sheetMap   map[string]string   // map tableName to sheetName
	inputPath  string
}

// Ensure ExcelConverter implements RowProvider
var _ RowProvider = (*ExcelConverter)(nil)

// NewExcelConverter creates a new ExcelConverter
func NewExcelConverter(inputPath string) (*ExcelConverter, error) {
	// Open Excel file
	f, err := excelize.OpenFile(inputPath)
	if err != nil {
		return nil, fmt.Errorf("failed to open Excel file: %w", err)
	}
	defer f.Close()

	// Get all sheets
	sheets := f.GetSheetList()
	if len(sheets) == 0 {
		return nil, fmt.Errorf("no sheets found in Excel file")
	}

	tableNames := GenTableNames(sheets)
	headersMap := make(map[string][]string)
	sheetMap := make(map[string]string)

	for idx, sheetName := range sheets {
		tableName := tableNames[idx]
		sheetMap[tableName] = sheetName

		// Use iterator to get just the first row for headers
		rows, err := f.Rows(sheetName)
		if err != nil {
			return nil, fmt.Errorf("failed to get rows iterator for sheet %s: %w", sheetName, err)
		}

		if rows.Next() {
			headerRow, err := rows.Columns()
			if err != nil {
				rows.Close()
				return nil, fmt.Errorf("failed to read header row for sheet %s: %w", sheetName, err)
			}
			headersMap[tableName] = GenColumnNames(headerRow)
		}
		rows.Close()
	}

	return &ExcelConverter{
		tableNames: tableNames,
		headers:    headersMap,
		sheetMap:   sheetMap,
		inputPath:  inputPath,
	}, nil
}

// ConvertFile implements FileConverter for Excel files (creates SQLite database)
func (e *ExcelConverter) ConvertFile(inputPath, outputPath string) error {
	f, err := excelize.OpenFile(inputPath)
	if err != nil {
		return fmt.Errorf("failed to open Excel file: %w", err)
	}
	defer f.Close()

	// Get all sheets
	sheets := f.GetSheetList()
	if len(sheets) == 0 {
		return fmt.Errorf("no sheets found in Excel file")
	}

	e.tableNames = GenTableNames(sheets)
	e.headers = make(map[string][]string)
	e.sheetMap = make(map[string]string)
	e.inputPath = inputPath

	for idx, sheetName := range sheets {
		tableName := e.tableNames[idx]
		e.sheetMap[tableName] = sheetName

		// We need headers. Use Rows iterator to just get first row.
		rows, err := f.Rows(sheetName)
		if err != nil {
			return fmt.Errorf("failed to get rows for sheet %s: %w", sheetName, err)
		}

		if rows.Next() {
			headerRow, err := rows.Columns()
			if err != nil {
				rows.Close()
				return fmt.Errorf("failed to read header row for sheet %s: %w", sheetName, err)
			}
			e.headers[tableName] = GenColumnNames(headerRow)
		}
		rows.Close()
	}

	return ImportToSQLite(e, outputPath)
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

	f, err := excelize.OpenFile(e.inputPath)
	if err != nil {
		return fmt.Errorf("failed to open Excel file: %w", err)
	}
	defer f.Close()

	rows, err := f.Rows(sheetName)
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
	f, err := excelize.OpenReader(reader)
	if err != nil {
		return fmt.Errorf("failed to open Excel stream: %w", err)
	}
	defer f.Close()

	sheets := f.GetSheetList()
	if len(sheets) == 0 {
		return fmt.Errorf("no sheets found in Excel stream")
	}

	tableNames := GenTableNames(sheets)

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
			headers = GenColumnNames(headerRow)
		} else {
			rows.Close()
			continue // Skip empty sheet
		}

		// Write CREATE TABLE statement
		createTableSQL := GenCreateTableSQL(tableName, headers)
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
