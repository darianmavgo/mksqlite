package converters

import (
	"fmt"
	"io"

	"github.com/xuri/excelize/v2"
)

// ExcelConverter converts Excel files to SQLite tables
type ExcelConverter struct {
	sheets map[string][][]string // map tableName to rows
	tableNames []string
	headers map[string][]string // map tableName to headers
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

	dataMap := make(map[string][][]string)
	headersMap := make(map[string][]string)

	for idx, sheetName := range sheets {
		rows, err := f.GetRows(sheetName)
		if err != nil {
			return nil, fmt.Errorf("failed to read sheet %s: %w", sheetName, err)
		}

		if len(rows) == 0 {
			continue // Skip empty sheets
		}

		tableName := tableNames[idx]

		headers := rows[0]
		dataRows := rows[1:]

		headersMap[tableName] = GenColumnNames(headers)
		dataMap[tableName] = dataRows
	}

	return &ExcelConverter{
		sheets: dataMap,
		tableNames: tableNames,
		headers: headersMap,
	}, nil
}


// ConvertFile implements FileConverter for Excel files (creates SQLite database)
func (e *ExcelConverter) ConvertFile(inputPath, outputPath string) error {
	// Re-initialize logic here for simplicity
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
	e.sheets = make(map[string][][]string)
	e.headers = make(map[string][]string)

	for idx, sheetName := range sheets {
		rows, err := f.GetRows(sheetName)
		if err != nil {
			return fmt.Errorf("failed to read sheet %s: %w", sheetName, err)
		}

		if len(rows) == 0 {
			continue // Skip empty sheets
		}

		tableName := e.tableNames[idx]

		headers := rows[0]
		dataRows := rows[1:]

		e.headers[tableName] = GenColumnNames(headers)
		e.sheets[tableName] = dataRows
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

// GetRows implements RowProvider
func (e *ExcelConverter) GetRows(tableName string) [][]interface{} {
	rows, ok := e.sheets[tableName]
	if !ok {
		return nil
	}

	interfaceRows := make([][]interface{}, len(rows))
	for i, row := range rows {
		interfaceRow := make([]interface{}, len(row))
		for j, val := range row {
			interfaceRow[j] = val
		}
		interfaceRows[i] = interfaceRow
	}
	return interfaceRows
}

// ConvertToSQL implements StreamConverter for Excel files (outputs SQL to writer)
// Note: This currently requires reading from a file path since Excel format parsing needs random access
func (e *ExcelConverter) ConvertToSQL(reader io.Reader, writer io.Writer) error {
	// For now, Excel stream conversion is not implemented
	// Excel files require random access reading which io.Reader doesn't provide
	// To implement this, we'd need io.ReaderAt or to buffer the entire content
	return fmt.Errorf("Excel stream conversion not yet implemented - use file-based conversion")
}
