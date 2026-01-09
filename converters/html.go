package converters

import (
	"database/sql"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	_ "github.com/mattn/go-sqlite3"
	"golang.org/x/net/html"
)

// HTMLConverter converts HTML files to SQLite tables
type HTMLConverter struct{}

type tableData struct {
	rawName string
	headers []string
	rows    [][]string
}

// ConvertFile implements FileConverter for HTML files (creates SQLite database)
func (c *HTMLConverter) ConvertFile(inputPath, outputPath string) error {
	// Ensure output directory exists
	dir := filepath.Dir(outputPath)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return fmt.Errorf("failed to create output directory: %w", err)
	}

	// Remove existing database file if it exists
	if _, err := os.Stat(outputPath); err == nil {
		if err := os.Remove(outputPath); err != nil {
			return fmt.Errorf("failed to remove existing database: %w", err)
		}
	}

	// Open input file
	file, err := os.Open(inputPath)
	if err != nil {
		return fmt.Errorf("failed to open input file: %w", err)
	}
	defer file.Close()

	return c.convertHTMLToSQLite(file, outputPath)
}

// convertHTMLToSQLite converts HTML data from reader to SQLite database
func (c *HTMLConverter) convertHTMLToSQLite(reader io.Reader, dbPath string) error {
	tables, err := parseHTML(reader)
	if err != nil {
		return err
	}

	if len(tables) == 0 {
		return fmt.Errorf("no tables found in HTML")
	}

	// Connect to SQLite database
	db, err := sql.Open("sqlite3", dbPath)
	if err != nil {
		return fmt.Errorf("failed to open database: %w", err)
	}
	defer db.Close()

	// Generate table names
	rawNames := make([]string, len(tables))
	for i, t := range tables {
		if t.rawName != "" {
			rawNames[i] = t.rawName
		} else {
			rawNames[i] = fmt.Sprintf("table%d", i)
		}
	}
	tableNames := GenTableNames(rawNames)

	for i, t := range tables {
		tableName := tableNames[i]

		if len(t.headers) == 0 && len(t.rows) == 0 {
			continue // Skip empty tables
		}

		// If we only have rows but no headers (extracted as headers=row[0]), logic handles it.
		// parseHTML puts first row into headers.

		// Sanitize headers
		sanitizedHeaders := GenColumnNames(t.headers)

		// Create table
		createTableSQL := GenCreateTableSQL(tableName, sanitizedHeaders)
		_, err = db.Exec(createTableSQL)
		if err != nil {
			return fmt.Errorf("failed to create table %s: %w", tableName, err)
		}

		// Prepare insert statement
		insertSQL, err := GenPreparedStmt(tableName, sanitizedHeaders, InsertStmt)
		if err != nil {
			return fmt.Errorf("failed to generate insert statement for table %s: %w", tableName, err)
		}
		stmt, err := db.Prepare(insertSQL)
		if err != nil {
			return fmt.Errorf("failed to prepare insert statement for table %s: %w", tableName, err)
		}

		// Insert rows
		for _, row := range t.rows {
			// Ensure row has the same number of columns as headers
			if len(row) < len(sanitizedHeaders) {
				for len(row) < len(sanitizedHeaders) {
					row = append(row, "")
				}
			} else if len(row) > len(sanitizedHeaders) {
				row = row[:len(sanitizedHeaders)]
			}

			values := make([]interface{}, len(row))
			for i, val := range row {
				values[i] = val
			}

			_, err = stmt.Exec(values...)
			if err != nil {
				stmt.Close()
				return fmt.Errorf("failed to insert row in table %s: %w", tableName, err)
			}
		}
		stmt.Close()
	}

	return nil
}

// ConvertToSQL implements StreamConverter for HTML files (outputs SQL to writer)
func (c *HTMLConverter) ConvertToSQL(reader io.Reader, writer io.Writer) error {
	tables, err := parseHTML(reader)
	if err != nil {
		return err
	}

	if len(tables) == 0 {
		return fmt.Errorf("no tables found in HTML")
	}

	// Generate table names
	rawNames := make([]string, len(tables))
	for i, t := range tables {
		if t.rawName != "" {
			rawNames[i] = t.rawName
		} else {
			rawNames[i] = fmt.Sprintf("table%d", i)
		}
	}
	tableNames := GenTableNames(rawNames)

	for i, t := range tables {
		if len(t.headers) == 0 && len(t.rows) == 0 {
			continue
		}

		sanitizedHeaders := GenColumnNames(t.headers)
		if err := writeHTMLTableSQL(tableNames[i], sanitizedHeaders, t.rows, writer); err != nil {
			return err
		}
	}

	return nil
}

func writeHTMLTableSQL(tableName string, headers []string, rows [][]string, writer io.Writer) error {
	createTableSQL := GenCreateTableSQL(tableName, headers)
	if _, err := fmt.Fprintf(writer, "%s;\n\n", createTableSQL); err != nil {
		return fmt.Errorf("failed to write CREATE TABLE: %w", err)
	}

	for _, row := range rows {
		if _, err := fmt.Fprintf(writer, "INSERT INTO %s (", tableName); err != nil {
			return fmt.Errorf("failed to write INSERT start: %w", err)
		}

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

		if _, err := writer.Write([]byte(") VALUES (")); err != nil {
			return fmt.Errorf("failed to write VALUES start: %w", err)
		}

		// Ensure row length matches headers
		currentRow := row
		if len(currentRow) < len(headers) {
			for len(currentRow) < len(headers) {
				currentRow = append(currentRow, "")
			}
		} else if len(currentRow) > len(headers) {
			currentRow = currentRow[:len(headers)]
		}

		for i, val := range currentRow {
			if i > 0 {
				if _, err := writer.Write([]byte(", ")); err != nil {
					return fmt.Errorf("failed to write value separator: %w", err)
				}
			}
			escapedVal := strings.ReplaceAll(val, "'", "''")
			if _, err := fmt.Fprintf(writer, "'%s'", escapedVal); err != nil {
				return fmt.Errorf("failed to write value: %w", err)
			}
		}

		if _, err := writer.Write([]byte(");\n")); err != nil {
			return fmt.Errorf("failed to write statement end: %w", err)
		}
	}
	if _, err := writer.Write([]byte("\n")); err != nil {
		return fmt.Errorf("failed to write table separator: %w", err)
	}
	return nil
}

func parseHTML(reader io.Reader) ([]tableData, error) {
	doc, err := html.Parse(reader)
	if err != nil {
		return nil, fmt.Errorf("failed to parse HTML: %w", err)
	}

	var tables []tableData
	var f func(*html.Node)
	f = func(n *html.Node) {
		if n.Type == html.ElementNode && n.Data == "table" {
			t := extractTable(n)
			tables = append(tables, t)
			// Don't traverse inside this table looking for more tables
			// (nested tables are handled by recursion but we want to flatten or treat them?
			// Current logic: we extract this table. If we want to find tables inside this table, we should continue recursion.
			// But extractTable does not consume the node from the tree, so we can continue recursion if we want nested tables to also be top-level tables.
			// However, extractTable traverses children.
			// Let's recurse to find nested tables as separate entities.
		}
		for c := n.FirstChild; c != nil; c = c.NextSibling {
			f(c)
		}
	}
	f(doc)
	return tables, nil
}

func extractTable(n *html.Node) tableData {
	var name string
	for _, attr := range n.Attr {
		if attr.Key == "id" {
			name = attr.Val
			break
		}
	}

	var rows [][]string
	var visitRows func(*html.Node)
	visitRows = func(node *html.Node) {
		if node.Type == html.ElementNode && node.Data == "tr" {
			var row []string
			for c := node.FirstChild; c != nil; c = c.NextSibling {
				if c.Type == html.ElementNode && (c.Data == "td" || c.Data == "th") {
					row = append(row, extractText(c))
				}
			}
			// Add row even if empty? Tables might have empty rows.
			// But meaningful data usually has cells.
			// If row is empty []string{}, it might cause issues later if we expect it to match headers.
			// But loop above ensures we just capture what's there.
			rows = append(rows, row)
			return // Don't look for TRs inside TRs
		}

		for c := node.FirstChild; c != nil; c = c.NextSibling {
			// Don't traverse into nested tables here, they are handled by main loop
			if c.Type == html.ElementNode && c.Data == "table" {
				continue
			}
			visitRows(c)
		}
	}
	visitRows(n)

	if len(rows) == 0 {
		return tableData{rawName: name}
	}

	return tableData{
		rawName: name,
		headers: rows[0],
		rows:    rows[1:],
	}
}

func extractText(n *html.Node) string {
	if n.Type == html.TextNode {
		return n.Data
	}
	var text string
	for c := n.FirstChild; c != nil; c = c.NextSibling {
		text += extractText(c)
	}
	// We might want to just concatenate without trim for inline elements,
	// but generally for table cells, trimming leading/trailing whitespace is good.
	// However, we should be careful about internal whitespace.
	// The simple concatenation preserves internal whitespace.
	// The final result should be trimmed.
	return strings.TrimSpace(text)
}
