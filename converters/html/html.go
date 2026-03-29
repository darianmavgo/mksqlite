package html

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"strings"

	"github.com/darianmavgo/mksqlite/converters"
	"github.com/darianmavgo/mksqlite/converters/common"

	"golang.org/x/net/html"
)

func init() {
	converters.Register("html", &htmlDriver{})
}

type htmlDriver struct{}

func (d *htmlDriver) Open(source io.Reader, config *common.ConversionConfig) (common.RowProvider, error) {
	return NewHTMLConverter(source)
}

// HTMLConverter converts HTML files to SQLite tables
type HTMLConverter struct {
	tables     []tableData
	tableNames []string
}

type tableData struct {
	rawName string
	headers []string
	rows    [][]string
}

// Ensure HTMLConverter implements RowProvider
var _ common.RowProvider = (*HTMLConverter)(nil)

// Ensure HTMLConverter implements StreamConverter
var _ common.StreamConverter = (*HTMLConverter)(nil)

// NewHTMLConverter creates a new HTMLConverter from an io.Reader
func NewHTMLConverter(r io.Reader) (*HTMLConverter, error) {
	tables, err := parseHTML(bufio.NewReaderSize(r, 65536))
	if err != nil {
		return nil, err
	}

	// Generate table names once
	rawNames := make([]string, len(tables))
	for i, t := range tables {
		if t.rawName != "" {
			rawNames[i] = t.rawName
		} else {
			rawNames[i] = fmt.Sprintf("table%d", i)
		}
	}
	tableNames := common.GenTableNames(rawNames)

	return &HTMLConverter{
		tables:     tables,
		tableNames: tableNames,
	}, nil
}

// GetTableNames implements RowProvider
func (c *HTMLConverter) GetTableNames() []string {
	return c.tableNames
}

// GetHeaders implements RowProvider
func (c *HTMLConverter) GetHeaders(tableName string) []string {
	for i, name := range c.tableNames {
		if name == tableName {
			return common.GenColumnNames(c.tables[i].headers)
		}
	}
	return nil
}

// GetColumnTypes implements RowProvider
func (c *HTMLConverter) GetColumnTypes(tableName string) []string {
	for i, name := range c.tableNames {
		if name == tableName {
			headers := c.tables[i].headers
			rows := c.tables[i].rows
			return common.InferColumnTypes(rows, len(headers))
		}
	}
	return nil
}

// ScanRows implements RowProvider.
// Note: The slice passed to the yield function is reused across iterations.
// The consumer must copy the data if retention is required.
func (c *HTMLConverter) ScanRows(ctx context.Context, tableName string, yield func([]interface{}, error) error) error {
	for i, name := range c.tableNames {
		if name == tableName {
			rows := c.tables[i].rows
			var interfaceRow []interface{}
			for _, row := range rows {
				// Optimization: Reuse slice to avoid allocation per row
				if cap(interfaceRow) >= len(row) {
					interfaceRow = interfaceRow[:len(row)]
				} else {
					interfaceRow = make([]interface{}, len(row))
				}

				for c, val := range row {
					interfaceRow[c] = val
				}
				if err := yield(interfaceRow, nil); err != nil {
					return err
				}
				// Check cancel
				select {
				case <-ctx.Done():
					return ctx.Err()
				default:
				}
			}
			return nil
		}
	}
	return nil
}

// ConvertToSQL implements StreamConverter for HTML files (outputs SQL to writer)
func (c *HTMLConverter) ConvertToSQL(ctx context.Context, writer io.Writer) error {
	if len(c.tables) == 0 {
		return fmt.Errorf("no tables found in HTML")
	}

	for i, t := range c.tables {
		if len(t.headers) == 0 && len(t.rows) == 0 {
			continue
		}

		tableName := c.tableNames[i]
		sanitizedHeaders := common.GenColumnNames(t.headers)
		colTypes := c.GetColumnTypes(tableName)

		if err := writeHTMLTableSQL(ctx, tableName, sanitizedHeaders, colTypes, t.rows, writer); err != nil {
			return err
		}
	}

	return nil
}

func writeHTMLTableSQL(ctx context.Context, tableName string, headers []string, colTypes []string, rows [][]string, writer io.Writer) error {
	createTableSQL := common.GenCreateTableSQLWithTypes(tableName, headers, colTypes)
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
		// Check cancel
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
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
			rows = append(rows, row)
			return // Don't look for TRs inside TRs
		}

		for c := node.FirstChild; c != nil; c = c.NextSibling {
			// Don't traverse into nested tables here
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
	var sb strings.Builder
	extractTextRecursive(n, &sb)
	return strings.TrimSpace(sb.String())
}

func extractTextRecursive(n *html.Node, sb *strings.Builder) {
	if n.Type == html.TextNode {
		sb.WriteString(n.Data)
		return
	}
	for c := n.FirstChild; c != nil; c = c.NextSibling {
		extractTextRecursive(c, sb)
	}
}
