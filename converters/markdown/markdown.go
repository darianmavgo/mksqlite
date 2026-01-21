package markdown

import (
	"bufio"
	"fmt"
	"io"
	"regexp"
	"strings"

	"github.com/darianmavgo/mksqlite/converters"
	"github.com/darianmavgo/mksqlite/converters/common"
)

func init() {
	converters.Register("markdown", &markdownDriver{})
}

type markdownDriver struct{}

func (d *markdownDriver) Open(source io.Reader) (common.RowProvider, error) {
	return NewMarkdownConverter(source)
}

// MarkdownConverter converts Markdown files to SQLite tables
type MarkdownConverter struct {
	tables     []tableData
	tableNames []string
}

type tableData struct {
	rawName string
	headers []string
	rows    [][]string
}

// Ensure MarkdownConverter implements RowProvider
var _ common.RowProvider = (*MarkdownConverter)(nil)

// Ensure MarkdownConverter implements StreamConverter
var _ common.StreamConverter = (*MarkdownConverter)(nil)

// NewMarkdownConverter creates a new MarkdownConverter from an io.Reader
func NewMarkdownConverter(r io.Reader) (*MarkdownConverter, error) {
	tables, err := parseMarkdown(r)
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

	return &MarkdownConverter{
		tables:     tables,
		tableNames: tableNames,
	}, nil
}

// GetTableNames implements RowProvider
func (c *MarkdownConverter) GetTableNames() []string {
	return c.tableNames
}

// GetHeaders implements RowProvider
func (c *MarkdownConverter) GetHeaders(tableName string) []string {
	for i, name := range c.tableNames {
		if name == tableName {
			return common.GenColumnNames(c.tables[i].headers)
		}
	}
	return nil
}

// ScanRows implements RowProvider
func (c *MarkdownConverter) ScanRows(tableName string, yield func([]interface{}) error) error {
	for i, name := range c.tableNames {
		if name == tableName {
			rows := c.tables[i].rows
			for _, row := range rows {
				interfaceRow := make([]interface{}, len(row))
				for c, val := range row {
					interfaceRow[c] = val
				}
				if err := yield(interfaceRow); err != nil {
					return err
				}
			}
			return nil
		}
	}
	return nil
}

// ConvertToSQL implements StreamConverter
func (c *MarkdownConverter) ConvertToSQL(writer io.Writer) error {
	if len(c.tables) == 0 {
		return fmt.Errorf("no tables found in Markdown")
	}

	for i, t := range c.tables {
		if len(t.headers) == 0 && len(t.rows) == 0 {
			continue
		}

		tableName := c.tableNames[i]
		sanitizedHeaders := common.GenColumnNames(t.headers)
		if err := writeTableSQL(tableName, sanitizedHeaders, t.rows, writer); err != nil {
			return err
		}
	}

	return nil
}

func writeTableSQL(tableName string, headers []string, rows [][]string, writer io.Writer) error {
	createTableSQL := common.GenCreateTableSQL(tableName, headers)
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
			// Escape single quotes for SQL
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

// Regex for headers and anchors
var (
	headerRegex = regexp.MustCompile(`^#+\s+(.*)$`)
	anchorRegex = regexp.MustCompile(`<a\s+.*(?:id|name)="([^"]+)".*>`)
	listRegex   = regexp.MustCompile(`^(\s*)([*+\-]|\d+\.)\s+(.*)$`)
	tableRegex  = regexp.MustCompile(`^\s*\|`)
)

func parseMarkdown(r io.Reader) ([]tableData, error) {
	scanner := bufio.NewScanner(r)
	var tables []tableData
	var currentName string
	var lines []string

	// Read all lines first (easier to handle multi-line lookahead/backtrack logic if needed,
	// though streaming is preferred, for simplicity with lists we read all lines or use state machine)
	// Given typical MD size, reading lines is acceptable.
	for scanner.Scan() {
		lines = append(lines, scanner.Text())
	}
	if err := scanner.Err(); err != nil {
		return nil, err
	}

	i := 0
	for i < len(lines) {
		line := lines[i]
		trimLine := strings.TrimSpace(line)

		// Check for Name (Header or Anchor)
		if match := headerRegex.FindStringSubmatch(trimLine); match != nil {
			currentName = strings.TrimSpace(match[1])
			i++
			continue
		}
		if match := anchorRegex.FindStringSubmatch(trimLine); match != nil {
			currentName = strings.TrimSpace(match[1])
			i++
			continue
		}

		// Check for Table Start
		if tableRegex.MatchString(trimLine) {
			// Validate it's a table by checking next line for separator
			if i+1 < len(lines) && strings.Contains(lines[i+1], "---") {
				table, consumed := parseTable(lines[i:], currentName)
				tables = append(tables, table)
				i += consumed
				currentName = "" // Reset name
				continue
			}
		}

		// Check for List Start
		if listRegex.MatchString(line) {
			listTable, consumed := parseList(lines[i:], currentName)
			tables = append(tables, listTable)
			i += consumed
			currentName = "" // Reset name
			continue
		}

		i++
	}

	return tables, nil
}

func parseTable(lines []string, name string) (tableData, int) {
	var rows [][]string
	consumed := 0

	// Helper to split pipe row
	splitRow := func(l string) []string {
		// remove leading/trailing pipes if present
		l = strings.TrimSpace(l)
		if strings.HasPrefix(l, "|") {
			l = l[1:]
		}
		if strings.HasSuffix(l, "|") {
			l = l[:len(l)-1]
		}
		parts := strings.Split(l, "|")
		for k, v := range parts {
			parts[k] = strings.TrimSpace(v)
		}
		return parts
	}

	// First line is headers
	headers := splitRow(lines[0])
	consumed++

	// Second line is separator, skip it
	if len(lines) > 1 {
		consumed++
	}

	// Subsequent lines are rows
	for j := 2; j < len(lines); j++ {
		line := lines[j]
		if !strings.Contains(line, "|") {
			break
		}
		rows = append(rows, splitRow(line))
		consumed++
	}

	return tableData{
		rawName: name,
		headers: headers,
		rows:    rows,
	}, consumed
}

func parseList(lines []string, name string) (tableData, int) {
	var rows [][]string
	consumed := 0

	type listItem struct {
		indent string
		key    string
		value  strings.Builder
	}

	var items []*listItem

	// We need to parse items.
	// List structure:
	// * Item 1
	//   Content
	// * Item 2

	for j := 0; j < len(lines); j++ {
		line := lines[j]

		// Check if empty line breaks list?
		// Markdown lists usually continue after one empty line if indented,
		// but break on two or if unindented text follows.
		// For simplicity: stop on double empty line or completely different block type.
		if strings.TrimSpace(line) == "" {
			// Look ahead: is next line a list item or indented content?
			if j+1 < len(lines) {
				nextLine := lines[j+1]
				if strings.TrimSpace(nextLine) == "" {
					// Double empty, break
					consumed++ // consume this empty line
					break
				}
				// If next line is a header or table, break
				if headerRegex.MatchString(nextLine) || tableRegex.MatchString(nextLine) {
					consumed++
					break
				}
				// If next line is not indented and not a list item, break
				if !listRegex.MatchString(nextLine) && !strings.HasPrefix(nextLine, "  ") && !strings.HasPrefix(nextLine, "\t") {
					consumed++
					break
				}
			}
		}

		// Check for new list item
		match := listRegex.FindStringSubmatch(line)
		if match != nil {
			indent := match[1]
			// content := match[3]
			// Actually key is match[3]

			// If this is a new item at root level (or same level as previous root), we treat it as a top level key?
			// The request says: "key" column is the immediate content of a list item and "value" is all sub content.
			// This implies we are flattening the list into key-value pairs.
			// But what if it's nested?
			// * A
			//   * B
			// Should this be Row(A, "* B")? Yes.

			// We only care about top-level items of this block.
			// How do we define top-level? The indentation of the first item found.

			if len(items) == 0 {
				// First item defines the root indentation
				newItem := &listItem{
					indent: indent,
					key:    strings.TrimSpace(match[3]),
				}
				items = append(items, newItem)
			} else {
				// Check indentation relative to first item
				rootIndent := items[0].indent
				if indent == rootIndent {
					// New root item
					newItem := &listItem{
						indent: indent,
						key:    strings.TrimSpace(match[3]),
					}
					items = append(items, newItem)
				} else {
					// It's sub-content of the last item
					lastItem := items[len(items)-1]
					lastItem.value.WriteString(line + "\n")
				}
			}
		} else {
			// Not a list start, just content
			if len(items) > 0 {
				lastItem := items[len(items)-1]
				lastItem.value.WriteString(line + "\n")
			}
		}
		consumed++
	}

	// Convert items to rows
	for _, item := range items {
		val := strings.TrimSpace(item.value.String())
		rows = append(rows, []string{item.key, val})
	}

	return tableData{
		rawName: name,
		headers: []string{"key", "value"},
		rows:    rows,
	}, consumed
}
