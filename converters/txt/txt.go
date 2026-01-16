package txt

import (
	"bufio"
	"fmt"
	"io"
	"mksqlite/converters/common"
	"strings"
)

const (
	TXTTB = "tb0"
)

// TxtConverter converts text files to SQLite tables (single column 'content')
type TxtConverter struct {
	scanner *bufio.Scanner
}

// Ensure TxtConverter implements RowProvider
var _ common.RowProvider = (*TxtConverter)(nil)

// NewTxtConverter creates a new TxtConverter from an io.Reader.
func NewTxtConverter(r io.Reader) (*TxtConverter, error) {
	return &TxtConverter{
		scanner: bufio.NewScanner(r),
	}, nil
}

// GetTableNames implements RowProvider
func (c *TxtConverter) GetTableNames() []string {
	return []string{TXTTB}
}

// GetHeaders implements RowProvider
func (c *TxtConverter) GetHeaders(tableName string) []string {
	if tableName == TXTTB {
		return []string{"content"}
	}
	return nil
}

// ScanRows implements RowProvider using a worker pattern (pipelining) to improve streaming performance.
func (c *TxtConverter) ScanRows(tableName string, yield func([]interface{}) error) error {
	if tableName != TXTTB {
		return nil
	}

	if c.scanner == nil {
		return fmt.Errorf("Txt scanner is not initialized")
	}

	// Channel to pipeline reading and processing
	rowsCh := make(chan []interface{}, 100)
	doneCh := make(chan error, 1)

	// Producer goroutine
	go func() {
		defer close(rowsCh)

		for c.scanner.Scan() {
			line := c.scanner.Text()
			rowsCh <- []interface{}{line}
		}

		if err := c.scanner.Err(); err != nil {
			doneCh <- fmt.Errorf("failed to read txt line: %w", err)
		} else {
			doneCh <- nil
		}
	}()

	// Consumer (Main Thread)
	for row := range rowsCh {
		if err := yield(row); err != nil {
			return err
		}
	}

	// Check for producer error
	select {
	case err := <-doneCh:
		return err
	default:
		return nil
	}
}

// ConvertToSQL implements StreamConverter for Txt files (outputs SQL to writer).
func (c *TxtConverter) ConvertToSQL(reader io.Reader, writer io.Writer) error {
	scanner := bufio.NewScanner(reader)

	// Write CREATE TABLE statement
	createTableSQL := common.GenCreateTableSQL(TXTTB, []string{"content"})
	if _, err := fmt.Fprintf(writer, "%s;\n\n", createTableSQL); err != nil {
		return fmt.Errorf("failed to write CREATE TABLE: %w", err)
	}

	for scanner.Scan() {
		line := scanner.Text()

		if _, err := fmt.Fprintf(writer, "INSERT INTO %s (content) VALUES (", TXTTB); err != nil {
			return fmt.Errorf("failed to write INSERT start: %w", err)
		}

		// Escape single quotes by doubling them
		escapedVal := strings.ReplaceAll(line, "'", "''")
		if _, err := fmt.Fprintf(writer, "'%s'", escapedVal); err != nil {
			return fmt.Errorf("failed to write value: %w", err)
		}

		if _, err := writer.Write([]byte(");\n")); err != nil {
			return fmt.Errorf("failed to write statement end: %w", err)
		}
	}

	if err := scanner.Err(); err != nil {
		return fmt.Errorf("failed to read txt line: %w", err)
	}

	return nil
}
