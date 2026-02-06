package txt

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/darianmavgo/mksqlite/converters"
	"github.com/darianmavgo/mksqlite/converters/common"
)

const (
	TXTTB = "tb0"
)

func init() {
	converters.Register("txt", &txtDriver{})
}

type txtDriver struct{}

func (d *txtDriver) Open(source io.Reader, config *common.ConversionConfig) (common.RowProvider, error) {
	return NewTxtConverterWithConfig(source, config)
}

// TxtConverter converts text files to SQLite tables (single column 'content')
type TxtConverter struct {
	scanner *bufio.Scanner

	Config  common.ConversionConfig
	timeout time.Duration
}

// Ensure TxtConverter implements RowProvider
var _ common.RowProvider = (*TxtConverter)(nil)

// Ensure TxtConverter implements StreamConverter
var _ common.StreamConverter = (*TxtConverter)(nil)

// NewTxtConverter creates a new TxtConverter from an io.Reader.
func NewTxtConverter(r io.Reader) (*TxtConverter, error) {
	return NewTxtConverterWithConfig(r, nil)
}

// NewTxtConverterWithConfig creates a new TxtConverter from an io.Reader with optional config.
func NewTxtConverterWithConfig(r io.Reader, config *common.ConversionConfig) (*TxtConverter, error) {
	if config == nil {
		config = &common.ConversionConfig{
			TableName: TXTTB,
		}
	}

	if config.TableName == "" {
		config.TableName = TXTTB
	}

	var timeout time.Duration
	if config.ScanTimeout != "" {
		if d, err := time.ParseDuration(config.ScanTimeout); err == nil {
			timeout = d
		}
	}

	return &TxtConverter{
		scanner: bufio.NewScanner(bufio.NewReaderSize(r, 65536)),
		Config:  *config,
		timeout: timeout,
	}, nil
}

// GetTableNames implements RowProvider
func (c *TxtConverter) GetTableNames() []string {
	return []string{c.Config.TableName}
}

// GetHeaders implements RowProvider
func (c *TxtConverter) GetHeaders(tableName string) []string {
	if tableName == c.Config.TableName {
		return []string{"content"}
	}
	return nil
}

// GetColumnTypes implements RowProvider
func (c *TxtConverter) GetColumnTypes(tableName string) []string {
	if tableName == c.Config.TableName {
		return []string{"TEXT"}
	}
	return nil
}

// ScanRows implements RowProvider using a worker pattern (pipelining) to improve streaming performance.
func (c *TxtConverter) ScanRows(ctx context.Context, tableName string, yield func([]interface{}, error) error) error {
	if tableName != c.Config.TableName {
		return nil
	}

	if c.scanner == nil {
		return fmt.Errorf("Txt scanner is not initialized")
	}

	// Channel to pipeline reading and processing
	rowsCh := make(chan []interface{}, 100)
	prodErrCh := make(chan error, 1)
	cancelCh := make(chan struct{})

	// Producer goroutine
	go func() {
		defer close(rowsCh)

		for c.scanner.Scan() {
			// Check cancel
			select {
			case <-cancelCh:
				return
			default:
			}

			line := c.scanner.Text()

			select {
			case rowsCh <- []interface{}{line}:
			case <-cancelCh:
				return
			}
		}

		if err := c.scanner.Err(); err != nil {
			select {
			case prodErrCh <- fmt.Errorf("failed to read txt line: %w", err):
			case <-cancelCh:
			}
		} else {
			close(prodErrCh)
		}
	}()

	// Consumer (Main Thread)
	defer close(cancelCh)

	wd := common.NewWatchdog(c.timeout)
	wdDone := wd.Start()
	defer wd.Stop()

	for {
		select {
		case row, ok := <-rowsCh:
			if !ok {
				// Check for producer error
				if err, ok := <-prodErrCh; ok {
					return err
				}
				return nil
			}

			wd.Kick()

			if err := yield(row, nil); err != nil {
				return err
			}
		case <-wdDone:
			return converters.ErrScanTimeout
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

// ConvertToSQL implements StreamConverter for Txt files (outputs SQL to writer).
func (c *TxtConverter) ConvertToSQL(ctx context.Context, writer io.Writer) error {
	if c.scanner == nil {
		return fmt.Errorf("Txt scanner is not initialized")
	}

	// Write CREATE TABLE statement
	// Write CREATE TABLE statement
	createTableSQL := common.GenCreateTableSQLWithTypes(c.Config.TableName, []string{"content"}, []string{"TEXT"})
	if _, err := fmt.Fprintf(writer, "%s;\n\n", createTableSQL); err != nil {
		return fmt.Errorf("failed to write CREATE TABLE: %w", err)
	}

	for c.scanner.Scan() {
		// Check context
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
		line := c.scanner.Text()

		if _, err := fmt.Fprintf(writer, "INSERT INTO %s (content) VALUES (", c.Config.TableName); err != nil {
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

	if err := c.scanner.Err(); err != nil {
		return fmt.Errorf("failed to read txt line: %w", err)
	}

	return nil
}
