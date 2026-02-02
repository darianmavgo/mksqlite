package zip

import (
	"archive/zip"
	"fmt"
	"io"
	"log"
	"os"
	"strings"
	"time"

	"github.com/darianmavgo/mksqlite/converters"
	"github.com/darianmavgo/mksqlite/converters/common"
)

func init() {
	converters.Register("zip", &zipDriver{})
}

type zipDriver struct{}

func (d *zipDriver) Open(source io.Reader, config *common.ConversionConfig) (common.RowProvider, error) {
	return NewZipConverterWithConfig(source, config)
}

// SizableReaderAt interface for inputs that support random access and size query
type SizableReaderAt interface {
	io.ReaderAt
	Size() (int64, error)
}

// ZipConverter converts ZIP archive file lists to SQLite tables
type ZipConverter struct {
	files    []FastZipEntry
	tempFile *os.File // To be cleaned up if a temp file was used
}

// Ensure ZipConverter implements RowProvider
var _ common.RowProvider = (*ZipConverter)(nil)

// Ensure ZipConverter implements StreamConverter
var _ common.StreamConverter = (*ZipConverter)(nil)

// Ensure ZipConverter implements io.Closer
var _ io.Closer = (*ZipConverter)(nil)

// Close closes and removes the temporary file if it exists.
func (z *ZipConverter) Close() error {
	if z.tempFile != nil {
		z.tempFile.Close()
		return os.Remove(z.tempFile.Name())
	}
	return nil
}

// NewZipConverter creates a new ZipConverter from an io.Reader
func NewZipConverter(r io.Reader) (*ZipConverter, error) {
	return NewZipConverterWithConfig(r, nil)
}

// progressReader wraps a Reader to kick a watchdog on successful reads
type progressReader struct {
	r  io.Reader
	fn func()
}

func (p *progressReader) Read(b []byte) (int, error) {
	n, err := p.r.Read(b)
	if n > 0 && p.fn != nil {
		p.fn()
	}
	return n, err
}

// NewZipConverterWithConfig creates a new ZipConverter with config
func NewZipConverterWithConfig(r io.Reader, config *common.ConversionConfig) (*ZipConverter, error) {
	var files []FastZipEntry
	var tempFile *os.File
	var err error

	if config == nil {
		config = &common.ConversionConfig{}
	}

	var timeout time.Duration
	if config.ScanTimeout != "" {
		if d, err := time.ParseDuration(config.ScanTimeout); err == nil {
			timeout = d
		}
	}

	// Check if input supports ReaderAt and Size (Fast Path)
	// 1. *os.File
	if f, ok := r.(*os.File); ok {
		info, err := f.Stat()
		if err != nil {
			return nil, fmt.Errorf("failed to stat file: %w", err)
		}
		log.Printf("FastZip: Using fast path for file %s (size %d)", f.Name(), info.Size())
		files, _, err = ParseCentralDirectoryFast(f, info.Size())
		if err != nil {
			return nil, fmt.Errorf("fast parsing failed: %w", err)
		}
	} else if sa, ok := r.(SizableReaderAt); ok {
		// 2. Custom SizableReaderAt (e.g. HTTP Range Reader)
		size, err := sa.Size()
		if err != nil {
			return nil, fmt.Errorf("failed to get size from reader: %w", err)
		}
		log.Printf("FastZip: Using fast path for SizableReaderAt (size %d)", size)
		files, _, err = ParseCentralDirectoryFast(sa, size)
		if err != nil {
			return nil, fmt.Errorf("fast parsing failed: %w", err)
		}
	} else {
		// 3. Fallback: stream to temp file
		log.Println("FastZip: Input is stream, falling back to temp file download")
		tempFile, err = os.CreateTemp("", "mksqlite-zip-*.zip")
		if err != nil {
			return nil, fmt.Errorf("failed to create temp file: %w", err)
		}

		// Clean up on error
		cleanup := func() {
			tempFile.Close()
			os.Remove(tempFile.Name())
		}

		// Copy with timeout support
		wd := common.NewWatchdog(timeout)
		done := wd.Start()
		defer wd.Stop()

		pr := &progressReader{r: r, fn: wd.Kick}

		type copyRes struct {
			n   int64
			err error
		}
		ch := make(chan copyRes, 1)

		go func() {
			n, err := io.Copy(tempFile, pr)
			ch <- copyRes{n, err}
		}()

		select {
		case res := <-ch:
			if res.err != nil {
				cleanup()
				return nil, fmt.Errorf("failed to copy stream to temp file: %w", res.err)
			}
		case <-done:
			cleanup() // Delete temp file
			return nil, converters.ErrScanTimeout
		}

		info, err := tempFile.Stat()
		if err != nil {
			cleanup()
			return nil, fmt.Errorf("failed to stat temp file: %w", err)
		}

		// Use standard library for temp file (robustness), then convert to FastZipEntry
		zReader, err := zip.NewReader(tempFile, info.Size())
		if err != nil {
			cleanup()
			return nil, fmt.Errorf("failed to create zip reader: %w", err)
		}

		for _, f := range zReader.File {
			isDir := false
			if f.FileInfo().IsDir() {
				isDir = true
			}
			files = append(files, FastZipEntry{
				Name:             f.Name,
				Comment:          f.Comment,
				Modified:         f.Modified,
				UncompressedSize: f.UncompressedSize64,
				CompressedSize:   f.CompressedSize64,
				CRC32:            f.CRC32,
				IsDir:            isDir,
			})
		}
	}

	return &ZipConverter{files: files, tempFile: tempFile}, nil
}

// GetTableNames implements RowProvider
func (z *ZipConverter) GetTableNames() []string {
	return []string{"file_list"}
}

// GetHeaders implements RowProvider
func (z *ZipConverter) GetHeaders(tableName string) []string {
	if tableName == "file_list" {
		rawHeaders := []string{
			"name",
			"comment",
			"modified",
			"uncompressed_size",
			"compressed_size",
			"crc32",
			"is_dir",
		}
		return common.GenColumnNames(rawHeaders)
	}
	return nil
}

// GetColumnTypes implements RowProvider
func (z *ZipConverter) GetColumnTypes(tableName string) []string {
	if tableName == "file_list" {
		// name: TEXT, comment: TEXT, modified: TEXT
		// uncompressed_size: INTEGER, compressed_size: INTEGER
		// crc32: INTEGER, is_dir: INTEGER
		return []string{"TEXT", "TEXT", "TEXT", "INTEGER", "INTEGER", "INTEGER", "INTEGER"}
	}
	return nil
}

// ScanRows implements RowProvider
func (z *ZipConverter) ScanRows(tableName string, yield func([]interface{}, error) error) error {
	if tableName != "file_list" {
		return nil
	}

	// Iterate through files
	for _, f := range z.files {
		// Prepare values
		isDir := "false"
		if f.IsDir {
			isDir = "true"
		}

		values := []interface{}{
			f.Name,
			f.Comment,
			f.Modified.Format(time.RFC3339),
			f.UncompressedSize,
			f.CompressedSize,
			f.CRC32,
			isDir,
		}

		if err := yield(values, nil); err != nil {
			return err
		}
	}
	return nil
}

// ConvertToSQL implements StreamConverter for ZIP files
func (z *ZipConverter) ConvertToSQL(writer io.Writer) error {
	// Write CREATE TABLE
	tableName := "file_list"
	headers := z.GetHeaders(tableName)
	colTypes := z.GetColumnTypes(tableName)

	createTableSQL := common.GenCreateTableSQLWithTypes(tableName, headers, colTypes)
	if _, err := fmt.Fprintf(writer, "%s;\n\n", createTableSQL); err != nil {
		return fmt.Errorf("failed to write CREATE TABLE: %w", err)
	}

	for _, f := range z.files {
		isDir := "false"
		if f.IsDir {
			isDir = "true"
		}

		// Values as strings for SQL
		row := []string{
			f.Name,
			f.Comment,
			f.Modified.Format(time.RFC3339),
			fmt.Sprintf("%d", f.UncompressedSize),
			fmt.Sprintf("%d", f.CompressedSize),
			fmt.Sprintf("%d", f.CRC32),
			isDir,
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

		if _, err := writer.Write([]byte(") VALUES (")); err != nil {
			return fmt.Errorf("failed to write VALUES start: %w", err)
		}

		// Write values
		for i, val := range row {
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

	return nil
}
