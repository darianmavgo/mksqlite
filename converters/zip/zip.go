package zip

import (
	"archive/zip"
	"fmt"
	"io"
	"mksqlite/converters"
	"mksqlite/converters/common"
	"os"
	"strings"
	"time"
)

func init() {
	converters.Register("zip", &zipDriver{})
}

type zipDriver struct{}

func (d *zipDriver) Open(source io.Reader) (common.RowProvider, error) {
	return NewZipConverter(source)
}

// ZipConverter converts ZIP archive file lists to SQLite tables
type ZipConverter struct {
	zipReader *zip.Reader
	tempFile  *os.File // To be cleaned up if a temp file was used
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
	var zipReader *zip.Reader
	var err error
	var tempFile *os.File

	if f, ok := r.(*os.File); ok {
		info, err := f.Stat()
		if err != nil {
			return nil, fmt.Errorf("failed to stat file: %w", err)
		}
		zipReader, err = zip.NewReader(f, info.Size())
	} else {
		// Create temp file instead of reading fully into memory
		tempFile, err = os.CreateTemp("", "mksqlite-zip-*.zip")
		if err != nil {
			return nil, fmt.Errorf("failed to create temp file: %w", err)
		}

		// Clean up on error
		cleanup := func() {
			tempFile.Close()
			os.Remove(tempFile.Name())
		}

		if _, err := io.Copy(tempFile, r); err != nil {
			cleanup()
			return nil, fmt.Errorf("failed to copy stream to temp file: %w", err)
		}

		info, err := tempFile.Stat()
		if err != nil {
			cleanup()
			return nil, fmt.Errorf("failed to stat temp file: %w", err)
		}

		zipReader, err = zip.NewReader(tempFile, info.Size())
		if err != nil {
			cleanup()
			return nil, fmt.Errorf("failed to create zip reader: %w", err)
		}
	}

	return &ZipConverter{zipReader: zipReader, tempFile: tempFile}, nil
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

// ScanRows implements RowProvider
func (z *ZipConverter) ScanRows(tableName string, yield func([]interface{}) error) error {
	if tableName != "file_list" {
		return nil
	}

	// Iterate through files in the zip archive
	for _, f := range z.zipReader.File {
		// Prepare values
		isDir := "false"
		if f.FileInfo().IsDir() {
			isDir = "true"
		}

		values := []interface{}{
			f.Name,
			f.Comment,
			f.Modified.Format(time.RFC3339),
			f.UncompressedSize64,
			f.CompressedSize64,
			f.CRC32,
			isDir,
		}

		if err := yield(values); err != nil {
			return err
		}
	}
	return nil
}

// ConvertToSQL implements StreamConverter for ZIP files
func (z *ZipConverter) ConvertToSQL(writer io.Writer) error {
	if z.zipReader == nil {
		return fmt.Errorf("ZipConverter not initialized")
	}

	// Write CREATE TABLE
	tableName := "file_list"
	headers := z.GetHeaders(tableName)
	createTableSQL := common.GenCreateTableSQL(tableName, headers)
	if _, err := fmt.Fprintf(writer, "%s;\n\n", createTableSQL); err != nil {
		return fmt.Errorf("failed to write CREATE TABLE: %w", err)
	}

	for _, f := range z.zipReader.File {
		isDir := "false"
		if f.FileInfo().IsDir() {
			isDir = "true"
		}

		// Values as strings for SQL
		row := []string{
			f.Name,
			f.Comment,
			f.Modified.Format(time.RFC3339),
			fmt.Sprintf("%d", f.UncompressedSize64),
			fmt.Sprintf("%d", f.CompressedSize64),
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
