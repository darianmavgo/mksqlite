package converters

import (
	"archive/zip"
	"bytes"
	"fmt"
	"io"
	"os"
	"strings"
	"time"
)

// ZipConverter converts ZIP archive file lists to SQLite tables
type ZipConverter struct {
	zipReader *zip.Reader
}

// Ensure ZipConverter implements RowProvider
var _ RowProvider = (*ZipConverter)(nil)

// NewZipConverter creates a new ZipConverter from an io.Reader
func NewZipConverter(r io.Reader) (*ZipConverter, error) {
	var zipReader *zip.Reader
	var err error

	if f, ok := r.(*os.File); ok {
		info, err := f.Stat()
		if err != nil {
			return nil, fmt.Errorf("failed to stat file: %w", err)
		}
		zipReader, err = zip.NewReader(f, info.Size())
	} else {
		// Read fully into memory
		data, err := io.ReadAll(r)
		if err != nil {
			return nil, fmt.Errorf("failed to read stream: %w", err)
		}
		readerAt := bytes.NewReader(data)
		zipReader, err = zip.NewReader(readerAt, int64(len(data)))
	}

	if err != nil {
		return nil, fmt.Errorf("failed to create zip reader: %w", err)
	}

	return &ZipConverter{zipReader: zipReader}, nil
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
		return GenColumnNames(rawHeaders)
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
// Note: This requires reading the entire content into memory or using a temp file
// because zip.NewReader requires io.ReaderAt and size.
// For now, we will return an error similar to ExcelConverter.
func (z *ZipConverter) ConvertToSQL(reader io.Reader, writer io.Writer) error {
	// If this method is called, we need to initialize a reader from the input stream
	// if z.zipReader is nil.
	// But usually this is called on a new instance.

	var zipReader *zip.Reader
	var err error

	if f, ok := reader.(*os.File); ok {
		info, err := f.Stat()
		if err != nil {
			return fmt.Errorf("failed to stat file: %w", err)
		}
		zipReader, err = zip.NewReader(f, info.Size())
	} else {
		// Read fully into memory
		data, err := io.ReadAll(reader)
		if err != nil {
			return fmt.Errorf("failed to read stream: %w", err)
		}
		readerAt := bytes.NewReader(data)
		zipReader, err = zip.NewReader(readerAt, int64(len(data)))
	}

	if err != nil {
		return fmt.Errorf("failed to create zip reader: %w", err)
	}

	// Now we can reuse the logic to write SQL
	// But StreamConverter writes SQL text, not populating SQLite DB directly.

	// Write CREATE TABLE
	tableName := "file_list"
	headers := z.GetHeaders(tableName)
	createTableSQL := GenCreateTableSQL(tableName, headers)
	if _, err := fmt.Fprintf(writer, "%s;\n\n", createTableSQL); err != nil {
		return fmt.Errorf("failed to write CREATE TABLE: %w", err)
	}

	for _, f := range zipReader.File {
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
