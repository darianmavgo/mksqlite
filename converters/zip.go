package converters

import (
	"archive/zip"
	"fmt"
	"io"
	"time"
)

// ZipConverter converts ZIP archive file lists to SQLite tables
type ZipConverter struct {
	inputPath string
}

// Ensure ZipConverter implements RowProvider
var _ RowProvider = (*ZipConverter)(nil)

// ConvertFile implements FileConverter for ZIP files (creates SQLite database)
func (z *ZipConverter) ConvertFile(inputPath, outputPath string) error {
	z.inputPath = inputPath
	return ImportToSQLiteFile(z, outputPath)
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

	// Open the ZIP file
	r, err := zip.OpenReader(z.inputPath)
	if err != nil {
		return fmt.Errorf("failed to open zip file: %w", err)
	}
	defer r.Close()

	// Iterate through files in the zip archive
	for _, f := range r.File {
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
	return fmt.Errorf("ZIP stream conversion not yet implemented - use file-based conversion")
}
