package converters

import (
	"archive/zip"
	"database/sql"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"

	_ "github.com/mattn/go-sqlite3"
)

// ZipConverter converts ZIP archive file lists to SQLite tables
type ZipConverter struct{}

// ConvertFile implements FileConverter for ZIP files (creates SQLite database)
func (z *ZipConverter) ConvertFile(inputPath, outputPath string) error {
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

	// Connect to SQLite database
	db, err := sql.Open("sqlite3", outputPath)
	if err != nil {
		return fmt.Errorf("failed to open database: %w", err)
	}
	defer db.Close()

	// Open the ZIP file
	r, err := zip.OpenReader(inputPath)
	if err != nil {
		return fmt.Errorf("failed to open zip file: %w", err)
	}
	defer r.Close()

	// Define headers based on metadata fields
	rawHeaders := []string{
		"name",
		"comment",
		"modified",
		"uncompressed_size",
		"compressed_size",
		"crc32",
		"is_dir",
	}

	// Sanitize headers
	headers := GenColumnNames(rawHeaders)
	tableName := "file_list"

	// Create table
	createTableSQL := GenCreateTableSQL(tableName, headers)
	_, err = db.Exec(createTableSQL)
	if err != nil {
		return fmt.Errorf("failed to create table: %w", err)
	}

	// Prepare insert statement
	insertSQL, err := GenPreparedStmt(tableName, headers, InsertStmt)
	if err != nil {
		return fmt.Errorf("failed to generate insert statement: %w", err)
	}
	stmt, err := db.Prepare(insertSQL)
	if err != nil {
		return fmt.Errorf("failed to prepare insert statement: %w", err)
	}
	defer stmt.Close()

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

		_, err = stmt.Exec(values...)
		if err != nil {
			return fmt.Errorf("failed to insert row for file %s: %w", f.Name, err)
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
