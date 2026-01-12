package converters

import (
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"time"
)

// FilesystemConverter converts directory listings to SQLite tables
type FilesystemConverter struct {
	rows [][]interface{}
}

// Ensure FilesystemConverter implements RowProvider
var _ RowProvider = (*FilesystemConverter)(nil)

// ConvertFile implements FileConverter for filesystem directories
func (c *FilesystemConverter) ConvertFile(inputPath, outputPath string) error {
	// Ensure input is a directory
	info, err := os.Stat(inputPath)
	if err != nil {
		return fmt.Errorf("failed to stat input path: %w", err)
	}
	if !info.IsDir() {
		return fmt.Errorf("input path is not a directory: %s", inputPath)
	}

	c.rows = make([][]interface{}, 0)

	// Walk directory
	err = filepath.WalkDir(inputPath, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}

		// Calculate relative path
		relPath, err := filepath.Rel(inputPath, path)
		if err != nil {
			relPath = path // Fallback to full path if rel fails
		}

		info, err := d.Info()
		if err != nil {
			return err
		}

		size := info.Size()
		modTime := info.ModTime().Format(time.RFC3339)
		isDir := 0
		if d.IsDir() {
			isDir = 1
		}
		ext := filepath.Ext(path)
		name := d.Name()

		c.rows = append(c.rows, []interface{}{
			relPath, name, size, ext, modTime, isDir,
		})

		return nil
	})

	if err != nil {
		return fmt.Errorf("failed to walk directory: %w", err)
	}

	return ImportToSQLite(c, outputPath)
}

// GetTableNames implements RowProvider
func (c *FilesystemConverter) GetTableNames() []string {
	return []string{"data"}
}

// GetHeaders implements RowProvider
func (c *FilesystemConverter) GetHeaders(tableName string) []string {
	if tableName == "data" {
		return []string{"path", "name", "size", "extension", "mod_time", "is_dir"}
	}
	return nil
}

// GetRows implements RowProvider
func (c *FilesystemConverter) GetRows(tableName string) [][]interface{} {
	if tableName == "data" {
		return c.rows
	}
	return nil
}

// ConvertToSQL implements StreamConverter for filesystem directories
func (c *FilesystemConverter) ConvertToSQL(reader io.Reader, writer io.Writer) error {
	// We need the path to walk the directory.
	// Try to get it from the reader if it's an *os.File.
	file, ok := reader.(*os.File)
	if !ok {
		return fmt.Errorf("FilesystemConverter.ConvertToSQL requires an *os.File reader to determine the directory path")
	}

	inputPath := file.Name()
	info, err := file.Stat()
	if err != nil {
		return fmt.Errorf("failed to stat file: %w", err)
	}
	if !info.IsDir() {
		return fmt.Errorf("input path is not a directory: %s", inputPath)
	}

	headers := []string{"path", "name", "size", "extension", "mod_time", "is_dir"}

	// Write CREATE TABLE statement
	createTableSQL := GenCreateTableSQL("data", headers)
	if _, err := fmt.Fprintf(writer, "%s;\n\n", createTableSQL); err != nil {
		return fmt.Errorf("failed to write CREATE TABLE: %w", err)
	}

	// Walk directory
	err = filepath.WalkDir(inputPath, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}

		// Calculate relative path
		relPath, err := filepath.Rel(inputPath, path)
		if err != nil {
			relPath = path
		}

		info, err := d.Info()
		if err != nil {
			return err
		}

		size := info.Size()
		modTime := info.ModTime().Format(time.RFC3339)
		isDir := 0
		if d.IsDir() {
			isDir = 1
		}
		ext := filepath.Ext(path)
		name := d.Name()

		// Row values
		row := []string{
			relPath,
			name,
			fmt.Sprintf("%d", size),
			ext,
			modTime,
			fmt.Sprintf("%d", isDir),
		}

		if _, err := fmt.Fprintf(writer, "INSERT INTO data (path, name, size, extension, mod_time, is_dir) VALUES ("); err != nil {
			return fmt.Errorf("failed to write INSERT start: %w", err)
		}

		// Write values
		for i, val := range row {
			if i > 0 {
				if _, err := writer.Write([]byte(", ")); err != nil {
					return fmt.Errorf("failed to write value separator: %w", err)
				}
			}
			// Escape single quotes by doubling them
			escapedVal := strings.ReplaceAll(val, "'", "''")
			if _, err := fmt.Fprintf(writer, "'%s'", escapedVal); err != nil {
				return fmt.Errorf("failed to write value: %w", err)
			}
		}

		if _, err := writer.Write([]byte(");\n")); err != nil {
			return fmt.Errorf("failed to write statement end: %w", err)
		}

		return nil
	})

	return err
}
