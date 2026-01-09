package converters

import (
	"database/sql"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"time"

	_ "github.com/mattn/go-sqlite3"
)

// FilesystemConverter converts directory listings to SQLite tables
type FilesystemConverter struct{}

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

	headers := []string{"path", "name", "size", "extension", "mod_time", "is_dir"}

	// Create table
	createTableSQL := GenCreateTableSQL("data", headers)
	_, err = db.Exec(createTableSQL)
	if err != nil {
		return fmt.Errorf("failed to create table: %w", err)
	}

	// Prepare insert statement
	insertSQL, err := GenPreparedStmt("data", headers, InsertStmt)
	if err != nil {
		return fmt.Errorf("failed to generate insert statement: %w", err)
	}
	stmt, err := db.Prepare(insertSQL)
	if err != nil {
		return fmt.Errorf("failed to prepare insert statement: %w", err)
	}
	defer stmt.Close()

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

		_, err = stmt.Exec(relPath, name, size, ext, modTime, isDir)
		if err != nil {
			return fmt.Errorf("failed to insert row for %s: %w", path, err)
		}

		return nil
	})

	if err != nil {
		return fmt.Errorf("failed to walk directory: %w", err)
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
