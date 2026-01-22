package filesystem

import (
	"fmt"
	"io"
	"io/fs"
	"github.com/darianmavgo/mksqlite/converters"
	"github.com/darianmavgo/mksqlite/converters/common"
	"os"
	"path/filepath"
	"strings"
	"time"
)

const (
	FSTB = "tb0"
)

func init() {
	converters.Register("filesystem", &filesystemDriver{})
}

type filesystemDriver struct{}

func (d *filesystemDriver) Open(source io.Reader, config *common.ConversionConfig) (common.RowProvider, error) {
	return NewFilesystemConverter(source)
}

// FilesystemConverter converts directory listings to SQLite tables
type FilesystemConverter struct {
	inputPath string
}

// Ensure FilesystemConverter implements RowProvider
var _ common.RowProvider = (*FilesystemConverter)(nil)

// Ensure FilesystemConverter implements StreamConverter
var _ common.StreamConverter = (*FilesystemConverter)(nil)

// NewFilesystemConverter creates a new FilesystemConverter from an io.Reader.
// It requires the reader to be an *os.File to determine the directory path.
func NewFilesystemConverter(r io.Reader) (*FilesystemConverter, error) {
	file, ok := r.(*os.File)
	if !ok {
		return nil, fmt.Errorf("FilesystemConverter requires an *os.File reader to determine the directory path")
	}

	inputPath := file.Name()
	info, err := file.Stat()
	if err != nil {
		return nil, fmt.Errorf("failed to stat file: %w", err)
	}
	if !info.IsDir() {
		return nil, fmt.Errorf("input path is not a directory: %s", inputPath)
	}

	return &FilesystemConverter{
		inputPath: inputPath,
	}, nil
}

// GetTableNames implements RowProvider
func (c *FilesystemConverter) GetTableNames() []string {
	return []string{FSTB}
}

// GetHeaders implements RowProvider
func (c *FilesystemConverter) GetHeaders(tableName string) []string {
	if tableName == FSTB {
		return []string{"path", "name", "size", "extension", "mod_time", "is_dir"}
	}
	return nil
}

// ScanRows implements RowProvider
func (c *FilesystemConverter) ScanRows(tableName string, yield func([]interface{}) error) error {
	if tableName != FSTB {
		return nil
	}

	// Walk directory
	err := filepath.WalkDir(c.inputPath, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}

		// Calculate relative path
		relPath, err := filepath.Rel(c.inputPath, path)
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

		row := []interface{}{
			relPath, name, size, ext, modTime, isDir,
		}

		return yield(row)
	})

	if err != nil {
		return fmt.Errorf("failed to walk directory: %w", err)
	}

	return nil
}

// ConvertToSQL implements StreamConverter for filesystem directories
func (c *FilesystemConverter) ConvertToSQL(writer io.Writer) error {
	// We need the path to walk the directory.
	// It is stored in c.inputPath

	if c.inputPath == "" {
		return fmt.Errorf("FilesystemConverter not initialized (inputPath is empty)")
	}

	inputPath := c.inputPath
	headers := []string{"path", "name", "size", "extension", "mod_time", "is_dir"}

	// Write CREATE TABLE statement
	createTableSQL := common.GenCreateTableSQL(FSTB, headers)
	if _, err := fmt.Fprintf(writer, "%s;\n\n", createTableSQL); err != nil {
		return fmt.Errorf("failed to write CREATE TABLE: %w", err)
	}

	// Walk directory
	err := filepath.WalkDir(inputPath, func(path string, d fs.DirEntry, err error) error {
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

		if _, err := fmt.Fprintf(writer, "INSERT INTO %s (path, name, size, extension, mod_time, is_dir) VALUES (", FSTB); err != nil {
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
