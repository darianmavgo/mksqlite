package filesystem

import (
	"fmt"
	"io"
	"io/fs"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/darianmavgo/mksqlite/converters"
	"github.com/darianmavgo/mksqlite/converters/common"
)

const (
	FSTB = "tb0"
)

func init() {
	converters.Register("filesystem", &filesystemDriver{})
}

type filesystemDriver struct{}

func (d *filesystemDriver) Open(source io.Reader, config *common.ConversionConfig) (common.RowProvider, error) {
	if config != nil && config.InputPath != "" {
		return NewFilesystemConverter(config.InputPath)
	}
	// Fallback to trying to get the path from the source reader if it's a file
	if f, ok := source.(*os.File); ok {
		return NewFilesystemConverter(f.Name())
	}
	return nil, fmt.Errorf("FilesystemConverter requires InputPath in config or *os.File source")
}

// FilesystemConverter converts directory listings to SQLite tables
type FilesystemConverter struct {
	inputPath string
}

// Ensure FilesystemConverter implements RowProvider
var _ common.RowProvider = (*FilesystemConverter)(nil)

// Ensure FilesystemConverter implements StreamConverter
var _ common.StreamConverter = (*FilesystemConverter)(nil)

// NewFilesystemConverter creates a new FilesystemConverter from a directory path.
func NewFilesystemConverter(inputPath string) (*FilesystemConverter, error) {
	info, err := os.Stat(inputPath)
	if err != nil {
		return nil, fmt.Errorf("failed to stat path: %w", err)
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
		return []string{
			"path", "name", "size", "extension",
			"mod_time", "create_time", "permissions",
			"is_dir", "mime_type",
		}
	}
	return nil
}

// ScanRows implements RowProvider
func (c *FilesystemConverter) ScanRows(tableName string, yield func([]interface{}, error) error) error {
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
		createTime := getCreateTime(info).Format(time.RFC3339)
		permissions := info.Mode().String()

		isDir := 0
		mimeType := ""

		if d.IsDir() {
			isDir = 1
			mimeType = "inode/directory"
		} else {
			// Detect mimetype
			mimeType = c.detectMimeType(path)
		}

		ext := filepath.Ext(path)
		name := d.Name()

		row := []interface{}{
			relPath, name, size, ext,
			modTime, createTime, permissions,
			isDir, mimeType,
		}

		return yield(row, nil)
	})

	if err != nil {
		return fmt.Errorf("failed to walk directory: %w", err)
	}

	return nil
}

func (c *FilesystemConverter) detectMimeType(path string) string {
	f, err := os.Open(path)
	if err != nil {
		return "application/octet-stream"
	}
	defer f.Close()

	buffer := make([]byte, 512)
	n, err := f.Read(buffer)
	if err != nil && err != io.EOF {
		return "application/octet-stream"
	}
	return http.DetectContentType(buffer[:n])
}

// ConvertToSQL implements StreamConverter for filesystem directories
func (c *FilesystemConverter) ConvertToSQL(writer io.Writer) error {
	// We need the path to walk the directory.
	// It is stored in c.inputPath

	if c.inputPath == "" {
		return fmt.Errorf("FilesystemConverter not initialized (inputPath is empty)")
	}

	inputPath := c.inputPath
	headers := []string{
		"path", "name", "size", "extension",
		"mod_time", "create_time", "permissions",
		"is_dir", "mime_type",
	}

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
		createTime := getCreateTime(info).Format(time.RFC3339)
		permissions := info.Mode().String()

		isDir := 0
		mimeType := ""

		if d.IsDir() {
			isDir = 1
			mimeType = "inode/directory"
		} else {
			mimeType = c.detectMimeType(path)
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
			createTime,
			permissions,
			fmt.Sprintf("%d", isDir),
			mimeType,
		}

		if _, err := fmt.Fprintf(writer, "INSERT INTO %s (path, name, size, extension, mod_time, create_time, permissions, is_dir, mime_type) VALUES (", FSTB); err != nil {
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
