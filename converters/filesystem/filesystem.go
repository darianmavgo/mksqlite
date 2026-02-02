package filesystem

import (
	"context"
	"fmt"
	"io"
	"io/fs"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
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
		c, err := NewFilesystemConverter(config.InputPath)
		if err != nil {
			return nil, err
		}
		if config.ResumePath != "" {
			c.SetResumptionPath(config.ResumePath)
		}
		if config.ScanTimeout != "" {
			if d, err := time.ParseDuration(config.ScanTimeout); err == nil {
				c.SetTimeout(d)
			}
		}
		return c, nil
	}
	// Fallback to trying to get the path from the source reader if it's a file
	if f, ok := source.(*os.File); ok {
		return NewFilesystemConverter(f.Name())
	}
	return nil, fmt.Errorf("FilesystemConverter requires InputPath in config or *os.File source")
}

// FilesystemConverter converts directory listings to SQLite tables
type FilesystemConverter struct {
	inputPath      string
	resumptionPath string
	timeout        time.Duration
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
		inputPath:      inputPath,
		resumptionPath: "",
		timeout:        10 * time.Second,
	}, nil
}

// SetResumptionPath sets the path to resume reading from.
// Any path strictly less than this (lexicographically) will be skipped.
func (c *FilesystemConverter) SetResumptionPath(path string) {
	c.resumptionPath = path
}

// SetTimeout sets the maximum duration for the scan.
func (c *FilesystemConverter) SetTimeout(d time.Duration) {
	c.timeout = d
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

// GetColumnTypes implements RowProvider
func (c *FilesystemConverter) GetColumnTypes(tableName string) []string {
	if tableName == FSTB {
		return []string{
			"TEXT", "TEXT", "INTEGER", "TEXT",
			"TEXT", "TEXT", "TEXT",
			"INTEGER", "TEXT",
		}
	}
	return nil
}

// ScanRows implements RowProvider
func (c *FilesystemConverter) ScanRows(tableName string, yield func([]interface{}, error) error) error {
	if tableName != FSTB {
		return nil
	}

	// Configuration
	const numWorkers = 32
	// Semaphore to limit concurrency
	sem := make(chan struct{}, numWorkers)

	// Context for cancellation
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// WaitGroup for all active tasks (dirs and files)
	var wg sync.WaitGroup

	// Results channel
	results := make(chan []interface{}, 1000)

	// Error channel for the consumer
	consumerErr := make(chan error, 1)

	// Consumer
	go func() {
		defer close(consumerErr)

		for {
			select {
			case row, ok := <-results:
				if !ok {
					// Results closed, we are done
					consumerErr <- nil
					return
				}
				if err := yield(row, nil); err != nil {
					consumerErr <- err
					cancel() // Stop producers
					return
				}
			case <-ctx.Done():
				consumerErr <- ctx.Err()
				return
			}
		}
	}()

	// Start walking
	wg.Add(1)
	go c.processDir(ctx, c.inputPath, &wg, sem, results)

	// Monitor completion
	go func() {
		wg.Wait()
		close(results)
	}()

	// Wait for consumer
	return <-consumerErr
}

func (c *FilesystemConverter) processDir(ctx context.Context, dirPath string, wg *sync.WaitGroup, sem chan struct{}, results chan<- []interface{}) {
	defer wg.Done()

	// Acquire semaphore for directory reading
	select {
	case sem <- struct{}{}:
	case <-ctx.Done():
		return
	}

	// Read directory with timeout
	// Default 30s timeout for directory listing
	entries, err := runWithTimeout(30*time.Second, func() ([]fs.DirEntry, error) {
		return os.ReadDir(dirPath)
	})

	// Release semaphore immediately after IO
	<-sem

	if err != nil {
		log.Printf("Error reading directory %s: %v", dirPath, err)
		return
	}

	for _, d := range entries {
		select {
		case <-ctx.Done():
			return
		default:
		}

		fullPath := filepath.Join(dirPath, d.Name())

		// Resumption check
		if c.resumptionPath != "" && fullPath < c.resumptionPath {
			continue
		}

		if d.IsDir() {
			wg.Add(1)
			go c.processDir(ctx, fullPath, wg, sem, results)
		} else {
			wg.Add(1)
			go c.processFile(ctx, fullPath, d, wg, sem, results)
		}
	}
}

func (c *FilesystemConverter) processFile(ctx context.Context, path string, d fs.DirEntry, wg *sync.WaitGroup, sem chan struct{}, results chan<- []interface{}) {
	defer wg.Done()

	// Acquire semaphore for file processing (stat/mime)
	select {
	case sem <- struct{}{}:
	case <-ctx.Done():
		return
	}
	defer func() { <-sem }()

	relPath, err := filepath.Rel(c.inputPath, path)
	if err != nil {
		relPath = path
	}

	info, err := d.Info()
	if err != nil {
		return
	}

	size := info.Size()
	modTime := info.ModTime().Format(time.RFC3339)
	createTime := getCreateTime(info).Format(time.RFC3339)
	permissions := info.Mode().String()
	isDir := 0

	// detectMimeType already has internal timeout/runWithTimeout logic now
	mimeType := c.detectMimeType(path)

	ext := filepath.Ext(path)
	name := d.Name()

	row := []interface{}{
		relPath, name, size, ext,
		modTime, createTime, permissions,
		isDir, mimeType,
	}

	select {
	case results <- row:
	case <-ctx.Done():
	}
}

func (c *FilesystemConverter) detectMimeType(path string) string {
	// Use a short timeout for individual file reads to prevent hangs
	timeout := 5 * time.Second
	if c.timeout > 0 && c.timeout < timeout {
		timeout = c.timeout
	}

	contentType, err := runWithTimeout(timeout, func() (string, error) {
		f, err := os.Open(path)
		if err != nil {
			return "", err
		}
		defer f.Close()

		buffer := make([]byte, 512)
		n, err := f.Read(buffer)
		if err != nil && err != io.EOF {
			return "", err
		}
		return http.DetectContentType(buffer[:n]), nil
	})

	if err != nil {
		return "application/octet-stream"
	}
	return contentType
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
	colTypes := c.GetColumnTypes(FSTB)
	createTableSQL := common.GenCreateTableSQLWithTypes(FSTB, headers, colTypes)
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

// runWithTimeout executes fn and returns its result, or an error if timeout is exceeded.
func runWithTimeout[T any](timeout time.Duration, fn func() (T, error)) (T, error) {
	done := make(chan struct{})
	var res T
	var err error

	go func() {
		defer close(done)
		res, err = fn()
	}()

	select {
	case <-done:
		return res, err
	case <-time.After(timeout):
		var zero T
		return zero, fmt.Errorf("operation timed out after %v", timeout)
	}
}
