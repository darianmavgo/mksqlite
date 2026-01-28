package filesystem

import (
	"errors"
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

	// Configuration for concurrency
	const numWorkers = 32
	var wg sync.WaitGroup

	// Create channels
	// jobs channel carries directory paths to scan
	jobs := make(chan string, 1000)
	// results channel carries the data rows or errors
	results := make(chan []interface{}, 1000)

	// Start workers
	// We need a way to track active workers to know when we are done.
	// Since the graph is discovered dynamically, standard "close jobs" pattern is tricky.
	// We'll use a WaitGroup where we Add(1) for every directory we intend to scan
	// and Done() when we finish scanning it.

	// Start result consumer
	consumerDone := make(chan error, 1)
	go func() {
		defer close(consumerDone)
		for row := range results {
			if err := yield(row, nil); err != nil {
				consumerDone <- err
				return
			}
		}
		consumerDone <- nil
	}()

	// Semaphore to limit number of concurrent directory reads/file stats
	sem := make(chan struct{}, numWorkers)

	// Helper to safely add a job
	addJob := func(path string) {
		wg.Add(1)
		go func() {
			jobs <- path
		}()
	}

	// Retry loop for permission errors
	retryOnPermission := func(path string) {
		log.Printf("Permission denied for %s. Waiting for permission granted...", path)
		ticker := time.NewTicker(2 * time.Second)
		defer ticker.Stop()

		for {
			select {
			case <-ticker.C:
				// Try to open just to check permission
				f, err := os.Open(path)
				if err == nil {
					f.Close()
					// Success! Re-queue the job
					log.Printf("Permission granted for %s. Resuming...", path)
					addJob(path)
					return
				}
				if !errors.Is(err, fs.ErrPermission) {
					// Some other error, give up
					log.Printf("Giving up on %s: %v", path, err)
					wg.Done() // We are done with this job, incorrectly but done.
					return
				}
				// Still permission denied, continue waiting
			}
		}
	}

	// Worker logic

	// Custom worker loop to handle the "retry logic" keeping WG count correct
	// Actually, the simplest way to avoid the "WG 0 race" is to count the retry loop as part of the job.

	// Real worker processor
	startWorker := func() {
		for path := range jobs {
			// Acquire token
			sem <- struct{}{}

			// We handle directory read inside the worker to manage the "permission wait" logic
			// without blocking a worker thread for long periods.

			// Try reading
			entries, err := os.ReadDir(path)

			// Handle Permission Error specifically
			if err != nil && errors.Is(err, fs.ErrPermission) {
				// Release token immediately so others can work
				<-sem

				// Spawn a waiter goroutine using the helper
				go retryOnPermission(path)
				continue
			}

			if err != nil {
				// Log and ignore other errors
				log.Printf("Error reading %s: %v", path, err)
				<-sem
				wg.Done()
				continue
			}

			// Process valid entries
			for _, d := range entries {
				fullPath := filepath.Join(path, d.Name())
				if d.IsDir() {
					wg.Add(1)
					go func(p string) {
						jobs <- p
					}(fullPath)
				} else {
					c.processFile(fullPath, d, results)
				}
			}

			<-sem
			wg.Done()
		}
	}

	// Start a fixed number of workers consuming from 'jobs'
	// Wait, standard pattern with dynamic graph:
	// We can't close 'jobs' until we know we are done.
	// So we need a separate "Done" mechanism.
	// Since we use WG to track "active jobs", we can have a goroutine that waits for WG
	// and then closes jobs?
	// If I close jobs, workers exit.

	for i := 0; i < numWorkers; i++ {
		go startWorker()
	}

	// Initial job
	wg.Add(1)
	jobs <- c.inputPath

	// Wait for completion in background
	go func() {
		wg.Wait()
		close(jobs)
		close(results)
	}()

	// Wait for consumer to finish
	return <-consumerDone
}

func (c *FilesystemConverter) processFile(path string, d fs.DirEntry, results chan<- []interface{}) {
	relPath, err := filepath.Rel(c.inputPath, path)
	if err != nil {
		relPath = path
	}

	info, err := d.Info()
	if err != nil {
		// If we can't stat, skip
		return
	}

	size := info.Size()
	modTime := info.ModTime().Format(time.RFC3339)
	createTime := getCreateTime(info).Format(time.RFC3339)
	permissions := info.Mode().String()
	isDir := 0
	mimeType := c.detectMimeType(path)

	ext := filepath.Ext(path)
	name := d.Name()

	row := []interface{}{
		relPath, name, size, ext,
		modTime, createTime, permissions,
		isDir, mimeType,
	}

	results <- row
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
