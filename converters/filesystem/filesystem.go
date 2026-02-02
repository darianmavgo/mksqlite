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

	// Configuration for concurrency
	const numWorkers = 32
	var wg sync.WaitGroup

	// Create channels
	// jobs channel carries directory paths to scan
	jobs := make(chan string, 10000)
	// results channel carries the data rows or errors
	results := make(chan []interface{}, 10000)

	// Cancellation mechanism
	doneCh := make(chan struct{})
	var idleTimeout time.Duration
	if c.timeout > 0 {
		idleTimeout = c.timeout
		log.Printf("Filesystem scan idle timeout set to %v", c.timeout)
	}

	// Progress tracker
	var rowCount int64
	lastLog := time.Now()

	// Consumer goroutine that writes to yield
	consumerDone := make(chan error, 1)
	go func() {
		defer close(consumerDone)

		var timer *time.Timer
		var timerCh <-chan time.Time

		if idleTimeout > 0 {
			timer = time.NewTimer(idleTimeout)
			timerCh = timer.C
			defer timer.Stop()
		}

		for {

			select {
			case row, ok := <-results:
				if !ok {
					consumerDone <- nil
					return
				}

				// Reset idle timer
				if timer != nil {
					if !timer.Stop() {
						select {
						case <-timer.C:
						default:
						}
					}
					timer.Reset(idleTimeout)
				}

				rowCount++
				if time.Since(lastLog) > 2*time.Second {
					log.Printf("Scanned %d files...", rowCount)
					lastLog = time.Now()
				}
				if err := yield(row, nil); err != nil {
					consumerDone <- err
					return
				}
			case <-timerCh:
				// Timed out due to inactivity
				log.Printf("Scan halted due to inactivity timeout (%v) after %d files.", idleTimeout, rowCount)
				close(doneCh) // Signal cancellation to workers
				consumerDone <- converters.ErrScanTimeout
				return
			case <-doneCh:
				// Externally cancelled (should not happen if we are the ones cancelling via timer,
				// but defensive in case we add other cancellation triggers)
				consumerDone <- converters.ErrScanTimeout
				return
			}
		}
	}()

	// Initial job tracking (Must happen before starting cleanup monitor)
	wg.Add(1)

	// Cleanup Monitor
	// This ensures that when everything stops (either by finish or timeout), we clean up
	go func() {
		wg.Wait()
		// Only close results if we finished normally (doneCh not closed)
		// Or if we know nobody is writing.
		// If timeout happened, doneCh is closed. Workers might still be stuck.
		// If we close results, stuck workers might panic if they blindly send.
		// However, processFile selects on doneCh:
		// case results <- row:
		// case <-doneCh: return
		// So if doneCh is closed, they stop sending.
		// Is it safe to close results then? Yes, but only after we are sure they saw doneCh.
		// wg.Wait() guarantees they are done (or saw doneCh and exited).
		// So if wg.Wait returns, it is safe to close results.
		close(results)
		close(jobs)
	}()

	// Semaphore to limit number of concurrent directory reads/file stats
	sem := make(chan struct{}, numWorkers)

	// Helper to safely add a job
	addJob := func(path string) {
		select {
		case <-doneCh:
			// Ensure we don't block if cancelled
		default:
			wg.Add(1)
			go func() {
				select {
				case jobs <- path:
				case <-doneCh:
					wg.Done()
				}
			}()
		}
	}

	// Retry loop for permission errors
	retryOnPermission := func(path string) {
		log.Printf("Permission denied for %s. Waiting (max 10s)...", path)

		// "Cap it at 10 seconds"
		retryTimeout := time.After(10 * time.Second)
		ticker := time.NewTicker(2 * time.Second)
		defer ticker.Stop()

		for {
			select {
			case <-doneCh:
				wg.Done()
				return
			case <-retryTimeout:
				log.Printf("Timeout waiting for permission on %s. Skipping.", path)
				wg.Done() // Give up cleanly
				return
			case <-ticker.C:
				// Try to open just to check permission
				f, err := os.Open(path)
				if err == nil {
					f.Close()
					// Success! Re-queue the job
					log.Printf("Permission granted for %s. Resuming...", path)
					addJob(path)
					wg.Done() // Done with this retry task (addJob adds a new wg)
					return
				}
				if !errors.Is(err, fs.ErrPermission) {
					// Some other error, give up
					log.Printf("Giving up on %s: %v", path, err)
					wg.Done()
					return
				}
				// Still permission denied, continue waiting
			}
		}
	}

	// Worker logic
	startWorker := func() {
		for {
			select {
			case <-doneCh:
				// Draining mode: consume remaining jobs to correct wg count
				for {
					select {
					case _, ok := <-jobs:
						if !ok {
							return
						}
						wg.Done()
					default:
						return // Queue empty, exit
					}
				}
			case path, ok := <-jobs:
				if !ok {
					return
				}

				// Check cancellation before processing to convert to drainer
				select {
				case <-doneCh:
					wg.Done()
					continue
				default:
				}

				// Acquire token
				select {
				case sem <- struct{}{}:
				case <-doneCh:
					wg.Done() // We took a job but couldn't process it
					continue
				}

				// We handle directory read inside the worker to manage the "permission wait" logic
				entries, err := os.ReadDir(path)

				// Handle Permission Error specifically
				if err != nil && errors.Is(err, fs.ErrPermission) {
					// Release token immediately so others can work
					<-sem

					// Transfer the wg responsibility to the retry routine
					go retryOnPermission(path)
					continue
				}

				if err != nil {
					<-sem
					wg.Done()
					continue
				}

				// Process valid entries
				for _, d := range entries {
					// Check cancellation periodically
					select {
					case <-doneCh:
						<-sem
						wg.Done()
						// Loop will catch doneCh and enter drain mode
						goto NextLoop
					default:
					}

					fullPath := filepath.Join(path, d.Name())

					// Resumption check
					if c.resumptionPath != "" && fullPath < c.resumptionPath {
						continue
					}

					if d.IsDir() {
						select {
						case <-doneCh:
						default:
							wg.Add(1)
							go func(p string) {
								select {
								case jobs <- p:
								case <-doneCh:
									wg.Done()
								}
							}(fullPath)
						}
					} else {
						c.processFile(fullPath, d, results, doneCh)
					}
				}

				<-sem
				wg.Done()
			}
		NextLoop:
		}
	}

	// Start workers
	for i := 0; i < numWorkers; i++ {
		go startWorker()
	}

	// Submit initial job
	select {
	case jobs <- c.inputPath:
	case <-doneCh:
		wg.Done()
	}

	// Main Wait Logic
	// We only wait for consumerDone because timeout is handled inside consumer
	return <-consumerDone
}

func (c *FilesystemConverter) processFile(path string, d fs.DirEntry, results chan<- []interface{}, doneCh <-chan struct{}) {
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

	select {
	case results <- row:
	case <-doneCh:
		// Cancelled
		return
	}
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
