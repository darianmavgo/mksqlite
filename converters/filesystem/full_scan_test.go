package filesystem

import (
	"io"
	"os"
	"testing"
	"time"

	"github.com/darianmavgo/mksqlite/converters"
	_ "modernc.org/sqlite"
)

// TestGenerateRootIndex scans the file system from root ("/") and generates Index.sqlite.
// WARNING: This test may take a very long time to complete as it scans the entire drive.
// It relies on the FilesystemConverter handling permission errors gracefully
// and timing out by default after 20 seconds.
func TestGenerateRootIndex(t *testing.T) {
	// Use a path relative to the package directory, putting it in the project root
	outputDB := "../../Index.sqlite"

	t.Logf("Starting full system scan from / to %s", outputDB)

	// Create the converter for root
	// Note: Timeout defaults to 20 seconds automatically
	converter, err := NewFilesystemConverter("/")
	if err != nil {
		t.Fatalf("Failed to initialize converter for /: %v", err)
	}

	// Create/Overwrite the output file
	f, err := os.Create(outputDB)
	if err != nil {
		t.Fatalf("Failed to create output file %s: %v", outputDB, err)
	}
	defer f.Close()

	// Wrap the file to hide it from ImportToSQLite's optimization
	// to avoid SQLITE_BUSY errors caused by open file handle
	wrappedWriter := struct{ io.Writer }{f}

	// Configure options
	opts := &converters.ImportOptions{
		LogErrors: true, // Log permission errors to _mksqlite_errors table
		Verbose:   true, // Show progress in test logs
	}

	// Track time
	start := time.Now()

	// Execute import
	// Note: This blocks until completion. Use go test -timeout 0 to disable timeout if needed.
	err = converters.ImportToSQLite(converter, wrappedWriter, opts)
	if err != nil {
		t.Errorf("ImportFromRoot failed: %v", err)
	}

	t.Logf("Scan completed in %s", time.Since(start))

	// Optional: verify it generated something valid
	info, err := f.Stat()
	if err == nil {
		t.Logf("Created Index.sqlite size: %d bytes", info.Size())
	}
}
