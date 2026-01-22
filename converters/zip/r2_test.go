package zip_test

import (
	"fmt"
	"io"
	"github.com/darianmavgo/mksqlite/converters"
	"github.com/darianmavgo/mksqlite/converters/zip"
	"net/http"
	"os"
	"path/filepath"
	"testing"
)

// HTTPRangeReader implements SizableReaderAt for HTTP resources
type HTTPRangeReader struct {
	url        string
	size       int64
	downloaded int64
	offset     int64
	client     *http.Client
}

func NewHTTPRangeReader(url string) (*HTTPRangeReader, error) {
	resp, err := http.Head(url)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("HEAD invalid status: %d", resp.StatusCode)
	}
	return &HTTPRangeReader{
		url:    url,
		size:   resp.ContentLength,
		client: &http.Client{},
	}, nil
}

func (r *HTTPRangeReader) Read(p []byte) (n int, err error) {
	n, err = r.ReadAt(p, r.offset)
	r.offset += int64(n)
	return n, err
}

func (r *HTTPRangeReader) ReadAt(p []byte, off int64) (n int, err error) {
	if off >= r.size {
		return 0, io.EOF
	}

	req, err := http.NewRequest("GET", r.url, nil)
	if err != nil {
		return 0, err
	}

	end := off + int64(len(p)) - 1
	if end >= r.size {
		end = r.size - 1
	}
	req.Header.Set("Range", fmt.Sprintf("bytes=%d-%d", off, end))

	resp, err := r.client.Do(req)
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusPartialContent && resp.StatusCode != http.StatusOK {
		return 0, fmt.Errorf("unexpected status: %d", resp.StatusCode)
	}

	n, err = io.ReadFull(resp.Body, p)
	// Update stats
	r.downloaded += int64(n)

	// Handle case where we requested more than available (EOF)
	if err == io.ErrUnexpectedEOF || (err == nil && int64(n) < int64(len(p))) {
		// If we hit the end of the file, checking if we got what was available
		if off+int64(n) == r.size {
			return n, io.EOF
		}
	}
	return n, err
}

func (r *HTTPRangeReader) Size() (int64, error) {
	return r.size, nil
}

func TestZipStreamingFromR2(t *testing.T) {
	url := "https://pub-a1c6b68deb9d48e1b5783f84723c93ec.r2.dev/sample_data/history.db.zip"

	// Create range reader instead of full stream
	reader, err := NewHTTPRangeReader(url)
	if err != nil {
		t.Fatalf("Failed to create HTTPRangeReader: %v", err)
	}
	t.Logf("Total file size: %d bytes", reader.size)

	converter, err := zip.NewZipConverter(reader)
	if err != nil {
		t.Fatalf("NewZipConverter failed: %v", err)
	}
	defer converter.Close()

	outputDir := "../../test_output"
	if err := os.MkdirAll(outputDir, 0755); err != nil {
		t.Fatalf("Failed to create output directory: %v", err)
	}
	dbPath := filepath.Join(outputDir, "r2_zip_test.db")
	dbFile, err := os.Create(dbPath)
	if err != nil {
		t.Fatalf("Failed to create db file: %v", err)
	}
	defer dbFile.Close()

	err = converters.ImportToSQLite(converter, dbFile, nil)
	if err != nil {
		t.Fatalf("ImportToSQLite failed: %v", err)
	}

	// Verify bandwidth savings
	t.Logf("ImportToSQLite succeeded. Downloaded %d bytes out of %d", reader.downloaded, reader.size)

	// Ideally, for a central directory listing, we should download very little.
	// history.db.zip is ~11MB. Central directory is tiny.
	// We expect downloaded < 5% of total size.
	if reader.downloaded > reader.size/10 {
		t.Errorf("Downloaded too much data! Got %d bytes, expected < %d", reader.downloaded, reader.size/10)
	} else {
		t.Logf("Bandwidth saved: %.2f%% skipped", 100.0*(1.0-float64(reader.downloaded)/float64(reader.size)))
	}
}
