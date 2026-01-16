package zip_test

import (
	"mksqlite/converters"
	"mksqlite/converters/zip"
	"net/http"
	"os"
	"path/filepath"
	"testing"
)

func TestZipStreamingFromR2(t *testing.T) {
	url := "https://pub-a1c6b68deb9d48e1b5783f84723c93ec.r2.dev/sample_data/history.db.zip"
	resp, err := http.Get(url)
	if err != nil {
		t.Fatalf("Failed to fetch from R2: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("Failed to fetch from R2: status %d", resp.StatusCode)
	}

	converter, err := zip.NewZipConverter(resp.Body)
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

	err = converters.ImportToSQLite(converter, dbFile)
	if err != nil {
		t.Fatalf("ImportToSQLite failed: %v", err)
	}
	t.Log("ImportToSQLite succeeded")
}
