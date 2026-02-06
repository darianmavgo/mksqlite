package filesystem

import (
	"context"
	"database/sql"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/darianmavgo/mksqlite/converters"

	_ "modernc.org/sqlite"
)

func TestFilesystemConvertFile(t *testing.T) {
	// Create a persistent directory for testing
	tempDir := "../../test_output/filesystem_test"
	if err := os.RemoveAll(tempDir); err != nil {
		t.Fatalf("failed to clean temp dir: %v", err)
	}
	if err := os.MkdirAll(tempDir, 0755); err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}

	// Create some files
	files := []struct {
		path    string
		content string
	}{
		{"file1.txt", "content1"},
		{"subdir/file2.log", "content2"},
	}

	for _, f := range files {
		path := filepath.Join(tempDir, f.path)
		err := os.MkdirAll(filepath.Dir(path), 0755)
		if err != nil {
			t.Fatalf("failed to create dir: %v", err)
		}
		err = os.WriteFile(path, []byte(f.content), 0644)
		if err != nil {
			t.Fatalf("failed to create file: %v", err)
		}
	}

	converter, err := NewFilesystemConverter(tempDir)
	if err != nil {
		t.Fatalf("Failed to create Filesystem converter: %v", err)
	}

	outputPath := filepath.Join(tempDir, "output.db")
	outFile, err := os.Create(outputPath)
	if err != nil {
		t.Fatalf("failed to create output file: %v", err)
	}
	defer outFile.Close()

	err = converters.ImportToSQLite(converter, outFile, nil)
	if err != nil {
		t.Fatalf("ImportToSQLite failed: %v", err)
	}

	// Verify database content
	db, err := sql.Open("sqlite", outputPath)
	if err != nil {
		t.Fatalf("failed to open db: %v", err)
	}
	defer db.Close()

	rows, err := db.Query("SELECT path, name, size, is_dir, mime_type, create_time, permissions FROM tb0 ORDER BY path")
	if err != nil {
		t.Fatalf("failed to query db: %v", err)
	}
	defer rows.Close()

	var count int
	for rows.Next() {
		var path, name, mimeType, createTime, permissions string
		var size int64
		var isDir int
		if err := rows.Scan(&path, &name, &size, &isDir, &mimeType, &createTime, &permissions); err != nil {
			t.Fatalf("failed to scan row: %v", err)
		}
		t.Logf("Found: path=%s, name=%s, size=%d, isDir=%d, mime=%s, created=%s, perms=%s",
			path, name, size, isDir, mimeType, createTime, permissions)

		if name == "file1.txt" {
			if !strings.Contains(mimeType, "text/plain") {
				t.Errorf("Expected file1.txt to have text/plain mime type, got %s", mimeType)
			}
		}

		if createTime == "" {
			t.Errorf("create_time is empty for %s", path)
		}

		count++
	}

	// We expect at least the files created, plus directories including root
	if count < len(files) {
		t.Errorf("Expected at least %d rows, got %d", len(files), count)
	}
}

func TestFilesystemConvertToSQL(t *testing.T) {
	// Create a persistent directory for testing
	tempDir := "../../test_output/filesystem_test_sql"
	if err := os.RemoveAll(tempDir); err != nil {
		t.Fatalf("failed to clean temp dir: %v", err)
	}
	if err := os.MkdirAll(tempDir, 0755); err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}

	// Create a file
	path := filepath.Join(tempDir, "test.txt")
	err := os.WriteFile(path, []byte("test"), 0644)
	if err != nil {
		t.Fatalf("failed to create file: %v", err)
	}

	converter, err := NewFilesystemConverter(tempDir)
	if err != nil {
		t.Fatalf("Failed to create Filesystem converter: %v", err)
	}

	outputPath := filepath.Join(tempDir, "output.sql")
	outputFile, err := os.Create(outputPath)
	if err != nil {
		t.Fatalf("failed to create output file: %v", err)
	}
	defer outputFile.Close()

	err = converter.ConvertToSQL(context.Background(), outputFile)
	if err != nil {
		t.Fatalf("ConvertToSQL failed: %v", err)
	}

	// Verify SQL content
	content, err := os.ReadFile(outputPath)
	if err != nil {
		t.Fatalf("failed to read sql file: %v", err)
	}
	sqlStr := string(content)

	if !strings.Contains(sqlStr, "CREATE TABLE tb0") {
		t.Error("Expected CREATE TABLE in output")
	}
	if !strings.Contains(sqlStr, "INSERT INTO tb0") {
		t.Error("Expected INSERT INTO in output")
	}
	if !strings.Contains(sqlStr, "test.txt") {
		t.Error("Expected filename in output")
	}
}
