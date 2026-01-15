package converters

import (
	"database/sql"
	"os"
	"path/filepath"
	"strings"
	"testing"

	_ "github.com/mattn/go-sqlite3"
)

func TestFilesystemConvertFile(t *testing.T) {
	// Create a temporary directory for testing
	tempDir := t.TempDir()

	// Create some files
	files := []struct {
		path string
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

	// Open the directory as a file
	dirFile, err := os.Open(tempDir)
	if err != nil {
		t.Fatalf("failed to open directory: %v", err)
	}
	defer dirFile.Close()

	converter, err := NewFilesystemConverter(dirFile)
	if err != nil {
		t.Fatalf("Failed to create Filesystem converter: %v", err)
	}

	outputPath := filepath.Join(tempDir, "output.db")
	outFile, err := os.Create(outputPath)
	if err != nil {
		t.Fatalf("failed to create output file: %v", err)
	}
	defer outFile.Close()

	err = ImportToSQLite(converter, outFile)
	if err != nil {
		t.Fatalf("ImportToSQLite failed: %v", err)
	}

	// Verify database content
	db, err := sql.Open("sqlite3", outputPath)
	if err != nil {
		t.Fatalf("failed to open db: %v", err)
	}
	defer db.Close()

	rows, err := db.Query("SELECT path, name, size, is_dir FROM tb0 ORDER BY path")
	if err != nil {
		t.Fatalf("failed to query db: %v", err)
	}
	defer rows.Close()

	var count int
	for rows.Next() {
		var path, name string
		var size int64
		var isDir int
		if err := rows.Scan(&path, &name, &size, &isDir); err != nil {
			t.Fatalf("failed to scan row: %v", err)
		}
		t.Logf("Found: path=%s, name=%s, size=%d, isDir=%d", path, name, size, isDir)
		count++
	}

	// We expect at least the files created, plus directories including root
	if count < len(files) {
		t.Errorf("Expected at least %d rows, got %d", len(files), count)
	}
}

func TestFilesystemConvertToSQL(t *testing.T) {
	// Create a temporary directory for testing
	tempDir := t.TempDir()

	// Create a file
	path := filepath.Join(tempDir, "test.txt")
	err := os.WriteFile(path, []byte("test"), 0644)
	if err != nil {
		t.Fatalf("failed to create file: %v", err)
	}

	// Open the directory as a file (needed for the hack)
	dirFile, err := os.Open(tempDir)
	if err != nil {
		t.Fatalf("failed to open temp dir: %v", err)
	}
	defer dirFile.Close()

	converter, err := NewFilesystemConverter(dirFile)
	if err != nil {
		t.Fatalf("Failed to create Filesystem converter: %v", err)
	}

	outputPath := filepath.Join(tempDir, "output.sql")
	outputFile, err := os.Create(outputPath)
	if err != nil {
		t.Fatalf("failed to create output file: %v", err)
	}
	defer outputFile.Close()

	// Note: ConvertToSQL also requires *os.File from reader.
	// But we initialized converter with dirFile, which is *os.File.
	// ConvertToSQL also takes reader, but it seems FilesystemConverter.ConvertToSQL ignores the reader argument?
	// Let's check FilesystemConverter.ConvertToSQL.
	// It does: `file, ok := reader.(*os.File)`.
	// So we need to pass dirFile again (or reopen it) to ConvertToSQL.
	// Since dirFile is already open, passing it again is fine (random access not strictly needed for the path check, but WalkDir uses path).
	// But `ConvertToSQL` calls `file.Stat()` and `file.Name()`.

	// Rewind? Not needed for Name(), Stat().
	// But `ConvertToSQL` doesn't consume the file content, it uses the path.
	// So passing dirFile is fine.

	err = converter.ConvertToSQL(dirFile, outputFile)
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
