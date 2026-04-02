package filesystem

import (
	"bytes"
	"context"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	_ "modernc.org/sqlite"
)

func BenchmarkExecutionSpeed(b *testing.B) {
	// Setup: Create a directory with many files
	tempDir := b.TempDir()

	// Create 1000 files to make the INSERTs significant
	for i := 0; i < 1000; i++ {
		fname := filepath.Join(tempDir, fmt.Sprintf("file_%d.txt", i))
		if err := os.WriteFile(fname, []byte("benchmark content"), 0644); err != nil {
			b.Fatalf("failed to create file: %v", err)
		}
	}

	converter, err := NewFilesystemConverter(tempDir)
	if err != nil {
		b.Fatalf("Failed to create converter: %v", err)
	}

	// Generate SQL once to avoid IO noise in measurement?
	// No, the task is about wrapping the generation itself so the OUTPUT contains the transaction.
	// So we need to generate the SQL and then measure how long it takes to execute it.

	// However, we want to measure the impact of the *change* in the SQL.
	// So we generate the SQL, then measure execution.

	var sqlBuffer bytes.Buffer
	if err := converter.ConvertToSQL(context.Background(), &sqlBuffer); err != nil {
		b.Fatalf("ConvertToSQL failed: %v", err)
	}

	sqlQuery := sqlBuffer.String()

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		// Create a fresh in-memory DB for each iteration to avoid unique constraint errors if any
		// or just use a new file.
		dbPath := filepath.Join(b.TempDir(), fmt.Sprintf("bench_%d.db", i))
		db, err := sql.Open("sqlite", dbPath)
		if err != nil {
			b.Fatalf("failed to open db: %v", err)
		}

		start := time.Now()
		_, err = db.Exec(sqlQuery)
		if err != nil {
			db.Close()
			b.Fatalf("failed to execute sql: %v", err)
		}
		duration := time.Since(start)
		b.ReportMetric(float64(duration.Nanoseconds()), "ns/op") // Custom metric just in case

		db.Close()
	}
}
