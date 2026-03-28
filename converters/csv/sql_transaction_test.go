package csv

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"
)

func TestSQLTransactionPerformance(t *testing.T) {
	// 1. Create a large CSV file
	tempDir := t.TempDir()
	csvPath := filepath.Join(tempDir, "large.csv")
	sqlPath := filepath.Join(tempDir, "output.sql")
	dbPath := filepath.Join(tempDir, "output.db")

	f, err := os.Create(csvPath)
	if err != nil {
		t.Fatalf("Failed to create CSV: %v", err)
	}

	// Write header
	f.WriteString("id,name,value,description,timestamp\n")

	// Write 10,000 rows
	for i := 0; i < 10000; i++ {
		fmt.Fprintf(f, "%d,Name%d,%d,Description for row %d,%s\n",
			i, i, i*100, i, time.Now().Format(time.RFC3339))
	}
	f.Close()

	// 2. Convert to SQL
	csvFile, err := os.Open(csvPath)
	if err != nil {
		t.Fatalf("Failed to open CSV: %v", err)
	}
	defer csvFile.Close()

	converter, err := NewCSVConverter(csvFile)
	if err != nil {
		t.Fatalf("Failed to create converter: %v", err)
	}

	sqlFile, err := os.Create(sqlPath)
	if err != nil {
		t.Fatalf("Failed to create SQL file: %v", err)
	}

	startGen := time.Now()
	err = converter.ConvertToSQL(context.Background(), sqlFile)
	sqlFile.Close()
	if err != nil {
		t.Fatalf("ConvertToSQL failed: %v", err)
	}
	genDuration := time.Since(startGen)
	t.Logf("SQL Generation time: %v", genDuration)

	// 3. Measure SQLite execution time
	// Check if sqlite3 is available
	if _, err := exec.LookPath("sqlite3"); err != nil {
		t.Skip("sqlite3 not found, skipping performance measurement")
	}

	cmd := exec.Command("sqlite3", dbPath)
	stdin, err := os.Open(sqlPath)
	if err != nil {
		t.Fatalf("Failed to open SQL file for reading: %v", err)
	}
	cmd.Stdin = stdin

	startExec := time.Now()
	output, err := cmd.CombinedOutput()
	execDuration := time.Since(startExec)

	if err != nil {
		t.Fatalf("sqlite3 execution failed: %v\nOutput: %s", err, output)
	}

	t.Logf("SQLite Execution time (10k rows): %v", execDuration)
}
