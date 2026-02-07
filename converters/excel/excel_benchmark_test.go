package excel

import (
	"context"
	"os"
	"testing"
)

func BenchmarkExcelConvertToSQL(b *testing.B) {
	inputPath := "../../sample_data/20mb.xlsx"
	if _, err := os.Stat(inputPath); os.IsNotExist(err) {
		b.Skipf("Sample file not found: %s", inputPath)
	}

	f, err := os.Open(inputPath)
	if err != nil {
		b.Fatalf("Failed to open input file: %v", err)
	}
	defer f.Close()

	converter, err := NewExcelConverter(f)
	if err != nil {
		b.Fatalf("Failed to create converter: %v", err)
	}
	defer converter.Close()

	// Create a temp file for output to measure real I/O impact (reduced syscalls)
	outFile, err := os.CreateTemp("", "benchmark_excel_*.sql")
	if err != nil {
		b.Fatalf("Failed to create temp file: %v", err)
	}
	defer os.Remove(outFile.Name())
	defer outFile.Close()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		// Reset file position
		_, err := outFile.Seek(0, 0)
		if err != nil {
			b.Fatalf("Failed to seek output file: %v", err)
		}

		// Truncate file to avoid growing indefinitely and affecting performance
		err = outFile.Truncate(0)
		if err != nil {
			b.Fatalf("Failed to truncate output file: %v", err)
		}

		err = converter.ConvertToSQL(context.Background(), outFile)
		if err != nil {
			b.Fatalf("ConvertToSQL failed: %v", err)
		}
	}
}
