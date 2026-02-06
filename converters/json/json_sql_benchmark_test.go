package json

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func BenchmarkJSONConvertToSQL(b *testing.B) {
	// Construct a large JSON array string
	var sb strings.Builder
	sb.WriteString("[")
	for i := 0; i < 1000; i++ {
		if i > 0 {
			sb.WriteString(",")
		}
		sb.WriteString(fmt.Sprintf(`{"id":%d,"name":"Person %d","data":{"a":%d,"b":"val"},"description":"Some long description text to make the payload larger and simulate real world data."}`, i, i, i))
	}
	sb.WriteString("]")
	content := sb.String()

	// Use temporary directory for benchmark output to avoid source tree pollution
	outDir := b.TempDir()
	outPath := filepath.Join(outDir, "json_benchmark.sql")

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		reader := strings.NewReader(content)
		conv, err := NewJSONConverter(reader)
		if err != nil {
			b.Fatalf("NewJSONConverter failed: %v", err)
		}

		f, err := os.Create(outPath)
		if err != nil {
			b.Fatalf("Failed to create output file: %v", err)
		}
		b.StartTimer()

		err = conv.ConvertToSQL(f)
		if err != nil {
			f.Close()
			b.Fatalf("ConvertToSQL failed: %v", err)
		}
		f.Close()
	}
}
