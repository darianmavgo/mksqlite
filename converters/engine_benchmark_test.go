package converters

import (
	"fmt"
	"io"
	"testing"
)

// BenchmarkImportToSQLite measures the performance of the ImportToSQLite function.
// It uses a MockProvider to supply a large number of rows.
func BenchmarkImportToSQLite(b *testing.B) {
	// Setup a large dataset
	rowCount := 5000 // Enough to trigger multiple batches (BatchSize=1000)
	tableName := "bench_table"
	headers := []string{"col1", "col2", "col3", "col4", "col5"}

	// Pre-generate rows to avoid allocation noise during benchmark
	rows := make([][]interface{}, rowCount)
	for i := 0; i < rowCount; i++ {
		rows[i] = []interface{}{
			fmt.Sprintf("val%d-1", i),
			fmt.Sprintf("val%d-2", i),
			fmt.Sprintf("val%d-3", i),
			fmt.Sprintf("val%d-4", i),
			fmt.Sprintf("val%d-5", i),
		}
	}

	provider := &MockProvider{
		tableNames: []string{tableName},
		headers: map[string][]string{
			tableName: headers,
		},
		rows: map[string][][]interface{}{
			tableName: rows,
		},
		colTypes: map[string][]string{
			tableName: []string{"TEXT", "TEXT", "TEXT", "TEXT", "TEXT"},
		},
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		err := ImportToSQLite(provider, io.Discard, nil)
		if err != nil {
			b.Fatalf("ImportToSQLite failed: %v", err)
		}
	}
}
