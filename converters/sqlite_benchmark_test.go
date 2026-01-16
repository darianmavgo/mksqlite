package converters

import (
	"fmt"
	"testing"
)

func BenchmarkGenCreateTableSQL(b *testing.B) {
	// Create a large number of column names
	numCols := 1000
	cols := make([]string, numCols)
	for i := 0; i < numCols; i++ {
		cols[i] = fmt.Sprintf("col_%d", i)
	}
	tableName := "bench_table"

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		GenCreateTableSQL(tableName, cols)
	}
}
