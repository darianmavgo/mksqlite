package common

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

func BenchmarkGenCompliantNames(b *testing.B) {
	// Mix of normal names and keywords
	rawNames := []string{
		"id", "user_name", "created_at", // normal
		"select", "from", "where", // keywords
		"Group", "Order", "Limit", // keywords mixed case
		"   spaced   ", "with.dots", // needs cleaning
		"123start", // starts with digit
	}
	// Repeat to have a larger slice
	var bigList []string
	for i := 0; i < 100; i++ {
		bigList = append(bigList, rawNames...)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		GenCompliantNames(bigList, "cl")
	}
}
