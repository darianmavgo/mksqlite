package converters

import (
	"context"
	"fmt"
	"io"
	"testing"
)

// ReusingMockProvider simulates a provider that reuses slice memory (like CSVConverter with sync.Pool)
type ReusingMockProvider struct {
	count     int
	row       []interface{}
	tableName string
	headers   []string
}

func (p *ReusingMockProvider) GetTableNames() []string {
	return []string{p.tableName}
}

func (p *ReusingMockProvider) GetHeaders(tableName string) []string {
	return p.headers
}

func (p *ReusingMockProvider) GetColumnTypes(tableName string) []string {
	return nil
}

func (p *ReusingMockProvider) ScanRows(ctx context.Context, tableName string, yield func([]interface{}, error) error) error {
	for i := 0; i < p.count; i++ {
		if err := yield(p.row[:5], nil); err != nil {
			return err
		}
	}
	return nil
}

func BenchmarkPadding(b *testing.B) {
	headerCount := 20
	headers := make([]string, headerCount)
	for i := range headers {
		headers[i] = fmt.Sprintf("col%d", i)
	}
	tableName := "bench_padding"

	b.Run("WithCapacity", func(b *testing.B) {
		rowCount := 10000
		backingRow := make([]interface{}, 5, headerCount)
		for j := 0; j < 5; j++ {
			backingRow[j] = "val"
		}

		provider := &ReusingMockProvider{
			count:     rowCount,
			row:       backingRow,
			tableName: tableName,
			headers:   headers,
		}

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			err := ImportToSQLite(provider, io.Discard, nil)
			if err != nil {
				b.Fatalf("ImportToSQLite failed: %v", err)
			}
		}
	})

	b.Run("WithoutCapacity", func(b *testing.B) {
		rowCount := 10000
		backingRow := make([]interface{}, 5, 5)
		for j := 0; j < 5; j++ {
			backingRow[j] = "val"
		}

		provider := &ReusingMockProvider{
			count:     rowCount,
			row:       backingRow,
			tableName: tableName,
			headers:   headers,
		}

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			err := ImportToSQLite(provider, io.Discard, nil)
			if err != nil {
				b.Fatalf("ImportToSQLite failed: %v", err)
			}
		}
	})
}

// BenchmarkPaddingLogic isolates the padding logic
func BenchmarkPaddingLogic(b *testing.B) {
	b.Run("Alloc", func(b *testing.B) {
		row := make([]interface{}, 5, 5)
		headers := make([]string, 20)
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			if len(row) < len(headers) {
				newRow := make([]interface{}, len(headers))
				copy(newRow, row)
				_ = newRow
			}
		}
	})
	b.Run("Reuse", func(b *testing.B) {
		row := make([]interface{}, 5, 20)
		headers := make([]string, 20)
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			if len(row) < len(headers) {
				targetLen := len(headers)
				currentLen := len(row)
				if cap(row) >= targetLen {
					row = row[:targetLen]
					clear(row[currentLen:])
					// Reset for next iter to simulate reuse
					row = row[:5]
				}
			}
		}
	})
}
