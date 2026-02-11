package html

import (
	"context"
	"fmt"
	"strings"
	"testing"
)

func BenchmarkScanRows(b *testing.B) {
	// Generate a large HTML table
	var sb strings.Builder
	sb.WriteString("<html><body><table id=\"bench_table\">\n")
	sb.WriteString("<thead><tr><th>Col1</th><th>Col2</th><th>Col3</th><th>Col4</th><th>Col5</th></tr></thead>\n")
	sb.WriteString("<tbody>\n")
	// 1000 rows
	for i := 0; i < 1000; i++ {
		sb.WriteString(fmt.Sprintf("<tr><td>val%d_1</td><td>val%d_2</td><td>val%d_3</td><td>val%d_4</td><td>val%d_5</td></tr>\n", i, i, i, i, i))
	}
	sb.WriteString("</tbody></table></body></html>")

	content := sb.String()
	conv, err := NewHTMLConverter(strings.NewReader(content))
	if err != nil {
		b.Fatalf("Failed to create converter: %v", err)
	}

	tableName := "bench_table"
	ctx := context.Background()

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		err := conv.ScanRows(ctx, tableName, func(row []interface{}, err error) error {
			return err
		})
		if err != nil {
			b.Fatalf("ScanRows failed: %v", err)
		}
	}
}
