package markdown

import (
	"context"
	"fmt"
	"io"
	"strings"
	"testing"
)

func BenchmarkScanRows(b *testing.B) {
	// Generate a large markdown table
	var sb strings.Builder
	sb.WriteString("### BenchTable\n")
	sb.WriteString("| Col1 | Col2 | Col3 | Col4 | Col5 |\n")
	sb.WriteString("|---|---|---|---|---|\n")
	// 1000 rows
	for i := 0; i < 1000; i++ {
		sb.WriteString(fmt.Sprintf("| val%d_1 | val%d_2 | val%d_3 | val%d_4 | val%d_5 |\n", i, i, i, i, i))
	}

	content := sb.String()
	conv, err := NewMarkdownConverter(strings.NewReader(content), nil)
	if err != nil {
		b.Fatalf("Failed to create converter: %v", err)
	}

	tableName := "benchtable"
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

func BenchmarkConvertToSQL(b *testing.B) {
	// Generate a large markdown table with values containing single quotes
	var sb strings.Builder
	sb.WriteString("### BenchTableSQL\n")
	sb.WriteString("| Col1 | Col2 |\n")
	sb.WriteString("|---|---|\n")
	// 1000 rows
	for i := 0; i < 1000; i++ {
		// Include single quotes to trigger escaping logic
		sb.WriteString(fmt.Sprintf("| val'ue%d | o'the'r%d |\n", i, i))
	}

	content := sb.String()
	conv, err := NewMarkdownConverter(strings.NewReader(content), nil)
	if err != nil {
		b.Fatalf("Failed to create converter: %v", err)
	}

	ctx := context.Background()

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		if err := conv.ConvertToSQL(ctx, io.Discard); err != nil {
			b.Fatalf("ConvertToSQL failed: %v", err)
		}
	}
}
