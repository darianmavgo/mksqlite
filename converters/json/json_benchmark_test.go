package json

import (
	"fmt"
	"strings"
	"testing"
)

func BenchmarkJSONScanRows(b *testing.B) {
	// Construct a large JSON array string
	var sb strings.Builder
	sb.WriteString("[")
	for i := 0; i < 5000; i++ {
		if i > 0 {
			sb.WriteString(",")
		}
		sb.WriteString(fmt.Sprintf(`{"id":%d,"name":"Person %d","data":{"a":%d,"b":"val"}}`, i, i, i))
	}
	sb.WriteString("]")
	content := sb.String()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		reader := strings.NewReader(content)
		conv, err := NewJSONConverter(reader)
		if err != nil {
			b.Fatalf("NewJSONConverter failed: %v", err)
		}

		err = conv.ScanRows("jsontb0", func(row []interface{}, err error) error {
			return nil
		})
		if err != nil {
			b.Fatalf("ScanRows failed: %v", err)
		}
	}
}
