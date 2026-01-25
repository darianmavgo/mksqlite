package csv

import (
	"bytes"
	"testing"
)

func BenchmarkScanRows(b *testing.B) {
	// Generate a CSV with 10 columns and many rows
	var buf bytes.Buffer
	headers := "col1,col2,col3,col4,col5,col6,col7,col8,col9,col10\n"
	buf.WriteString(headers)
	rowStr := "val1,val2,val3,val4,val5,val6,val7,val8,val9,val10\n"
	// 1000 rows per iteration
	for i := 0; i < 1000; i++ {
		buf.WriteString(rowStr)
	}
	content := buf.Bytes()

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		reader := bytes.NewReader(content)
		converter, err := NewCSVConverter(reader)
		if err != nil {
			b.Fatalf("NewCSVConverter failed: %v", err)
		}

		err = converter.ScanRows(CSVTB, func(row []interface{}, err error) error {
			if err != nil {
				return err
			}
			// Simulate doing something with the row
			_ = row[0]
			return nil
		})
		if err != nil {
			b.Fatalf("ScanRows failed: %v", err)
		}
	}
}
