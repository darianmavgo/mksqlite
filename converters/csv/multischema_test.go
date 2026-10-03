package csv

import (

	"strings"
	"testing"

	"github.com/darianmavgo/mksqlite/converters/common"
)

func TestMultiSchemaDetection(t *testing.T) {
	csvContent := `Statement,Header,Field Name,Field Value
Statement,Data,Title,Transaction History
Statement,Data,Period,"October 1, 2025 - October 1, 2026"
Statement,Data,WhenGenerated,"2026-10-02, 11:36:25 EDT"
Summary,Header,Field Name,Field Value
Summary,Data,Base Currency,USD
Summary,Data,Starting Cash,0.0
Summary,Data,Change,-6435.969704068
Summary,Data,Ending Cash,-6435.969704068
Transaction History,Header,Date,Account,Description,Transaction Type,Symbol,Quantity,Price,Price Currency,Gross Amount ,Commission,Net Amount
Transaction History,Data,2026-10-01,U***62325,Disbursement,Withdrawal,-,-,-,-,-5000.0,-,-5000.0
Transaction History,Data,2026-10-01,U***62325,NVDA(US67066G1040) Cash Dividend USD 0.25 per Share (Ordinary Dividend),Dividend,NVDA,-,-,-,63.25,-,63.25`

	reader := strings.NewReader(csvContent)
	config := &common.ConversionConfig{
		MultiSchema: true,
	}

	converter, err := NewCSVConverterWithConfig(reader, config)
	if err != nil {
		t.Fatalf("Failed to create converter: %v", err)
	}

	tables := converter.GetTableNames()
	t.Logf("Tables detected: %v", tables)

	if len(tables) != 3 {
		t.Errorf("Expected 3 tables, got %d", len(tables))
	}
}
