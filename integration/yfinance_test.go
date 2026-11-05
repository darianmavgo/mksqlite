package integration_test

import (
	"bytes"
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/darianmavgo/mksqlite/converters"
	_ "github.com/darianmavgo/mksqlite/converters/all"
	"github.com/darianmavgo/mksqlite/converters/common"
	_ "modernc.org/sqlite"
)

// TestYFinanceToSQLite pulls daily AAPL prices from Yahoo Finance (the endpoint
// yfinance wraps) and lands them in a local SQLite file using only the public
// library API. The output file is intentionally left on disk.
func TestYFinanceToSQLite(t *testing.T) {
	rows := fetchAAPLRows(t)
	payload, err := json.Marshal(rows)
	if err != nil {
		t.Fatal(err)
	}

	provider, err := converters.AutoOpen(bytes.NewReader(payload), &common.ConversionConfig{})
	if err != nil {
		t.Fatalf("AutoOpen: %v", err)
	}

	outDir := "test_output"
	if err := os.MkdirAll(outDir, 0o755); err != nil {
		t.Fatal(err)
	}
	outPath, _ := filepath.Abs(filepath.Join(outDir, "yfinance_AAPL.db"))
	_ = os.Remove(outPath) // start fresh; the new file is kept after the test
	out, err := os.Create(outPath)
	if err != nil {
		t.Fatal(err)
	}
	if err := converters.ImportToSQLite(provider, out, nil); err != nil {
		out.Close()
		t.Fatalf("ImportToSQLite: %v", err)
	}
	out.Close()
	t.Logf("wrote %s", outPath)
	fmt.Printf("SQLite file: %s\n", outPath)

	db, err := sql.Open("sqlite", outPath)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	tables := provider.GetTableNames()
	if len(tables) == 0 {
		t.Fatal("no tables produced")
	}
	table := tables[0]

	var n int
	if err := db.QueryRow(fmt.Sprintf(`SELECT COUNT(*) FROM "%s"`, table)).Scan(&n); err != nil {
		t.Fatal(err)
	}
	if n < 10 {
		t.Fatalf("expected >10 rows, got %d", n)
	}
	var minClose float64
	if err := db.QueryRow(fmt.Sprintf(`SELECT MIN(CAST(close AS REAL)) FROM "%s"`, table)).Scan(&minClose); err != nil {
		t.Fatalf("query close: %v", err)
	}
	if minClose <= 0 {
		t.Fatalf("expected positive close prices, min=%v", minClose)
	}
	t.Logf("table %q: %d rows, min close %.2f", table, n, minClose)
}
