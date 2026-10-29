package integration_test

import (
	"bytes"
	"database/sql"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/darianmavgo/mksqlite/converters"
	"github.com/darianmavgo/mksqlite/converters/common"
	_ "github.com/darianmavgo/mksqlite/converters/all"
	_ "modernc.org/sqlite"
)

const chartURL = "https://query1.finance.yahoo.com/v8/finance/chart/AAPL?range=1mo&interval=1d"

type chartResponse struct {
	Chart struct {
		Result []struct {
			Timestamp  []int64 `json:"timestamp"`
			Indicators struct {
				Quote []struct {
					Open   []*float64 `json:"open"`
					High   []*float64 `json:"high"`
					Low    []*float64 `json:"low"`
					Close  []*float64 `json:"close"`
					Volume []*int64   `json:"volume"`
				} `json:"quote"`
			} `json:"indicators"`
		} `json:"result"`
	} `json:"chart"`
}

// TestYFinanceToSQLite pulls daily AAPL prices from Yahoo Finance (the endpoint
// yfinance wraps) and lands them in a local SQLite file using only the public
// library API. The output file is intentionally left on disk.
func TestYFinanceToSQLite(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping network test in -short mode")
	}

	req, _ := http.NewRequest("GET", chartURL, nil)
	req.Header.Set("User-Agent", "Mozilla/5.0 (compatible; mksqlite-test)")
	resp, err := (&http.Client{Timeout: 15 * time.Second}).Do(req)
	if err != nil {
		t.Skipf("yahoo finance unreachable: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Skipf("yahoo finance returned %s", resp.Status)
	}

	var cr chartResponse
	if err := json.NewDecoder(resp.Body).Decode(&cr); err != nil {
		t.Fatalf("decode chart response: %v", err)
	}
	if len(cr.Chart.Result) == 0 || len(cr.Chart.Result[0].Indicators.Quote) == 0 {
		t.Fatal("chart response has no data")
	}
	res := cr.Chart.Result[0]
	q := res.Indicators.Quote[0]

	// Columnar response -> row objects.
	var rows []map[string]interface{}
	for i, ts := range res.Timestamp {
		if q.Close[i] == nil || q.Open[i] == nil || q.High[i] == nil || q.Low[i] == nil || q.Volume[i] == nil {
			continue
		}
		rows = append(rows, map[string]interface{}{
			"date":   time.Unix(ts, 0).UTC().Format("2006-01-02"),
			"open":   *q.Open[i],
			"high":   *q.High[i],
			"low":    *q.Low[i],
			"close":  *q.Close[i],
			"volume": *q.Volume[i],
		})
	}
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
