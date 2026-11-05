package integration_test

import (
	"encoding/json"
	"net/http"
	"testing"
	"time"
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

// fetchAAPLRows pulls one month of daily AAPL prices from Yahoo Finance (the
// endpoint yfinance wraps) as row objects. It skips the test under -short or
// when Yahoo is unreachable or rate-limiting.
func fetchAAPLRows(t *testing.T) []map[string]any {
	t.Helper()
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
	var rows []map[string]any
	for i, ts := range res.Timestamp {
		if q.Close[i] == nil || q.Open[i] == nil || q.High[i] == nil || q.Low[i] == nil || q.Volume[i] == nil {
			continue
		}
		rows = append(rows, map[string]any{
			"date":   time.Unix(ts, 0).UTC().Format("2006-01-02"),
			"open":   *q.Open[i],
			"high":   *q.High[i],
			"low":    *q.Low[i],
			"close":  *q.Close[i],
			"volume": *q.Volume[i],
		})
	}
	return rows
}
