package integration_test

import (
	"bytes"
	"context"
	"encoding/csv"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/darianmavgo/mksqlite/converters"
	_ "github.com/darianmavgo/mksqlite/converters/all"
	"github.com/darianmavgo/mksqlite/converters/common"
	"github.com/darianmavgo/mksqlite/sink"
	"github.com/darianmavgo/mksqlite/sink/turso"
)

// TestYFinanceToTurso streams the same Yahoo Finance rows into a remote Turso
// table without a local file. It needs TURSO_DATABASE_URL and TURSO_AUTH_TOKEN.
func TestYFinanceToTurso(t *testing.T) {
	if os.Getenv("TURSO_DATABASE_URL") == "" || os.Getenv("TURSO_AUTH_TOKEN") == "" {
		t.Skip("TURSO_DATABASE_URL / TURSO_AUTH_TOKEN not set")
	}
	rows := fetchAAPLRows(t)

	// Render as CSV so ConversionConfig.TableName picks the destination table.
	var buf bytes.Buffer
	w := csv.NewWriter(&buf)
	w.Write([]string{"date", "open", "high", "low", "close", "volume"})
	for _, r := range rows {
		w.Write([]string{
			r["date"].(string),
			fmt.Sprint(r["open"]), fmt.Sprint(r["high"]), fmt.Sprint(r["low"]),
			fmt.Sprint(r["close"]), fmt.Sprint(r["volume"]),
		})
	}
	w.Flush()

	const table = "yfinance_aapl_test"
	provider, err := converters.Open("csv", &buf, &common.ConversionConfig{TableName: table})
	if err != nil {
		t.Fatal(err)
	}
	conv, ok := provider.(common.StreamConverter)
	if !ok {
		t.Fatal("csv converter is not a StreamConverter")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	db, err := turso.OpenFromEnv()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	t.Cleanup(func() { db.Exec("DROP TABLE IF EXISTS " + table) }) // test-owned table

	if err := turso.Stream(ctx, db, conv, sink.Options{IfExists: sink.Replace}); err != nil {
		t.Fatalf("stream to turso: %v", err)
	}

	var n int
	if err := db.QueryRowContext(ctx, "SELECT COUNT(*) FROM "+table).Scan(&n); err != nil {
		t.Fatal(err)
	}
	if n != len(rows) {
		t.Fatalf("remote rows = %d, want %d", n, len(rows))
	}
	fmt.Printf("Turso table: %s (%d rows)\n", table, n)
}
