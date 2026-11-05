package sink_test

import (
	"context"
	"database/sql"
	"strings"
	"testing"

	"github.com/darianmavgo/mksqlite/converters/common"
	"github.com/darianmavgo/mksqlite/converters/csv"
	"github.com/darianmavgo/mksqlite/sink"
	_ "modernc.org/sqlite"
)

// sqliteExec applies batches to a local database in one transaction each.
type sqliteExec struct{ db *sql.DB }

func (e sqliteExec) ExecBatch(ctx context.Context, stmts []string) error {
	tx, err := e.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	for _, s := range stmts {
		if _, err := tx.ExecContext(ctx, s); err != nil {
			tx.Rollback()
			return err
		}
	}
	return tx.Commit()
}

// Streams CSV through the sink into SQLite, proving the SQL stream lands
// intact (quotes, embedded ";\n") with no destination-specific code.
func TestStreamCSVIntoLocalSQLite(t *testing.T) {
	data := "a,b\n1,x\n2,it's\n3,\"p;\nq\"\n"
	conv, err := csv.NewCSVConverterWithConfig(strings.NewReader(data), &common.ConversionConfig{TableName: "t"})
	if err != nil {
		t.Fatal(err)
	}
	db, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	db.SetMaxOpenConns(1)

	if err := sink.Stream(context.Background(), conv, sqliteExec{db}, sink.Options{BatchSize: 2, IfExists: sink.Replace}); err != nil {
		t.Fatal(err)
	}

	var n int
	if err := db.QueryRow(`SELECT COUNT(*) FROM t`).Scan(&n); err != nil || n != 3 {
		t.Fatalf("count = %d, err = %v", n, err)
	}
	var b string
	if err := db.QueryRow(`SELECT b FROM t WHERE a = 3`).Scan(&b); err != nil || b != "p;\nq" {
		t.Fatalf("b = %q, err = %v", b, err)
	}
}
