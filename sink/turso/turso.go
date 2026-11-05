// Package turso streams SQL from a common.StreamConverter into a remote Turso
// (libSQL) database, with no local file.
package turso

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"net/url"
	"os"

	"github.com/darianmavgo/mksqlite/converters/common"
	"github.com/darianmavgo/mksqlite/sink"
	_ "github.com/tursodatabase/libsql-client-go/libsql" // registers the "libsql" driver
)

// Open connects to a Turso database, e.g. url "libsql://mydb-org.turso.io".
// The auth token is passed to the driver and never logged.
func Open(dbURL, authToken string) (*sql.DB, error) {
	u, err := url.Parse(dbURL)
	if err != nil {
		return nil, fmt.Errorf("invalid turso url: %w", err)
	}
	if authToken != "" {
		q := u.Query()
		q.Set("authToken", authToken)
		u.RawQuery = q.Encode()
	}
	return sql.Open("libsql", u.String())
}

// OpenFromEnv connects using TURSO_DATABASE_URL and TURSO_AUTH_TOKEN.
func OpenFromEnv() (*sql.DB, error) {
	dbURL := os.Getenv("TURSO_DATABASE_URL")
	if dbURL == "" {
		return nil, errors.New("TURSO_DATABASE_URL is not set")
	}
	return Open(dbURL, os.Getenv("TURSO_AUTH_TOKEN"))
}

// Executor implements sink.Executor: one transaction per batch.
type Executor struct{ DB *sql.DB }

var _ sink.Executor = Executor{}

func (e Executor) ExecBatch(ctx context.Context, stmts []string) error {
	tx, err := e.DB.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("turso: begin: %w", err)
	}
	for _, s := range stmts {
		if _, err := tx.ExecContext(ctx, s); err != nil {
			tx.Rollback()
			return fmt.Errorf("turso: exec %.80q: %w", s, err)
		}
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("turso: commit: %w", err)
	}
	return nil
}

// New returns a Writer that lands written SQL in db.
func New(ctx context.Context, db *sql.DB, opts sink.Options) *sink.Writer {
	return sink.NewWriter(ctx, Executor{DB: db}, opts)
}

// Stream runs conv's SQL output into db.
func Stream(ctx context.Context, db *sql.DB, conv common.StreamConverter, opts sink.Options) error {
	return sink.Stream(ctx, conv, Executor{DB: db}, opts)
}
