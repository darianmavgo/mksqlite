// Package sink lands the SQL text produced by common.StreamConverter into a
// destination (e.g. a remote database) without an intermediate file.
package sink

import (
	"context"
	"errors"
	"io"
	"strings"

	"github.com/darianmavgo/mksqlite/converters/common"
)

// Executor is the one thing a destination implements: run statements, in
// order, as a unit. ExecBatch is never called with an empty slice.
type Executor interface {
	ExecBatch(ctx context.Context, stmts []string) error
}

// IfExists says what to do when the target table already exists.
type IfExists int

const (
	Fail    IfExists = iota // plain CREATE TABLE; errors if the table exists
	Replace                 // DROP TABLE IF EXISTS first, then CREATE TABLE
	Append                  // CREATE TABLE IF NOT EXISTS
)

// DefaultBatchSize is the number of statements sent per ExecBatch call.
const DefaultBatchSize = 500

// Options configures a Writer.
type Options struct {
	BatchSize int // statements per batch; <= 0 means DefaultBatchSize
	IfExists  IfExists
}

// Writer is an io.WriteCloser that splits written SQL into statements and
// sends them to an Executor in batches. Close flushes the remainder.
type Writer struct {
	ctx     context.Context
	ex      Executor
	opts    Options
	split   Splitter
	pending []string
	err     error
}

var _ io.WriteCloser = (*Writer)(nil)

// NewWriter returns a Writer that sends batches to ex.
func NewWriter(ctx context.Context, ex Executor, opts Options) *Writer {
	if opts.BatchSize <= 0 {
		opts.BatchSize = DefaultBatchSize
	}
	return &Writer{ctx: ctx, ex: ex, opts: opts}
}

// Write implements io.Writer. After the first error every Write fails fast.
func (w *Writer) Write(p []byte) (int, error) {
	if w.err != nil {
		return 0, w.err
	}
	for _, stmt := range w.split.Feed(p) {
		w.add(stmt)
	}
	if w.err == nil && len(w.pending) >= w.opts.BatchSize {
		w.flush()
	}
	if w.err != nil {
		return 0, w.err
	}
	return len(p), nil
}

// Close flushes buffered statements. It does not close the Executor.
func (w *Writer) Close() error {
	if w.err == nil {
		if stmt := w.split.Flush(); stmt != "" {
			w.add(stmt)
		}
		w.flush()
	}
	return w.err
}

func (w *Writer) add(stmt string) {
	w.pending = append(w.pending, rewriteCreate(stmt, w.opts.IfExists)...)
}

func (w *Writer) flush() {
	if len(w.pending) == 0 {
		return
	}
	batch := w.pending
	w.pending = nil
	if err := w.ex.ExecBatch(w.ctx, batch); err != nil {
		w.err = err
	}
}

// Stream runs conv.ConvertToSQL into a Writer for ex and returns the first error.
func Stream(ctx context.Context, conv common.StreamConverter, ex Executor, opts Options) error {
	w := NewWriter(ctx, ex, opts)
	convErr := conv.ConvertToSQL(ctx, w)
	closeErr := w.Close()
	return errors.Join(convErr, closeErr)
}

const createTable = "CREATE TABLE "

// rewriteCreate applies the IfExists policy to a CREATE TABLE statement and
// passes every other statement through unchanged.
func rewriteCreate(stmt string, mode IfExists) []string {
	if mode == Fail || len(stmt) <= len(createTable) || !strings.EqualFold(stmt[:len(createTable)], createTable) {
		return []string{stmt}
	}
	rest := stmt[len(createTable):]
	if strings.HasPrefix(strings.ToUpper(rest), "IF NOT EXISTS ") {
		return []string{stmt}
	}
	if mode == Append {
		return []string{createTable + "IF NOT EXISTS " + rest}
	}
	name := rest
	if i := strings.IndexAny(rest, " ("); i >= 0 {
		name = rest[:i]
	}
	return []string{"DROP TABLE IF EXISTS " + name, stmt}
}
