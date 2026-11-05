package sink

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"testing"
)

type fakeExec struct {
	batches [][]string
	failAt  int // 1-based batch number to fail; 0 = never
}

func (f *fakeExec) ExecBatch(_ context.Context, stmts []string) error {
	f.batches = append(f.batches, append([]string(nil), stmts...))
	if f.failAt > 0 && len(f.batches) == f.failAt {
		return errors.New("boom")
	}
	return nil
}

func TestWriterBatchesAndFlush(t *testing.T) {
	f := &fakeExec{}
	w := NewWriter(context.Background(), f, Options{BatchSize: 2})
	for i := 0; i < 5; i++ {
		fmt.Fprintf(w, "INSERT INTO t VALUES (%d);\n", i)
	}
	fmt.Fprint(w, "INSERT INTO t VALUES (5)") // unterminated tail
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	var sizes []int
	for _, b := range f.batches {
		sizes = append(sizes, len(b))
	}
	if want := []int{2, 2, 2}; !reflect.DeepEqual(sizes, want) {
		t.Fatalf("batch sizes %v, want %v", sizes, want)
	}
	if last := f.batches[2][1]; last != "INSERT INTO t VALUES (5)" {
		t.Fatalf("last stmt %q", last)
	}
}

func TestWriterErrorStopsWrites(t *testing.T) {
	f := &fakeExec{failAt: 1}
	w := NewWriter(context.Background(), f, Options{BatchSize: 1})
	if _, err := w.Write([]byte("SELECT 1;")); err == nil {
		t.Fatal("expected error from first batch")
	}
	if _, err := w.Write([]byte("SELECT 2;")); err == nil {
		t.Fatal("expected fail-fast on later write")
	}
	if err := w.Close(); err == nil || !strings.Contains(err.Error(), "boom") {
		t.Fatalf("Close err = %v", err)
	}
	if len(f.batches) != 1 {
		t.Fatalf("executor called %d times", len(f.batches))
	}
}

func TestIfExistsRewrite(t *testing.T) {
	cases := []struct {
		mode IfExists
		want []string
	}{
		{Fail, []string{"CREATE TABLE t (a TEXT)"}},
		{Replace, []string{"DROP TABLE IF EXISTS t", "CREATE TABLE t (a TEXT)"}},
		{Append, []string{"CREATE TABLE IF NOT EXISTS t (a TEXT)"}},
	}
	for _, c := range cases {
		f := &fakeExec{}
		w := NewWriter(context.Background(), f, Options{IfExists: c.mode})
		w.Write([]byte("CREATE TABLE t (a TEXT);\nINSERT INTO t VALUES ('x');"))
		if err := w.Close(); err != nil {
			t.Fatal(err)
		}
		got := f.batches[0][:len(f.batches[0])-1] // drop the INSERT
		if !reflect.DeepEqual(got, c.want) {
			t.Errorf("mode %d: got %q, want %q", c.mode, got, c.want)
		}
	}
}
