package sink

import (
	"reflect"
	"testing"
)

func TestSplitterFragmentedAndQuoted(t *testing.T) {
	input := "CREATE TABLE t (a TEXT);\n\nINSERT INTO t (a) VALUES ('x;\ny''s;');\nINSERT INTO t (a) VALUES (\"q;\");\nSELECT 1"
	want := []string{
		"CREATE TABLE t (a TEXT)",
		"INSERT INTO t (a) VALUES ('x;\ny''s;')",
		"INSERT INTO t (a) VALUES (\"q;\")",
	}

	var s Splitter
	var got []string
	for i := 0; i < len(input); i++ { // byte-at-a-time
		got = append(got, s.Feed([]byte{input[i]})...)
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("got %q, want %q", got, want)
	}
	if tail := s.Flush(); tail != "SELECT 1" {
		t.Fatalf("tail = %q", tail)
	}
}

func TestSplitterEmpty(t *testing.T) {
	var s Splitter
	if got := s.Feed([]byte(" ;\n;; ")); len(got) != 0 {
		t.Fatalf("expected no statements, got %q", got)
	}
	if s.Flush() != "" {
		t.Fatal("expected empty flush")
	}
}
