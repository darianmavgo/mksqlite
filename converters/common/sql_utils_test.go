package common

import (
	"testing"
)

func TestGenTablesNames(t *testing.T) {
	rawnames := []string{"Organized", "Timeline", "Raw Content", ""}
	expected := []string{"organized", "timeline", "raw_content", "tb3"}
	clean := GenTableNames(rawnames)
	t.Logf("Input: %v", rawnames)
	t.Logf("Output: %v", clean)
	for i, v := range clean {
		if v != expected[i] {
			t.Errorf("at index %d: got %s, want %s", i, v, expected[i])
		}
	}
}

func TestGenCompliantNamesDigits(t *testing.T) {
	rawnames := []string{"4658.25", "123", "abc"}
	// idx 0: "4658.25" -> "465825" -> starts with digit -> prefix "cl" + idx 0 + "465825" -> "cl0465825"
	// idx 1: "123" -> "123" -> starts with digit -> prefix "cl" + idx 1 + "123" -> "cl1123"
	// idx 2: "abc" -> "abc"
	expected := []string{"cl0465825", "cl1123", "abc"}
	clean := GenCompliantNames(rawnames, "cl")
	for i, v := range clean {
		if v != expected[i] {
			t.Errorf("at index %d: got %s, want %s", i, v, expected[i])
		}
	}
}

func TestGenCompliantNamesKeywords(t *testing.T) {
	rawnames := []string{"group", "order", "select", "table", "where"}
	expected := []string{"group_", "order_", "select_", "table_", "where_"}
	clean := GenCompliantNames(rawnames, "cl")
	for i, v := range clean {
		if v != expected[i] {
			t.Errorf("at index %d: got %s, want %s", i, v, expected[i])
		}
	}
}
