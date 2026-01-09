package converters

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
