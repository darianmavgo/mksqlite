package csv

import (
	"strings"
	"testing"
)

func TestCSVDelimiterDetection(t *testing.T) {
	tests := []struct {
		name     string
		content  string
		expected rune
		cols     int
	}{
		{
			name:     "Comma",
			content:  "col1,col2,col3\nval1,val2,val3",
			expected: ',',
			cols:     3,
		},
		{
			name:     "Tab",
			content:  "col1\tcol2\tcol3\nval1\tval2\tval3",
			expected: '\t',
			cols:     3,
		},
		{
			name:     "Pipe",
			content:  "col1|col2|col3\nval1|val2|val3",
			expected: '|',
			cols:     3,
		},
		{
			name:     "Semicolon",
			content:  "col1;col2;col3\nval1;val2;val3",
			expected: ';',
			cols:     3,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := strings.NewReader(tt.content)
			c, err := NewCSVConverter(r)
			if err != nil {
				t.Fatalf("Failed to create converter: %v", err)
			}

			if c.Config.Delimiter != tt.expected {
				t.Errorf("Detected delimiter %q, want %q", c.Config.Delimiter, tt.expected)
			}

			if len(c.headers) != tt.cols {
				t.Errorf("Detected %d headers, want %d", len(c.headers), tt.cols)
			}
		})
	}
}
