package common

import "testing"

func TestDetectDelimiter(t *testing.T) {
	tests := []struct {
		name     string
		line     string
		expected rune
	}{
		{"Empty", "", ','},
		{"Comma", "a,b,c", ','},
		{"Tab", "a\tb\tc", '\t'},
		{"Semicolon", "a;b;c", ';'},
		{"Pipe", "a|b|c", '|'},
		{"MixedPreferComma", "a,b;c", ','}, // 1 comma, 1 semicolon. Logic picks first max.
		{"MixedPreferTab", "a\tb\tc,d", '\t'},
		{"NoDelimiter", "abc", ','}, // Default
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := DetectDelimiter(tt.line)
			if got != tt.expected {
				t.Errorf("DetectDelimiter(%q) = %q, want %q", tt.line, got, tt.expected)
			}
		})
	}
}

func TestColumnCount(t *testing.T) {
	tests := []struct {
		name      string
		line      string
		delimiter rune
		expected  int
	}{
		{"Empty", "", ',', 0},
		{"Single", "abc", ',', 1},
		{"CommaTwo", "a,b", ',', 2},
		{"CommaThree", "a,b,c", ',', 3},
		{"TabTwo", "a\tb", '\t', 2},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := ColumnCount(tt.line, tt.delimiter)
			if got != tt.expected {
				t.Errorf("ColumnCount(%q, %q) = %d, want %d", tt.line, tt.delimiter, got, tt.expected)
			}
		})
	}
}
