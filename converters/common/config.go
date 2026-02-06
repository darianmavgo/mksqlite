package common

import (
	"strings"
)

// ConversionConfig stores configuration options for the conversion process.
type ConversionConfig struct {
	Delimiter               rune   // Delimiter used for CSV/text parsing
	TableName               string // Name of the table
	AdvancedHeaderDetection bool
	Verbose                 bool   // Enable detailed logging
	InputPath               string // Path to the input file or directory
	ResumePath              string // Path to resume processing from (for filesystem)
	ScanTimeout             string // Duration string (e.g. "20s") for timeout
}

// DetectDelimiter attempts to detect the delimiter from a raw line of text.
// It checks common delimiters and returns the one that produces the most fields.
// Defaults to comma if line is empty or no clear winner.
func DetectDelimiter(line string) rune {
	if line == "" {
		return ','
	}

	delimiters := []rune{',', '\t', ';', '|'}
	maxCount := -1
	winner := ','

	for _, delim := range delimiters {
		count := strings.Count(line, string(delim))
		if count > maxCount {
			maxCount = count
			winner = delim
		}
	}

	return winner
}

// ColumnCount calculates the number of columns based on a line and delimiter.
// It assumes the delimiter splits the line directly (ignoring quotes for estimation).
func ColumnCount(line string, delimiter rune) int {
	if line == "" {
		return 0
	}
	return strings.Count(line, string(delimiter)) + 1
}
