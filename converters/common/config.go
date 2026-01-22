package common

// ConversionConfig holds configuration options for the conversion process.
type ConversionConfig struct {
	// AdvancedHeaderDetection enables heuristic scanning of the first 10 rows
	// to determine the best header row.
	AdvancedHeaderDetection bool
}
