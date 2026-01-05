package converters

import "io"

// Request represents the minimal request structure needed for file conversion
type Request struct {
	AbsFilePath string // Path to the input file
	StageSqlite string // Path to the output SQLite file
	Table       string // Table name (optional, defaults to "data")
}

// FileConverter defines the interface for converting files to SQLite databases
type FileConverter interface {
	ConvertFile(inputPath, outputPath string) error
}

// StreamConverter defines the interface for converting data streams to SQL output
type StreamConverter interface {
	ConvertToSQL(reader io.Reader, writer io.Writer) error
}

// Converter combines both file and stream conversion capabilities
type Converter interface {
	FileConverter
	StreamConverter
}
