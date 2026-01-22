package common

import "io"

// Request represents the minimal request structure needed for file conversion
type Request struct {
	AbsFilePath string // Path to the input file
	StageSqlite string // Path to the output SQLite file
	Table       string // Table name (optional, defaults to CSVTB)
}

// StreamConverter defines the interface for converting data streams to SQL output
type StreamConverter interface {
	ConvertToSQL(writer io.Writer) error
}

// RowProvider defines the interface for providing data to be inserted into SQLite
type RowProvider interface {
	GetTableNames() []string
	GetHeaders(tableName string) []string
	// ScanRows iterates over rows for the given table.
	// It calls the yield function for each row.
	// The yield function accepts a row and an optional error associated with that row.
	// If yield returns an error, iteration stops and that error is returned.
	ScanRows(tableName string, yield func([]interface{}, error) error) error
}

// Driver defines the interface that must be implemented by each converter driver.
type Driver interface {
	// Open returns a new RowProvider instance that reads from the given source.
	// The returned RowProvider should also implement StreamConverter if SQL export is supported.
	Open(source io.Reader, config *ConversionConfig) (RowProvider, error)
}
