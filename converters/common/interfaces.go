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
	ConvertToSQL(reader io.Reader, writer io.Writer) error
}

// RowProvider defines the interface for providing data to be inserted into SQLite
type RowProvider interface {
	GetTableNames() []string
	GetHeaders(tableName string) []string
	// ScanRows iterates over rows for the given table.
	// It calls the yield function for each row.
	// If yield returns an error, iteration stops and that error is returned.
	ScanRows(tableName string, yield func([]interface{}) error) error
}
