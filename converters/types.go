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

// RowProvider defines the interface for providing data to be inserted into SQLite
type RowProvider interface {
	GetTableNames() []string
	GetHeaders(tableName string) []string
	// ScanRows iterates over rows for the given table.
	// It calls the yield function for each row.
	// If yield returns an error, iteration stops and that error is returned.
	ScanRows(tableName string, yield func([]interface{}) error) error
}

// sqliteKeywords is a slice containing all possible SQLite SQL keywords.
// This list is based on the complete set recognized by SQLite (as of recent versions),
// sourced from the official documentation: https://sqlite.org/lang_keywords.html
//
// https://sqlite.org/lang_keywords.html
// These are the keywords that may require quoting if used as identifiers.
var KEYWORDS = []string{
	"ABORT",
	"ACTION",
	"ADD",
	"AFTER",
	"ALL",
	"ALTER",
	"ALWAYS",
	"ANALYZE",
	"AND",
	"AS",
	"ASC",
	"ATTACH",
	"AUTOINCREMENT",
	"BEFORE",
	"BEGIN",
	"BETWEEN",
	"BY",
	"CASCADE",
	"CASE",
	"CAST",
	"CHECK",
	"COLLATE",
	"COLUMN",
	"COMMIT",
	"CONFLICT",
	"CONSTRAINT",
	"CREATE",
	"CROSS",
	"CURRENT",
	"CURRENT_DATE",
	"CURRENT_TIME",
	"CURRENT_TIMESTAMP",
	"DATABASE",
	"DEFAULT",
	"DEFERRABLE",
	"DEFERRED",
	"DELETE",
	"DESC",
	"DETACH",
	"DISTINCT",
	"DO",
	"DROP",
	"EACH",
	"ELSE",
	"END",
	"ESCAPE",
	"EXCEPT",
	"EXCLUDE",
	"EXCLUSIVE",
	"EXISTS",
	"EXPLAIN",
	"FAIL",
	"FILTER",
	"FIRST",
	"FOLLOWING",
	"FOR",
	"FOREIGN",
	"FROM",
	"FULL",
	"GENERATED",
	"GLOB",
	"GROUP",
	"GROUPS",
	"HAVING",
	"IF",
	"IGNORE",
	"IMMEDIATE",
	"IN",
	"INDEX",
	"INDEXED",
	"INITIALLY",
	"INNER",
	"INSERT",
	"INSTEAD",
	"INTERSECT",
	"INTO",
	"IS",
	"ISNULL",
	"JOIN",
	"KEY",
	"LAST",
	"LEFT",
	"LIKE",
	"LIMIT",
	"MATCH",
	"MATERIALIZED",
	"NATURAL",
	"NO",
	"NOT",
	"NOTHING",
	"NOTNULL",
	"NULL",
	"NULLS",
	"OF",
	"OFFSET",
	"ON",
	"OR",
	"ORDER",
	"OTHERS",
	"OUTER",
	"OVER",
	"PARTITION",
	"PLAN",
	"PRAGMA",
	"PRECEDING",
	"PRIMARY",
	"QUERY",
	"RAISE",
	"RANGE",
	"RECURSIVE",
	"REFERENCES",
	"REGEXP",
	"REINDEX",
	"RELEASE",
	"RENAME",
	"REPLACE",
	"RESTRICT",
	"RETURNING",
	"RIGHT",
	"ROLLBACK",
	"ROW",
	"ROWS",
	"SAVEPOINT",
	"SELECT",
	"SET",
	"TABLE",
	"TEMP",
	"TEMPORARY",
	"THEN",
	"TIES",
	"TO",
	"TRANSACTION",
	"TRIGGER",
	"UNBOUNDED",
	"UNION",
	"UNIQUE",
	"UPDATE",
	"USING",
	"VACUUM",
	"VALUES",
	"VIEW",
	"VIRTUAL",
	"WHEN",
	"WHERE",
	"WINDOW",
	"WITH",
	"WITHOUT",
}
