package converters

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

// Driver defines the interface that must be implemented by a converter package.
type Driver interface {
	// Open returns a new RowProvider for the given input.
	Open(io.Reader) (RowProvider, error)

	// ConvertToSQL converts the input stream to SQL statements written to writer.
	ConvertToSQL(io.Reader, io.Writer) error
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

// KEYWORDS_LOWER is the hardcoded lowercase version of KEYWORDS.
var KEYWORDS_LOWER = []string{
	"abort", "action", "add", "after", "all", "alter", "always", "analyze", "and", "as",
	"asc", "attach", "autoincrement", "before", "begin", "between", "by", "cascade", "case", "cast",
	"check", "collate", "column", "commit", "conflict", "constraint", "create", "cross", "current", "current_date",
	"current_time", "current_timestamp", "database", "default", "deferrable", "deferred", "delete", "desc", "detach", "distinct",
	"do", "drop", "each", "else", "end", "escape", "except", "exclude", "exclusive", "exists",
	"explain", "fail", "filter", "first", "following", "for", "foreign", "from", "full", "generated",
	"glob", "group", "groups", "having", "if", "ignore", "immediate", "in", "index", "indexed",
	"initially", "inner", "insert", "instead", "intersect", "into", "is", "isnull", "join", "key",
	"last", "left", "like", "limit", "match", "materialized", "natural", "no", "not", "nothing",
	"notnull", "null", "nulls", "of", "offset", "on", "or", "order", "others", "outer",
	"over", "partition", "plan", "pragma", "preceding", "primary", "query", "raise", "range", "recursive",
	"references", "regexp", "reindex", "release", "rename", "replace", "restrict", "returning", "right", "rollback",
	"row", "rows", "savepoint", "select", "set", "table", "temp", "temporary", "then", "ties",
	"to", "transaction", "trigger", "unbounded", "union", "unique", "update", "using", "vacuum", "values",
	"view", "virtual", "when", "where", "window", "with", "without",
}
