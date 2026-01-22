package common

import (
	"fmt"
	"regexp"
	"strings"
)

// SQLStmtType defines the type of SQL statement to generate
type SQLStmtType string

const (
	InsertStmt SQLStmtType = "INSERT"
	UpdateStmt SQLStmtType = "UPDATE"
	SelectStmt SQLStmtType = "SELECT"
	DeleteStmt SQLStmtType = "DELETE"

	TBPRE = "tb"
	CLPRE = "cl"
)

var (
	space = regexp.MustCompile(`\s+`)
	reg   = regexp.MustCompile(`[^a-zA-Z0-9 _]+`)
)

/*
	GenCompliantNames generates names that can be used sqlite.

The rules for column names and table names are so similar I made one function
that taxes a prefix as input. lower case, snake case, strip disallowed characters.
Still need to add logic dodging sqlite keywords.
If a standardized name results in an  unusable result then the name is {prefix}{idx}
*/
func GenCompliantNames(rawnames []string, prefix string) []string {
	gorgeous := make([]string, len(rawnames))

	counter := map[string]int{}
	for idx, item := range rawnames {
		item = strings.TrimSpace(item)
		item = reg.ReplaceAllString(item, "")
		item = space.ReplaceAllString(item, "_")
		item = strings.ToLower(item)
		// remove keywords
		for _, keyword := range KEYWORDS_LOWER {
			if item == keyword {
				item = fmt.Sprintf("%s%d", prefix, idx)
				break
			}
		}

		// If stripping non-compliant chars leaves us with nothing, give it a default index name
		if len(item) == 0 {
			gorgeous[idx] = fmt.Sprintf("%s%d", prefix, idx)
			continue
		}

		// specific sqlite rule: cannot start with a number
		if item[0] >= '0' && item[0] <= '9' {
			item = fmt.Sprintf("%s%d%s", prefix, idx, item)
		}

		counter[item]++
		if counter[item] == 1 {
			gorgeous[idx] = item
		} else {
			// use counter to avoid collision
			gorgeous[idx] = fmt.Sprintf("%s%d", item, counter[item])
		}
	}
	return gorgeous
}

// GenColumnNames generates sanitized SQL column names from raw headers
// if columns are complete junk it will return cl0, cl2, cl2, etc.
func GenColumnNames(rawheaders []string) []string {
	return GenCompliantNames(rawheaders, CLPRE)
}

// GenTableNames generates sanitized SQL table names from raw table names.
// if table names are complete junk it will return tb0, tb2, tb2, etc.
func GenTableNames(rawtables []string) []string {
	return GenCompliantNames(rawtables, TBPRE)
}

func GenColumnTypes(columnnames []string) []string {
	// This is going to make everything text for now.
	// Until there is a quality way to discern types without manual input from user.
	coltypes := make([]string, len(columnnames))
	for idx := range columnnames {
		coltypes[idx] = "TEXT"
	}
	return coltypes
}

// AssessHeaderRow scans up to N rows and returns the index of the best candidate for the header row.
// It returns -1 if no suitable header is found (which implies defaults should be used).
func AssessHeaderRow(rows [][]string, maxScan int) int {
	if len(rows) == 0 {
		return 0
	}

	// Scan up to maxScan rows
	limit := len(rows)
	if limit > maxScan {
		limit = maxScan
	}

	bestScore := -1.0
	bestIndex := 0

	for i := 0; i < limit; i++ {
		row := rows[i]
		if len(row) == 0 {
			continue
		}

		score := 0.0

		// Criterion 1: All columns are non-empty strings
		nonEmptyCount := 0
		for _, val := range row {
			if strings.TrimSpace(val) != "" {
				nonEmptyCount++
			}
		}
		if nonEmptyCount == len(row) {
			score += 2.0
		} else if nonEmptyCount > len(row)/2 {
			score += 1.0
		}

		// Criterion 2: Uniqueness
		seen := make(map[string]bool)
		unique := true
		for _, val := range row {
			if seen[val] {
				unique = false
				break
			}
			seen[val] = true
		}
		if unique {
			score += 2.0
		}

		// Criterion 3: Length comparison with next row (if exists)
		if i+1 < len(rows) {
			nextRow := rows[i+1]
			// Ideally header row should have same column count as data
			if len(row) == len(nextRow) {
				score += 1.0
			}
		}

		// Criterion 4: Row length bonus
		// Prefer rows with more columns (up to a point) to avoid picking 1-column metadata rows
		score += float64(len(row)) * 0.5

		// Preference for earlier rows
		score -= float64(i) * 0.1

		if score > bestScore {
			bestScore = score
			bestIndex = i
		}
	}

	return bestIndex
}

// GenPreparedStmt generates a prepared statement for the specified operation
func GenPreparedStmt(table string, fields []string, stmtType SQLStmtType) (string, error) {
	// Validate inputs
	if table == "" || len(fields) == 0 {
		return "", fmt.Errorf("table name and fields are required")
	}

	var stmtSQL string
	switch stmtType {
	case InsertStmt:
		stmtSQL = fmt.Sprintf(`
INSERT INTO %s (
	%s
) VALUES (%s)`,
			table,
			strings.Join(fields, ","),
			strings.Repeat("?,", len(fields)-1)+"?",
		)

	case UpdateStmt:
		// Create SET clause (field1 = ?, field2 = ?, ...)
		setClause := make([]string, len(fields))
		for i, field := range fields {
			setClause[i] = fmt.Sprintf("%s = ?", field)
		}
		stmtSQL = fmt.Sprintf(`
UPDATE %s
SET %s
WHERE id = ?`,
			table,
			strings.Join(setClause, ","),
		)

	case SelectStmt:
		stmtSQL = fmt.Sprintf(`
SELECT %s
FROM %s
WHERE id = ?`,
			strings.Join(fields, ","),
			table,
		)

	case DeleteStmt:
		stmtSQL = fmt.Sprintf(`
DELETE FROM %s
WHERE id = ?`,
			table,
		)

	default:
		return "", fmt.Errorf("unsupported statement type: %s", stmtType)
	}

	// Remove any extra whitespace and validate
	stmtSQL = strings.TrimSpace(stmtSQL)

	return stmtSQL, nil
}

// GenCreateTableSQL generates a CREATE TABLE SQL statement
func GenCreateTableSQL(tableName string, columnNames []string) string {
	colTypes := GenColumnTypes(columnNames)
	var builder strings.Builder
	builder.Grow(len(tableName) + len(columnNames)*20) // Heuristic pre-allocation

	builder.WriteString("CREATE TABLE ")
	builder.WriteString(tableName)
	builder.WriteString(" (")
	for i, name := range columnNames {
		builder.WriteString(name)
		builder.WriteByte(' ')
		builder.WriteString(colTypes[i])
		if i < len(columnNames)-1 {
			builder.WriteString(", ")
		}
	}
	builder.WriteByte(')')
	return builder.String()
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
