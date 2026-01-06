package converters

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
)

const (
	TBPRE = "tb"
	CLPRE = "cl"
)

var (
	space = regexp.MustCompile(`\s+`)
	reg   = regexp.MustCompile(`[^a-zA-Z0-9 _]+`)
)

func GenCompliantNames(rawnames []string, prefix string) []string {
	gorgeous := make([]string, len(rawnames))

	counter := map[string]int{}
	for idx, item := range rawnames {
		item = strings.TrimSpace(item)
		item = reg.ReplaceAllString(item, "")
		item = space.ReplaceAllString((item), "_")
		item = strings.ToLower(item)

		// If stripping non-compliant chars leaves us with nothing, give it a default index name
		if len(item) == 0 {
			item = fmt.Sprintf("%s%d", prefix, idx)
		}

		// Now check for duplicates
		counter[item]++
		switch {
		case counter[item] == 1:
			// normal and ready.
			gorgeous[idx] = strings.ToLower(item)
		case counter[item] > 1:
			// duplicate found, will need to rename.
			gorgeous[idx] = fmt.Sprintf("%s%d_%s", prefix, idx, item)
		}
		if len(item) > 0 && counter[item] < 2 {
			if strings.HasPrefix(strings.ToLower(item), prefix) {
				gorgeous[idx] = strings.ToLower(item)
			} else {
				gorgeous[idx] = CLPRE + strings.ToLower(item)
			}
		} else {
			gorgeous[idx] = fmt.Sprintf("f%s_%d", strings.ToLower(item), counter[item])
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

// CalcColumnCount calculates the maximum number of columns based on one raw line.
// We can make this smarter later by sampling more lines.
// This where I should eventually document detected/assumed options as some kind of config object.
func ColumnCount(rawline string, delimiter string) int {
	// make this smarter later.
	if delimiter == "" {
		commonDelimiters := []string{",", "\t", ";", "|"}
		winner := 0
		// count each common delimiter and pick the one with the most splits.
		for idx, candidate := range commonDelimiters {
			ct := strings.Count(rawline, candidate)
			if ct > winner {
				winner = ct
				delimiter = commonDelimiters[idx]
			}

		}
	}
	return strings.Count(rawline, delimiter)

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
