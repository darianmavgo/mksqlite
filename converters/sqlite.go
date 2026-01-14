// DO NOT MODIFY: This file is finalized. Any changes should be discussed and approved.
package converters

import (
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strings"

	_ "github.com/mattn/go-sqlite3"
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

// GenCreateTableSQL generates a CREATE TABLE SQL statement
func GenCreateTableSQL(tableName string, columnNames []string) string {
	colTypes := GenColumnTypes(columnNames)
	sql := "CREATE TABLE " + tableName + " ("
	for i, name := range columnNames {
		sql += name + " " + colTypes[i]
		if i < len(columnNames)-1 {
			sql += ", "
		}
	}
	sql += ")"
	return sql
}

// ImportToSQLite imports data from a RowProvider into a SQLite database
func ImportToSQLite(provider RowProvider, dbPath string) error {
	// Ensure output directory exists
	dir := filepath.Dir(dbPath)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return fmt.Errorf("failed to create output directory: %w", err)
	}

	// Remove existing database file if it exists
	if _, err := os.Stat(dbPath); err == nil {
		if err := os.Remove(dbPath); err != nil {
			return fmt.Errorf("failed to remove existing database: %w", err)
		}
	}

	// Connect to SQLite database
	db, err := sql.Open("sqlite3", dbPath)
	if err != nil {
		return fmt.Errorf("failed to open database: %w", err)
	}
	defer db.Close()

	tableNames := provider.GetTableNames()
	for _, tableName := range tableNames {
		headers := provider.GetHeaders(tableName)
		rows := provider.GetRows(tableName)

		if len(headers) == 0 && len(rows) == 0 {
			continue // Skip empty tables
		}

		// Create table
		createTableSQL := GenCreateTableSQL(tableName, headers)
		_, err = db.Exec(createTableSQL)
		if err != nil {
			return fmt.Errorf("failed to create table %s: %w", tableName, err)
		}

		// Prepare insert statement
		insertSQL, err := GenPreparedStmt(tableName, headers, InsertStmt)
		if err != nil {
			return fmt.Errorf("failed to generate insert statement for table %s: %w", tableName, err)
		}
		stmt, err := db.Prepare(insertSQL)
		if err != nil {
			return fmt.Errorf("failed to prepare insert statement for table %s: %w", tableName, err)
		}
		defer stmt.Close()

		// Insert rows
		for _, row := range rows {
			// Ensure row has the same number of columns as headers
			if len(row) < len(headers) {
				// Pad with nil or empty string?
				// Interface{} allows anything, but we need to match the placeholders.
				// For simplicity, let's append nil (which becomes NULL in sqlite)
				newRow := make([]interface{}, len(headers))
				copy(newRow, row)
				row = newRow
			} else if len(row) > len(headers) {
				row = row[:len(headers)]
			}

			_, err = stmt.Exec(row...)
			if err != nil {
				return fmt.Errorf("failed to insert row in table %s: %w", tableName, err)
			}
		}
	}

	return nil
}
