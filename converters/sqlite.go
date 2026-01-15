// DO NOT MODIFY: This file is finalized. Any changes should be discussed and approved.
package converters

import (
	"database/sql"
	"fmt"
	"io"
	"os"
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

	// BatchSize defines the number of rows to insert before committing a transaction.
	// This ensures that long-running streams save progress periodically.
	BatchSize = 1000
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

// ImportToSQLite imports data from a RowProvider and writes the resulting SQLite database
// to the provided io.Writer.
// If writer is an *os.File, it writes directly to that file to allow partial data persistence.
// Otherwise, it uses a temporary file for construction and copies it to the writer.
func ImportToSQLite(provider RowProvider, writer io.Writer) error {
	var dbPath string
	var useTemp bool = true

	// Check if writer is a file we can use directly
	if f, ok := writer.(*os.File); ok {
		stat, err := f.Stat()
		// Ensure it's a regular file (not stdout/pipe)
		if err == nil && (stat.Mode()&os.ModeCharDevice) == 0 {
			dbPath = f.Name()
			useTemp = false
			// We need to close the file handle passed in?
			// The caller owns it.
			// SQLite will open its own handle.
			// Ideally, we should sync and close the handle, but we can't close it as we don't own it.
			// But if we write via SQLite, the file content changes.
			// The file handle 'f' might have an offset. SQLite ignores that as it opens by path.
		}
	}

	if useTemp {
		// Create a temporary file
		tmpFile, err := os.CreateTemp("", "mksqlite-*.db")
		if err != nil {
			return fmt.Errorf("failed to create temp file: %w", err)
		}
		dbPath = tmpFile.Name()
		tmpFile.Close() // Close it so sql.Open can use it

		defer os.Remove(dbPath) // Clean up temp file
	} else {
		// If writing directly to file, we should probably truncate it or ensure it's empty?
		// ImportToSQLiteFile used to remove it.
		// But here we received an open file handle. It might have been created with O_TRUNC.
		// We will assume the caller prepared the file (e.g. os.Create).
		// However, SQLite expects to manage the file structure.
		// If 'f' is empty, it's fine.
		// If 'f' has garbage, SQLite might fail or corrupt.
		// We'll proceed assuming it's a new or empty file.
	}

	// Connect to SQLite database
	db, err := sql.Open("sqlite3", dbPath)
	if err != nil {
		return fmt.Errorf("failed to open database: %w", err)
	}

	// Populate database
	err = populateDB(db, provider)
	db.Close() // Close database connection

	if useTemp {
		if err != nil {
			return err // If failed, don't copy
		}

		// Open temp file for reading
		f, err := os.Open(dbPath)
		if err != nil {
			return fmt.Errorf("failed to open temp file for reading: %w", err)
		}
		defer f.Close()

		// Copy to writer
		if _, err := io.Copy(writer, f); err != nil {
			return fmt.Errorf("failed to write to output: %w", err)
		}
	}

	return err
}

// populateDB handles the common logic of creating tables and inserting rows
func populateDB(db *sql.DB, provider RowProvider) error {
	tableNames := provider.GetTableNames()
	for _, tableName := range tableNames {
		headers := provider.GetHeaders(tableName)

		if len(headers) == 0 {
			continue // Skip tables without headers
		}

		// Create table
		createTableSQL := GenCreateTableSQL(tableName, headers)
		_, err := db.Exec(createTableSQL)
		if err != nil {
			return fmt.Errorf("failed to create table %s: %w", tableName, err)
		}

		// Begin transaction
		tx, err := db.Begin()
		if err != nil {
			return fmt.Errorf("failed to begin transaction: %w", err)
		}

		// Prepare insert statement
		insertSQL, err := GenPreparedStmt(tableName, headers, InsertStmt)
		if err != nil {
			tx.Rollback()
			return fmt.Errorf("failed to generate insert statement for table %s: %w", tableName, err)
		}
		stmt, err := tx.Prepare(insertSQL)
		if err != nil {
			tx.Rollback()
			return fmt.Errorf("failed to prepare insert statement for table %s: %w", tableName, err)
		}

		rowCount := 0

		// Insert rows using streaming ScanRows
		err = provider.ScanRows(tableName, func(row []interface{}) error {
			// Ensure row has the same number of columns as headers
			if len(row) < len(headers) {
				// Pad with nil (NULL)
				newRow := make([]interface{}, len(headers))
				copy(newRow, row)
				row = newRow
			} else if len(row) > len(headers) {
				row = row[:len(headers)]
			}

			_, err := stmt.Exec(row...)
			if err != nil {
				return fmt.Errorf("failed to insert row in table %s: %w", tableName, err)
			}

			rowCount++
			if rowCount%BatchSize == 0 {
				stmt.Close()
				if err := tx.Commit(); err != nil {
					return fmt.Errorf("failed to commit transaction for table %s: %w", tableName, err)
				}

				// Start new transaction
				tx, err = db.Begin()
				if err != nil {
					return fmt.Errorf("failed to begin transaction: %w", err)
				}
				stmt, err = tx.Prepare(insertSQL)
				if err != nil {
					tx.Rollback()
					return fmt.Errorf("failed to prepare insert statement for table %s: %w", tableName, err)
				}
			}
			return nil
		})

		stmt.Close() // Close statement before commit/rollback

		if err != nil {
			tx.Rollback()
			return fmt.Errorf("failed to scan rows for table %s: %w", tableName, err)
		}

		if err := tx.Commit(); err != nil {
			return fmt.Errorf("failed to commit transaction for table %s: %w", tableName, err)
		}
	}
	return nil
}
