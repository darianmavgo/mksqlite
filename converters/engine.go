// DO NOT MODIFY: This file is finalized. Any changes should be discussed and approved.
package converters

import (
	"database/sql"
	"fmt"
	"io"
	"github.com/darianmavgo/mksqlite/converters/common"
	"os"

	_ "github.com/mattn/go-sqlite3"
)

var (
	// BatchSize defines the number of rows to insert before committing a transaction.
	// This ensures that long-running streams save progress periodically.
	BatchSize = 1000
)

// ImportOptions defines configuration for the import process.
type ImportOptions struct {
	LogErrors bool // If true, errors are logged to a table instead of aborting.
}

// ImportToSQLite imports data from a RowProvider and writes the resulting SQLite database
// to the provided io.Writer.
// If writer is an *os.File, it writes directly to that file to allow partial data persistence.
// Otherwise, it uses a temporary file for construction and copies it to the writer.
func ImportToSQLite(provider common.RowProvider, writer io.Writer, opts *ImportOptions) error {
	var dbPath string
	var useTemp bool = true

	// Check if writer is a file we can use directly
	if f, ok := writer.(*os.File); ok {
		stat, err := f.Stat()
		// Ensure it's a regular file (not stdout/pipe)
		if err == nil && (stat.Mode()&os.ModeCharDevice) == 0 {
			dbPath = f.Name()
			useTemp = false
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
	}

	// Connect to SQLite database
	db, err := sql.Open("sqlite3", dbPath)
	if err != nil {
		return fmt.Errorf("failed to open database: %w", err)
	}

	// Populate database
	err = populateDB(db, provider, opts)
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
func populateDB(db *sql.DB, provider common.RowProvider, opts *ImportOptions) error {
	logErrors := opts != nil && opts.LogErrors

	if logErrors {
		_, err := db.Exec(`CREATE TABLE IF NOT EXISTS _mksqlite_errors (
			timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,
			message TEXT,
			table_name TEXT,
			row_data TEXT
		)`)
		if err != nil {
			return fmt.Errorf("failed to create error log table: %w", err)
		}
	}

	tableNames := provider.GetTableNames()
	for _, tableName := range tableNames {
		headers := provider.GetHeaders(tableName)

		if len(headers) == 0 {
			continue // Skip tables without headers
		}

		// Create table
		createTableSQL := common.GenCreateTableSQL(tableName, headers)
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
		insertSQL, err := common.GenPreparedStmt(tableName, headers, common.InsertStmt)
		if err != nil {
			tx.Rollback()
			return fmt.Errorf("failed to generate insert statement for table %s: %w", tableName, err)
		}
		stmt, err := tx.Prepare(insertSQL)
		if err != nil {
			tx.Rollback()
			return fmt.Errorf("failed to prepare insert statement for table %s: %w", tableName, err)
		}

		// Prepare log statement if needed
		var logStmt *sql.Stmt
		if logErrors {
			logStmt, err = tx.Prepare(`INSERT INTO _mksqlite_errors (message, table_name, row_data) VALUES (?, ?, ?)`)
			if err != nil {
				tx.Rollback()
				return fmt.Errorf("failed to prepare log statement: %w", err)
			}
		}

		rowCount := 0

		// Insert rows using streaming ScanRows
		err = provider.ScanRows(tableName, func(row []interface{}, rowErr error) error {
			if rowErr != nil {
				if logErrors {
					// Log provider error
					rowData := fmt.Sprintf("%v", row) // Best effort string rep (might be nil or empty)
					if _, err := logStmt.Exec(rowErr.Error(), tableName, rowData); err != nil {
						return fmt.Errorf("failed to log error: %w", err)
					}
					return nil // Continue
				}
				return rowErr
			}

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
				if logErrors {
					// Log insertion error
					rowData := fmt.Sprintf("%v", row)
					if _, err := logStmt.Exec(err.Error(), tableName, rowData); err != nil {
						return fmt.Errorf("failed to log insert error: %w", err)
					}
					return nil // Continue
				}
				return fmt.Errorf("failed to insert row in table %s: %w", tableName, err)
			}

			rowCount++
			if rowCount%BatchSize == 0 {
				stmt.Close()
				if logStmt != nil {
					logStmt.Close()
				}
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
				if logErrors {
					logStmt, err = tx.Prepare(`INSERT INTO _mksqlite_errors (message, table_name, row_data) VALUES (?, ?, ?)`)
					if err != nil {
						tx.Rollback()
						return fmt.Errorf("failed to prepare log statement: %w", err)
					}
				}
			}
			return nil
		})

		stmt.Close() // Close statement before commit/rollback
		if logStmt != nil {
			logStmt.Close()
		}

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
