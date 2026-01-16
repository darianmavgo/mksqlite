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

// ImportToSQLite imports data from a RowProvider and writes the resulting SQLite database
// to the provided io.Writer.
// If writer is an *os.File, it writes directly to that file to allow partial data persistence.
// Otherwise, it uses a temporary file for construction and copies it to the writer.
func ImportToSQLite(provider common.RowProvider, writer io.Writer) error {
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
func populateDB(db *sql.DB, provider common.RowProvider) error {
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
