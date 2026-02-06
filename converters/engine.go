// DO NOT MODIFY: This file is finalized. Any changes should be discussed and approved.
package converters

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/darianmavgo/mksqlite/converters/common"

	_ "modernc.org/sqlite"
)

var ErrInterrupted = errors.New("operation interrupted by user")
var ErrScanTimeout = errors.New("scan timed out")

var (
	// BatchSize defines the number of rows to insert before committing a transaction.
	// This ensures that long-running streams save progress periodically.
	BatchSize = 1000
)

// ImportOptions defines configuration for the import process.
type ImportOptions struct {
	LogErrors bool // If true, errors are logged to a table instead of aborting.
	Verbose   bool // If true, enables detailed logging.
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
			if opts != nil && opts.Verbose {
				log.Printf("[MKSQLITE] Using direct file: %s", dbPath)
			}
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

		if opts != nil && opts.Verbose {
			log.Printf("[MKSQLITE] Created temp file: %s", dbPath)
		}
		defer os.Remove(dbPath) // Clean up temp file
	}

	// Connect to SQLite database
	db, err := sql.Open("sqlite", dbPath)
	if err != nil {
		return fmt.Errorf("failed to open database: %w", err)
	}

	// Limit to 1 connection to avoid locking issues and improve tx.Stmt performance
	db.SetMaxOpenConns(1)

	// Set PRAGMA page_size and cache_size for performance
	if _, err := db.Exec("PRAGMA page_size = 65536; PRAGMA cache_size = -2000;"); err != nil {
		return fmt.Errorf("failed to set PRAGMAs: %w", err)
	}

	// Populate database
	if opts != nil && opts.Verbose {
		log.Printf("[MKSQLITE] Starting database population...")
	}
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

		if opts != nil && opts.Verbose {
			log.Printf("[MKSQLITE] Copying temp database to final output...")
		}
		// Copy to writer
		if _, err := io.Copy(writer, f); err != nil {
			return fmt.Errorf("failed to write to output: %w", err)
		}
	}

	if opts != nil && opts.Verbose {
		log.Printf("[MKSQLITE] Conversion completed successfully.")
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
		if opts != nil && opts.Verbose {
			log.Printf("[MKSQLITE] Creating table: %s with headers: %v", tableName, headers)
		}

		colTypes := provider.GetColumnTypes(tableName)
		createTableSQL := common.GenCreateTableSQLWithTypes(tableName, headers, colTypes)
		_, err := db.Exec(createTableSQL)
		if err != nil {
			return fmt.Errorf("failed to create table %s: %w", tableName, err)
		}

		// Generate insert statement
		insertSQL, err := common.GenPreparedStmt(tableName, headers, common.InsertStmt)
		if err != nil {
			return fmt.Errorf("failed to generate insert statement for table %s: %w", tableName, err)
		}

		// Prepare statement on connection
		mainStmt, err := db.Prepare(insertSQL)
		if err != nil {
			return fmt.Errorf("failed to prepare insert statement for table %s: %w", tableName, err)
		}
		defer mainStmt.Close()

		var mainLogStmt *sql.Stmt
		if logErrors {
			mainLogStmt, err = db.Prepare(`INSERT INTO _mksqlite_errors (message, table_name, row_data) VALUES (?, ?, ?)`)
			if err != nil {
				return fmt.Errorf("failed to prepare log statement: %w", err)
			}
			defer mainLogStmt.Close()
		}

		// Begin transaction
		tx, err := db.Begin()
		if err != nil {
			return fmt.Errorf("failed to begin transaction: %w", err)
		}

		// Use tx-specific statement
		stmt := tx.Stmt(mainStmt)

		// Prepare log statement if needed
		var logStmt *sql.Stmt
		if logErrors {
			logStmt = tx.Stmt(mainLogStmt)
		}

		rowCount := 0

		// Setup signal handling context
		ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
		defer cancel()

		// Insert rows using streaming ScanRows
		err = provider.ScanRows(ctx, tableName, func(row []interface{}, rowErr error) error {
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
				targetLen := len(headers)
				currentLen := len(row)
				if cap(row) >= targetLen {
					// Optimization: Reuse existing capacity
					row = row[:targetLen]
					clear(row[currentLen:])
				} else {
					newRow := make([]interface{}, targetLen)
					copy(newRow, row)
					row = newRow
				}
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
				stmt = tx.Stmt(mainStmt)
				if logErrors {
					logStmt = tx.Stmt(mainLogStmt)
				}
			}
			return nil
		})

		stmt.Close() // Close statement before commit/rollback
		if logStmt != nil {
			logStmt.Close()
		}

		if err != nil {
			if errors.Is(err, ErrInterrupted) || errors.Is(err, ErrScanTimeout) {
				if opts != nil && opts.Verbose {
					log.Printf("[MKSQLITE] Stopped (%v). Committing partial transaction for table %s...", err, tableName)
				}
				// Commit what we have
				if commitErr := tx.Commit(); commitErr != nil {
					log.Printf("[MKSQLITE] Failed to commit on stop: %v", commitErr)
				}
				return err
			}
			tx.Rollback()
			return fmt.Errorf("failed to scan rows for table %s: %w", tableName, err)
		}

		if err := tx.Commit(); err != nil {
			return fmt.Errorf("failed to commit transaction for table %s: %w", tableName, err)
		}
		if opts != nil && opts.Verbose {
			log.Printf("[MKSQLITE] Finished table %s, total rows: %d", tableName, rowCount)
		}
	}
	return nil
}
