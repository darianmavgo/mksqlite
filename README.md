# mksqlite

![Go Version](https://img.shields.io/badge/Go-1.25-blue)

A robust library and command-line tool designed to convert various file formats and data streams into SQLite databases or SQL statements.

## Features Supported

*   **Multi-Format Conversion**:
    *   **PDF (.pdf)**: Automatically extracts tables with spatial layout analysis and multi-line row aggregation.
    *   **CSV**: Converts delimiters, handles headers, sanitizes column names.
    *   **Excel (.xlsx, .xls)**: Converts each sheet into a separate table.
    *   **HTML**: Extracts data from standard HTML `<table>` elements.
    *   **JSON**: Converts JSON data into structured tables.
    *   **Markdown (.md)**: Extracts data from Markdown tables.
    *   **Text (.txt)**: Parses text files into tables.
    *   **ZIP (.zip)**: Processes formats contained within ZIP archives.
    *   **Filesystem**: Recursively crawls directories to create a metadata index (`path`, `size`, etc.) in SQLite.
*   **Dual Output Modes**:
    *   **SQLite Database**: Direct binary creation of `.db` files.
    *   **SQL Dump**: Generates `CREATE TABLE` and `INSERT` statements to stdout (great for piping).
*   **Flexible Usage**: Available as both a standalone CLI tool and a Go library (`package converters`).
*   **Stream Processing**: capable of processing data streams without loading entire files into memory.

## Installation

Install `mksqlite` as a system-wide command:

```bash
./install.sh
```

Or install to a custom directory:

```bash
./install.sh --prefix /usr/local/bin
```

## Use as a Go Library

```bash
go get github.com/darianmavgo/mksqlite
```

```go
import (
	"context"
	"os"

	"github.com/darianmavgo/mksqlite/converters"
	"github.com/darianmavgo/mksqlite/converters/common"
	_ "github.com/darianmavgo/mksqlite/converters/all" // registers all format drivers (required)
)

provider, err := converters.AutoOpen(reader, &common.ConversionConfig{}) // sniffs csv/json/html/excel/pdf/zip/...
if err != nil { /* ... */ }

out, _ := os.Create("data.db")
defer out.Close()
err = converters.ImportToSQLiteContext(ctx, provider, out, nil)
```

Notes:
- Without the `converters/all` blank import (or a specific driver import), `AutoOpen` fails with `unknown driver`.
- To skip detection, use a format constructor directly, e.g. `csv.NewCSVConverter(r)`.
- `ImportToSQLiteContext` is driven by your context and installs no signal handlers; `ImportToSQLite` stops on SIGINT/SIGTERM (CLI behavior).
- Library output is silent by default; set `common.Logger = log.Default()` to see converter diagnostics.
- SQLite is provided by `modernc.org/sqlite` (driver name `sqlite`).
- See `integration/yfinance_test.go` for a complete consumer example (run with `go test ./integration -v`; skipped by `-short`).

## Area of Responsibility

`mksqlite` is the **Ingestion Engine**. Its job is to take unstructured or semi-structured data from the "wild" (files, scrapes, spreadsheets) and normalize it into the universal structured format: SQLite. It bridges the gap between raw data files and SQL-capable tools.

## Scope (What it explicitly doesn't do)

*   **No Long-Running Service**: `mksqlite` is a task-based tool. It runs, converts, and exits. It is not an HTTP server or a daemon.
*   **No Query Execution**: It does not run user queries (SELECT, etc.). It only performs `CREATE` and `INSERT` operations necessary for conversion.
*   **No Visualization**: It does not provide a UI to view the data; it only prepares the data for other tools (like `sqliter`) to view.

## Quick Usage

### Create a Database
```bash
# Convert a CSV to a SQLite DB (output DB is optional, defaults to data.csv.db)
mksqlite data.csv [data.db]

# Index a directory
mksqlite ./documents/ index.db

# Convert with error logging
mksqlite --log data.csv

# Resume an interrupted directory conversion from a specific path
mksqlite --resume-path ./documents/some/file.txt ./documents/ index.db
```

### Generate SQL
```bash
# Pipe SQL output to stdout
mksqlite --sql data.csv > dump.sql

# Export directly to an output file
mksqlite --sql data.csv dump.sql
```
