# mksqlite

![Go Version](https://img.shields.io/badge/Go-1.25-blue)

A robust library and command-line tool designed to convert various file formats and data streams into SQLite databases or SQL statements.

## Features Supported

*   **Multi-Format Conversion**:
    *   **CSV**: Converts delimiters, handles headers, sanitizes column names.
    *   **Excel (.xlsx, .xls)**: Converts each sheet into a separate table.
    *   **HTML**: Extracts data from standard HTML `<table>` elements.
    *   **Filesystem**: Recursively crawls directories to create a metadata index (`path`, `size`, etc.) in SQLite.
*   **Dual Output Modes**:
    *   **SQLite Database**: Direct binary creation of `.db` files.
    *   **SQL Dump**: Generates `CREATE TABLE` and `INSERT` statements to stdout (great for piping).
*   **Flexible Usage**: Available as both a standalone CLI tool and a Go library (`package converters`).
*   **Stream Processing**: capable of processing data streams without loading entire files into memory.

## Area of Responsibility

`mksqlite` is the **Ingestion Engine**. Its job is to take unstructured or semi-structured data from the "wild" (files, scrapes, spreadsheets) and normalize it into the universal structured format: SQLite. It bridges the gap between raw data files and SQL-capable tools.

## Scope (What it explicitly doesn't do)

*   **No Long-Running Service**: `mksqlite` is a task-based tool. It runs, converts, and exits. It is not an HTTP server or a daemon.
*   **No Query Execution**: It does not run user queries (SELECT, etc.). It only performs `CREATE` and `INSERT` operations necessary for conversion.
*   **No Visualization**: It does not provide a UI to view the data; it only prepares the data for other tools (like `sqliter`) to view.

## Quick Usage

### Create a Database
```bash
# Convert a CSV to a SQLite DB
mksqlite data.csv data.db

# Index a directory
mksqlite ./documents/ index.db
```

### Generate SQL
```bash
# Pipe SQL output
mksqlite --sql data.csv > dump.sql
```
