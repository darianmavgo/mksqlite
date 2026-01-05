# mksqlite

![Go Version](https://img.shields.io/badge/Go-1.25-blue)

A library and command-line tool to convert files to SQLite databases with generalized I/O support.

## Version 1

mksqlite was born from Mavgo Flight.  Realizing that just a library that converts a file type or data format into sqlite is valuable by itself independent of super useful URLs and web browsing. 
The goal is to first duplicate the code specific to conversion then upgrade it, then expand it. Then use this resulting library in Mavgo Flight and other tools. 
Supports converting CSV and Excel files to SQLite databases or SQL export.

## Installation

```bash
# Clone the repository
git clone <repository-url>
cd mksqlite

# Build the CLI
go build ./cmd/mksqlite
```

## Usage

```bash
# Build the CLI
go build ./cmd/mksqlite

# Convert files to SQLite databases
./mksqlite input.csv output.db
./mksqlite input.xlsx output.db

# Export files as SQL statements (to stdout)
./mksqlite --sql input.csv
./mksqlite --sql input.xlsx  # Not yet implemented for Excel
```

## Examples

### Converting Sample Data

Using the included sample files:

```bash
# Convert CSV to SQLite database
./mksqlite sample_data/sample.csv sample.db

# View the created database (requires sqlite3 CLI)
sqlite3 sample.db "SELECT * FROM sample;"

# Export CSV as SQL statements
./mksqlite --sql sample_data/sample.csv
```

### Sample CSV Content

The `sample_data/sample.csv` contains:

```
Name,Age,City
John,25,New York
Jane,30,London
Bob,35,Paris
```

## Features

- **Dual Output Modes**: Create SQLite databases or export SQL DDL/DML statements
- **Generalized I/O**: Supports both file paths and io.Reader/Writer interfaces
- **Robust Parsing**: Handles CSV/Excel files with variable column counts and complex data
- **Column Sanitization**: Automatically cleans column names for SQL compatibility
- **Extensible Architecture**: Easy to add new file format converters

## Library Usage

### File-based Conversion (creates SQLite databases)

```go
import "mksqlite/pkg/parsers"

// For CSV files
converter := &parsers.CSVConverter{}
err := converter.ConvertFile("input.csv", "output.db")

// For Excel files
converter := &parsers.ExcelConverter{}
err := converter.ConvertFile("input.xlsx", "output.db")
```

### Stream-based Conversion (exports SQL to io.Writer)

```go
import (
    "os"
    "mksqlite/pkg/parsers"
)

// Export CSV as SQL statements
converter := &parsers.CSVConverter{}
file, _ := os.Open("input.csv")
defer file.Close()
err := converter.ConvertToSQL(file, os.Stdout)
```

### Using Interfaces

```go
// FileConverter interface - for creating SQLite databases
var fileConv parsers.FileConverter = &parsers.CSVConverter{}

// StreamConverter interface - for SQL export
var streamConv parsers.StreamConverter = &parsers.CSVConverter{}

// Combined interface
var conv parsers.Converter = &parsers.CSVConverter{}
```

## Project Structure

- `cmd/mksqlite/`: Command-line interface with dual mode support
- `pkg/parsers/`: File format parsers implementing generalized I/O interfaces
  - `types.go`: Interface definitions
  - `csv.go`: CSV parser with stream support
  - `excel.go`: Excel parser (file-based for now)

## Dependencies

- `github.com/mattn/go-sqlite3` for SQLite database operations
- `github.com/xuri/excelize/v2` for Excel file parsing

## Architecture

The library uses a dual-interface approach:

- **`FileConverter`**: Converts files to SQLite databases (requires file system access)
- **`StreamConverter`**: Converts data streams to SQL output (works with any `io.Reader`/`io.Writer`)

This design allows the library to work with:
- Local files
- Network streams
- In-memory data
- Pipes and redirects
- Any `io.Reader`/`io.Writer` implementation
