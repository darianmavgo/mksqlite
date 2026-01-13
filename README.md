# mksqlite

![Go Version](https://img.shields.io/badge/Go-1.25-blue)

A robust library and command-line tool designed to convert various file formats and data streams into SQLite databases or SQL statements.

## Goal

`mksqlite` generalizes the conversion of structured data into SQLite. Born from the need to process diverse data sources for projects like Mavgo Flight, it aims to be a universal adapter that turns files (CSV, Excel, HTML, Zip archives) and directories into queryable SQL tables.

The project emphasizes:
- **Generalized I/O**: Abstractions that work with local files, streams, and directories.
- **Portability**: Generating standard SQLite databases (`.db`) or SQL dump files.
- **Extensibility**: Easy addition of new format converters.

## Installation

```bash
# Clone the repository
git clone <repository-url>
cd mksqlite

# Build the CLI
go build -o mksqlite cmd/mksqlite/main.go
```

## CLI Usage

The `mksqlite` tool operates in two modes: **Database Creation** and **SQL Export**.

### 1. Create SQLite Database

Converts the input file or directory into a SQLite database file.

```bash
# Syntax
./mksqlite <input_path> [output_db_path]

# Examples
./mksqlite data.csv                  # Creates data.csv.db
./mksqlite data.xlsx my_data.db      # Creates my_data.db
./mksqlite ./my_folder/ index.db     # Creates index.db from directory listing
```

### 2. Export SQL

Generates `CREATE TABLE` and `INSERT` statements to standard output. Useful for piping to other tools or databases.

```bash
# Syntax
./mksqlite --sql <input_path> > output.sql

# Examples
./mksqlite --sql data.csv > dump.sql
./mksqlite --sql ./my_folder/ > folder_structure.sql
```

## Supported Formats & Capabilities

`mksqlite` automatically detects the input type based on file extension or if the path is a directory.

| Input Type | Extensions | Output Table Structure | SQL Export Support |
|------------|------------|------------------------|--------------------|
| **CSV** | `.csv` | Table columns match CSV headers. Column names are sanitized for SQL. | ✅ Yes |
| **Excel** | `.xlsx`, `.xls` | Each sheet becomes a table. First row is used as headers. | ❌ No (Planned) |
| **HTML** | `.html`, `.htm` | Extracts `<table id="...">` elements. If no ID, tables are named `table0`, `table1`, etc. | ✅ Yes |
| **Zip** | `.zip` | Creates a `file_list` table containing metadata of files inside the archive (name, size, CRC, etc.). | ❌ No (Planned) |
| **Filesystem** | (Directory) | Creates a `data` table listing all files recursively with columns: `path`, `name`, `size`, `extension`, `mod_time`, `is_dir`. | ✅ Yes |

## Library Usage

`mksqlite` can be used as a Go library to integrate conversion logic into your own applications.

### Interfaces

The core logic is built around flexible interfaces defined in `converters/types.go`:

- **`FileConverter`**: Converts a source file directly to a destination SQLite DB file.
- **`StreamConverter`**: Converts an `io.Reader` to SQL statements written to an `io.Writer`.
- **`RowProvider`**: (Internal) standardized interface for fetching rows/headers from any source.

### Example: Converting a File to SQLite

```go
import "mksqlite/converters"

func main() {
    // CSV
    csvConv := &converters.CSVConverter{}
    err := csvConv.ConvertFile("input.csv", "output.db")

    // HTML
    htmlConv := &converters.HTMLConverter{}
    err = htmlConv.ConvertFile("report.html", "output.db")

    // Directory (Filesystem)
    fsConv := &converters.FilesystemConverter{}
    err = fsConv.ConvertFile("./data_dir", "index.db")
}
```

### Example: Exporting SQL to Stdout

```go
import (
    "os"
    "mksqlite/converters"
)

func main() {
    // Open input file
    file, _ := os.Open("data.csv")
    defer file.Close()

    // Export SQL
    converter := &converters.CSVConverter{}
    err := converter.ConvertToSQL(file, os.Stdout)
}
```

## Development

### Requirements
- Go 1.25+
- Dependencies:
    - `github.com/mattn/go-sqlite3`
    - `github.com/xuri/excelize/v2`
    - `golang.org/x/net/html`

### Running Tests
```bash
go test ./...
```
