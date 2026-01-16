package main

import (
	"fmt"
	"io"
	"mksqlite/converters"
	"mksqlite/converters/common"
	"mksqlite/converters/csv"
	"mksqlite/converters/excel"
	"mksqlite/converters/filesystem"
	"mksqlite/converters/html"
	"mksqlite/converters/json"
	"mksqlite/converters/zip"
	"os"
	"path/filepath"
)

// FileToSQLite converts a file to SQLite using the appropriate converter
func FileToSQLite(inputPath, outputPath string) error {
	// Check if input is a directory
	info, err := os.Stat(inputPath)
	if err != nil {
		return fmt.Errorf("failed to stat input path: %w", err)
	}

	var inputFile *os.File
	inputFile, err = os.Open(inputPath)
	if err != nil {
		return fmt.Errorf("failed to open input: %w", err)
	}
	defer inputFile.Close()

	var converter common.RowProvider
	var convErr error

	if info.IsDir() {
		converter, convErr = filesystem.NewFilesystemConverter(inputFile)
	} else {
		ext := filepath.Ext(inputPath)
		switch ext {
		case ".csv":
			converter, convErr = csv.NewCSVConverter(inputFile)
		case ".xlsx", ".xls":
			converter, convErr = excel.NewExcelConverter(inputFile)
		case ".zip":
			converter, convErr = zip.NewZipConverter(inputFile)
		case ".html", ".htm":
			converter, convErr = html.NewHTMLConverter(inputFile)
		case ".json":
			converter, convErr = json.NewJSONConverter(inputFile)
		default:
			return fmt.Errorf("unsupported file type: %s", ext)
		}
	}

	if convErr != nil {
		return fmt.Errorf("failed to initialize converter: %w", convErr)
	}

	// Clean up converter resources if it implements io.Closer
	if c, ok := converter.(io.Closer); ok {
		defer c.Close()
	}

	// Ensure output directory exists
	dir := filepath.Dir(outputPath)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return fmt.Errorf("failed to create output directory: %w", err)
	}

	// Create output file
	outputFile, err := os.Create(outputPath)
	if err != nil {
		return fmt.Errorf("failed to create output file: %w", err)
	}
	defer outputFile.Close()

	return converters.ImportToSQLite(converter, outputFile)
}

func main() {
	if len(os.Args) < 2 {
		fmt.Println("Usage:")
		fmt.Println("  mksqlite <input_file> [output_db]          # Convert to SQLite database")
		fmt.Println("  mksqlite --sql <input_file> [output_file]  # Export as SQL statements")
		os.Exit(1)
	}

	if os.Args[1] == "--sql" {
		if len(os.Args) < 3 {
			fmt.Println("Usage: mksqlite --sql <input_file> [output_file]")
			os.Exit(1)
		}
		inputPath := os.Args[2]

		// If output file is provided (arg 3 is output), use it, else stdout?
		// Usage says [output_file].
		// If 3 args: mksqlite --sql input output
		// If 2 args (excluding --sql): wait, os.Args[0] is prog.
		// os.Args[1] is --sql. os.Args[2] is input. os.Args[3] is output (optional).

		var writer io.Writer
		if len(os.Args) >= 4 {
			outputPath := os.Args[3]
			f, err := os.Create(outputPath)
			if err != nil {
				fmt.Printf("Error creating output file: %v\n", err)
				os.Exit(1)
			}
			defer f.Close()
			writer = f
		} else {
			writer = os.Stdout
		}

		err := exportToSQL(inputPath, writer)
		if err != nil {
			fmt.Printf("Error exporting SQL: %v\n", err)
			os.Exit(1)
		}
	} else {
		inputPath := os.Args[1]
		var outputPath string
		if len(os.Args) >= 3 {
			outputPath = os.Args[2]
		} else {
			outputPath = inputPath + ".db"
		}

		err := FileToSQLite(inputPath, outputPath)
		if err != nil {
			fmt.Printf("Error converting file: %v\n", err)
			os.Exit(1)
		}

		fmt.Printf("Successfully converted %s to %s\n", inputPath, outputPath)
	}
}

// exportToSQL exports a file as SQL statements to writer
func exportToSQL(inputPath string, writer io.Writer) error {
	// Check if input is a directory
	info, err := os.Stat(inputPath)
	if err != nil {
		return fmt.Errorf("failed to stat input path: %w", err)
	}

	file, err := os.Open(inputPath)
	if err != nil {
		return fmt.Errorf("failed to open input file: %w", err)
	}
	defer file.Close()

	if info.IsDir() {
		converter := &filesystem.FilesystemConverter{}
		return converter.ConvertToSQL(file, writer)
	}

	ext := filepath.Ext(inputPath)
	var converter common.StreamConverter

	switch ext {
	case ".csv":
		converter = &csv.CSVConverter{}
	case ".xlsx", ".xls":
		converter = &excel.ExcelConverter{}
	case ".zip":
		converter = &zip.ZipConverter{}
	case ".html", ".htm":
		converter = &html.HTMLConverter{}
	case ".json":
		converter = &json.JSONConverter{}
	default:
		return fmt.Errorf("unsupported file type: %s", ext)
	}

	return converter.ConvertToSQL(file, writer)
}
