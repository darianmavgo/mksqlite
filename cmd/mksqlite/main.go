package main

import (
	"fmt"
	"io"
	"mksqlite/converters"
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

	if info.IsDir() {
		converter := &converters.FilesystemConverter{}
		return converter.ConvertFile(inputPath, outputPath)
	}

	ext := filepath.Ext(inputPath)
	var converter converters.FileConverter

	switch ext {
	case ".csv":
		converter = &converters.CSVConverter{}
	case ".xlsx", ".xls":
		converter = &converters.ExcelConverter{}
	case ".zip":
		converter = &converters.ZipConverter{}
	case ".html", ".htm":
		converter = &converters.HTMLConverter{}
	default:
		return fmt.Errorf("unsupported file type: %s", ext)
	}

	return converter.ConvertFile(inputPath, outputPath)
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
		err := exportToSQL(inputPath, os.Stdout)
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

	if info.IsDir() {
		converter := &converters.FilesystemConverter{}
		file, err := os.Open(inputPath)
		if err != nil {
			return fmt.Errorf("failed to open input directory: %w", err)
		}
		defer file.Close()
		return converter.ConvertToSQL(file, writer)
	}

	ext := filepath.Ext(inputPath)
	var converter converters.StreamConverter

	switch ext {
	case ".csv":
		converter = &converters.CSVConverter{}
	case ".xlsx", ".xls":
		fmt.Printf("Excel SQL export not yet implemented\n")
		return fmt.Errorf("Excel SQL export not yet implemented")
	case ".zip":
		fmt.Printf("Zip SQL export not yet implemented\n")
		return fmt.Errorf("Zip SQL export not yet implemented")
	case ".html", ".htm":
		converter = &converters.HTMLConverter{}
	default:
		return fmt.Errorf("unsupported file type: %s", ext)
	}

	file, err := os.Open(inputPath)
	if err != nil {
		return fmt.Errorf("failed to open input file: %w", err)
	}
	defer file.Close()

	return converter.ConvertToSQL(file, writer)
}
