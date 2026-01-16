package main

import (
	"fmt"
	"io"
	"mksqlite/converters"
	"mksqlite/converters/common"
	_ "mksqlite/converters/csv"
	_ "mksqlite/converters/excel"
	_ "mksqlite/converters/filesystem"
	_ "mksqlite/converters/html"
	_ "mksqlite/converters/json"
	_ "mksqlite/converters/txt"
	_ "mksqlite/converters/zip"
	"os"
	"path/filepath"
	"strings"
)

func getDriverName(path string, isDir bool) (string, error) {
	if isDir {
		return "filesystem", nil
	}
	ext := strings.ToLower(filepath.Ext(path))
	switch ext {
	case ".csv":
		return "csv", nil
	case ".xlsx", ".xls":
		return "excel", nil
	case ".zip":
		return "zip", nil
	case ".html", ".htm":
		return "html", nil
	case ".json":
		return "json", nil
	case ".txt":
		return "txt", nil
	}
	return "", fmt.Errorf("unsupported file type: %s", ext)
}

// FileToSQLite converts a file to SQLite using the appropriate converter
func FileToSQLite(inputPath, outputPath string) error {
	info, err := os.Stat(inputPath)
	if err != nil {
		return fmt.Errorf("failed to stat input path: %w", err)
	}

	driverName, err := getDriverName(inputPath, info.IsDir())
	if err != nil {
		return err
	}

	inputFile, err := os.Open(inputPath)
	if err != nil {
		return fmt.Errorf("failed to open input: %w", err)
	}
	defer inputFile.Close()

	converter, err := converters.Open(driverName, inputFile)
	if err != nil {
		return fmt.Errorf("failed to initialize converter: %w", err)
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

// exportToSQL exports a file as SQL statements to writer
func exportToSQL(inputPath string, writer io.Writer) error {
	info, err := os.Stat(inputPath)
	if err != nil {
		return fmt.Errorf("failed to stat input path: %w", err)
	}

	driverName, err := getDriverName(inputPath, info.IsDir())
	if err != nil {
		return err
	}

	file, err := os.Open(inputPath)
	if err != nil {
		return fmt.Errorf("failed to open input file: %w", err)
	}
	defer file.Close()

	converter, err := converters.Open(driverName, file)
	if err != nil {
		return fmt.Errorf("failed to initialize converter: %w", err)
	}

	// Clean up converter resources if it implements io.Closer
	if c, ok := converter.(io.Closer); ok {
		defer c.Close()
	}

	streamConv, ok := converter.(common.StreamConverter)
	if !ok {
		return fmt.Errorf("converter for %s does not support SQL export", driverName)
	}

	return streamConv.ConvertToSQL(writer)
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
