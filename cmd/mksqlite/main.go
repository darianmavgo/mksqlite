package main

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	"github.com/darianmavgo/mksqlite/converters"
	_ "github.com/darianmavgo/mksqlite/converters/all"
	"github.com/darianmavgo/mksqlite/converters/common"
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
	case ".md":
		return "markdown", nil
	case ".txt":
		return "txt", nil
	}
	return "", fmt.Errorf("unsupported file type: %s", ext)
}

// FileToSQLite converts a file to SQLite using the appropriate converter
func FileToSQLite(inputPath, outputPath string, config *common.ConversionConfig, opts *converters.ImportOptions) error {
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

	converter, err := converters.Open(driverName, inputFile, config)
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

	return converters.ImportToSQLite(converter, outputFile, opts)
}

// exportToSQL exports a file as SQL statements to writer
func exportToSQL(inputPath string, writer io.Writer, config *common.ConversionConfig) error {
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

	converter, err := converters.Open(driverName, file, config)
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
	args := os.Args[1:]
	logMode := false

	// Filter out --log flag
	var cleanArgs []string
	for _, arg := range args {
		if arg == "--log" {
			logMode = true
		} else {
			cleanArgs = append(cleanArgs, arg)
		}
	}

	if len(cleanArgs) < 1 {
		fmt.Println("Usage:")
		fmt.Println("  mksqlite [--log] <input_file> [output_db]          # Convert to SQLite database")
		fmt.Println("  mksqlite --sql <input_file> [output_file]          # Export as SQL statements")
		os.Exit(1)
	}

	if cleanArgs[0] == "--sql" {
		if len(cleanArgs) < 2 {
			fmt.Println("Usage: mksqlite --sql <input_file> [output_file]")
			os.Exit(1)
		}
		inputPath := cleanArgs[1]

		var writer io.Writer
		if len(cleanArgs) >= 3 {
			outputPath := cleanArgs[2]
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

		err := exportToSQL(inputPath, writer, nil)
		if err != nil {
			fmt.Printf("Error exporting SQL: %v\n", err)
			os.Exit(1)
		}
	} else {
		inputPath := cleanArgs[0]
		var outputPath string
		if len(cleanArgs) >= 2 {
			outputPath = cleanArgs[1]
		} else {
			outputPath = inputPath + ".db"
		}

		err := FileToSQLite(inputPath, outputPath, nil, &converters.ImportOptions{LogErrors: logMode})
		if err != nil {
			fmt.Printf("Error converting file: %v\n", err)
			os.Exit(1)
		}

		fmt.Printf("Successfully converted %s to %s\n", inputPath, outputPath)
	}
}
