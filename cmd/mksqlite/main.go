package main

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	"github.com/darianmavgo/mksqlite/config"
	"github.com/darianmavgo/mksqlite/converters"
	"github.com/darianmavgo/mksqlite/converters/common"
	_ "github.com/darianmavgo/mksqlite/converters/csv"
	_ "github.com/darianmavgo/mksqlite/converters/excel"
	_ "github.com/darianmavgo/mksqlite/converters/filesystem"
	_ "github.com/darianmavgo/mksqlite/converters/html"
	_ "github.com/darianmavgo/mksqlite/converters/json"
	_ "github.com/darianmavgo/mksqlite/converters/txt"
	_ "github.com/darianmavgo/mksqlite/converters/zip"
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
func FileToSQLite(inputPath, outputPath string, config *common.ConversionConfig) error {
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

	return converters.ImportToSQLite(converter, outputFile)
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
	// Parse arguments manually to support --config flag anywhere and export-config
	var configPath string
	var args []string // Filtered arguments (excluding flags handled here)
	args = append(args, os.Args[0]) // Keep program name

	for i := 1; i < len(os.Args); i++ {
		if os.Args[i] == "--config" {
			if i+1 < len(os.Args) {
				configPath = os.Args[i+1]
				i++ // skip value
			} else {
				fmt.Println("Error: --config requires a file path")
				os.Exit(1)
			}
		} else {
			args = append(args, os.Args[i])
		}
	}

	// Initialize config with defaults
	cfg := config.DefaultConfig()

	// Load config if provided (overriding defaults)
	if configPath != "" {
		loadedCfg, err := config.Load(configPath)
		if err != nil {
			fmt.Printf("Error loading config: %v\n", err)
			os.Exit(1)
		}
		cfg = loadedCfg
	}

	// Apply configuration to global state
	if cfg.BatchSize > 0 {
		converters.BatchSize = cfg.BatchSize
	}

	// Handle export-config subcommand
	if len(args) > 1 && args[1] == "export-config" {
		outputPath := "config.hcl"
		if len(args) > 2 {
			outputPath = args[2]
		}

		if err := config.Export(outputPath, cfg); err != nil {
			fmt.Printf("Error exporting config: %v\n", err)
			os.Exit(1)
		}
		fmt.Printf("Configuration exported to %s\n", outputPath)
		return
	}

	if len(args) < 2 {
		fmt.Println("Usage:")
		fmt.Println("  mksqlite <input_file> [output_db]          # Convert to SQLite database")
		fmt.Println("  mksqlite --sql <input_file> [output_file]  # Export as SQL statements")
		fmt.Println("  mksqlite export-config [output_file]       # Export configuration")
		fmt.Println("\nOptions:")
		fmt.Println("  --config <file>                            # Use specified configuration file")
		os.Exit(1)
	}

	if args[1] == "--sql" {
		if len(args) < 3 {
			fmt.Println("Usage: mksqlite --sql <input_file> [output_file]")
			os.Exit(1)
		}
		inputPath := args[2]

		var writer io.Writer
		if len(args) >= 4 {
			outputPath := args[3]
		fmt.Println("  --advanced-header                          # Enable advanced header detection")
		os.Exit(1)
	}

	config := &common.ConversionConfig{}
	var args []string

	for i := 1; i < len(os.Args); i++ {
		if os.Args[i] == "--advanced-header" {
			config.AdvancedHeaderDetection = true
		} else {
			args = append(args, os.Args[i])
		}
	}

	if len(args) < 1 {
		fmt.Println("Usage: mksqlite [--advanced-header] <input_file> [output_db]")
		fmt.Println("       mksqlite [--advanced-header] --sql <input_file> [output_file]")
		os.Exit(1)
	}

	if args[0] == "--sql" {
		if len(args) < 2 {
			fmt.Println("Usage: mksqlite [--advanced-header] --sql <input_file> [output_file]")
			os.Exit(1)
		}
		inputPath := args[1]

		var writer io.Writer
		if len(args) >= 3 {
			outputPath := args[2]
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

		err := exportToSQL(inputPath, writer, config)
		if err != nil {
			fmt.Printf("Error exporting SQL: %v\n", err)
			os.Exit(1)
		}
	} else {
		inputPath := args[1]
		var outputPath string
		if len(args) >= 3 {
			outputPath = args[2]
		inputPath := args[0]
		var outputPath string
		if len(args) >= 2 {
			outputPath = args[1]
		} else {
			outputPath = inputPath + ".db"
		}

		err := FileToSQLite(inputPath, outputPath, config)
		if err != nil {
			fmt.Printf("Error converting file: %v\n", err)
			os.Exit(1)
		}

		fmt.Printf("Successfully converted %s to %s\n", inputPath, outputPath)
	}
}
