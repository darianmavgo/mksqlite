package config

import (
	"fmt"
	"os"

	"github.com/hashicorp/hcl/v2/gohcl"
	"github.com/hashicorp/hcl/v2/hclparse"
	"github.com/hashicorp/hcl/v2/hclwrite"
	"github.com/zclconf/go-cty/cty"
)

// Config represents the application configuration.
type Config struct {
	BatchSize int `hcl:"batch_size,optional"`
}

// DefaultConfig returns the default configuration.
func DefaultConfig() *Config {
	return &Config{
		BatchSize: 1000,
	}
}

// Load reads the configuration from the given HCL file.
func Load(path string) (*Config, error) {
	content, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("failed to read config file: %w", err)
	}

	parser := hclparse.NewParser()
	file, diags := parser.ParseHCL(content, path)
	if diags.HasErrors() {
		return nil, fmt.Errorf("failed to parse config file: %s", diags.Error())
	}

	cfg := DefaultConfig()
	diags = gohcl.DecodeBody(file.Body, nil, cfg)
	if diags.HasErrors() {
		return nil, fmt.Errorf("failed to decode config: %s", diags.Error())
	}

	return cfg, nil
}

// Export writes the configuration to the specified file in HCL format.
func Export(path string, cfg *Config) error {
	f := hclwrite.NewEmptyFile()
	root := f.Body()

	// Add comments and values
	root.SetAttributeValue("batch_size", cty.NumberIntVal(int64(cfg.BatchSize)))

	file, err := os.Create(path)
	if err != nil {
		return fmt.Errorf("failed to create config file: %w", err)
	}
	defer file.Close()

	_, err = file.Write(f.Bytes())
	if err != nil {
		return fmt.Errorf("failed to write config to file: %w", err)
	}

	return nil
}
