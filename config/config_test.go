package config

import (
	"os"
	"path/filepath"
	"testing"
)

func TestExportAndLoad(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "config_test")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	configPath := filepath.Join(tempDir, "config.hcl")

	// Test Export
	defaultCfg := DefaultConfig()
	defaultCfg.BatchSize = 500
	err = Export(configPath, defaultCfg)
	if err != nil {
		t.Fatalf("Export failed: %v", err)
	}

	// Test Load
	loadedCfg, err := Load(configPath)
	if err != nil {
		t.Fatalf("Load failed: %v", err)
	}

	if loadedCfg.BatchSize != 500 {
		t.Errorf("expected BatchSize 500, got %d", loadedCfg.BatchSize)
	}
}

func TestLoadDefaults(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "config_test_empty")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	configPath := filepath.Join(tempDir, "empty.hcl")
	err = os.WriteFile(configPath, []byte(""), 0644)
	if err != nil {
		t.Fatalf("failed to write empty config: %v", err)
	}

	loadedCfg, err := Load(configPath)
	if err != nil {
		t.Fatalf("Load failed: %v", err)
	}

	if loadedCfg.BatchSize != 1000 {
		t.Errorf("expected default BatchSize 1000, got %d", loadedCfg.BatchSize)
	}
}
