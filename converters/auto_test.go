package converters_test

import (
	"archive/zip"
	"bytes"
	"strings"
	"testing"
	"io"

	"github.com/darianmavgo/mksqlite/converters"
	"github.com/darianmavgo/mksqlite/converters/common"
	// Ensure we import the all package to register everything for AutoOpen tests
	_ "github.com/darianmavgo/mksqlite/converters/all"
)

func TestDetectFormat(t *testing.T) {
	// Helper to create a dummy zip with a specific file
	createZip := func(filename string) []byte {
		buf := new(bytes.Buffer)
		zw := zip.NewWriter(buf)
		f, _ := zw.Create(filename)
		f.Write([]byte("dummy"))
		zw.Close()
		return buf.Bytes()
	}

	tests := []struct {
		name     string
		input    []byte
		expected string
	}{
		{"pdf", []byte("%PDF-1.4..."), "pdf"},
		{"excel xlsx", createZip("[Content_Types].xml"), "excel"},
		{"zip generic", createZip("hello.txt"), "zip"},
		{"excel xls", []byte{0xD0, 0xCF, 0x11, 0xE0, 0xA1, 0xB1, 0x1A, 0xE1, 0x00}, "excel"},
		{"json array", []byte(`  [{"a":1}] `), "json"},
		{"json object", []byte(`{"key":"value"}`), "json"},
		{"html doctype", []byte(`<!DOCTYPE html><html><body></body></html>`), "html"},
		{"html tag", []byte(`<html><body></body></html>`), "html"},
		{"markdown table", []byte(`| a | b |
|---|---|
| 1 | 2 |`), "markdown"},
		{"csv comma", []byte("a,b,c\n1,2,3"), "csv"},
		{"csv tab", []byte("a\tb\tc\n1\t2\t3"), "csv"},
		{"txt fallback", []byte("hello world\nline 2"), "txt"},
		{"txt empty space", []byte("   \n  "), "txt"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := converters.DetectFormat(tt.input)
			if got != tt.expected {
				t.Errorf("DetectFormat() = %v, want %v", got, tt.expected)
			}
		})
	}
}

func TestAutoOpen(t *testing.T) {
	input := "a,b,c\n1,2,3"
	reader := strings.NewReader(input)

	config := &common.ConversionConfig{}

	provider, err := converters.AutoOpen(reader, config)
	if err != nil {
		t.Fatalf("AutoOpen failed: %v", err)
	}

	if provider == nil {
		t.Fatal("AutoOpen returned nil provider")
	}

	// CSV should be detected and opened
	tables := provider.GetTableNames()
	if len(tables) == 0 {
		t.Fatalf("Expected tables, got none")
	}
	headers := provider.GetHeaders(tables[0])
	if len(headers) != 3 || headers[0] != "a" {
		t.Errorf("Expected headers [a b c], got %v", headers)
	}
}

func TestAutoOpenStream(t *testing.T) {
	// bufio.Reader does not implement io.ReadSeeker
	input := "a,b,c\n1,2,3"

	// Create a pipe so it's purely a stream
	r, w := io.Pipe()
	go func() {
		w.Write([]byte(input))
		w.Close()
	}()

	config := &common.ConversionConfig{}
	provider, err := converters.AutoOpen(r, config)
	if err != nil {
		t.Fatalf("AutoOpen stream failed: %v", err)
	}

	if provider == nil {
		t.Fatal("AutoOpen stream returned nil provider")
	}
}
