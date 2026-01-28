//go:build mage

package main

import (
	"fmt"
	"os"

	"github.com/magefile/mage/mg"
	"github.com/magefile/mage/sh"
)

// Default target to run when none is specified
var Default = Build

// Build compiles the project binaries into the bin/ directory.
func Build() error {
	fmt.Println("Building...")
	return sh.Run("go", "build", "-o", "./bin", "./...")
}

// Install copies the mksqlite binary to /usr/local/bin.
func Install() error {
	mg.Deps(Build)
	fmt.Println("Installing...")
	return sh.Run("cp", "bin/mksqlite", "/usr/local/bin/mksqlite")
}

// Test runs all tests in the project with verbose output.
func Test() error {
	fmt.Println("Running Tests...")
	return sh.Run("go", "test", "-v", "./...")
}

// TestSpecific runs the specific test case defined in run_test.sh.
func TestSpecific() error {
	fmt.Println("Running Specific Test...")
	return sh.Run("go", "test", "-test.fullpath=true", "-timeout", "30s", "-run", "^TestGenTablesNames$", "github.com/darianmavgo/mksqlite/converters/common")
}

// Clean removes the bin directory and test outputs.
func Clean() error {
	fmt.Println("Cleaning...")
	if err := os.RemoveAll("bin"); err != nil {
		return err
	}
	if err := os.RemoveAll("test_output"); err != nil {
		return err
	}
	return nil
}

// Tidy runs go mod tidy.
func Tidy() error {
	fmt.Println("Running go mod tidy...")
	return sh.Run("go", "mod", "tidy")
}

// Check runs formatting and linting checks (fmt, vet).
func Check() error {
	mg.Deps(Fmt, Vet)
	return nil
}

// Fmt runs go fmt ./...
func Fmt() error {
	fmt.Println("Running go fmt...")
	return sh.Run("go", "fmt", "./...")
}

// Vet runs go vet ./...
func Vet() error {
	fmt.Println("Running go vet...")
	return sh.Run("go", "vet", "./...")
}
