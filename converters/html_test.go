package converters

import (
	"bytes"
	"database/sql"
	"os"
	"path/filepath"
	"strings"
	"testing"

	_ "github.com/mattn/go-sqlite3"
)

func TestHTMLConverter_ConvertFile(t *testing.T) {
	// Create temporary directory for test files
	tmpDir, err := os.MkdirTemp("", "html_test")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	inputPath := filepath.Join(tmpDir, "test.html")
	outputPath := filepath.Join(tmpDir, "test.db")

	htmlContent := `
<!DOCTYPE html>
<html>
<body>
	<table id="users">
		<thead>
			<tr>
				<th>ID</th>
				<th>Name</th>
				<th>Email</th>
			</tr>
		</thead>
		<tbody>
			<tr>
				<td>1</td>
				<td>Alice</td>
				<td>alice@example.com</td>
			</tr>
			<tr>
				<td>2</td>
				<td>Bob</td>
				<td>bob@example.com</td>
			</tr>
		</tbody>
	</table>

	<table>
		<tr>
			<td>Product</td>
			<td>Price</td>
		</tr>
		<tr>
			<td>Apple</td>
			<td>1.20</td>
		</tr>
	</table>
</body>
</html>
`
	if err := os.WriteFile(inputPath, []byte(htmlContent), 0644); err != nil {
		t.Fatalf("failed to write test file: %v", err)
	}

	converter := &HTMLConverter{}
	if err := converter.ConvertFile(inputPath, outputPath); err != nil {
		t.Fatalf("ConvertFile failed: %v", err)
	}

	// Verify database content
	db, err := sql.Open("sqlite3", outputPath)
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}
	defer db.Close()

	// Verify "users" table
	var count int
	err = db.QueryRow("SELECT COUNT(*) FROM users").Scan(&count)
	if err != nil {
		t.Fatalf("failed to query users table: %v", err)
	}
	if count != 2 {
		t.Errorf("expected 2 rows in users table, got %d", count)
	}

	var name string
	err = db.QueryRow("SELECT name FROM users WHERE id = '1'").Scan(&name)
	if err != nil {
		t.Fatalf("failed to query user: %v", err)
	}
	if name != "Alice" {
		t.Errorf("expected name 'Alice', got '%s'", name)
	}

	// Verify second table (should be named table1 because index 1, and first table used "users")
	// The first table had id="users", so it should be named "users".
	// The second table had no id. It is at index 1.
	// GenTableNames logic:
	// rawNames: ["users", "table1"]
	// sanitized: ["users", "table1"]

	// Wait, in my implementation:
	// rawNames[i] = t.rawName (if not empty) else fmt.Sprintf("table%d", i)
	// First table: "users"
	// Second table: "table1"

	err = db.QueryRow("SELECT COUNT(*) FROM table1").Scan(&count)
	if err != nil {
		t.Fatalf("failed to query table1: %v", err)
	}
	if count != 1 { // One data row (Apple), one header row (Product/Price)
		t.Errorf("expected 1 row in table1, got %d", count)
	}

	// Check header logic for table1
	// Rows:
	// 0: Product, Price -> becomes Headers
	// 1: Apple, 1.20 -> becomes Data

	var price string
	// Column name "Price" might be sanitized to "cl1" or "price".
	// GenColumnNames sanitizes. "Price" -> "price"
	err = db.QueryRow("SELECT price FROM table1 WHERE product = 'Apple'").Scan(&price)
	if err != nil {
		// If "price" failed, maybe check column names
		// rows, _ := db.Query("SELECT * FROM table1")
		// cols, _ := rows.Columns()
		// t.Logf("Columns: %v", cols)
		t.Fatalf("failed to query table1 price: %v", err)
	}
	if price != "1.20" {
		t.Errorf("expected price '1.20', got '%s'", price)
	}
}

func TestHTMLConverter_ConvertToSQL(t *testing.T) {
	htmlContent := `
<table>
	<tr>
		<th>A</th>
		<th>B</th>
	</tr>
	<tr>
		<td>1</td>
		<td>2</td>
	</tr>
</table>
`
	reader := strings.NewReader(htmlContent)
	var writer bytes.Buffer

	converter := &HTMLConverter{}
	if err := converter.ConvertToSQL(reader, &writer); err != nil {
		t.Fatalf("ConvertToSQL failed: %v", err)
	}

	output := writer.String()

	// Expect CREATE TABLE table0 (a TEXT, b TEXT);
	if !strings.Contains(output, "CREATE TABLE table0") {
		t.Errorf("output missing CREATE TABLE table0. Got: %s", output)
	}

	if !strings.Contains(output, "INSERT INTO table0") {
		t.Errorf("output missing INSERT INTO table0. Got: %s", output)
	}

	if !strings.Contains(output, "'1'") || !strings.Contains(output, "'2'") {
		t.Errorf("output missing values. Got: %s", output)
	}
}
