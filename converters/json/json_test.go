package json

import (
	"database/sql"
	"github.com/darianmavgo/mksqlite/converters"
	"os"
	"path/filepath"
	"strings"
	"testing"

	_ "github.com/mattn/go-sqlite3"
)

func TestJSONArray(t *testing.T) {
	jsonContent := `[
        {"name": "Alice", "age": 30},
        {"name": "Bob", "age": 25, "city": "NY"},
        {"name": "Charlie", "age": 35}
    ]`

	reader := strings.NewReader(jsonContent)
	conv, err := NewJSONConverter(reader)
	if err != nil {
		t.Fatalf("Failed to create converter: %v", err)
	}

	tables := conv.GetTableNames()
	if len(tables) != 1 || tables[0] != "jsontb0" {
		t.Errorf("Expected 1 table 'jsontb0', got %v", tables)
	}

	headers := conv.GetHeaders("jsontb0")
	// headers should be sorted: age, name.
	if len(headers) != 2 {
		t.Errorf("Expected 2 headers, got %v", headers)
	}
	if headers[0] != "age" || headers[1] != "name" {
		t.Errorf("Headers mismatch: %v", headers)
	}

	outputDir := "../../test_output/json_test"
	if err := os.MkdirAll(outputDir, 0755); err != nil {
		t.Fatalf("Failed to create output directory: %v", err)
	}
	outPath := filepath.Join(outputDir, "json_array.db")
	os.MkdirAll(filepath.Dir(outPath), 0755)
	os.Remove(outPath)

	f, err := os.Create(outPath)
	if err != nil {
		t.Fatal(err)
	}
	err = converters.ImportToSQLite(conv, f, nil)
	f.Close()

	if err != nil {
		t.Fatalf("Import failed: %v", err)
	}

	db, err := sql.Open("sqlite3", outPath)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	var count int
	err = db.QueryRow("SELECT COUNT(*) FROM jsontb0").Scan(&count)
	if err != nil {
		t.Fatal(err)
	}
	if count != 3 {
		t.Errorf("Expected 3 rows, got %d", count)
	}

	// Verify data
	var name string
	err = db.QueryRow("SELECT name FROM jsontb0 WHERE CAST(age AS INT)=30").Scan(&name)
	if err != nil {
		rows, _ := db.Query("SELECT age, name FROM jsontb0")
		for rows.Next() {
			var a, n interface{}
			rows.Scan(&a, &n)
			t.Logf("Row: age=%v (%T), name=%v", a, a, n)
		}
		t.Fatalf("Query failed: %v", err)
	}
	if name != "Alice" {
		t.Errorf("Expected Alice, got %s", name)
	}
}

func TestJSONObject(t *testing.T) {
	jsonContent := `{
        "users": [{"id": 1, "name": "A"}, {"id": 2, "name": "B"}],
        "posts": [{"id": 100, "title": "P1"}]
    }`

	// strings.Reader implements ReadSeeker
	reader := strings.NewReader(jsonContent)
	conv, err := NewJSONConverter(reader)
	if err != nil {
		t.Fatalf("Failed to create converter: %v", err)
	}

	tables := conv.GetTableNames()
	// Should contain "posts", "users" (sorted).
	if len(tables) != 2 {
		t.Errorf("Expected 2 tables, got %d: %v", len(tables), tables)
	}

	// Check headers
	// For users: id, name.
	uHeaders := conv.GetHeaders("users")
	if len(uHeaders) != 2 {
		t.Errorf("Users headers mismatch: %v", uHeaders)
	}

	pHeaders := conv.GetHeaders("posts")
	if len(pHeaders) != 2 {
		t.Errorf("Posts headers mismatch: %v", pHeaders)
	}

	outputDir := "../../test_output/json_test"
	if err := os.MkdirAll(outputDir, 0755); err != nil {
		t.Fatalf("Failed to create output directory: %v", err)
	}
	outPath := filepath.Join(outputDir, "json_object.db")
	os.MkdirAll(filepath.Dir(outPath), 0755)
	os.Remove(outPath)

	f, err := os.Create(outPath)
	if err != nil {
		t.Fatal(err)
	}
	err = converters.ImportToSQLite(conv, f, nil)
	f.Close()
	if err != nil {
		t.Fatalf("Import failed: %v", err)
	}

	db, err := sql.Open("sqlite3", outPath)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	var count int
	db.QueryRow("SELECT COUNT(*) FROM users").Scan(&count)
	if count != 2 {
		t.Errorf("Expected 2 users, got %d", count)
	}
	db.QueryRow("SELECT COUNT(*) FROM posts").Scan(&count)
	if count != 1 {
		t.Errorf("Expected 1 post, got %d", count)
	}
}

func TestJSONNested(t *testing.T) {
	jsonContent := `[
        {"id": 1, "meta": {"foo": "bar", "baz": [1,2]}}
    ]`

	reader := strings.NewReader(jsonContent)
	conv, _ := NewJSONConverter(reader)

	outputDir := "../../test_output/json_test"
	if err := os.MkdirAll(outputDir, 0755); err != nil {
		t.Fatalf("Failed to create output directory: %v", err)
	}
	outPath := filepath.Join(outputDir, "json_nested.db")
	os.MkdirAll(filepath.Dir(outPath), 0755)
	os.Remove(outPath)

	f, _ := os.Create(outPath)
	converters.ImportToSQLite(conv, f, nil)
	f.Close()

	db, _ := sql.Open("sqlite3", outPath)
	defer db.Close()

	var meta string
	err := db.QueryRow("SELECT meta FROM jsontb0 WHERE CAST(id AS INT)=1").Scan(&meta)
	if err != nil {
		rows, _ := db.Query("SELECT id, meta FROM jsontb0")
		for rows.Next() {
			var id, m interface{}
			rows.Scan(&id, &m)
			t.Logf("Row: id=%v (%T), meta=%v", id, id, m)
		}
		t.Fatalf("Query failed: %v", err)
	}

	if !strings.Contains(meta, "foo") {
		t.Errorf("Expected JSON string in meta column, got: %s", meta)
	}
}

func TestJSONConvertToSQL(t *testing.T) {
	jsonContent := `[{"col": "val'ue"}]`
	reader := strings.NewReader(jsonContent)

	conv, err := NewJSONConverter(reader)
	if err != nil {
		t.Fatal(err)
	}

	var buf strings.Builder
	err = conv.ConvertToSQL(&buf)
	if err != nil {
		t.Fatal(err)
	}

	sqlOutput := buf.String()
	if !strings.Contains(sqlOutput, "INSERT INTO jsontb0") {
		t.Error("Missing INSERT")
	}
	if !strings.Contains(sqlOutput, "'val''ue'") { // check escaping
		t.Error("Escaping failed")
	}
}

func TestJSONPrimitiveFirst(t *testing.T) {
	jsonContent := `[
		"first",
		{"value": "second"},
        [1, 2]
	]`

	reader := strings.NewReader(jsonContent)
	conv, err := NewJSONConverter(reader)
	if err != nil {
		t.Fatalf("Failed to create converter: %v", err)
	}

    // Headers should be ["value"]
    headers := conv.GetHeaders("jsontb0")
    if len(headers) != 1 || headers[0] != "value" {
        t.Errorf("Expected headers [value], got %v", headers)
    }

	outputDir := "../../test_output/json_test"
	os.MkdirAll(outputDir, 0755)
	outPath := filepath.Join(outputDir, "json_prim.db")
	os.Remove(outPath)

	f, err := os.Create(outPath)
	if err != nil {
		t.Fatal(err)
	}
	err = converters.ImportToSQLite(conv, f, nil)
	f.Close()

    db, err := sql.Open("sqlite3", outPath)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	rows, err := db.Query("SELECT value FROM jsontb0")
    if err != nil { t.Fatal(err) }
    defer rows.Close()

    var val string

    // Row 1: "first"
    rows.Next()
    rows.Scan(&val)
    if val != "first" { t.Errorf("Row 1: expected 'first', got %s", val) }

    // Row 2: "second"
    rows.Next()
    rows.Scan(&val)
    if val != "second" { t.Errorf("Row 2: expected 'second', got %s", val) }

    // Row 3: "[1,2]" (array)
    rows.Next()
    rows.Scan(&val)
    if val != "[1,2]" { t.Errorf("Row 3: expected '[1,2]', got '%s'", val) }
}
