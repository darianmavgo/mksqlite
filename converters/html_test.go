package converters

import (
	"bytes"
	"database/sql"
	"os"
	"strings"
	"testing"

	_ "github.com/mattn/go-sqlite3"
)

func TestHTMLConverter_ConvertFile(t *testing.T) {
	inputPath := "../sample_data/demo_mavgo_flight/BERKSHIRE HATHAWAY INC.html"
	if _, err := os.Stat(inputPath); os.IsNotExist(err) {
		t.Skipf("Sample file not found: %s", inputPath)
	}

	// Create a temp file for output to avoid polluting the source tree
	f, err := os.CreateTemp("", "html_convert_*.db")
	if err != nil {
		t.Fatalf("failed to create temp file: %v", err)
	}
	outputPath := f.Name()
	f.Close()
	defer os.Remove(outputPath)

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

	// Get list of tables
	rows, err := db.Query("SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'")
	if err != nil {
		t.Fatalf("Failed to query tables: %v", err)
	}
	defer rows.Close()

	var tables []string
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			t.Fatalf("Failed to scan table name: %v", err)
		}
		tables = append(tables, name)
	}

	if len(tables) == 0 {
		t.Fatalf("Expected at least one table, found 0")
	}

	foundText := false
	for _, tableName := range tables {
		// Try to find "Warren" in any column of the table.
		// Since we don't know exact column names without querying schema,
		// we will just select * and scan into interface{}

		// First get columns
		colRows, err := db.Query("SELECT * FROM " + tableName + " LIMIT 1")
		if err != nil {
			continue // Maybe empty table
		}
		cols, _ := colRows.Columns()
		colRows.Close()

		if len(cols) == 0 {
			continue
		}

		query := "SELECT * FROM " + tableName
		dataRows, err := db.Query(query)
		if err != nil {
			t.Logf("Failed to query table %s: %v", tableName, err)
			continue
		}
		defer dataRows.Close()

		values := make([]interface{}, len(cols))
		scanArgs := make([]interface{}, len(cols))
		for i := range values {
			scanArgs[i] = &values[i]
		}

		for dataRows.Next() {
			err = dataRows.Scan(scanArgs...)
			if err != nil {
				t.Fatalf("Failed to scan row in table %s: %v", tableName, err)
			}
			for _, val := range values {
				if strVal, ok := val.(string); ok {
					if strings.Contains(strVal, "Warren") {
						foundText = true
						break
					}
				} else if bytesVal, ok := val.([]byte); ok {
					if strings.Contains(string(bytesVal), "Warren") {
						foundText = true
						break
					}
				}
			}
			if foundText {
				break
			}
		}
		if foundText {
			break
		}
	}

	if !foundText {
		t.Errorf("Expected to find 'Warren' in one of the tables, but did not.")
	}
}

func TestHTMLConverter_ConvertToSQL(t *testing.T) {
	// Simple unit test for SQL generation logic
	htmlContent := `
<table>
	<tr>
		<th>ColA</th>
		<th>ColB</th>
	</tr>
	<tr>
		<td>Val1</td>
		<td>Val2</td>
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

	// Expect CREATE TABLE table0 (cola TEXT, colb TEXT);
	// Note: sanitization might lowercase them.
	if !strings.Contains(output, "CREATE TABLE table0") {
		t.Errorf("output missing CREATE TABLE table0. Got: %s", output)
	}

	if !strings.Contains(output, "INSERT INTO table0") {
		t.Errorf("output missing INSERT INTO table0. Got: %s", output)
	}

	if !strings.Contains(output, "'Val1'") || !strings.Contains(output, "'Val2'") {
		t.Errorf("output missing values. Got: %s", output)
	}
}
