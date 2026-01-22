package markdown

import (
	"bytes"
	"strings"
	"testing"
)

func TestMarkdownConverter(t *testing.T) {
	tests := []struct {
		name           string
		input          string
		expectedTables []string
		expectedData   map[string][][]string // tableName -> rows (excluding headers)
		expectedHeader map[string][]string   // tableName -> headers
	}{
		{
			name: "Simple Table",
			input: `
| Col1 | Col2 |
|---|---|
| Val1 | Val2 |
| Val3 | Val4 |
`,
			expectedTables: []string{"table0"},
			expectedHeader: map[string][]string{"table0": {"col1", "col2"}},
			expectedData: map[string][][]string{
				"table0": {
					{"Val1", "Val2"},
					{"Val3", "Val4"},
				},
			},
		},
		{
			name: "Named Table Header",
			input: `
### Users
| ID | Name |
|---|---|
| 1 | Alice |
`,
			expectedTables: []string{"users"},
			expectedHeader: map[string][]string{"users": {"id", "name"}},
			expectedData: map[string][][]string{
				"users": {
					{"1", "Alice"},
				},
			},
		},
		{
			name: "Named Table Anchor",
			input: `
<a id="products"></a>
| ID | Product |
|---|---|
| 10 | Apple |
`,
			expectedTables: []string{"products"},
			expectedHeader: map[string][]string{"products": {"id", "product"}},
			expectedData: map[string][][]string{
				"products": {
					{"10", "Apple"},
				},
			},
		},
		{
			name: "List Simple",
			input: `
### MyList
* Item 1
* Item 2
`,
			expectedTables: []string{"mylist"},
			// "key" is a reserved keyword in SQLite, so GenColumnNames changes it to cl{index} -> cl0
			expectedHeader: map[string][]string{"mylist": {"cl0", "value"}},
			expectedData: map[string][][]string{
				"mylist": {
					{"Item 1", ""},
					{"Item 2", ""},
				},
			},
		},
		{
			name: "List Nested",
			input: `
### NestedList
* Parent 1
  * Child 1
  * Child 2
* Parent 2
  Description
`,
			expectedTables: []string{"nestedlist"},
			expectedHeader: map[string][]string{"nestedlist": {"cl0", "value"}},
			expectedData: map[string][][]string{
				"nestedlist": {
					{"Parent 1", "  * Child 1\n  * Child 2"},
					{"Parent 2", "  Description"},
				},
			},
		},
		{
			name: "Mixed Content",
			input: `
# Section 1

<a name="t1"></a>
| A | B |
|---|---|
| 1 | 2 |

Some text.

### L1
* K1
  V1

`,
			expectedTables: []string{"t1", "l1"},
			expectedHeader: map[string][]string{
				"t1": {"a", "b"},
				"l1": {"cl0", "value"},
			},
			expectedData: map[string][][]string{
				"t1": {{"1", "2"}},
				"l1": {{"K1", "V1"}},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			conv, err := NewMarkdownConverter(strings.NewReader(tt.input))
			if err != nil {
				t.Fatalf("NewMarkdownConverter failed: %v", err)
			}

			tables := conv.GetTableNames()
			if len(tables) != len(tt.expectedTables) {
				t.Errorf("Expected %d tables, got %d (%v)", len(tt.expectedTables), len(tables), tables)
			}

			// Check table names
			// Convert to map for easy checking
			tableMap := make(map[string]bool)
			for _, n := range tables {
				tableMap[n] = true
			}
			for _, n := range tt.expectedTables {
				if !tableMap[n] {
					t.Errorf("Expected table %s not found (got %v)", n, tables)
				}
			}

			// Check content
			for _, tableName := range tables {
				headers := conv.GetHeaders(tableName)
				expHeaders := tt.expectedHeader[tableName]

				if len(headers) != len(expHeaders) {
					t.Errorf("Table %s: Expected headers %v, got %v", tableName, expHeaders, headers)
				} else {
					for i := range headers {
						if headers[i] != expHeaders[i] {
							t.Errorf("Table %s: Header mismatch at %d: %s vs %s", tableName, i, expHeaders[i], headers[i])
						}
					}
				}

				var rows [][]interface{}
				err := conv.ScanRows(tableName, func(row []interface{}) error {
					rows = append(rows, row)
					return nil
				})
				if err != nil {
					t.Fatalf("ScanRows failed for %s: %v", tableName, err)
				}

				expRows := tt.expectedData[tableName]
				if len(rows) != len(expRows) {
					t.Errorf("Table %s: Expected %d rows, got %d", tableName, len(expRows), len(rows))
				}

				for i, row := range rows {
					if i >= len(expRows) {
						break
					}
					expRow := expRows[i]
					if len(row) != len(expRow) {
						t.Errorf("Table %s Row %d: length mismatch", tableName, i)
						continue
					}
					for j, val := range row {
						strVal, ok := val.(string)
						if !ok {
							t.Errorf("Table %s Row %d Col %d: not a string", tableName, i, j)
						}
						if strings.TrimSpace(strVal) != strings.TrimSpace(expRow[j]) {
							t.Errorf("Table %s Row %d Col %d: expected %q, got %q", tableName, i, j, expRow[j], strVal)
						}
					}
				}
			}
		})
	}
}

func TestConvertToSQL(t *testing.T) {
	input := `
### Settings
| Item | Value |
|---|---|
| dark_mode | true |
`
	conv, err := NewMarkdownConverter(strings.NewReader(input))
	if err != nil {
		t.Fatalf("NewMarkdownConverter failed: %v", err)
	}

	var buf bytes.Buffer
	err = conv.ConvertToSQL(&buf)
	if err != nil {
		t.Fatalf("ConvertToSQL failed: %v", err)
	}

	sql := buf.String()
	// Table name sanitized: Settings -> settings
	// Columns: Item -> item, Value -> value
	if !strings.Contains(sql, "CREATE TABLE settings") {
		t.Errorf("SQL missing CREATE TABLE: %s", sql)
	}
	if !strings.Contains(sql, "INSERT INTO settings") {
		t.Errorf("SQL missing INSERT: %s", sql)
	}
	if !strings.Contains(sql, "'dark_mode'") {
		t.Errorf("SQL missing value: %s", sql)
	}
}
