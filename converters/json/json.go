package json

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"github.com/darianmavgo/mksqlite/converters"
	"github.com/darianmavgo/mksqlite/converters/common"
	"sort"
	"strings"
)

func init() {
	converters.Register("json", &jsonDriver{})
}

type jsonDriver struct{}

func (d *jsonDriver) Open(source io.Reader, config *common.ConversionConfig) (common.RowProvider, error) {
	return NewJSONConverter(source)
}

// JSONConverter converts JSON files to SQLite tables
type JSONConverter struct {
	reader     io.Reader
	tableNames []string
	tables     map[string]*jsonTableInfo

	// For streaming array
	decoder    *json.Decoder
	firstRow   map[string]interface{}
	arrayTable string // Name of the table if root is array

	// For object (in-memory fallback or seeker)
	isSeeker bool
	seeker   io.ReadSeeker
	objData  map[string]interface{} // If we load fully
}

type jsonTableInfo struct {
	headers    []string
	rawHeaders []string
	// For object-based streaming (seeker)
	arrayKey string
}

// Ensure JSONConverter implements RowProvider
var _ common.RowProvider = (*JSONConverter)(nil)

// Ensure JSONConverter implements StreamConverter
var _ common.StreamConverter = (*JSONConverter)(nil)

// NewJSONConverter creates a new JSONConverter from an io.Reader.
func NewJSONConverter(r io.Reader) (*JSONConverter, error) {
	seeker, isSeeker := r.(io.ReadSeeker)

	dec := json.NewDecoder(r)

	// Peek the first token to determine structure
	token, err := dec.Token()
	if err != nil {
		return nil, fmt.Errorf("failed to read JSON start: %w", err)
	}

	delim, ok := token.(json.Delim)
	if !ok {
		return nil, fmt.Errorf("expected JSON object or array at root")
	}

	c := &JSONConverter{
		reader:   r,
		isSeeker: isSeeker,
		seeker:   seeker,
		tables:   make(map[string]*jsonTableInfo),
	}

	if delim == '[' {
		// Root is Array
		c.arrayTable = "jsontb0"
		c.tableNames = []string{c.arrayTable}
		c.decoder = dec // Keep using this decoder

		// Read first element to determine headers
		if dec.More() {
			var firstElem interface{}
			if err := dec.Decode(&firstElem); err != nil {
				return nil, fmt.Errorf("failed to decode first element: %w", err)
			}

			rowMap, ok := firstElem.(map[string]interface{})
			if !ok {
				// If strictly not an object, maybe it's a list of primitives?
				rowMap = map[string]interface{}{"value": firstElem}
			}

			c.firstRow = rowMap
			rawHeaders := extractRawHeaders(rowMap)
			c.tables[c.arrayTable] = &jsonTableInfo{
				rawHeaders: rawHeaders,
				headers:    common.GenColumnNames(rawHeaders),
			}
		} else {
			// Empty array
			c.tables[c.arrayTable] = &jsonTableInfo{headers: []string{}, rawHeaders: []string{}}
		}

	} else if delim == '{' {
		// Root is Object
		c.objData = make(map[string]interface{})

		// Parse the object manually
		for dec.More() {
			keyToken, err := dec.Token()
			if err != nil {
				return nil, fmt.Errorf("failed to read key: %w", err)
			}
			key, ok := keyToken.(string)
			if !ok {
				return nil, fmt.Errorf("expected string key")
			}

			var val interface{}
			if err := dec.Decode(&val); err != nil {
				return nil, fmt.Errorf("failed to decode value for key %s: %w", key, err)
			}
			c.objData[key] = val
		}

		// Consume closing '}'
		if _, err := dec.Token(); err != nil {
			return nil, fmt.Errorf("expected closing '}'")
		}

		// Analyze for tables
		var names []string
		for k, v := range c.objData {
			if arr, ok := v.([]interface{}); ok {
				names = append(names, k)
				// Determine headers from first element of array
				var rawHeaders []string
				if len(arr) > 0 {
					if firstObj, ok := arr[0].(map[string]interface{}); ok {
						rawHeaders = extractRawHeaders(firstObj)
					} else {
						rawHeaders = []string{"value"}
					}
				}
				c.tables[k] = &jsonTableInfo{
					rawHeaders: rawHeaders,
					headers:    common.GenColumnNames(rawHeaders),
				}
			}
		}
		sort.Strings(names)
		c.tableNames = common.GenTableNames(names)

		// Rebuild c.tables with sanitized names
		newTables := make(map[string]*jsonTableInfo)
		for i, rawName := range names {
			sanitized := c.tableNames[i]
			newTables[sanitized] = c.tables[rawName]
			newTables[sanitized].arrayKey = rawName // Store original key
		}
		c.tables = newTables

	} else {
		return nil, fmt.Errorf("unexpected delimiter: %v", delim)
	}

	return c, nil
}

func extractRawHeaders(row map[string]interface{}) []string {
	keys := make([]string, 0, len(row))
	for k := range row {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

// GetTableNames implements RowProvider
func (c *JSONConverter) GetTableNames() []string {
	return c.tableNames
}

// GetHeaders implements RowProvider
func (c *JSONConverter) GetHeaders(tableName string) []string {
	if info, ok := c.tables[tableName]; ok {
		return info.headers
	}
	return nil
}

// ScanRows implements RowProvider
func (c *JSONConverter) ScanRows(tableName string, yield func([]interface{}, error) error) error {
	info, ok := c.tables[tableName]
	if !ok {
		return nil
	}

	// Case 1: Root Array Streaming
	if c.arrayTable != "" && tableName == c.arrayTable {
		// Yield first row if exists
		if c.firstRow != nil {
			row := flattenRow(c.firstRow, info.rawHeaders)
			if err := yield(row, nil); err != nil {
				return err
			}
			c.firstRow = nil // Consumed
		}

		// Stream the rest
		for c.decoder.More() {
			t, err := c.decoder.Token()
			if err != nil {
				return fmt.Errorf("error reading token: %w", err)
			}

			var rowMap map[string]json.RawMessage
			if delim, ok := t.(json.Delim); ok && delim == '{' {
				// Object optimization: stream keys
				rowMap = make(map[string]json.RawMessage)
				for c.decoder.More() {
					keyToken, err := c.decoder.Token()
					if err != nil {
						return fmt.Errorf("error reading key: %w", err)
					}
					key, ok := keyToken.(string)
					if !ok {
						return fmt.Errorf("expected string key")
					}
					var val json.RawMessage
					if err := c.decoder.Decode(&val); err != nil {
						return fmt.Errorf("error decoding value for key %s: %w", key, err)
					}
					rowMap[key] = val
				}
				// Consume closing '}'
				if _, err := c.decoder.Token(); err != nil {
					return fmt.Errorf("error reading closing brace: %w", err)
				}
			} else {
				// Non-object: fallback
				raw, err := reconstructRawJSON(t, c.decoder)
				if err != nil {
					return fmt.Errorf("error reconstructing raw json: %w", err)
				}
				rowMap = map[string]json.RawMessage{"value": raw}
			}

			row := flattenRowRaw(rowMap, info.rawHeaders)
			if err := yield(row, nil); err != nil {
				return err
			}
		}
		return nil
	}

	// Case 2: In-Memory Object
	if c.objData != nil {
		originalKey := info.arrayKey
		if arr, ok := c.objData[originalKey].([]interface{}); ok {
			for _, val := range arr {
				rowMap, ok := val.(map[string]interface{})
				if !ok {
					rowMap = map[string]interface{}{"value": val}
				}
				row := flattenRow(rowMap, info.rawHeaders)
				if err := yield(row, nil); err != nil {
					return err
				}
			}
		}
	}

	return nil
}

func flattenRow(rowMap map[string]interface{}, rawHeaders []string) []interface{} {
	row := make([]interface{}, len(rawHeaders))
	for i, key := range rawHeaders {
		val, ok := rowMap[key]
		if !ok {
			row[i] = nil
			continue
		}

		// Handle nesting: "Anything more nested than that can be added to a json field in each row."
		switch v := val.(type) {
		case map[string]interface{}, []interface{}:
			b, err := json.Marshal(v)
			if err == nil {
				row[i] = string(b)
			} else {
				row[i] = fmt.Sprintf("%v", v) // Fallback
			}
		default:
			row[i] = v
		}
	}
	return row
}

func reconstructRawJSON(t json.Token, dec *json.Decoder) (json.RawMessage, error) {
	delim, ok := t.(json.Delim)
	if !ok {
		// Primitive
		return json.Marshal(t)
	}

	// It's a delimiter.
	if delim == '[' {
		// Array
		var sb bytes.Buffer
		sb.WriteByte('[')
		first := true
		for dec.More() {
			if !first {
				sb.WriteByte(',')
			}
			first = false

			// We can use Decode(&raw) for elements
			var val json.RawMessage
			if err := dec.Decode(&val); err != nil {
				return nil, err
			}
			sb.Write(val)
		}
		// Consume ']'
		if _, err := dec.Token(); err != nil {
			return nil, err
		}
		sb.WriteByte(']')
		return json.RawMessage(sb.Bytes()), nil
	}

	// Should not happen for object roots handled by ScanRows, but good for completeness
	if delim == '{' {
		var sb bytes.Buffer
		sb.WriteByte('{')
		first := true
		for dec.More() {
			if !first {
				sb.WriteByte(',')
			}
			first = false

			// Key
			k, err := dec.Token()
			if err != nil {
				return nil, err
			}
			key, ok := k.(string)
			if !ok {
				return nil, fmt.Errorf("expected string key")
			}

			keyBytes, _ := json.Marshal(key)
			sb.Write(keyBytes)
			sb.WriteByte(':')

			// Value
			var val json.RawMessage
			if err := dec.Decode(&val); err != nil {
				return nil, err
			}
			sb.Write(val)
		}
		if _, err := dec.Token(); err != nil {
			return nil, err
		}
		sb.WriteByte('}')
		return json.RawMessage(sb.Bytes()), nil
	}

	return nil, fmt.Errorf("unexpected delimiter: %v", delim)
}

func flattenRowRaw(rowMap map[string]json.RawMessage, rawHeaders []string) []interface{} {
	row := make([]interface{}, len(rawHeaders))
	for i, key := range rawHeaders {
		val, ok := rowMap[key]
		if !ok || len(val) == 0 {
			row[i] = nil
			continue
		}

		// Check for null
		if string(val) == "null" {
			row[i] = nil
			continue
		}

		// Check if it's a complex type (object or array)
		firstChar := val[0]
		if firstChar == '{' || firstChar == '[' {
			// It's complex, keep as string
			row[i] = string(val)
		} else {
			// It's primitive, unmarshal it
			var primitive interface{}
			if err := json.Unmarshal(val, &primitive); err != nil {
				row[i] = string(val) // Fallback
			} else {
				row[i] = primitive
			}
		}
	}
	return row
}

// ConvertToSQL implements StreamConverter
func (c *JSONConverter) ConvertToSQL(writer io.Writer) error {
	for _, tableName := range c.GetTableNames() {
		headers := c.GetHeaders(tableName)
		createSQL := common.GenCreateTableSQL(tableName, headers)
		if _, err := fmt.Fprintf(writer, "%s;\n\n", createSQL); err != nil {
			return err
		}

		err := c.ScanRows(tableName, func(row []interface{}, err error) error {
			if err != nil {
				return err
			}
			if _, err := fmt.Fprintf(writer, "INSERT INTO %s (", tableName); err != nil {
				return err
			}
			// columns
			for i, h := range headers {
				if i > 0 {
					if _, err := fmt.Fprint(writer, ", "); err != nil {
						return err
					}
				}
				if _, err := fmt.Fprint(writer, h); err != nil {
					return err
				}
			}
			if _, err := fmt.Fprint(writer, ") VALUES ("); err != nil {
				return err
			}
			// values
			for i, val := range row {
				if i > 0 {
					if _, err := fmt.Fprint(writer, ", "); err != nil {
						return err
					}
				}
				// handle types
				switch v := val.(type) {
				case nil:
					if _, err := fmt.Fprint(writer, "NULL"); err != nil {
						return err
					}
				case string:
					escaped := strings.ReplaceAll(v, "'", "''")
					if _, err := fmt.Fprintf(writer, "'%s'", escaped); err != nil {
						return err
					}
				default:
					if _, err := fmt.Fprintf(writer, "'%v'", v); err != nil {
						return err
					}
				}
			}
			if _, err := fmt.Fprint(writer, ");\n"); err != nil {
				return err
			}
			return nil
		})
		if err != nil {
			return err
		}
		if _, err := fmt.Fprint(writer, "\n"); err != nil {
			return err
		}
	}
	return nil
}
