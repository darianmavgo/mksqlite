package pdf

import (
	"context"
	"fmt"
	"io"
	"math"
	"os"
	"sort"
	"strings"
	"unicode"

	"github.com/darianmavgo/mksqlite/converters"
	"github.com/darianmavgo/mksqlite/converters/common"
	"github.com/dslipak/pdf"
)

func init() {
	converters.Register("pdf", &pdfDriver{})
}

type pdfDriver struct{}

func (d *pdfDriver) Open(source io.Reader, config *common.ConversionConfig) (common.RowProvider, error) {
	return NewPDFConverterWithConfig(source, config)
}

// SizableReaderAt interface for inputs that support random access and size query
type SizableReaderAt interface {
	io.ReaderAt
	Size() (int64, error)
}

// PDFConverter converts PDF files to SQLite tables
type PDFConverter struct {
	tables     []tableData
	tableNames []string
	tempFile   *os.File
}

type tableData struct {
	rawName string
	headers []string
	rows    [][]string
}

// Ensure PDFConverter implements RowProvider, StreamConverter, and io.Closer
var (
	_ common.RowProvider    = (*PDFConverter)(nil)
	_ common.StreamConverter = (*PDFConverter)(nil)
	_ io.Closer             = (*PDFConverter)(nil)
)

// Close cleans up temporary files created during processing
func (c *PDFConverter) Close() error {
	if c.tempFile != nil {
		c.tempFile.Close()
		return os.Remove(c.tempFile.Name())
	}
	return nil
}

// NewPDFConverter creates a new PDFConverter from an io.Reader
func NewPDFConverter(r io.Reader) (*PDFConverter, error) {
	return NewPDFConverterWithConfig(r, nil)
}

// NewPDFConverterWithConfig creates a new PDFConverter with configuration
func NewPDFConverterWithConfig(r io.Reader, config *common.ConversionConfig) (*PDFConverter, error) {
	var tempFile *os.File
	var pdfReader *pdf.Reader

	if config == nil {
		config = &common.ConversionConfig{}
	}

	// 1. Direct file reader
	if f, ok := r.(*os.File); ok {
		info, err := f.Stat()
		if err != nil {
			return nil, fmt.Errorf("failed to stat file: %w", err)
		}
		reader, err := pdf.NewReader(f, info.Size())
		if err != nil {
			return nil, fmt.Errorf("failed to create pdf reader: %w", err)
		}
		pdfReader = reader
	} else if sa, ok := r.(SizableReaderAt); ok {
		// 2. Custom SizableReaderAt
		size, err := sa.Size()
		if err != nil {
			return nil, fmt.Errorf("failed to get size from reader: %w", err)
		}
		reader, err := pdf.NewReader(sa, size)
		if err != nil {
			return nil, fmt.Errorf("failed to create pdf reader: %w", err)
		}
		pdfReader = reader
	} else {
		// 3. Fallback: stream to temporary file
		var err error
		tempFile, err = os.CreateTemp("", "mksqlite-pdf-*.pdf")
		if err != nil {
			return nil, fmt.Errorf("failed to create temp file: %w", err)
		}

		cleanup := func() {
			tempFile.Close()
			os.Remove(tempFile.Name())
		}

		if _, err := io.Copy(tempFile, r); err != nil {
			cleanup()
			return nil, fmt.Errorf("failed to copy pdf stream: %w", err)
		}

		info, err := tempFile.Stat()
		if err != nil {
			cleanup()
			return nil, fmt.Errorf("failed to stat temp file: %w", err)
		}

		reader, err := pdf.NewReader(tempFile, info.Size())
		if err != nil {
			cleanup()
			return nil, fmt.Errorf("failed to parse pdf from temp file: %w", err)
		}
		pdfReader = reader
	}

	tables, err := parsePDF(pdfReader, config)
	if err != nil {
		if tempFile != nil {
			tempFile.Close()
			os.Remove(tempFile.Name())
		}
		return nil, err
	}

	// Generate table names
	rawNames := make([]string, len(tables))
	for i, t := range tables {
		if config.TableName != "" && len(tables) == 1 {
			rawNames[i] = config.TableName
		} else if t.rawName != "" {
			rawNames[i] = t.rawName
		} else {
			rawNames[i] = fmt.Sprintf("table%d", i)
		}
	}
	tableNames := common.GenTableNames(rawNames)
	if config.TableName != "" && len(tables) == 1 {
		tableNames = []string{config.TableName}
	}

	return &PDFConverter{
		tables:     tables,
		tableNames: tableNames,
		tempFile:   tempFile,
	}, nil
}

// GetTableNames implements RowProvider
func (c *PDFConverter) GetTableNames() []string {
	return c.tableNames
}

// GetHeaders implements RowProvider
func (c *PDFConverter) GetHeaders(tableName string) []string {
	for i, name := range c.tableNames {
		if name == tableName {
			return common.GenColumnNames(c.tables[i].headers)
		}
	}
	return nil
}

// GetColumnTypes implements RowProvider
func (c *PDFConverter) GetColumnTypes(tableName string) []string {
	for i, name := range c.tableNames {
		if name == tableName {
			headers := c.tables[i].headers
			rows := c.tables[i].rows
			return common.InferColumnTypes(rows, len(headers))
		}
	}
	return nil
}

// ScanRows implements RowProvider
func (c *PDFConverter) ScanRows(ctx context.Context, tableName string, yield func([]interface{}, error) error) error {
	for i, name := range c.tableNames {
		if name == tableName {
			rows := c.tables[i].rows
			for _, row := range rows {
				interfaceRow := make([]interface{}, len(row))
				for colIdx, val := range row {
					interfaceRow[colIdx] = val
				}
				if err := yield(interfaceRow, nil); err != nil {
					return err
				}
				select {
				case <-ctx.Done():
					return ctx.Err()
				default:
				}
			}
			return nil
		}
	}
	return nil
}

// ConvertToSQL implements StreamConverter for PDF files
func (c *PDFConverter) ConvertToSQL(ctx context.Context, writer io.Writer) error {
	if len(c.tables) == 0 {
		return fmt.Errorf("no tables found in PDF")
	}

	for i, t := range c.tables {
		if len(t.headers) == 0 && len(t.rows) == 0 {
			continue
		}

		tableName := c.tableNames[i]
		sanitizedHeaders := common.GenColumnNames(t.headers)
		colTypes := c.GetColumnTypes(tableName)

		if err := writePDFTableSQL(ctx, tableName, sanitizedHeaders, colTypes, t.rows, writer); err != nil {
			return err
		}
	}

	return nil
}

func writePDFTableSQL(ctx context.Context, tableName string, headers []string, colTypes []string, rows [][]string, writer io.Writer) error {
	createTableSQL := common.GenCreateTableSQLWithTypes(tableName, headers, colTypes)
	if _, err := fmt.Fprintf(writer, "%s;\n\n", createTableSQL); err != nil {
		return fmt.Errorf("failed to write CREATE TABLE: %w", err)
	}

	for _, row := range rows {
		if _, err := fmt.Fprintf(writer, "INSERT INTO %s (", tableName); err != nil {
			return fmt.Errorf("failed to write INSERT start: %w", err)
		}

		for i, header := range headers {
			if i > 0 {
				if _, err := writer.Write([]byte(", ")); err != nil {
					return fmt.Errorf("failed to write column separator: %w", err)
				}
			}
			if _, err := fmt.Fprintf(writer, "%s", header); err != nil {
				return fmt.Errorf("failed to write column name: %w", err)
			}
		}

		if _, err := writer.Write([]byte(") VALUES (")); err != nil {
			return fmt.Errorf("failed to write VALUES start: %w", err)
		}

		currentRow := row
		if len(currentRow) < len(headers) {
			for len(currentRow) < len(headers) {
				currentRow = append(currentRow, "")
			}
		} else if len(currentRow) > len(headers) {
			currentRow = currentRow[:len(headers)]
		}

		for i, val := range currentRow {
			if i > 0 {
				if _, err := writer.Write([]byte(", ")); err != nil {
					return fmt.Errorf("failed to write value separator: %w", err)
				}
			}
			escapedVal := strings.ReplaceAll(val, "'", "''")
			if _, err := fmt.Fprintf(writer, "'%s'", escapedVal); err != nil {
				return fmt.Errorf("failed to write value: %w", err)
			}
		}

		if _, err := writer.Write([]byte(");\n")); err != nil {
			return fmt.Errorf("failed to write statement end: %w", err)
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
	}

	if _, err := writer.Write([]byte("\n")); err != nil {
		return fmt.Errorf("failed to write table separator: %w", err)
	}
	return nil
}

// -------------------------------------------------------------
// PDF Layout Analysis & Table Extraction
// -------------------------------------------------------------

type pdfLine struct {
	Page  int
	Y     float64
	Chars []pdf.Text
}

type pdfToken struct {
	X    float64
	EndX float64
	Text string
}

type tableColumn struct {
	Name  string
	Left  float64
	Right float64
}

func parsePDF(r *pdf.Reader, config *common.ConversionConfig) ([]tableData, error) {
	totalPage := r.NumPage()
	var allPageLines [][]*pdfLine
	var allPagesText []string

	for pNum := 1; pNum <= totalPage; pNum++ {
		page := r.Page(pNum)
		if page.V.IsNull() {
			allPageLines = append(allPageLines, nil)
			continue
		}

		texts := page.Content().Text
		sort.SliceStable(texts, func(i, j int) bool {
			return texts[i].Y > texts[j].Y
		})

		var lines []*pdfLine
		for _, t := range texts {
			if strings.TrimSpace(t.S) == "" && t.S != " " {
				continue
			}
			var matchedLine *pdfLine
			for _, l := range lines {
				if math.Abs(l.Y-t.Y) <= 2.5 {
					matchedLine = l
					break
				}
			}
			if matchedLine != nil {
				matchedLine.Chars = append(matchedLine.Chars, t)
			} else {
				lines = append(lines, &pdfLine{
					Page:  pNum,
					Y:     t.Y,
					Chars: []pdf.Text{t},
				})
			}
		}

		sort.SliceStable(lines, func(i, j int) bool {
			return lines[i].Y > lines[j].Y
		})

		allPageLines = append(allPageLines, lines)

		// Page plain text for fallback
		var pText strings.Builder
		for _, l := range lines {
			tokens := lineToMergedTokens(l)
			for idx, tok := range tokens {
				if idx > 0 {
					pText.WriteString(" ")
				}
				pText.WriteString(tok.Text)
			}
			pText.WriteString("\n")
		}
		allPagesText = append(allPagesText, pText.String())
	}

	// Try extracting structured tables
	extracted := extractTablesFromLines(allPageLines)
	if len(extracted) > 0 {
		return extracted, nil
	}

	// Fallback for non-tabular PDFs: page-based / line-based table
	var fallbackRows [][]string
	for pIdx, lines := range allPageLines {
		for lIdx, l := range lines {
			tokens := lineToMergedTokens(l)
			if len(tokens) == 0 {
				continue
			}
			var sb strings.Builder
			for idx, tok := range tokens {
				if idx > 0 {
					sb.WriteString(" ")
				}
				sb.WriteString(tok.Text)
			}
			lineContent := strings.TrimSpace(sb.String())
			if lineContent != "" {
				fallbackRows = append(fallbackRows, []string{
					fmt.Sprintf("%d", pIdx+1),
					fmt.Sprintf("%d", lIdx+1),
					lineContent,
				})
			}
		}
	}

	if len(fallbackRows) == 0 {
		return []tableData{
			{
				rawName: "tb0",
				headers: []string{"content"},
				rows:    [][]string{},
			},
		}, nil
	}

	return []tableData{
		{
			rawName: "tb0",
			headers: []string{"page", "line", "content"},
			rows:    fallbackRows,
		},
	}, nil
}

func extractTablesFromLines(allPageLines [][]*pdfLine) []tableData {
	type HeaderCandidate struct {
		Page    int
		LineIdx int
		Columns []tableColumn
		Headers []string
		Score   int
	}

	var candidates []HeaderCandidate

	for pIdx, lines := range allPageLines {
		for lIdx, line := range lines {
			tokens := lineToMergedTokens(line)
			if len(tokens) < 3 {
				continue
			}

			fullStr := ""
			for _, tok := range tokens {
				fullStr += tok.Text + " "
			}
			if strings.Contains(fullStr, "S&P 500") || strings.Contains(fullStr, "NASDAQ Comp") || strings.Contains(fullStr, "Time Period:") {
				continue
			}

			var headers []string
			var validTokens []pdfToken
			for _, tok := range tokens {
				hText := cleanHeaderText(tok.Text)
				if hText == "" {
					continue
				}
				headers = append(headers, hText)
				validTokens = append(validTokens, tok)
			}

			if len(headers) >= 3 && isLikelyHeader(headers) {
				colBounds := make([]tableColumn, len(headers))
				for i := 0; i < len(headers); i++ {
					left := 0.0
					if i > 0 {
						left = colBounds[i-1].Right
					}
					right := 2000.0
					if i < len(headers)-1 {
						estW := float64(len(validTokens[i].Text)) * 3.8
						hRight := math.Max(validTokens[i].EndX, validTokens[i].X+estW)
						nextX := validTokens[i+1].X
						// If next column is at nextX, boundary is midpoint between hRight and nextX
						if hRight < nextX {
							right = (hRight + nextX) / 2.0
						} else {
							right = nextX - 4.0
						}
					}
					colBounds[i] = tableColumn{
						Name:  headers[i],
						Left:  left,
						Right: right,
					}
				}

				candidates = append(candidates, HeaderCandidate{
					Page:    pIdx + 1,
					LineIdx: lIdx,
					Columns: colBounds,
					Headers: headers,
					Score:   len(headers) * 10,
				})
			}
		}
	}

	if len(candidates) == 0 {
		return nil
	}

	bestHeader := candidates[0]
	for _, c := range candidates {
		if c.Score > bestHeader.Score {
			bestHeader = c
		}
	}

	columns := bestHeader.Columns
	headers := bestHeader.Headers

	var allRows [][]string

	for pIdx, lines := range allPageLines {
		pageNum := pIdx + 1
		var contentLines []*pdfLine
		startCollecting := false
		if pageNum > bestHeader.Page {
			startCollecting = true
		}

		for lIdx, line := range lines {
			if !startCollecting {
				if pageNum == bestHeader.Page && lIdx == bestHeader.LineIdx {
					startCollecting = true
				}
				continue
			}

			tokens := lineToMergedTokens(line)
			if len(tokens) == 0 {
				continue
			}

			lineStr := ""
			for _, t := range tokens {
				lineStr += t.Text + " "
			}

			if isBannerOrNoise(lineStr, headers) {
				continue
			}

			contentLines = append(contentLines, line)
		}

		pageRows := extractRowsFromLines(contentLines, columns)
		allRows = append(allRows, pageRows...)
	}

	var cleanedRows [][]string
	for _, row := range allRows {
		filled := 0
		for _, col := range row {
			if strings.TrimSpace(col) != "" {
				filled++
			}
		}
		if filled >= 2 {
			cleanedRows = append(cleanedRows, row)
		}
	}

	if len(cleanedRows) == 0 {
		return nil
	}

	return []tableData{
		{
			rawName: "table0",
			headers: headers,
			rows:    cleanedRows,
		},
	}
}

func isBannerOrNoise(lineStr string, headers []string) bool {
	noisePhrases := []string{
		"S&P 500", "NASDAQ Comp", "RUSSELL 1000", "Time Period:",
		"Beginning Cash Balance", "Ending Cash Balance", "Statements Flex Queries",
		"Welcome ", "Trade Portfolio", "Transaction History", "Third-Party Reports",
		"Tax Documents", "Other Reports", "Search Date Account", "AllRows",
	}
	for _, np := range noisePhrases {
		if strings.Contains(lineStr, np) {
			return true
		}
	}

	matchCount := 0
	for _, h := range headers {
		if strings.Contains(lineStr, h) {
			matchCount++
		}
	}
	if matchCount >= 4 || (len(headers) > 0 && matchCount >= len(headers)/2) {
		return true
	}

	return false
}

func extractRowsFromLines(lines []*pdfLine, columns []tableColumn) [][]string {
	var rows [][]string
	numCols := len(columns)

	type CellAcc struct {
		cells [][]string
	}
	newCellAcc := func() *CellAcc {
		c := &CellAcc{cells: make([][]string, numCols)}
		for i := range c.cells {
			c.cells[i] = []string{}
		}
		return c
	}

	var curRow *CellAcc
	var prevLineY float64 = -1

	flushRow := func() {
		if curRow == nil {
			return
		}
		var rowValues []string
		hasAny := false
		for cIdx := 0; cIdx < numCols; cIdx++ {
			val := strings.TrimSpace(strings.Join(curRow.cells[cIdx], " "))
			if val != "" {
				hasAny = true
			}
			rowValues = append(rowValues, val)
		}
		if hasAny {
			if numCols > 0 && strings.Contains(strings.ToLower(columns[0].Name), "date") {
				rowValues[0] = strings.ReplaceAll(rowValues[0], " ", "")
			}
			rows = append(rows, rowValues)
		}
		curRow = nil
	}

	for _, line := range lines {
		tokens := lineToMergedTokens(line)
		if len(tokens) == 0 {
			continue
		}

		lineCells := make(map[int]string)
		for _, tok := range tokens {
			// Skip standalone page number tokens in header/footer margin
			if (line.Y > 720 || line.Y < 80) && len(tok.Text) <= 3 && isAllDigits(tok.Text) {
				continue
			}

			colIdx := -1
			for i, col := range columns {
				if tok.X >= col.Left && tok.X < col.Right {
					colIdx = i
					break
				}
			}
			if colIdx >= 0 {
				if existing, ok := lineCells[colIdx]; ok {
					lineCells[colIdx] = existing + " " + tok.Text
				} else {
					lineCells[colIdx] = tok.Text
				}
			}
		}

		isNewRecord := false
		if prevLineY > 0 && (prevLineY-line.Y > 10.0) {
			isNewRecord = true
		}
		val0, has0 := lineCells[0]
		trim0 := strings.TrimSpace(val0)
		if has0 && (strings.HasPrefix(trim0, "202") || strings.HasPrefix(trim0, "199") || strings.HasPrefix(trim0, "201")) && strings.HasSuffix(trim0, "-") {
			isNewRecord = true
		}

		if isNewRecord && curRow != nil {
			flushRow()
			curRow = newCellAcc()
		} else if curRow == nil {
			curRow = newCellAcc()
		}

		for colIdx, val := range lineCells {
			curRow.cells[colIdx] = append(curRow.cells[colIdx], val)
		}
		prevLineY = line.Y
	}
	flushRow()

	return rows
}

func lineToMergedTokens(line *pdfLine) []pdfToken {
	sort.SliceStable(line.Chars, func(i, j int) bool {
		if line.Chars[i].X != line.Chars[j].X {
			return line.Chars[i].X < line.Chars[j].X
		}
		return line.Chars[i].Y > line.Chars[j].Y
	})

	var tokens []pdfToken
	var b strings.Builder
	startX := -1.0
	currentRight := -1.0

	for _, ch := range line.Chars {
		charW := 3.6
		if ch.FontSize > 0 {
			charW = ch.FontSize * 0.52
		}

		if startX < 0 {
			startX = ch.X
			currentRight = ch.X + charW
			b.WriteString(ch.S)
		} else {
			gap := ch.X - currentRight
			if gap > 5.0 {
				txt := strings.TrimSpace(b.String())
				if txt != "" {
					tokens = append(tokens, pdfToken{
						X:    startX,
						EndX: currentRight,
						Text: txt,
					})
				}
				b.Reset()
				startX = ch.X
				currentRight = ch.X + charW
			} else {
				if ch.X+charW > currentRight {
					currentRight = ch.X + charW
				} else {
					currentRight += charW
				}
			}
			b.WriteString(ch.S)
		}
	}
	txt := strings.TrimSpace(b.String())
	if txt != "" {
		tokens = append(tokens, pdfToken{
			X:    startX,
			EndX: currentRight,
			Text: txt,
		})
	}

	var merged []pdfToken
	for i := 0; i < len(tokens); i++ {
		t := tokens[i]
		if i+1 < len(tokens) {
			nextT := tokens[i+1]
			if t.Text == "Transaction T" && (nextT.Text == "ype" || nextT.Text == "Type") {
				merged = append(merged, pdfToken{
					X:    t.X,
					EndX: nextT.EndX,
					Text: "Transaction Type",
				})
				i++
				continue
			}
		}
		merged = append(merged, t)
	}

	return merged
}

func isAllDigits(s string) bool {
	if len(s) == 0 {
		return false
	}
	for _, r := range s {
		if !unicode.IsDigit(r) {
			return false
		}
	}
	return true
}

func isLikelyHeader(headers []string) bool {
	lettersCount := 0
	for _, h := range headers {
		hasLetter := false
		for _, r := range h {
			if unicode.IsLetter(r) {
				hasLetter = true
				break
			}
		}
		if hasLetter {
			lettersCount++
		}
	}
	return lettersCount >= 3 && lettersCount >= len(headers)/2
}

func cleanHeaderText(s string) string {
	s = strings.TrimSpace(s)
	s = strings.TrimFunc(s, func(r rune) bool {
		return !unicode.IsLetter(r) && !unicode.IsDigit(r) && r != '_' && r != ' ' && r != '-'
	})
	return strings.TrimSpace(s)
}
