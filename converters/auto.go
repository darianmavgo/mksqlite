package converters

import (
	"bufio"
	"bytes"
	"io"
	"strings"
	"unicode"

	"github.com/darianmavgo/mksqlite/converters/common"
)

// DetectFormat infers the format of the data from the given bytes.
func DetectFormat(data []byte) string {
	if bytes.HasPrefix(data, []byte("%PDF-")) {
		return "pdf"
	}
	if bytes.HasPrefix(data, []byte("PK\x03\x04")) {
		if bytes.Contains(data, []byte("xl/")) || bytes.Contains(data, []byte("[Content_Types].xml")) || bytes.Contains(data, []byte("_rels/")) {
			return "excel"
		}
		return "zip"
	}

	// Excel old format (xls)
	if bytes.HasPrefix(data, []byte{0xD0, 0xCF, 0x11, 0xE0, 0xA1, 0xB1, 0x1A, 0xE1}) {
		return "excel"
	}

	// Trim space
	s := string(bytes.TrimLeftFunc(data, unicode.IsSpace))
	if len(s) == 0 {
		return "txt" // fallback
	}

	if strings.HasPrefix(s, "{") || strings.HasPrefix(s, "[") {
		return "json"
	}

	lower := strings.ToLower(s)
	if strings.HasPrefix(lower, "<html") || strings.HasPrefix(lower, "<!doctype html") {
		return "html"
	}

	if strings.Contains(s, "|") && strings.Contains(s, "\n") {
		// Could be markdown, check for |---|
		if strings.Contains(s, "|-") || strings.Contains(s, "-|") {
			return "markdown"
		}
	}

	// CSV vs TXT
	// Simple check: does the first line have commas or tabs?
	firstLine := strings.SplitN(s, "\n", 2)[0]
	if strings.Contains(firstLine, ",") || strings.Contains(firstLine, "\t") {
		return "csv"
	}

	return "txt"
}

// AutoOpen peeks into the stream to detect its format and opens it with the correct converter.
func AutoOpen(source io.Reader, config *common.ConversionConfig) (common.RowProvider, error) {
	// If it's a seeker, we can read, seek back, and pass the original source.
	// This preserves fast paths for *os.File and io.ReaderAt.
	if seeker, ok := source.(io.ReadSeeker); ok {
		buf := make([]byte, 4096)
		n, err := seeker.Read(buf)
		if err != nil && err != io.EOF {
			return nil, err
		}
		if _, err := seeker.Seek(0, io.SeekStart); err != nil {
			return nil, err
		}
		driverName := DetectFormat(buf[:n])
		return Open(driverName, source, config)
	}

	// Otherwise, wrap in bufio.Reader
	br := bufio.NewReader(source)
	buf, err := br.Peek(4096)
	if err != nil && err != io.EOF {
		return nil, err
	}

	driverName := DetectFormat(buf)
	return Open(driverName, br, config)
}
