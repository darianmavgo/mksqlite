package sink

import "strings"

// Splitter incrementally splits a stream of SQL text into complete statements.
// It is quote-aware: a ';' inside '...' or "..." (including doubled-quote
// escapes) does not end a statement. Input may be fragmented arbitrarily.
type Splitter struct {
	buf      strings.Builder
	inSingle bool
	inDouble bool
}

// Feed consumes p and returns any statements completed by it, trimmed and
// without the terminating ';'. Empty statements are dropped.
func (s *Splitter) Feed(p []byte) []string {
	var out []string
	for _, b := range p {
		switch {
		case b == '\'' && !s.inDouble:
			s.inSingle = !s.inSingle
		case b == '"' && !s.inSingle:
			s.inDouble = !s.inDouble
		case b == ';' && !s.inSingle && !s.inDouble:
			if stmt := strings.TrimSpace(s.buf.String()); stmt != "" {
				out = append(out, stmt)
			}
			s.buf.Reset()
			continue
		}
		s.buf.WriteByte(b)
	}
	return out
}

// Flush returns any trailing statement that was not terminated by ';'.
func (s *Splitter) Flush() string {
	stmt := strings.TrimSpace(s.buf.String())
	s.buf.Reset()
	s.inSingle, s.inDouble = false, false
	return stmt
}
