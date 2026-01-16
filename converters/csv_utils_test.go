package converters

import "testing"

func TestColumnCount(t *testing.T) {
	tests := []struct {
		name      string
		rawlines  []string
		delimiter string
		want      int
	}{
		{
			name:      "Explicit delimiter",
			rawlines:  []string{"a,b,c", "d,e,f"},
			delimiter: ",",
			want:      3,
		},
		{
			name:      "Auto detect comma",
			rawlines:  []string{"a,b,c", "d,e,f"},
			delimiter: "",
			want:      3,
		},
		{
			name:      "Auto detect pipe",
			rawlines:  []string{"a|b|c", "d|e|f"},
			delimiter: "",
			want:      3,
		},
		{
			name:      "Inconsistent delimiters - comma wins by frequency",
			rawlines:  []string{"a,b|c", "d,e,f"},
			// Comma: 1, 2 (Total 3, Avg 1.5)
			// Pipe: 1, 0 (Total 1, Avg 0.5)
			// Comma wins. Max cols: 3 (from line 2).
			delimiter: "",
			want:      3,
		},
		{
			name:      "Real world consistent vs random",
			rawlines:  []string{"id,name,desc", "1,apple,fruit", "2,pear,fruit|yummy"},
			// Comma: 2, 2, 2 (Consistent)
			// Pipe: 0, 0, 1 (Inconsistent)
			// Comma should win.
			delimiter: "",
			want:      3,
		},
		{
			name:      "Empty lines",
			rawlines:  []string{},
			delimiter: "",
			want:      0,
		},
		{
			name:      "Single empty line",
			rawlines:  []string{""},
			delimiter: "",
			want:      1, // 1 column (empty)
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := ColumnCount(tt.rawlines, tt.delimiter)
			if got != tt.want {
				t.Errorf("ColumnCount() = %v, want %v", got, tt.want)
			}
		})
	}
}
