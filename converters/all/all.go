package all

import (
	// Import all the converters so they register themselves
	_ "github.com/darianmavgo/mksqlite/converters/csv"
	_ "github.com/darianmavgo/mksqlite/converters/excel"
	_ "github.com/darianmavgo/mksqlite/converters/filesystem"
	_ "github.com/darianmavgo/mksqlite/converters/html"
	_ "github.com/darianmavgo/mksqlite/converters/json"
	_ "github.com/darianmavgo/mksqlite/converters/markdown"
	_ "github.com/darianmavgo/mksqlite/converters/txt"
	_ "github.com/darianmavgo/mksqlite/converters/zip"
)
