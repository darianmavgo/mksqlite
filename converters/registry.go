package converters

import (
	"fmt"
	"io"
	"sort"
	"sync"
)

var (
	driversMu sync.RWMutex
	drivers   = make(map[string]Driver)
)

// Register makes a converter driver available by the provided name.
// If Register is called twice with the same name or if driver is nil,
// it panics.
func Register(name string, driver Driver) {
	driversMu.Lock()
	defer driversMu.Unlock()
	if driver == nil {
		panic("converters: Register driver is nil")
	}
	if _, dup := drivers[name]; dup {
		panic("converters: Register called twice for driver " + name)
	}
	drivers[name] = driver
}

// Open returns a new RowProvider using the driver with the given name.
func Open(name string, r io.Reader) (RowProvider, error) {
	driversMu.RLock()
	driver, ok := drivers[name]
	driversMu.RUnlock()
	if !ok {
		return nil, fmt.Errorf("converters: unknown driver %q (forgotten import?)", name)
	}
	return driver.Open(r)
}

// StreamSQL converts the input stream to SQL statements using the driver with the given name.
func StreamSQL(name string, r io.Reader, w io.Writer) error {
	driversMu.RLock()
	driver, ok := drivers[name]
	driversMu.RUnlock()
	if !ok {
		return fmt.Errorf("converters: unknown driver %q (forgotten import?)", name)
	}
	return driver.ConvertToSQL(r, w)
}

// Drivers returns a sorted list of the names of the registered drivers.
func Drivers() []string {
	driversMu.RLock()
	defer driversMu.RUnlock()
	var list []string
	for name := range drivers {
		list = append(list, name)
	}
	sort.Strings(list)
	return list
}
