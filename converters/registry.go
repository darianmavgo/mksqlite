package converters

import (
	"fmt"
	"io"
	"github.com/darianmavgo/mksqlite/converters/common"
	"sort"
	"sync"
)

var (
	driversMu sync.RWMutex
	drivers   = make(map[string]common.Driver)
)

// Register makes a converter driver available by the provided name.
// If Register is called twice with the same name or if driver is nil, it panics.
func Register(name string, driver common.Driver) {
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

// Open opens a converter by driver name and source reader.
func Open(driverName string, source io.Reader, config *common.ConversionConfig) (common.RowProvider, error) {
	driversMu.RLock()
	driver, ok := drivers[driverName]
	driversMu.RUnlock()
	if !ok {
		return nil, fmt.Errorf("converters: unknown driver %q (forgotten import?)", driverName)
	}
	return driver.Open(source, config)
}

// Drivers returns a sorted list of the names of the registered drivers.
func Drivers() []string {
	driversMu.RLock()
	defer driversMu.RUnlock()
	list := make([]string, 0, len(drivers))
	for name := range drivers {
		list = append(list, name)
	}
	sort.Strings(list)
	return list
}
