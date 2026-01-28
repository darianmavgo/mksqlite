//go:build darwin

package filesystem

import (
	"io/fs"
	"syscall"
	"time"
)

func getCreateTime(info fs.FileInfo) time.Time {
	if stat, ok := info.Sys().(*syscall.Stat_t); ok {
		ts := stat.Birthtimespec
		return time.Unix(int64(ts.Sec), int64(ts.Nsec))
	}
	return info.ModTime()
}
