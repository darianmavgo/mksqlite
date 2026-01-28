//go:build !darwin

package filesystem

import (
	"io/fs"
	"time"
)

func getCreateTime(info fs.FileInfo) time.Time {
	return info.ModTime()
}
