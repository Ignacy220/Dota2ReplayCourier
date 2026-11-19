//go:build !windows && !linux && !darwin && !freebsd && !openbsd && !netbsd && !dragonfly

package headless

import (
	"io"
	"os"
)

func isTerminalFile(v any) (*os.File, bool) {
	f, ok := v.(*os.File)
	if !ok || f == nil {
		return nil, false
	}
	fi, err := f.Stat()
	if err != nil {
		return nil, false
	}
	return f, (fi.Mode() & os.ModeCharDevice) != 0
}

func enterRawMode(_ io.Reader, _ io.Writer) (func(), bool) {
	return func() {}, false
}
