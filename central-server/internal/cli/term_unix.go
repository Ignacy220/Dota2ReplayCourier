//go:build linux || darwin || freebsd || openbsd || netbsd || dragonfly

package cli

import (
	"io"
	"os"

	"golang.org/x/sys/unix"
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

func enterRawMode(in io.Reader, _ io.Writer) (func(), bool) {
	inFile, ok := isTerminalFile(in)
	if !ok {
		return func() {}, false
	}
	fd := int(inFile.Fd())
	orig, err := unix.IoctlGetTermios(fd, ioctlGetTermios)
	if err != nil {
		return func() {}, false
	}

	raw := *orig
	raw.Lflag &^= unix.ECHO | unix.ICANON | unix.IEXTEN
	raw.Lflag |= unix.ISIG
	raw.Iflag &^= unix.ICRNL | unix.IXON
	raw.Cc[unix.VMIN] = 1
	raw.Cc[unix.VTIME] = 0

	if err := unix.IoctlSetTermios(fd, ioctlSetTermios, &raw); err != nil {
		return func() {}, false
	}
	return func() {
		_ = unix.IoctlSetTermios(fd, ioctlSetTermios, orig)
	}, true
}
