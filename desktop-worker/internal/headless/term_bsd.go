//go:build darwin || freebsd || openbsd || netbsd || dragonfly

package headless

import "golang.org/x/sys/unix"

const (
	ioctlGetTermios = unix.TIOCGETA
	ioctlSetTermios = unix.TIOCSETA
)
