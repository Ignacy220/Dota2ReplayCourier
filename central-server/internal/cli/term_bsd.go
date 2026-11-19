//go:build darwin || freebsd || openbsd || netbsd || dragonfly

package cli

import "golang.org/x/sys/unix"

const (
	ioctlGetTermios = unix.TIOCGETA
	ioctlSetTermios = unix.TIOCSETA
)
