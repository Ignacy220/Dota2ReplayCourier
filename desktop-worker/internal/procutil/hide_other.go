//go:build !windows

package procutil

import "os/exec"

// HideConsoleWindow is a no-op on non-Windows operating systems.
func HideConsoleWindow(_ *exec.Cmd) {}
