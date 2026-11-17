//go:build windows

package procutil

import (
	"os/exec"
	"syscall"
)

const createNoWindowFlag = 0x08000000 // Win32 CREATE_NO_WINDOW

// HideConsoleWindow prevents a console child process (such as ping.exe or powershell.exe)
// from opening a visible CMD/Terminal window when spawned by a -H windowsgui binary.
func HideConsoleWindow(cmd *exec.Cmd) {
	if cmd == nil {
		return
	}
	if cmd.SysProcAttr == nil {
		cmd.SysProcAttr = &syscall.SysProcAttr{}
	}
	cmd.SysProcAttr.HideWindow = true
	cmd.SysProcAttr.CreationFlags |= createNoWindowFlag
}
