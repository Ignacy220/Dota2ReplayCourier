//go:build windows

package procutil

import (
	"os/exec"
	"testing"
)

func TestHideConsoleWindowSetsNoWindowFlags(t *testing.T) {
	cmd := exec.Command("ping", "-n", "1", "127.0.0.1")
	HideConsoleWindow(cmd)

	if cmd.SysProcAttr == nil {
		t.Fatalf("expected SysProcAttr to be initialized")
	}
	if !cmd.SysProcAttr.HideWindow {
		t.Fatalf("expected HideWindow=true")
	}
	if cmd.SysProcAttr.CreationFlags&createNoWindowFlag == 0 {
		t.Fatalf("expected CREATE_NO_WINDOW (0x%x) flag, got 0x%x", createNoWindowFlag, cmd.SysProcAttr.CreationFlags)
	}
}
