//go:build windows

package netprobe

import (
	"context"
	"testing"
)

func TestBuildPingCmdHidesConsoleWindow(t *testing.T) {
	cmd := buildPingCmd(context.Background(), "155.133.226.68")
	if cmd.SysProcAttr == nil {
		t.Fatalf("expected SysProcAttr to be set on Windows ping command")
	}
	if !cmd.SysProcAttr.HideWindow {
		t.Fatalf("expected HideWindow=true on ping command")
	}
	const createNoWindow = 0x08000000
	if cmd.SysProcAttr.CreationFlags&createNoWindow == 0 {
		t.Fatalf("expected CREATE_NO_WINDOW flag on ping command, got 0x%x", cmd.SysProcAttr.CreationFlags)
	}
}
