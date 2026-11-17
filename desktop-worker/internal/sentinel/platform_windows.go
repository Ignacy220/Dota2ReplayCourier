//go:build windows

package sentinel

import (
	"os/exec"
	"strings"
	"syscall"
	"time"
	"unsafe"

	"github.com/dota2-analytics/desktop-worker/internal/procutil"
)

const (
	th32csSnapProcess         = 0x00000002
	maxPath                   = 260
	belowNormalPriorityClass  = 0x00004000
	idlePriorityClass         = 0x00000040
)

var (
	modKernel32                  = syscall.NewLazyDLL("kernel32.dll")
	procCreateToolhelp32Snapshot = modKernel32.NewProc("CreateToolhelp32Snapshot")
	procProcess32FirstW          = modKernel32.NewProc("Process32FirstW")
	procProcess32NextW           = modKernel32.NewProc("Process32NextW")
	procGetCurrentProcess        = modKernel32.NewProc("GetCurrentProcess")
	procSetPriorityClass         = modKernel32.NewProc("SetPriorityClass")
	procGetTickCount             = modKernel32.NewProc("GetTickCount")

	modUser32            = syscall.NewLazyDLL("user32.dll")
	procGetLastInputInfo = modUser32.NewProc("GetLastInputInfo")
)

type processEntry32W struct {
	Size            uint32
	Usage           uint32
	ProcessID       uint32
	DefaultHeapID   uintptr
	ModuleID        uint32
	Threads         uint32
	ParentProcessID uint32
	PriClassBase    int32
	Flags           uint32
	ExeFile         [maxPath]uint16
}

type lastInputInfo struct {
	 cbSize uint32
	 dwTime uint32
}

// detectWatchedGameProcess uses CreateToolhelp32Snapshot to enumerate process names
// without ever calling OpenProcess on any protected game executable (anti-cheat safe).
func detectWatchedGameProcess(watchlist []string) string {
	handle, _, _ := procCreateToolhelp32Snapshot.Call(th32csSnapProcess, 0)
	if handle == uintptr(syscall.InvalidHandle) || handle == 0 {
		return ""
	}
	defer syscall.CloseHandle(syscall.Handle(handle))

	var entry processEntry32W
	entry.Size = uint32(unsafe.Sizeof(entry))

	ret, _, _ := procProcess32FirstW.Call(handle, uintptr(unsafe.Pointer(&entry)))
	if ret == 0 {
		return ""
	}

	var running []string
	for {
		name := syscall.UTF16ToString(entry.ExeFile[:])
		if name != "" {
			running = append(running, name)
		}
		entry.Size = uint32(unsafe.Sizeof(entry))
		ret, _, _ = procProcess32NextW.Call(handle, uintptr(unsafe.Pointer(&entry)))
		if ret == 0 {
			break
		}
	}

	return matchProcessName(running, watchlist)
}

// getOSUserIdleDuration calls user32.dll GetLastInputInfo to determine how long the user
// has been inactive (no mouse movement or keyboard presses).
func getOSUserIdleDuration() (time.Duration, error) {
	var lii lastInputInfo
	lii.cbSize = uint32(unsafe.Sizeof(lii))

	ret, _, err := procGetLastInputInfo.Call(uintptr(unsafe.Pointer(&lii)))
	if ret == 0 {
		return 0, err
	}

	tickCount, _, _ := procGetTickCount.Call()
	currentTick := uint32(tickCount)
	var elapsedMs uint32
	if currentTick >= lii.dwTime {
		elapsedMs = currentTick - lii.dwTime
	}
	return time.Duration(elapsedMs) * time.Millisecond, nil
}

// setOSProcessEcoPriority sets the current worker process priority to IDLE_PRIORITY_CLASS (when eco=true)
// or BELOW_NORMAL_PRIORITY_CLASS (when eco=false) so foreground games never suffer CPU contention.
func setOSProcessEcoPriority(eco bool) error {
	hProc, _, _ := procGetCurrentProcess.Call()
	prio := uintptr(belowNormalPriorityClass)
	if eco {
		prio = uintptr(idlePriorityClass)
	}
	ret, _, err := procSetPriorityClass.Call(hProc, prio)
	if ret == 0 {
		return err
	}
	return nil
}

// sendOSDesktopNotification triggers a non-intrusive Windows Toast notification.
func sendOSDesktopNotification(title, message string) error {
	safeTitle := strings.ReplaceAll(title, "'", "")
	safeMsg := strings.ReplaceAll(message, "'", "")
	psScript := `[Windows.UI.Notifications.ToastNotificationManager, Windows.UI.Notifications, ContentType = WindowsRuntime] > $null; ` +
		`$template = [Windows.UI.Notifications.ToastNotificationManager]::GetTemplateContent([Windows.UI.Notifications.ToastTemplateType]::ToastText02); ` +
		`$textNodes = $template.GetElementsByTagName('text'); ` +
		`$textNodes.Item(0).AppendChild($template.CreateTextNode('` + safeTitle + `')) > $null; ` +
		`$textNodes.Item(1).AppendChild($template.CreateTextNode('` + safeMsg + `')) > $null; ` +
		`$toast = [Windows.UI.Notifications.ToastNotification]::new($template); ` +
		`[Windows.UI.Notifications.ToastNotificationManager]::CreateToastNotifier('Dota2ReplayCourier').Show($toast);`

	cmd := exec.Command("powershell", "-NoProfile", "-NonInteractive", "-WindowStyle", "Hidden", "-Command", psScript)
	procutil.HideConsoleWindow(cmd)
	return cmd.Run()
}
