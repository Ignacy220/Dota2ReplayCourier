//go:build linux

package sentinel

import (
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"time"
)

// detectWatchedGameProcess scans /proc/[pid]/comm directly in pure Go on Linux.
func detectWatchedGameProcess(watchlist []string) string {
	entries, err := os.ReadDir("/proc")
	if err != nil {
		return ""
	}

	var running []string
	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}
		name := entry.Name()
		if len(name) == 0 || name[0] < '0' || name[0] > '9' {
			continue
		}
		commBytes, err := os.ReadFile(filepath.Join("/proc", name, "comm"))
		if err != nil {
			continue
		}
		comm := strings.TrimSpace(string(commBytes))
		if comm != "" {
			running = append(running, comm)
		}
	}
	return matchProcessName(running, watchlist)
}

// getOSUserIdleDuration queries X11 / Wayland idle time via xprintidle or DBus ScreenSaver on Linux.
func getOSUserIdleDuration() (time.Duration, error) {
	if out, err := exec.Command("xprintidle").Output(); err == nil {
		ms, err := strconv.ParseInt(strings.TrimSpace(string(out)), 10, 64)
		if err == nil {
			return time.Duration(ms) * time.Millisecond, nil
		}
	}

	// Fallback to freedesktop ScreenSaver DBus interface
	out, err := exec.Command(
		"dbus-send",
		"--session",
		"--dest=org.freedesktop.ScreenSaver",
		"--type=method_call",
		"--print-reply=literal",
		"/org/freedesktop/ScreenSaver",
		"org.freedesktop.ScreenSaver.GetSessionIdleTime",
	).Output()
	if err != nil {
		return 0, nil
	}
	fields := strings.Fields(string(out))
	if len(fields) >= 2 {
		sec, err := strconv.ParseInt(fields[len(fields)-1], 10, 64)
		if err == nil {
			return time.Duration(sec) * time.Second, nil
		}
	}
	return 0, nil
}

// setOSProcessEcoPriority adjusts the Linux process nice value (10 = below normal, 19 = idle).
func setOSProcessEcoPriority(eco bool) error {
	niceVal := 10
	if eco {
		niceVal = 19
	}
	return syscall.Setpriority(syscall.PRIO_PROCESS, 0, niceVal)
}

// sendOSDesktopNotification sends a desktop notification via notify-send on Linux.
func sendOSDesktopNotification(title, message string) error {
	return exec.Command("notify-send", title, message).Run()
}
