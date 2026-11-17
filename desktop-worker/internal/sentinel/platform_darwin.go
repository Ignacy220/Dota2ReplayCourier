//go:build darwin

package sentinel

import (
	"fmt"
	"os/exec"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"syscall"
	"time"
)

var hidIdleRegex = regexp.MustCompile(`"HIDIdleTime"\s*=\s*(\d+)`)

// detectWatchedGameProcess scans running processes on macOS via ps without opening process handles.
func detectWatchedGameProcess(watchlist []string) string {
	out, err := exec.Command("ps", "-axo", "comm").Output()
	if err != nil {
		return ""
	}
	lines := strings.Split(string(out), "\n")
	running := make([]string, 0, len(lines))
	for _, line := range lines {
		base := filepath.Base(strings.TrimSpace(line))
		if base != "" {
			running = append(running, base)
		}
	}
	return matchProcessName(running, watchlist)
}

// getOSUserIdleDuration reads HIDIdleTime (in nanoseconds) from macOS IOHIDSystem.
func getOSUserIdleDuration() (time.Duration, error) {
	out, err := exec.Command("ioreg", "-c", "IOHIDSystem", "-d", "4").Output()
	if err != nil {
		return 0, err
	}
	matches := hidIdleRegex.FindStringSubmatch(string(out))
	if len(matches) < 2 {
		return 0, nil
	}
	ns, err := strconv.ParseInt(matches[1], 10, 64)
	if err != nil {
		return 0, err
	}
	return time.Duration(ns), nil
}

// setOSProcessEcoPriority adjusts the macOS process nice level (10 = below normal, 19 = idle).
func setOSProcessEcoPriority(eco bool) error {
	niceVal := 10
	if eco {
		niceVal = 19
	}
	return syscall.Setpriority(syscall.PRIO_PROCESS, 0, niceVal)
}

// sendOSDesktopNotification sends a native macOS notification via osascript.
func sendOSDesktopNotification(title, message string) error {
	safeTitle := strings.ReplaceAll(title, `"`, `\"`)
	safeMsg := strings.ReplaceAll(message, `"`, `\"`)
	script := fmt.Sprintf(`display notification "%s" with title "%s"`, safeMsg, safeTitle)
	return exec.Command("osascript", "-e", script).Run()
}
