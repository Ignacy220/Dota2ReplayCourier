//go:build windows

package headless

import (
	"io"
	"os"

	"golang.org/x/sys/windows"
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

func enterRawMode(in io.Reader, out io.Writer) (func(), bool) {
	inFile, inOK := isTerminalFile(in)
	if !inOK {
		return func() {}, false
	}

	inHandle := windows.Handle(inFile.Fd())
	var origInMode uint32
	if err := windows.GetConsoleMode(inHandle, &origInMode); err != nil {
		return func() {}, false
	}

	rawInMode := (origInMode &^ (windows.ENABLE_LINE_INPUT | windows.ENABLE_ECHO_INPUT)) |
		windows.ENABLE_PROCESSED_INPUT | windows.ENABLE_VIRTUAL_TERMINAL_INPUT
	if err := windows.SetConsoleMode(inHandle, rawInMode); err != nil {
		return func() {}, false
	}

	var outFile *os.File
	var origOutMode uint32
	var restoreOut bool
	if f, outOK := isTerminalFile(out); outOK {
		outHandle := windows.Handle(f.Fd())
		if err := windows.GetConsoleMode(outHandle, &origOutMode); err == nil {
			_ = windows.SetConsoleMode(outHandle, origOutMode|windows.ENABLE_PROCESSED_OUTPUT|windows.ENABLE_VIRTUAL_TERMINAL_PROCESSING)
			outFile = f
			restoreOut = true
		}
	}

	restore := func() {
		_ = windows.SetConsoleMode(inHandle, origInMode)
		if restoreOut {
			_ = windows.SetConsoleMode(windows.Handle(outFile.Fd()), origOutMode)
		}
	}
	return restore, true
}
