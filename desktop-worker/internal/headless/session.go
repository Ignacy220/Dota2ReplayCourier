package headless

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
	"unicode"
)

const maxLogBuffer = 100
const maxHistory = 100

var errInterrupted = fmt.Errorf("interrupted")

type commandSpec struct {
	Name        string
	Description string
}

type session struct {
	br          *bufio.Reader
	out         io.Writer
	outMu       sync.Mutex
	prompt      string
	input       []rune
	promptShown bool
	masking     bool
	rawMode     bool
	ansi        bool

	logMu       sync.Mutex
	logMode     string // "on", "off", "errors"
	logBuf      []string
	history     []string
	watchActive atomic.Bool
	completer   func(line string) []string
}

func newSession(in io.Reader, out io.Writer, prompt string, completer func(string) []string) (*session, func()) {
	_, outTTY := isTerminalFile(out)
	ansi := outTTY && os.Getenv("NO_COLOR") == ""
	restore, raw := enterRawMode(in, out)

	return &session{
		br:        bufio.NewReader(in),
		out:       out,
		prompt:    prompt,
		rawMode:   raw,
		ansi:      ansi,
		logMode:   "on",
		completer: completer,
	}, restore
}

func (s *session) color(code, text string) string {
	if !s.ansi {
		return text
	}
	return "\x1b[" + code + "m" + text + "\x1b[0m"
}

func (s *session) WriteLine(format string, args ...any) {
	msg := fmt.Sprintf(format, args...)
	s.outMu.Lock()
	defer s.outMu.Unlock()

	if s.promptShown {
		if s.ansi || s.rawMode {
			fmt.Fprint(s.out, "\r\033[K")
		} else {
			fmt.Fprintln(s.out)
		}
	}
	fmt.Fprintln(s.out, msg)
	if s.promptShown && !s.watchActive.Load() {
		s.redrawPromptLocked()
	}
}

func (s *session) WriteWatchLine(line string) {
	s.outMu.Lock()
	defer s.outMu.Unlock()
	if s.ansi || s.rawMode {
		fmt.Fprintf(s.out, "\r\033[K%s", line)
	} else {
		fmt.Fprintf(s.out, "\r%s", line)
	}
}

func (s *session) ClearWatchLine() {
	s.outMu.Lock()
	defer s.outMu.Unlock()
	if s.ansi || s.rawMode {
		fmt.Fprint(s.out, "\r\033[K")
	} else {
		fmt.Fprintln(s.out)
	}
}

func (s *session) redrawPromptLocked() {
	display := string(s.input)
	if s.masking {
		display = strings.Repeat("*", len(s.input))
	}
	if s.rawMode || s.ansi {
		fmt.Fprintf(s.out, "\r\033[K%s%s", s.color("1;36", s.prompt), display)
	} else {
		fmt.Fprintf(s.out, "%s%s", s.prompt, display)
	}
}

func (s *session) LogEvent(isError bool, entry string) {
	s.logMu.Lock()
	s.logBuf = append(s.logBuf, entry)
	if len(s.logBuf) > maxLogBuffer {
		s.logBuf = s.logBuf[len(s.logBuf)-maxLogBuffer:]
	}
	mode := s.logMode
	s.logMu.Unlock()

	if s.watchActive.Load() {
		return
	}
	if mode == "off" || (mode == "errors" && !isError) {
		return
	}
	if isError {
		s.WriteLine("%s", s.color("31", entry))
		return
	}
	s.WriteLine("%s", s.color("2", entry))
}

func (s *session) SetLogMode(mode string) {
	s.logMu.Lock()
	s.logMode = mode
	s.logMu.Unlock()
}

func (s *session) GetLogMode() string {
	s.logMu.Lock()
	defer s.logMu.Unlock()
	return s.logMode
}

func (s *session) RecentLogs(n int) []string {
	s.logMu.Lock()
	defer s.logMu.Unlock()
	if n <= 0 || n >= len(s.logBuf) {
		cp := make([]string, len(s.logBuf))
		copy(cp, s.logBuf)
		return cp
	}
	cp := make([]string, n)
	copy(cp, s.logBuf[len(s.logBuf)-n:])
	return cp
}

func (s *session) LogWriter() io.Writer {
	return &sessionLogWriter{s: s}
}

type sessionLogWriter struct {
	s *session
}

func (w *sessionLogWriter) Write(p []byte) (int, error) {
	for _, line := range bytes.Split(p, []byte("\n")) {
		txt := strings.TrimSpace(string(line))
		if txt == "" {
			continue
		}
		lower := strings.ToLower(txt)
		isErr := strings.Contains(lower, "error") || strings.Contains(lower, "fail") || strings.Contains(lower, "warn")
		w.s.LogEvent(isErr, fmt.Sprintf("[%s] %s", time.Now().Format("15:04:05"), txt))
	}
	return len(p), nil
}

func (s *session) AddHistory(line string) {
	trimmed := strings.TrimSpace(line)
	if trimmed == "" {
		return
	}
	if n := len(s.history); n > 0 && s.history[n-1] == trimmed {
		return
	}
	s.history = append(s.history, trimmed)
	if len(s.history) > maxHistory {
		s.history = s.history[len(s.history)-maxHistory:]
	}
}

func (s *session) History() []string {
	cp := make([]string, len(s.history))
	copy(cp, s.history)
	return cp
}

func (s *session) ExpandHistory(line string) (string, error) {
	trimmed := strings.TrimSpace(line)
	if trimmed == "!!" {
		if len(s.history) == 0 {
			return "", fmt.Errorf("history is empty")
		}
		return s.history[len(s.history)-1], nil
	}
	if strings.HasPrefix(trimmed, "!") && len(trimmed) > 1 {
		idx, err := strconv.Atoi(trimmed[1:])
		if err != nil || idx < 1 || idx > len(s.history) {
			return "", fmt.Errorf("no such history entry %q", trimmed)
		}
		return s.history[idx-1], nil
	}
	return line, nil
}

// ReadLine reads one line from the session, using interactive line editing on raw TTYs.
func (s *session) ReadLine(prompt string, mask bool) (string, error) {
	if s.rawMode {
		return s.readRawLine(prompt, mask)
	}
	if prompt != "" && s.ansi {
		s.outMu.Lock()
		fmt.Fprint(s.out, prompt)
		s.outMu.Unlock()
	}
	line, err := s.br.ReadString('\n')
	line = strings.TrimRight(line, "\r\n")
	if err != nil && len(line) == 0 {
		return "", err
	}
	return line, nil
}

// WaitForEnter consumes bytes until a newline or EOF arrives (used to exit watch mode).
func (s *session) WaitForEnter() {
	for {
		b, err := s.br.ReadByte()
		if err != nil || b == '\n' || b == '\r' || b == 0x03 || b == 0x04 {
			if b == '\r' && s.br.Buffered() > 0 {
				if next, err := s.br.Peek(1); err == nil && next[0] == '\n' {
					_, _ = s.br.ReadByte()
				}
			}
			return
		}
	}
}

func (s *session) readRawLine(prompt string, mask bool) (string, error) {
	s.outMu.Lock()
	origPrompt := s.prompt
	s.prompt = prompt
	s.masking = mask
	s.input = nil
	s.promptShown = prompt != ""
	if s.promptShown {
		s.redrawPromptLocked()
	}
	s.outMu.Unlock()

	defer func() {
		s.outMu.Lock()
		s.promptShown = false
		s.input = nil
		s.masking = false
		s.prompt = origPrompt
		s.outMu.Unlock()
	}()

	histIdx := len(s.history)
	savedDraft := ""

	for {
		r, _, err := s.br.ReadRune()
		if err != nil {
			if len(s.input) > 0 {
				return string(s.input), nil
			}
			return "", err
		}

		switch r {
		case '\r', '\n':
			if r == '\r' && s.br.Buffered() > 0 {
				if next, err := s.br.Peek(1); err == nil && next[0] == '\n' {
					_, _ = s.br.ReadByte()
				}
			}
			s.outMu.Lock()
			s.promptShown = false
			res := string(s.input)
			s.input = nil
			fmt.Fprint(s.out, "\r\n")
			s.outMu.Unlock()
			return res, nil

		case 0x03: // Ctrl+C
			s.outMu.Lock()
			fmt.Fprint(s.out, "^C\r\n")
			s.outMu.Unlock()
			return "", errInterrupted

		case 0x04: // Ctrl+D
			if len(s.input) == 0 {
				s.outMu.Lock()
				fmt.Fprint(s.out, "\r\n")
				s.outMu.Unlock()
				return "", io.EOF
			}

		case 0x7f, '\b': // Backspace
			s.outMu.Lock()
			if len(s.input) > 0 {
				s.input = s.input[:len(s.input)-1]
				s.redrawPromptLocked()
			}
			s.outMu.Unlock()

		case '\t': // Tab completion
			if mask || s.completer == nil {
				continue
			}
			s.outMu.Lock()
			cur := string(s.input)
			s.outMu.Unlock()

			completed, matches := tabComplete(cur, s.completer(cur))
			if completed != "" && completed != cur {
				s.outMu.Lock()
				s.input = []rune(completed)
				s.redrawPromptLocked()
				s.outMu.Unlock()
			} else if len(matches) > 1 {
				s.WriteLine("%s", strings.Join(matches, "  "))
			}

		case '\x1b': // Escape sequence (arrows)
			r2, _, err2 := s.br.ReadRune()
			if err2 != nil || r2 != '[' {
				continue
			}
			r3, _, err3 := s.br.ReadRune()
			if err3 != nil {
				continue
			}
			if mask {
				continue
			}
			switch r3 {
			case 'A': // Up arrow
				if histIdx > 0 {
					if histIdx == len(s.history) {
						s.outMu.Lock()
						savedDraft = string(s.input)
						s.outMu.Unlock()
					}
					histIdx--
					s.outMu.Lock()
					s.input = []rune(s.history[histIdx])
					s.redrawPromptLocked()
					s.outMu.Unlock()
				}
			case 'B': // Down arrow
				if histIdx < len(s.history) {
					histIdx++
					nextLine := savedDraft
					if histIdx < len(s.history) {
						nextLine = s.history[histIdx]
					}
					s.outMu.Lock()
					s.input = []rune(nextLine)
					s.redrawPromptLocked()
					s.outMu.Unlock()
				}
			case '3': // Delete key (\x1b[3~)
				_, _, _ = s.br.ReadRune()
			}

		default:
			if unicode.IsPrint(r) {
				s.outMu.Lock()
				s.input = append(s.input, r)
				s.redrawPromptLocked()
				s.outMu.Unlock()
			}
		}
	}
}

func tabComplete(line string, candidates []string) (string, []string) {
	if len(candidates) == 0 {
		return line, nil
	}
	hasTrailingSpace := len(line) > 0 && (line[len(line)-1] == ' ' || line[len(line)-1] == '\t')
	prefixPart := ""
	token := ""
	if !hasTrailingSpace {
		lastSpace := strings.LastIndexAny(line, " \t")
		if lastSpace >= 0 {
			prefixPart = line[:lastSpace+1]
			token = line[lastSpace+1:]
		} else {
			token = line
		}
	} else {
		prefixPart = line
	}

	var matches []string
	lowerTok := strings.ToLower(token)
	for _, c := range candidates {
		if strings.HasPrefix(strings.ToLower(c), lowerTok) {
			matches = append(matches, c)
		}
	}
	if len(matches) == 0 {
		return line, nil
	}
	if len(matches) == 1 {
		return prefixPart + matches[0] + " ", matches
	}
	lcp := longestCommonPrefix(matches)
	if len(lcp) > len(token) {
		return prefixPart + lcp, matches
	}
	return line, matches
}

func longestCommonPrefix(items []string) string {
	if len(items) == 0 {
		return ""
	}
	prefix := items[0]
	for _, item := range items[1:] {
		for !strings.HasPrefix(item, prefix) {
			if len(prefix) == 0 {
				return ""
			}
			prefix = prefix[:len(prefix)-1]
		}
	}
	return prefix
}

func resolveCommandName(raw string, specs []commandSpec, aliases map[string]string) (string, string, error) {
	cmd := strings.ToLower(strings.TrimSpace(raw))
	if cmd == "" {
		return "", "", fmt.Errorf("empty command")
	}
	if target, ok := aliases[cmd]; ok {
		return target, "", nil
	}
	for _, sp := range specs {
		if sp.Name == cmd {
			return cmd, "", nil
		}
	}
	if len(cmd) >= 2 {
		var prefixMatches []string
		for _, sp := range specs {
			if strings.HasPrefix(sp.Name, cmd) {
				prefixMatches = append(prefixMatches, sp.Name)
			}
		}
		if len(prefixMatches) == 1 {
			return prefixMatches[0], "", nil
		}
		if len(prefixMatches) > 1 {
			return "", "", fmt.Errorf("ambiguous command %q (could be: %s)", cmd, strings.Join(prefixMatches, ", "))
		}
	}

	bestName := ""
	bestDist := 3
	for _, sp := range specs {
		if d := levenshtein(cmd, sp.Name); d < bestDist {
			bestDist = d
			bestName = sp.Name
		}
	}
	if bestName != "" {
		return "", bestName, fmt.Errorf("unknown command %q — did you mean %q? (type 'help' for commands)", raw, bestName)
	}
	return "", "", fmt.Errorf("unknown command %q (type 'help' for commands)", raw)
}

func levenshtein(a, b string) int {
	ra := []rune(a)
	rb := []rune(b)
	if len(ra) == 0 {
		return len(rb)
	}
	if len(rb) == 0 {
		return len(ra)
	}
	prev := make([]int, len(rb)+1)
	curr := make([]int, len(rb)+1)
	for j := range prev {
		prev[j] = j
	}
	for i := 1; i <= len(ra); i++ {
		curr[0] = i
		for j := 1; j <= len(rb); j++ {
			cost := 0
			if ra[i-1] != rb[j-1] {
				cost = 1
			}
			delCost := prev[j] + 1
			insCost := curr[j-1] + 1
			subCost := prev[j-1] + cost
			m := delCost
			if insCost < m {
				m = insCost
			}
			if subCost < m {
				m = subCost
			}
			curr[j] = m
		}
		prev, curr = curr, prev
	}
	return prev[len(rb)]
}

func progressBar(pct float64, width int) string {
	if width <= 0 {
		width = 20
	}
	if pct < 0 {
		pct = 0
	}
	if pct > 100 {
		pct = 100
	}
	filled := int((pct / 100.0) * float64(width))
	if filled > width {
		filled = width
	}
	return "[" + strings.Repeat("█", filled) + strings.Repeat("░", width-filled) + "]"
}
