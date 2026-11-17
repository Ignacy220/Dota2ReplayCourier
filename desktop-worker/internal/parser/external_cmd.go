package parser

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os/exec"
	"strings"

	"github.com/dota2-analytics/desktop-worker/internal/procutil"
)

// ExternalCommandParser pipes the in-RAM decompressed .dem stream directly into an external
// black-box command's stdin and captures its stdout as arbitrary JSON (zero disk writes).
type ExternalCommandParser struct {
	commandLine string
}

// NewExternalCommandParser creates an adapter for an external black-box CLI parser.
func NewExternalCommandParser(commandLine string) *ExternalCommandParser {
	return &ExternalCommandParser{
		commandLine: strings.TrimSpace(commandLine),
	}
}

func (p *ExternalCommandParser) Name() string {
	return "external-blackbox-cli"
}

func (p *ExternalCommandParser) Version() string {
	return "1.0.0"
}

// Parse runs the external command, streaming the decompressed .dem from RAM to stdin
// and reading the resulting JSON payload from stdout.
func (p *ExternalCommandParser) Parse(ctx context.Context, ctrl ExecutionController, input ParseInput) (*ParseOutput, error) {
	if p.commandLine == "" {
		return nil, fmt.Errorf("external parser command is empty")
	}

	parts := strings.Fields(p.commandLine)
	bin := parts[0]
	var args []string
	if len(parts) > 1 {
		args = parts[1:]
	}

	cmd := exec.CommandContext(ctx, bin, args...)
	procutil.HideConsoleWindow(cmd)
	cmd.Env = append(cmd.Environ(),
		fmt.Sprintf("DOTA_MATCH_ID=%d", input.MatchID),
		fmt.Sprintf("DOTA_CLUSTER=%d", input.Cluster),
		fmt.Sprintf("DOTA_REGION=%s", input.Region),
	)

	stdinPipe, err := cmd.StdinPipe()
	if err != nil {
		return nil, err
	}

	var stdoutBuf, stderrBuf bytes.Buffer
	cmd.Stdout = &stdoutBuf
	cmd.Stderr = &stderrBuf

	if err := cmd.Start(); err != nil {
		return nil, fmt.Errorf("failed to start external black-box parser (%s): %w", p.commandLine, err)
	}

	stream := input.NewDecompressedStream()
	defer stream.Close()
	copyErrCh := make(chan error, 1)

	go func() {
		defer stdinPipe.Close()
		buf := make([]byte, 32*1024)
		for {
			if ctrl != nil {
				if err := ctrl.CheckGate(ctx); err != nil {
					copyErrCh <- err
					return
				}
			}
			n, rErr := stream.Read(buf)
			if n > 0 {
				if _, wErr := stdinPipe.Write(buf[:n]); wErr != nil {
					copyErrCh <- wErr
					return
				}
				if ctrl != nil {
					ctrl.ReportProgress(stream.ProgressPercent())
				}
			}
			if rErr != nil {
				if rErr == io.EOF {
					copyErrCh <- nil
					return
				}
				copyErrCh <- rErr
				return
			}
		}
	}()

	waitErr := cmd.Wait()
	streamErr := <-copyErrCh
	if waitErr != nil {
		return nil, fmt.Errorf("external black-box parser exited with error (%v): %s", waitErr, strings.TrimSpace(stderrBuf.String()))
	}
	if streamErr != nil {
		return nil, fmt.Errorf("failed streaming replay to external parser: %w", streamErr)
	}

	rawOut := bytes.TrimSpace(stdoutBuf.Bytes())
	if len(rawOut) == 0 {
		rawOut = []byte(`{"status":"ok"}`)
	}

	// If the external tool printed plain text instead of JSON, wrap it in a valid JSON object
	// so the Central Server always receives valid JSON.
	if !json.Valid(rawOut) {
		wrapped, _ := json.Marshal(map[string]any{
			"match_id":   input.MatchID,
			"raw_output": string(rawOut),
		})
		rawOut = wrapped
	}

	if ctrl != nil {
		ctrl.ReportProgress(100.0)
	}

	return &ParseOutput{
		ParserName:    p.Name(),
		ParserVersion: p.Version(),
		Payload:       json.RawMessage(rawOut),
	}, nil
}
