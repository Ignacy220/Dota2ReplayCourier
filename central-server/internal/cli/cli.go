package cli

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/dota2-analytics/central-server/internal/api"
	"github.com/dota2-analytics/central-server/internal/harvester"
	"github.com/dota2-analytics/central-server/internal/models"
	"github.com/dota2-analytics/central-server/internal/parsers"
	"github.com/dota2-analytics/central-server/internal/queue"
	"github.com/dota2-analytics/central-server/internal/storage"
)

// Flags holds parsed command-line options for central-server.
type Flags struct {
	Headless        bool
	Port            string
	Harvester       bool
	SteamKey        string
	StratzKey       string
	OpenDotaKey     string
	Regions         string
	LobbyTypes      string
	GameModes       string
	MinDuration     int
	MaxDuration     int
	Require10Humans bool
	FileStorage     bool
	WebhookURL      string
	WebhookEnabled  bool
	Parser          string
	AdminPassword   string
	Set             map[string]bool
}

// Dependencies bundles runtime Coordinator services controlled by the interactive CLI.
type Dependencies struct {
	FilterEngine *harvester.FilterEngine
	Harvester    *harvester.Harvester
	Store        *storage.AnalyticsStore
	LeaseMgr     *queue.LeaseManager
	ParserMgr    *parsers.Manager
	Server       *api.Server
	DataDir      string
	BaseURL      string
	Headless     bool
}

var serverCommands = []commandSpec{
	{Name: "help", Description: "Show available commands"},
	{Name: "status", Description: "Show harvester, queue, parser, storage, and API key status"},
	{Name: "harvester", Description: "Inspect or toggle harvester (<on|off|toggle>)"},
	{Name: "keys", Description: "Inspect or update API keys (<steam|stratz|opendota> <key|clear>)"},
	{Name: "filter", Description: "Inspect or update match filter rules"},
	{Name: "file-storage", Description: "Inspect or toggle local JSON file saving (<on|off|toggle>)"},
	{Name: "webhook", Description: "Inspect or update webhook forwarding (<url|on|off|clear>)"},
	{Name: "parser", Description: "List, activate, upload, or delete replay parsers"},
	{Name: "inject", Description: "Enqueue 1..100 sample jobs or a specific [match_id] [cluster]"},
	{Name: "password", Description: "Set or change the /admin Console password (saved to data/admin_auth.json)"},
	{Name: "watch", Description: "Live single-line telemetry ticker (press Enter to exit)"},
	{Name: "logs", Description: "Set background logs (<on|off|errors>) or show last [N] entries"},
	{Name: "history", Description: "Show command history (use !! or !<n> to repeat)"},
	{Name: "save", Description: "Persist current in-memory settings to data/"},
	{Name: "quit", Description: "Stop the server"},
}

var serverAliases = map[string]string{
	"?":    "help",
	"s":    "status",
	"h":    "harvester",
	"i":    "inject",
	"w":    "watch",
	"l":              "logs",
	"admin-password": "password",
	"passwd":         "password",
	"q":              "quit",
	"exit":           "quit",
}

// ParseFlags parses central-server CLI flags without touching global flag.CommandLine.
func ParseFlags(args []string) (Flags, error) {
	var f Flags
	f.Set = make(map[string]bool)

	fs := flag.NewFlagSet("central-server", flag.ContinueOnError)
	fs.BoolVar(&f.Headless, "headless", false, "Run headless without /admin web UI")
	fs.StringVar(&f.Port, "port", "", "Override HTTP listen port (in-memory)")
	fs.BoolVar(&f.Harvester, "harvester", false, "Enable Harvester on startup (in-memory only)")
	fs.StringVar(&f.SteamKey, "steam-key", "", "Override Steam Web API key for this session")
	fs.StringVar(&f.StratzKey, "stratz-key", "", "Override STRATZ API token for this session")
	fs.StringVar(&f.OpenDotaKey, "opendota-key", "", "Override OpenDota API key for this session")
	fs.StringVar(&f.Regions, "regions", "", "Comma-separated allowed regions (e.g. EU,NA,SEA)")
	fs.StringVar(&f.LobbyTypes, "lobby-types", "", "Comma-separated allowed lobby type IDs (e.g. 7,0)")
	fs.StringVar(&f.GameModes, "game-modes", "", "Comma-separated allowed game mode IDs (e.g. 22,2)")
	fs.IntVar(&f.MinDuration, "min-duration", 0, "Minimum match duration in seconds")
	fs.IntVar(&f.MaxDuration, "max-duration", 0, "Maximum match duration in seconds")
	fs.BoolVar(&f.Require10Humans, "require-10-humans", true, "Require 10 human players in matches")
	fs.BoolVar(&f.FileStorage, "file-storage", true, "Enable local JSON match file storage")
	fs.StringVar(&f.WebhookURL, "webhook-url", "", "Override webhook URL for this session")
	fs.BoolVar(&f.WebhookEnabled, "webhook", false, "Enable webhook forwarding for this session")
	fs.StringVar(&f.Parser, "parser", "", "Activate parser ('default' or custom ID) for this session")
	fs.StringVar(&f.AdminPassword, "admin-password", "", "Override /admin password in-memory for this session")

	if err := fs.Parse(args); err != nil {
		return Flags{}, err
	}
	fs.Visit(func(fl *flag.Flag) {
		f.Set[fl.Name] = true
	})
	if fs.NFlag() > 0 {
		f.Headless = true
	}
	return f, nil
}

// ApplyStartupFlags applies parsed CLI flags strictly in-memory for the current session.
func ApplyStartupFlags(
	flags Flags,
	filterEngine *harvester.FilterEngine,
	harv *harvester.Harvester,
	store *storage.AnalyticsStore,
	parserMgr *parsers.Manager,
	srv *api.Server,
) error {
	if srv != nil {
		srv.SetAdminEnabled(!flags.Headless)
	}

	if harv != nil && (flags.Set["steam-key"] || flags.Set["stratz-key"] || flags.Set["opendota-key"]) {
		steam, stratz, openDota := harv.GetAPIKeys()
		if flags.Set["steam-key"] {
			steam = strings.TrimSpace(flags.SteamKey)
		}
		if flags.Set["stratz-key"] {
			stratz = strings.TrimSpace(flags.StratzKey)
		}
		if flags.Set["opendota-key"] {
			openDota = strings.TrimSpace(flags.OpenDotaKey)
		}
		harv.SetAPIKeys(steam, stratz, openDota)
	}

	if filterEngine != nil && hasAnyFilterFlag(flags.Set) {
		cfg := filterEngine.GetConfig()
		if flags.Set["harvester"] {
			cfg.Enabled = flags.Harvester
		}
		if flags.Set["regions"] {
			regs, err := parseRegionsCSV(flags.Regions)
			if err != nil {
				return err
			}
			cfg.AllowedRegions = regs
		}
		if flags.Set["lobby-types"] {
			lobbies, err := parseIntsCSV(flags.LobbyTypes)
			if err != nil {
				return fmt.Errorf("invalid --lobby-types: %w", err)
			}
			cfg.AllowedLobbyTypes = lobbies
		}
		if flags.Set["game-modes"] {
			modes, err := parseIntsCSV(flags.GameModes)
			if err != nil {
				return fmt.Errorf("invalid --game-modes: %w", err)
			}
			cfg.AllowedGameModes = modes
		}
		if flags.Set["min-duration"] {
			cfg.MinDurationSeconds = flags.MinDuration
		}
		if flags.Set["max-duration"] {
			cfg.MaxDurationSeconds = flags.MaxDuration
		}
		if flags.Set["require-10-humans"] {
			cfg.Require10Humans = flags.Require10Humans
		}
		filterEngine.UpdateConfig(cfg)
	}

	if store != nil {
		if flags.Set["file-storage"] {
			store.SetFileStorageEnabled(flags.FileStorage)
		}
		if flags.Set["webhook-url"] || flags.Set["webhook"] {
			st := store.GetSinkStats()
			url := st.WebhookURL
			enabled := st.WebhookEnabled
			if flags.Set["webhook-url"] {
				url = strings.TrimSpace(flags.WebhookURL)
			}
			if flags.Set["webhook"] {
				enabled = flags.WebhookEnabled
			}
			store.UpdateSinkConfig(url, enabled)
		}
	}

	if parserMgr != nil && flags.Set["parser"] {
		if _, err := parserMgr.ActivateEphemeral(flags.Parser); err != nil {
			return err
		}
	}

	if srv != nil && flags.Set["admin-password"] {
		if err := srv.SetAdminPassword(flags.AdminPassword, false); err != nil {
			return err
		}
	}

	return nil
}

// RunInteractiveLoop reads commands from in and executes them until EOF, quit, or context cancellation.
func RunInteractiveLoop(ctx context.Context, in io.Reader, out io.Writer, stop context.CancelFunc, deps Dependencies) {
	sess, restore := newSession(in, out, "server> ", func(line string) []string {
		return serverCompleter(deps, line)
	})
	defer restore()

	if _, tty := isTerminalFile(in); tty {
		prevLogOut := log.Writer()
		log.SetOutput(sess.LogWriter())
		defer log.SetOutput(prevLogOut)
	}

	if deps.Headless {
		sess.WriteLine("Dota2ReplayCourier Coordinator (headless) ready on %s. Type 'help' for commands.", deps.BaseURL)
	}

	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		rawLine, err := sess.ReadLine("server> ", false)
		if err != nil {
			if errors.Is(err, errInterrupted) && stop != nil {
				stop()
			}
			return
		}

		line, expErr := sess.ExpandHistory(rawLine)
		if expErr != nil {
			sess.WriteLine("error: %v", expErr)
			continue
		}
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}

		fields := strings.Fields(line)
		rawCmd := strings.ToLower(fields[0])
		args := fields[1:]

		if rawCmd == "h" && len(args) == 0 {
			args = []string{"toggle"}
		}

		cmd, _, resErr := resolveCommandName(rawCmd, serverCommands, serverAliases)
		if cmd == "password" {
			sess.AddHistory("password")
		} else {
			sess.AddHistory(line)
		}
		if resErr != nil {
			sess.WriteLine("%v", resErr)
			continue
		}

		if cmd == "quit" {
			sess.WriteLine("Shutting down Coordinator...")
			if stop != nil {
				stop()
			}
			return
		}

		if err := dispatchCommand(ctx, sess, deps, cmd, args); err != nil {
			sess.WriteLine("error: %v", err)
		}
	}
}

func serverCompleter(deps Dependencies, line string) []string {
	fields := strings.Fields(line)
	trailingSpace := len(line) > 0 && (line[len(line)-1] == ' ' || line[len(line)-1] == '\t')
	if len(fields) == 0 || (len(fields) == 1 && !trailingSpace) {
		names := make([]string, 0, len(serverCommands))
		for _, sp := range serverCommands {
			names = append(names, sp.Name)
		}
		return names
	}

	cmd, _, err := resolveCommandName(fields[0], serverCommands, serverAliases)
	if err != nil {
		return nil
	}
	switch cmd {
	case "harvester", "file-storage":
		return []string{"on", "off", "toggle"}
	case "logs":
		return []string{"on", "off", "errors"}
	case "keys":
		return []string{"show", "steam", "stratz", "opendota"}
	case "webhook":
		return []string{"on", "off", "clear"}
	case "filter":
		if len(fields) == 1 || (len(fields) == 2 && !trailingSpace) {
			return []string{"show", "regions", "lobbies", "modes", "min-duration", "max-duration", "humans"}
		}
		if len(fields) >= 2 && strings.EqualFold(fields[1], "regions") {
			return []string{"EU", "NA", "SEA", "SA", "CN", "OCE"}
		}
		return nil
	case "parser":
		if len(fields) == 1 || (len(fields) == 2 && !trailingSpace) {
			return []string{"list", "activate", "upload", "delete"}
		}
		if len(fields) >= 2 && (strings.EqualFold(fields[1], "activate") || strings.EqualFold(fields[1], "delete")) {
			out := []string{"default", "raw"}
			if deps.ParserMgr != nil {
				_, custom := deps.ParserMgr.ListAll()
				for _, p := range custom {
					out = append(out, p.ID)
				}
			}
			return out
		}
		return nil
	}
	return nil
}

func dispatchCommand(ctx context.Context, sess *session, deps Dependencies, cmd string, args []string) error {
	switch cmd {
	case "help":
		printHelp(sess)
		return nil
	case "status":
		printStatus(ctx, sess, deps)
		return nil
	case "harvester":
		return handleHarvesterCmd(ctx, sess, deps, args)
	case "keys":
		return handleKeysCmd(sess, deps, args)
	case "filter":
		return handleFilterCmd(ctx, sess, deps, args)
	case "file-storage":
		return handleFileStorageCmd(sess, deps, args)
	case "webhook":
		return handleWebhookCmd(sess, deps, args)
	case "parser":
		return handleParserCmd(ctx, sess, deps, args)
	case "inject":
		return handleInjectCmd(ctx, sess, deps, args)
	case "password":
		return handlePasswordCmd(sess, deps, args)
	case "logs":
		return handleLogsCmd(sess, args)
	case "watch":
		return handleWatchCmd(ctx, sess, deps)
	case "history":
		return handleHistoryCmd(sess)
	case "save":
		return handleSaveCmd(ctx, sess, deps)
	default:
		return fmt.Errorf("unknown command %q (type 'help' for commands)", cmd)
	}
}

func printHelp(sess *session) {
	sess.WriteLine(`Commands (aliases: s=status, h=harvester toggle, i=inject, w=watch, l=logs, q=quit):
  status                                    Show aligned Coordinator status card
  harvester [on|off|toggle]                 Inspect or toggle harvester in-memory (not saved)
  keys [show|<steam|stratz|opendota> <key|clear>] Inspect or update API keys
  filter [show|regions|lobbies|modes|min-duration|max-duration|humans] [val] Inspect or update filters
  file-storage [on|off|toggle]              Inspect or toggle local JSON file saving
  webhook [url|on|off|clear]                Inspect or configure webhook forwarding
  parser [list|activate|upload|delete]      Inspect or manage active replay parsers
  inject [count|match_id] [cluster]         Enqueue 1..100 sample jobs or a specific match
  password [new_password]                   Set or change /admin password (prompts masked input if omitted)
  watch                                     Live single-line ticker (press Enter to exit)
  logs [on|off|errors|N]                    Control live logs or print last N entries
  history                                   List recent commands (use !! or !<n> to repeat)
  save                                      Save session flags (keys, filters, sink, parser) to disk
  quit | exit                               Stop the server`)
}

func printStatus(ctx context.Context, sess *session, deps Dependencies) {
	fCfg := deps.FilterEngine.GetConfig()
	fStats := deps.FilterEngine.GetStats()
	totalParsed, todayParsed, _ := deps.Store.GetCounters()
	sink := deps.Store.GetSinkStats()
	depths, delayed, _ := deps.LeaseMgr.GetQueueDepths(ctx)
	leases, _ := deps.LeaseMgr.ListActiveLeases(ctx)
	activeParser := deps.ParserMgr.GetActive()
	steam, stratz, od := deps.Harvester.GetAPIKeys()

	harvBadge := sess.color("33", "OFF")
	if fCfg.Enabled {
		harvBadge = sess.color("32", "ON")
	}
	acceptPct := 0.0
	if fStats.TotalScanned > 0 {
		acceptPct = (float64(fStats.TotalAccepted) / float64(fStats.TotalScanned)) * 100.0
	}
	totalQ := depths[models.RegionEU] + depths[models.RegionNA] + depths[models.RegionSEA] +
		depths[models.RegionSA] + depths[models.RegionCN] + depths[models.RegionOCE] + depths[models.RegionGlobal]

	sess.WriteLine("%s HARVESTER  %s · Scanned=%d Accepted=%d (%.0f%%) · Regions=%v",
		sess.color("36", "●"), harvBadge, fStats.TotalScanned, fStats.TotalAccepted, acceptPct, fCfg.AllowedRegions)
	sess.WriteLine("%s QUEUES     Ready=%d (EU=%d NA=%d SEA=%d SA=%d CN=%d OCE=%d GLOBAL=%d) · Delayed=%d · Leases=%d",
		sess.color("36", "●"), totalQ, depths[models.RegionEU], depths[models.RegionNA], depths[models.RegionSEA],
		depths[models.RegionSA], depths[models.RegionCN], depths[models.RegionOCE], depths[models.RegionGlobal], delayed, len(leases))
	sess.WriteLine("%s PIPELINE   Parsed today=%d total=%d · Parser=%s (%s rev=%d) · Files=%s (%d)",
		sess.color("36", "●"), todayParsed, totalParsed, activeParser.ReportedName, activeParser.Kind, activeParser.Revision,
		onOff(sink.FileStorageEnabled), sink.FilesStored)
	adminAuthStr := "not configured (locked until 'password' is set)"
	if deps.Server != nil && deps.Server.HasAdminPassword() {
		adminAuthStr = "configured"
	}
	sess.WriteLine("%s HOOK/KEYS  Webhook=%s (%q sent=%d fail=%d) · Keys: steam=%s stratz=%s opendota=%s",
		sess.color("36", "●"), onOff(sink.WebhookEnabled), sink.WebhookURL, sink.WebhookDelivered, sink.WebhookFailed,
		keyState(steam), keyState(stratz), keyState(od))
	sess.WriteLine("%s ADMIN AUTH Password=%s", sess.color("36", "●"), adminAuthStr)
}

func handlePasswordCmd(sess *session, deps Dependencies, args []string) error {
	if deps.Server == nil {
		return errors.New("server unavailable")
	}
	var pass string
	switch len(args) {
	case 0:
		p, err := sess.ReadLine("New Admin Password (min 6 chars): ", true)
		if err != nil {
			return err
		}
		pass = p
	case 1:
		pass = args[0]
	default:
		return errors.New("usage: password [new_password]")
	}
	if err := deps.Server.SetAdminPassword(pass, true); err != nil {
		return err
	}
	sess.WriteLine("Admin Console password updated and saved (all browser sessions reset)")
	return nil
}

func handleHarvesterCmd(ctx context.Context, sess *session, deps Dependencies, args []string) error {
	cfg := deps.FilterEngine.GetConfig()
	if len(args) == 0 {
		sess.WriteLine("Harvester is %s (poll=%dms, runtime-only; starts OFF on restart)", onOff(cfg.Enabled), cfg.PollIntervalMs)
		return nil
	}
	if len(args) != 1 {
		return errors.New("usage: harvester [on|off|toggle]")
	}
	var enabled bool
	if strings.EqualFold(strings.TrimSpace(args[0]), "toggle") {
		enabled = !cfg.Enabled
	} else {
		var err error
		enabled, err = parseToggle(args[0])
		if err != nil {
			return err
		}
	}
	cfg.Enabled = enabled
	deps.FilterEngine.UpdateConfig(cfg)
	if enabled {
		go func() {
			_ = deps.Harvester.PollOnce(ctx)
		}()
	}
	sess.WriteLine("Harvester is now %s (runtime-only; starts OFF on restart)", onOff(enabled))
	return nil
}

func handleKeysCmd(sess *session, deps Dependencies, args []string) error {
	if len(args) == 0 || strings.EqualFold(args[0], "show") {
		steam, stratz, od := deps.Harvester.GetAPIKeys()
		sess.WriteLine("API Keys: steam=%s stratz=%s opendota=%s", keyState(steam), keyState(stratz), keyState(od))
		return nil
	}
	if len(args) != 2 {
		return errors.New("usage: keys [show | <steam|stratz|opendota> <key|clear>]")
	}
	provider := strings.ToLower(args[0])
	val := strings.TrimSpace(args[1])
	if strings.EqualFold(val, "clear") {
		val = ""
	}
	steam, stratz, od := deps.Harvester.GetAPIKeys()
	switch provider {
	case "steam":
		steam = val
	case "stratz":
		stratz = val
	case "opendota":
		od = val
	default:
		return fmt.Errorf("unknown provider %q (use steam, stratz, or opendota)", provider)
	}
	deps.Harvester.SetAPIKeys(steam, stratz, od)
	if deps.DataDir != "" {
		if err := api.SavePersistedAPIKeys(filepath.Join(deps.DataDir, "api_keys.json"), steam, stratz, od); err != nil {
			return err
		}
	}
	sess.WriteLine("Updated %s API key (%s)", provider, keyState(val))
	return nil
}

func handleFilterCmd(ctx context.Context, sess *session, deps Dependencies, args []string) error {
	if len(args) == 0 || strings.EqualFold(args[0], "show") {
		cfg := deps.FilterEngine.GetConfig()
		sess.WriteLine("Filter: regions=%v lobbies=%v modes=%v duration=%d..%ds humans10=%v",
			cfg.AllowedRegions, cfg.AllowedLobbyTypes, cfg.AllowedGameModes,
			cfg.MinDurationSeconds, cfg.MaxDurationSeconds, cfg.Require10Humans)
		return nil
	}
	if len(args) < 2 {
		return errors.New("usage: filter [show|regions|lobbies|modes|min-duration|max-duration|humans] <value>")
	}
	sub := strings.ToLower(args[0])
	val := strings.Join(args[1:], ",")
	cfg := deps.FilterEngine.GetConfig()

	switch sub {
	case "regions":
		regs, err := parseRegionsCSV(val)
		if err != nil {
			return err
		}
		cfg.AllowedRegions = regs
	case "lobbies":
		lobbies, err := parseIntsCSV(val)
		if err != nil {
			return err
		}
		cfg.AllowedLobbyTypes = lobbies
	case "modes":
		modes, err := parseIntsCSV(val)
		if err != nil {
			return err
		}
		cfg.AllowedGameModes = modes
	case "min-duration":
		n, err := strconv.Atoi(strings.TrimSpace(args[1]))
		if err != nil || n < 0 {
			return fmt.Errorf("invalid min-duration %q", args[1])
		}
		cfg.MinDurationSeconds = n
	case "max-duration":
		n, err := strconv.Atoi(strings.TrimSpace(args[1]))
		if err != nil || n < 0 {
			return fmt.Errorf("invalid max-duration %q", args[1])
		}
		cfg.MaxDurationSeconds = n
	case "humans":
		b, err := parseToggle(args[1])
		if err != nil {
			return err
		}
		cfg.Require10Humans = b
	default:
		return fmt.Errorf("unknown filter subcommand %q", sub)
	}

	updated := deps.FilterEngine.UpdateConfig(cfg)
	if deps.LeaseMgr != nil {
		_ = deps.LeaseMgr.SaveFilterConfig(ctx, updated)
	}
	if deps.DataDir != "" {
		if err := api.SavePersistedFilterConfig(filepath.Join(deps.DataDir, "filter_config.json"), updated); err != nil {
			return err
		}
	}
	sess.WriteLine("Saved filter config: regions=%v lobbies=%v modes=%v duration=%d..%ds humans10=%v",
		updated.AllowedRegions, updated.AllowedLobbyTypes, updated.AllowedGameModes,
		updated.MinDurationSeconds, updated.MaxDurationSeconds, updated.Require10Humans)
	return nil
}

func handleFileStorageCmd(sess *session, deps Dependencies, args []string) error {
	if len(args) == 0 {
		sink := deps.Store.GetSinkStats()
		sess.WriteLine("Local file storage is %s (dir=%s, stored=%d)", onOff(sink.FileStorageEnabled), sink.StorageDir, sink.FilesStored)
		return nil
	}
	if len(args) != 1 {
		return errors.New("usage: file-storage [on|off|toggle]")
	}
	var enabled bool
	if strings.EqualFold(strings.TrimSpace(args[0]), "toggle") {
		enabled = !deps.Store.GetSinkStats().FileStorageEnabled
	} else {
		var err error
		enabled, err = parseToggle(args[0])
		if err != nil {
			return err
		}
	}
	cfg := deps.Store.SetFileStorageEnabled(enabled)
	if deps.DataDir != "" {
		if err := api.SavePersistedSinkConfig(filepath.Join(deps.DataDir, "sink_config.json"), cfg.WebhookURL, cfg.WebhookEnabled, cfg.FileStorageEnabled); err != nil {
			return err
		}
	}
	sess.WriteLine("Local file storage is now %s", onOff(cfg.FileStorageEnabled))
	return nil
}

func handleWebhookCmd(sess *session, deps Dependencies, args []string) error {
	st := deps.Store.GetSinkStats()
	if len(args) == 0 {
		sess.WriteLine("Webhook: enabled=%s url=%q delivered=%d failed=%d", onOff(st.WebhookEnabled), st.WebhookURL, st.WebhookDelivered, st.WebhookFailed)
		return nil
	}
	if len(args) != 1 {
		return errors.New("usage: webhook [url|on|off|clear]")
	}
	url := st.WebhookURL
	enabled := st.WebhookEnabled

	switch strings.ToLower(args[0]) {
	case "on", "true", "enable":
		enabled = true
	case "off", "false", "disable":
		enabled = false
	case "clear":
		url = ""
		enabled = false
	default:
		url = strings.TrimSpace(args[0])
	}
	cfg := deps.Store.UpdateSinkConfig(url, enabled)
	if deps.DataDir != "" {
		if err := api.SavePersistedSinkConfig(filepath.Join(deps.DataDir, "sink_config.json"), cfg.WebhookURL, cfg.WebhookEnabled, cfg.FileStorageEnabled); err != nil {
			return err
		}
	}
	sess.WriteLine("Webhook updated: enabled=%s url=%q", onOff(cfg.WebhookEnabled), cfg.WebhookURL)
	return nil
}

func handleParserCmd(ctx context.Context, sess *session, deps Dependencies, args []string) error {
	if len(args) == 0 || strings.EqualFold(args[0], "list") {
		active, custom := deps.ParserMgr.ListAll()
		sess.WriteLine("Active Parser: %s (%s, rev=%d)", active.ReportedName, active.Kind, active.Revision)
		for _, p := range custom {
			sess.WriteLine("  custom id=%s kind=%s sha256=%.8s file=%s", p.ID, p.Kind, p.SHA256, p.Filename)
		}
		return nil
	}

	switch strings.ToLower(args[0]) {
	case "activate":
		if len(args) != 2 {
			return errors.New("usage: parser activate <default|raw|id>")
		}
		var desc models.ParserDescriptor
		var err error
		if strings.EqualFold(args[1], "default") {
			desc, err = deps.ParserMgr.ActivateDefault()
		} else if strings.EqualFold(args[1], "raw") || strings.EqualFold(args[1], "none") {
			desc, err = deps.ParserMgr.ActivateRaw()
		} else {
			desc, err = deps.ParserMgr.ActivateCustom(args[1])
		}
		if err != nil {
			return err
		}
		sess.WriteLine("Activated parser: %s (rev=%d)", desc.ReportedName, desc.Revision)
		return nil

	case "upload":
		if len(args) < 2 {
			return errors.New("usage: parser upload <filepath> [name]")
		}
		src, err := os.ReadFile(args[1])
		if err != nil {
			return err
		}
		name := ""
		if len(args) >= 3 {
			name = args[2]
		}
		desc, err := deps.ParserMgr.UploadCustom(ctx, name, filepath.Base(args[1]), src, true)
		if err != nil {
			return err
		}
		sess.WriteLine("Uploaded and activated custom parser: %s (id=%s rev=%d)", desc.ReportedName, desc.ID, desc.Revision)
		return nil

	case "delete":
		if len(args) != 2 {
			return errors.New("usage: parser delete <id>")
		}
		active, err := deps.ParserMgr.DeleteCustom(args[1])
		if err != nil {
			return err
		}
		sess.WriteLine("Deleted custom parser %q; active parser is now %s", args[1], active.ReportedName)
		return nil

	default:
		return fmt.Errorf("unknown parser subcommand %q", args[0])
	}
}

func handleInjectCmd(ctx context.Context, sess *session, deps Dependencies, args []string) error {
	if len(args) == 1 {
		if count, err := strconv.Atoi(args[0]); err == nil && count >= 1 && count <= 100 {
			for n := 0; n < count; n++ {
				if _, err := deps.Server.InjectJob(ctx, models.InjectJobRequest{BypassDelay: true}, deps.BaseURL); err != nil {
					return err
				}
			}
			sess.WriteLine("Enqueued %d sample jobs", count)
			return nil
		}
	}

	req := models.InjectJobRequest{BypassDelay: true}
	if len(args) >= 1 {
		id, err := strconv.ParseUint(args[0], 10, 64)
		if err != nil {
			return fmt.Errorf("invalid match_id or count %q: %w", args[0], err)
		}
		req.MatchID = id
	}
	if len(args) >= 2 {
		c, err := strconv.ParseUint(args[1], 10, 32)
		if err != nil {
			return fmt.Errorf("invalid cluster %q: %w", args[1], err)
		}
		req.Cluster = uint32(c)
	}
	job, err := deps.Server.InjectJob(ctx, req, deps.BaseURL)
	if err != nil {
		return err
	}
	sess.WriteLine("Enqueued job: match_id=%d cluster=%d region=%s url=%s", job.MatchID, job.Cluster, job.Region, job.URL)
	return nil
}

func handleLogsCmd(sess *session, args []string) error {
	if len(args) == 0 {
		sess.WriteLine("Logs mode: %s", sess.GetLogMode())
		for _, entry := range sess.RecentLogs(10) {
			sess.WriteLine("  %s", entry)
		}
		return nil
	}
	arg := strings.ToLower(strings.TrimSpace(args[0]))
	switch arg {
	case "on", "off", "errors":
		sess.SetLogMode(arg)
		sess.WriteLine("Background logs mode: %s", arg)
		return nil
	default:
		n, err := strconv.Atoi(arg)
		if err != nil || n <= 0 {
			return fmt.Errorf("usage: logs [on|off|errors|N]")
		}
		entries := sess.RecentLogs(n)
		if len(entries) == 0 {
			sess.WriteLine("(no buffered logs)")
			return nil
		}
		for _, entry := range entries {
			sess.WriteLine("  %s", entry)
		}
		return nil
	}
}

func handleWatchCmd(ctx context.Context, sess *session, deps Dependencies) error {
	sess.watchActive.Store(true)
	defer func() {
		sess.watchActive.Store(false)
		sess.ClearWatchLine()
	}()

	stopWatch := make(chan struct{})
	enterDone := make(chan struct{})
	go func() {
		defer close(enterDone)
		sess.WaitForEnter()
		close(stopWatch)
	}()

	renderLine := func() string {
		fCfg := deps.FilterEngine.GetConfig()
		totalParsed, todayParsed, _ := deps.Store.GetCounters()
		depths, delayed, _ := deps.LeaseMgr.GetQueueDepths(ctx)
		leases, _ := deps.LeaseMgr.ListActiveLeases(ctx)
		totalQ := depths[models.RegionEU] + depths[models.RegionNA] + depths[models.RegionSEA] +
			depths[models.RegionSA] + depths[models.RegionCN] + depths[models.RegionOCE] + depths[models.RegionGlobal]
		return fmt.Sprintf("[WATCH] Harvester: %s | Ready: %d | Delayed: %d | Leases: %d | Parsed: %d (%d total)  (press Enter to exit)",
			onOff(fCfg.Enabled), totalQ, delayed, len(leases), todayParsed, totalParsed)
	}

	sess.WriteWatchLine(renderLine())
	ticker := time.NewTicker(400 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return nil
		case <-stopWatch:
			return nil
		case <-ticker.C:
			sess.WriteWatchLine(renderLine())
		}
	}
}

func handleHistoryCmd(sess *session) error {
	hist := sess.History()
	if len(hist) == 0 {
		sess.WriteLine("(history empty)")
		return nil
	}
	for i, h := range hist {
		sess.WriteLine("  %3d  %s", i+1, h)
	}
	return nil
}

func handleSaveCmd(ctx context.Context, sess *session, deps Dependencies) error {
	if deps.DataDir == "" {
		return errors.New("no data directory configured")
	}
	steam, stratz, od := deps.Harvester.GetAPIKeys()
	if err := api.SavePersistedAPIKeys(filepath.Join(deps.DataDir, "api_keys.json"), steam, stratz, od); err != nil {
		return err
	}
	filterCfg := deps.FilterEngine.GetConfig()
	if err := api.SavePersistedFilterConfig(filepath.Join(deps.DataDir, "filter_config.json"), filterCfg); err != nil {
		return err
	}
	if deps.LeaseMgr != nil {
		_ = deps.LeaseMgr.SaveFilterConfig(ctx, filterCfg)
	}
	sink := deps.Store.GetSinkStats()
	if err := api.SavePersistedSinkConfig(filepath.Join(deps.DataDir, "sink_config.json"), sink.WebhookURL, sink.WebhookEnabled, sink.FileStorageEnabled); err != nil {
		return err
	}
	if deps.ParserMgr != nil {
		if err := deps.ParserMgr.SaveState(); err != nil {
			return err
		}
	}
	sess.WriteLine("Saved API keys, match filters (harvester kept OFF on disk), sink, and parser settings to %s", deps.DataDir)
	return nil
}

func hasAnyFilterFlag(set map[string]bool) bool {
	for _, k := range []string{"harvester", "regions", "lobby-types", "game-modes", "min-duration", "max-duration", "require-10-humans"} {
		if set[k] {
			return true
		}
	}
	return false
}

func parseRegionsCSV(raw string) ([]models.MacroRegion, error) {
	parts := strings.Split(raw, ",")
	out := make([]models.MacroRegion, 0, len(parts))
	for _, p := range parts {
		clean := strings.ToUpper(strings.TrimSpace(p))
		if clean == "" {
			continue
		}
		switch models.MacroRegion(clean) {
		case models.RegionEU, models.RegionNA, models.RegionSEA, models.RegionSA, models.RegionCN, models.RegionOCE, models.RegionGlobal:
			out = append(out, models.MacroRegion(clean))
		default:
			return nil, fmt.Errorf("unknown region %q", clean)
		}
	}
	if len(out) == 0 {
		return nil, errors.New("regions list cannot be empty")
	}
	return out, nil
}

func parseIntsCSV(raw string) ([]int, error) {
	parts := strings.Split(raw, ",")
	out := make([]int, 0, len(parts))
	for _, p := range parts {
		clean := strings.TrimSpace(p)
		if clean == "" {
			continue
		}
		n, err := strconv.Atoi(clean)
		if err != nil {
			return nil, err
		}
		out = append(out, n)
	}
	return out, nil
}

func parseToggle(s string) (bool, error) {
	switch strings.ToLower(strings.TrimSpace(s)) {
	case "on", "true", "1", "yes":
		return true, nil
	case "off", "false", "0", "no":
		return false, nil
	default:
		return false, fmt.Errorf("expected on/off or true/false, got %q", s)
	}
}

func onOff(v bool) string {
	if v {
		return "ON"
	}
	return "OFF"
}

func keyState(k string) string {
	if strings.TrimSpace(k) == "" {
		return "not set"
	}
	return fmt.Sprintf("configured(len=%d)", len(k))
}
