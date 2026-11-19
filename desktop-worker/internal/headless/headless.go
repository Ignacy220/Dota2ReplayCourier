package headless

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/dota2-analytics/desktop-worker/internal/config"
	"github.com/dota2-analytics/desktop-worker/internal/orchestrator"
	"github.com/dota2-analytics/desktop-worker/internal/sentinel"
)

// Flags holds parsed command-line options for the headless worker.
type Flags struct {
	Headless       bool
	ServerURL      string
	Region         string
	StratzKey      string
	OpenDotaKey    string
	PauseOnGaming  bool
	GamingBehavior string
	IdlePolicy     string
	IdleThreshold  int
	BandwidthLimit int
	SpeedLimit     int
	Paused         bool
	NoWizard       bool
	Set            map[string]bool
}

var workerCommands = []commandSpec{
	{Name: "help", Description: "Show available commands"},
	{Name: "status", Description: "Show aligned status cards with live progress bars"},
	{Name: "pause", Description: "Pause replay leasing and parsing (or toggle with 'p')"},
	{Name: "resume", Description: "Resume replay leasing and parsing"},
	{Name: "server", Description: "Inspect or set Coordinator server URL (<url|clear>)"},
	{Name: "region", Description: "Inspect or set preferred region (<AUTO|EU|NA|SEA|SA|CN|OCE>)"},
	{Name: "regions", Description: "Probe Valve regional relay latencies"},
	{Name: "keys", Description: "Inspect or update local salt keys (<stratz|opendota> <key|clear>)"},
	{Name: "guard", Description: "Inspect or configure Gamer Guard (<on|off> [HOLD_IN_RAM|ECO_SLOWDOWN])"},
	{Name: "idle", Description: "Inspect or configure idle policy (<POLICY> [threshold_sec])"},
	{Name: "limits", Description: "Inspect or set daily bandwidth (MB) and speed limit (KB/s)"},
	{Name: "login", Description: "Sign into contributor account (<nickname> [password])"},
	{Name: "register", Description: "Create contributor account (<nickname> [RADIANT|DIRE])"},
	{Name: "logout", Description: "Sign out of contributor account"},
	{Name: "watch", Description: "Live single-line telemetry ticker (press Enter to exit)"},
	{Name: "logs", Description: "Control live logs (<on|off|errors>) or show last [N] entries"},
	{Name: "setup", Description: "Run interactive first-time server & account setup"},
	{Name: "history", Description: "Show command history (use !! or !<n> to repeat)"},
	{Name: "save", Description: "Persist current session overrides to config.json"},
	{Name: "quit", Description: "Stop the worker"},
}

var workerAliases = map[string]string{
	"?":    "help",
	"s":    "status",
	"p":    "toggle_pause",
	"r":    "resume",
	"w":    "watch",
	"l":    "logs",
	"init": "setup",
	"q":    "quit",
	"exit": "quit",
}

// ParseFlags parses worker CLI flags without mutating global flag.CommandLine.
func ParseFlags(args []string) (Flags, error) {
	var f Flags
	f.Set = make(map[string]bool)

	fs := flag.NewFlagSet("dota-analytics-worker", flag.ContinueOnError)
	fs.BoolVar(&f.Headless, "headless", false, "Run headless in terminal mode without GUI or local UI HTTP server")
	fs.StringVar(&f.ServerURL, "server", "", "Override Central Server URL for this session")
	fs.StringVar(&f.Region, "region", "", "Override preferred region (AUTO, EU, NA, SEA, SA, CN, OCE)")
	fs.StringVar(&f.StratzKey, "stratz-key", "", "Override local STRATZ API key for this session")
	fs.StringVar(&f.OpenDotaKey, "opendota-key", "", "Override local OpenDota API key for this session")
	fs.BoolVar(&f.PauseOnGaming, "pause-on-gaming", true, "Pause/slow down when a watched game runs")
	fs.StringVar(&f.GamingBehavior, "gaming-behavior", "", "Gaming parse behavior (HOLD_IN_RAM or ECO_SLOWDOWN)")
	fs.StringVar(&f.IdlePolicy, "idle-policy", "", "Idle policy (ALWAYS_RUN, TURBO_WHEN_IDLE, or ONLY_WHEN_IDLE)")
	fs.IntVar(&f.IdleThreshold, "idle-threshold", 0, "Idle threshold in seconds")
	fs.IntVar(&f.BandwidthLimit, "bandwidth-limit", 0, "Daily bandwidth limit in MB")
	fs.IntVar(&f.SpeedLimit, "speed-limit", 0, "Download speed limit in KB/s (0 = unlimited)")
	fs.BoolVar(&f.Paused, "paused", false, "Start worker paused")
	fs.BoolVar(&f.NoWizard, "no-wizard", false, "Skip interactive first-run server/account prompt")

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

// ApplyStartupFlags applies parsed CLI flags in-memory for this session without saving config.json.
func ApplyStartupFlags(flags Flags, cfgMgr *config.Manager, orch *orchestrator.Orchestrator) error {
	var region string
	if flags.Set["region"] {
		r, err := normalizeRegion(flags.Region)
		if err != nil {
			return err
		}
		region = r
	}

	var gBeh config.GamingParseBehavior
	if flags.Set["gaming-behavior"] {
		b, err := normalizeGamingBehavior(flags.GamingBehavior)
		if err != nil {
			return err
		}
		gBeh = b
	}

	var idlePol config.IdlePolicy
	if flags.Set["idle-policy"] {
		p, err := normalizeIdlePolicy(flags.IdlePolicy)
		if err != nil {
			return err
		}
		idlePol = p
	}

	if hasConfigFlag(flags.Set) {
		cfgMgr.UpdateEphemeral(func(c *config.WorkerConfig) {
			if flags.Set["server"] {
				c.ServerURL = strings.TrimRight(strings.TrimSpace(flags.ServerURL), "/")
			}
			if flags.Set["region"] {
				c.PreferredRegion = region
			}
			if flags.Set["stratz-key"] {
				c.StratzAPIKey = strings.TrimSpace(flags.StratzKey)
			}
			if flags.Set["opendota-key"] {
				c.OpenDotaAPIKey = strings.TrimSpace(flags.OpenDotaKey)
			}
			if flags.Set["pause-on-gaming"] {
				c.PauseOnGaming = flags.PauseOnGaming
			}
			if flags.Set["gaming-behavior"] {
				c.GamingParseBehavior = gBeh
			}
			if flags.Set["idle-policy"] {
				c.IdlePolicy = idlePol
			}
			if flags.Set["idle-threshold"] && flags.IdleThreshold > 0 {
				c.IdleThresholdSeconds = flags.IdleThreshold
			}
			if flags.Set["bandwidth-limit"] && flags.BandwidthLimit >= 0 {
				c.MaxDailyBandwidthMB = flags.BandwidthLimit
			}
			if flags.Set["speed-limit"] && flags.SpeedLimit >= 0 {
				c.DownloadSpeedLimitKBps = flags.SpeedLimit
			}
		})
	}

	if flags.Set["paused"] && orch != nil {
		orch.SetUserPaused(flags.Paused)
	}
	return nil
}

// Run starts Sentinel and Orchestrator with default options.
func Run(
	ctx context.Context,
	in io.Reader,
	out io.Writer,
	stop context.CancelFunc,
	cfgMgr *config.Manager,
	sent *sentinel.Sentinel,
	orch *orchestrator.Orchestrator,
) {
	RunWithFlags(ctx, in, out, stop, cfgMgr, sent, orch, Flags{})
}

// RunWithFlags starts Sentinel and Orchestrator, streams activity logs, processes interactive commands,
// and blocks until ctx is cancelled (stdin EOF exits only the command reader so daemons stay running).
func RunWithFlags(
	ctx context.Context,
	in io.Reader,
	out io.Writer,
	stop context.CancelFunc,
	cfgMgr *config.Manager,
	sent *sentinel.Sentinel,
	orch *orchestrator.Orchestrator,
	flags Flags,
) {
	sess, restore := newSession(in, out, "courier> ", func(line string) []string {
		return workerCompleter(line)
	})
	defer restore()

	if _, tty := isTerminalFile(in); tty {
		prevLogOut := log.Writer()
		log.SetOutput(sess.LogWriter())
		defer log.SetOutput(prevLogOut)
	}

	if sess.rawMode && !flags.NoWizard && strings.TrimSpace(cfgMgr.Get().ServerURL) == "" {
		runFirstRunWizard(ctx, sess, cfgMgr, orch)
	}

	if sent != nil {
		sent.Start(ctx)
	}
	orch.Start(ctx)

	sess.WriteLine("Dota2ReplayCourier Worker (headless) started. Type 'help' for commands.")

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		streamActivityLogs(ctx, sess, orch)
	}()

	cmdDone := make(chan struct{})
	go func() {
		defer close(cmdDone)
		runCommandReader(ctx, sess, stop, cfgMgr, orch)
	}()

	select {
	case <-ctx.Done():
	case <-cmdDone:
		<-ctx.Done()
	}
	wg.Wait()
}

func runFirstRunWizard(ctx context.Context, sess *session, cfgMgr *config.Manager, orch *orchestrator.Orchestrator) {
	sess.WriteLine("First-run setup (press Enter to skip any step):")
	urlLine, err := sess.ReadLine("Coordinator URL (e.g. https://example.com): ", false)
	if err != nil {
		return
	}
	cleanURL := strings.TrimRight(strings.TrimSpace(urlLine), "/")
	if cleanURL == "" {
		return
	}

	cfgMgr.Update(func(c *config.WorkerConfig) {
		c.ServerURL = cleanURL
	})
	orch.ProbeServerNow()
	snap := orch.GetStatusSnapshot()
	if snap.Connected {
		sess.WriteLine("Connected to %s (%d ms) and saved URL.", cleanURL, snap.ServerLatencyMs)
	} else {
		sess.WriteLine("Saved server URL %s (not yet reachable: %s).", cleanURL, snap.LastError)
	}

	if snap.Connected && !snap.Authenticated {
		choice, err := sess.ReadLine("Account — [l]ogin, [r]egister, or [Enter] to continue anonymously: ", false)
		if err != nil {
			return
		}
		switch strings.ToLower(strings.TrimSpace(choice)) {
		case "l", "login":
			nick, err := sess.ReadLine("Nickname: ", false)
			if err != nil || strings.TrimSpace(nick) == "" {
				return
			}
			if err := handleLoginCmd(ctx, sess, orch, []string{strings.TrimSpace(nick)}); err != nil {
				sess.WriteLine("error: %v", err)
			}
		case "r", "register":
			nick, err := sess.ReadLine("Nickname: ", false)
			if err != nil || strings.TrimSpace(nick) == "" {
				return
			}
			if err := handleRegisterCmd(ctx, sess, orch, []string{strings.TrimSpace(nick)}); err != nil {
				sess.WriteLine("error: %v", err)
			}
		}
	}
}

func streamActivityLogs(ctx context.Context, sess *session, orch *orchestrator.Orchestrator) {
	var seenNewest *orchestrator.ActivityEvent
	ticker := time.NewTicker(400 * time.Millisecond)
	defer ticker.Stop()

	flush := func() {
		feed := orch.GetStatusSnapshot().ActivityFeed
		if len(feed) == 0 {
			return
		}
		cutoff := len(feed)
		if seenNewest != nil {
			for i, ev := range feed {
				if ev == *seenNewest {
					cutoff = i
					break
				}
			}
		}
		for i := cutoff - 1; i >= 0; i-- {
			ev := feed[i]
			lower := strings.ToLower(ev.Message)
			isErr := strings.Contains(lower, "fail") || strings.Contains(lower, "error") ||
				strings.Contains(lower, "disconnected") || strings.Contains(lower, "quota reached")
			sess.LogEvent(isErr, fmt.Sprintf("[%s] [%-6s] %s", ev.Timestamp, ev.Category, ev.Message))
		}
		latest := feed[0]
		seenNewest = &latest
	}

	flush()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			flush()
		}
	}
}

func runCommandReader(
	ctx context.Context,
	sess *session,
	stop context.CancelFunc,
	cfgMgr *config.Manager,
	orch *orchestrator.Orchestrator,
) {
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		rawLine, err := sess.ReadLine("courier> ", false)
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

		cmd, _, resErr := resolveCommandName(rawCmd, workerCommands, workerAliases)
		recordSafeHistory(sess, line, cmd, args)
		if resErr != nil {
			sess.WriteLine("%v", resErr)
			continue
		}

		if cmd == "quit" {
			sess.WriteLine("Shutting down Worker...")
			if stop != nil {
				stop()
			}
			return
		}

		if err := dispatchCommand(ctx, sess, cfgMgr, orch, cmd, args); err != nil {
			sess.WriteLine("error: %v", err)
		}
	}
}

func recordSafeHistory(sess *session, line, cmd string, args []string) {
	if cmd == "login" && len(args) >= 2 {
		sess.AddHistory("login " + args[0])
		return
	}
	if cmd == "register" && len(args) >= 2 {
		if len(args) >= 3 {
			sess.AddHistory("register " + args[0] + " " + args[2])
		} else {
			sess.AddHistory("register " + args[0])
		}
		return
	}
	sess.AddHistory(line)
}

func workerCompleter(line string) []string {
	fields := strings.Fields(line)
	trailingSpace := len(line) > 0 && (line[len(line)-1] == ' ' || line[len(line)-1] == '\t')
	if len(fields) == 0 || (len(fields) == 1 && !trailingSpace) {
		names := make([]string, 0, len(workerCommands))
		for _, sp := range workerCommands {
			names = append(names, sp.Name)
		}
		return names
	}

	cmd, _, err := resolveCommandName(fields[0], workerCommands, workerAliases)
	if err != nil {
		return nil
	}
	switch cmd {
	case "region":
		return []string{"AUTO", "EU", "NA", "SEA", "SA", "CN", "OCE"}
	case "logs":
		return []string{"on", "off", "errors"}
	case "keys":
		return []string{"show", "stratz", "opendota"}
	case "guard":
		if len(fields) == 1 || (len(fields) == 2 && !trailingSpace) {
			return []string{"on", "off"}
		}
		return []string{"HOLD_IN_RAM", "ECO_SLOWDOWN"}
	case "idle":
		return []string{"ALWAYS_RUN", "TURBO_WHEN_IDLE", "ONLY_WHEN_IDLE"}
	case "limits":
		return []string{"unlimited", "0"}
	case "server":
		return []string{"clear"}
	}
	return nil
}

func dispatchCommand(
	ctx context.Context,
	sess *session,
	cfgMgr *config.Manager,
	orch *orchestrator.Orchestrator,
	cmd string,
	args []string,
) error {
	switch cmd {
	case "help":
		printHelp(sess)
		return nil
	case "status":
		printStatus(sess, orch)
		return nil
	case "pause":
		orch.SetUserPaused(true)
		sess.WriteLine("Worker paused")
		return nil
	case "resume":
		orch.SetUserPaused(false)
		sess.WriteLine("Worker resumed")
		return nil
	case "toggle_pause":
		paused := orch.ToggleUserPaused()
		if paused {
			sess.WriteLine("Worker paused")
		} else {
			sess.WriteLine("Worker resumed")
		}
		return nil
	case "server":
		return handleServerCmd(sess, cfgMgr, orch, args)
	case "region":
		return handleRegionCmd(sess, cfgMgr, orch, args)
	case "regions":
		if err := orch.ReprobeRegions(ctx); err != nil {
			return err
		}
		snap := orch.GetStatusSnapshot()
		sess.WriteLine("Region priority: %s", strings.Join(snap.RegionPriority, " > "))
		return nil
	case "keys":
		return handleKeysCmd(sess, cfgMgr, orch, args)
	case "guard":
		return handleGuardCmd(sess, cfgMgr, args)
	case "idle":
		return handleIdleCmd(sess, cfgMgr, orch, args)
	case "limits":
		return handleLimitsCmd(sess, cfgMgr, args)
	case "login":
		return handleLoginCmd(ctx, sess, orch, args)
	case "register":
		return handleRegisterCmd(ctx, sess, orch, args)
	case "logout":
		orch.LogoutAccount(ctx)
		sess.WriteLine("Signed out (anonymous mode)")
		return nil
	case "watch":
		return handleWatchCmd(ctx, sess, orch)
	case "logs":
		return handleLogsCmd(sess, args)
	case "setup":
		runFirstRunWizard(ctx, sess, cfgMgr, orch)
		return nil
	case "history":
		return handleHistoryCmd(sess)
	case "save":
		if err := cfgMgr.SaveCurrentToDisk(); err != nil {
			return err
		}
		sess.WriteLine("Saved current worker configuration to disk")
		return nil
	default:
		return fmt.Errorf("unknown command %q (type 'help' for commands)", cmd)
	}
}

func printHelp(sess *session) {
	sess.WriteLine(`Commands (aliases: s=status, p=pause/resume, r=resume, w=watch, l=logs, q=quit):
  status                                    Show aligned status cards and progress bars
  pause | resume                            Pause or resume leasing/parsing (or 'p' to toggle)
  server [url|clear]                        Inspect or set Coordinator URL, probe, and save
  region [AUTO|EU|NA|SEA|SA|CN|OCE]         Inspect or set preferred region and save
  regions                                   Probe Valve regional relay latencies
  keys [show|<stratz|opendota> <key|clear>] Inspect or update local salt resolver keys
  guard [on|off] [HOLD_IN_RAM|ECO_SLOWDOWN] Inspect or configure Gamer Guard and save
  idle [POLICY] [threshold_sec]             Inspect or set idle policy (ALWAYS_RUN/TURBO_WHEN_IDLE/ONLY_WHEN_IDLE)
  limits [daily_mb|unlimited] [speed_kbps|unlimited] Inspect or set bandwidth/speed limits (0=unlimited)
  login <nickname> [password]               Sign in (prompts masked password if omitted)
  register <nickname> [RADIANT|DIRE]        Register account (prompts masked password cleanly)
  logout                                    Sign out of contributor account
  watch                                     Live single-line telemetry ticker (press Enter to exit)
  logs [on|off|errors|N]                    Control live logs or print last N entries
  setup                                     Run first-time Coordinator & account setup wizard
  history                                   List recent commands (use !! or !<n> to repeat)
  save                                      Save session overrides to config.json
  quit | exit                               Stop the worker`)
}

func printStatus(sess *session, orch *orchestrator.Orchestrator) {
	s := orch.GetStatusSnapshot()
	dot := sess.color("33", "●")
	if s.Connected && !s.UserPaused {
		dot = sess.color("32", "●")
	} else if !s.Connected {
		dot = sess.color("31", "●")
	}
	acct := "anonymous"
	if s.Authenticated {
		acct = fmt.Sprintf("%s (%s)", s.AccountNickname, s.AccountFaction)
	}
	server := s.ServerURL
	if server == "" {
		server = "(none configured)"
	}

	sess.WriteLine("%s CONNECTION  %s · Server=%s (%dms) · Region=%s (%s)",
		dot, s.StateLabel, server, s.ServerLatencyMs, s.TargetRegion, strings.Join(s.RegionPriority, ">"))

	if s.CurrentMatchID > 0 {
		sess.WriteLine("%s PIPELINE    Match #%d (%s) %s %.0f%% · %.1f MB/s · %d ticks/s · Parser=%s",
			dot, s.CurrentMatchID, s.CurrentStage, progressBar(s.ProgressPercent, 20), s.ProgressPercent,
			s.DownloadMBps, s.TicksPerSecond, s.ActiveParserName)
	} else {
		sess.WriteLine("%s PIPELINE    Parsed today=%d all_time=%d · Parser=%s · State=%s",
			dot, s.YourParsedToday, s.TotalParsedAllTime, s.ActiveParserName, s.State)
	}

	sess.WriteLine("%s ACCOUNT     %s · %s (%s) %s %.0f%% -> %s · XP=%d",
		dot, acct, s.ContributorRank, s.ContributorRankIcon, progressBar(s.RankProgressPercent, 20),
		s.RankProgressPercent, s.NextRankName, s.ContributorXP)

	usedMB := float64(s.UsedBandwidthTodayBytes) / (1024.0 * 1024.0)
	dailyStr := "unlimited (0=unlimited)"
	if s.MaxDailyBandwidthMB > 0 {
		quotaPct := (usedMB / float64(s.MaxDailyBandwidthMB)) * 100.0
		dailyStr = fmt.Sprintf("%d MB %s", s.MaxDailyBandwidthMB, progressBar(quotaPct, 20))
	}
	speedStr := "unlimited"
	if s.Config.DownloadSpeedLimitKBps > 0 {
		speedStr = fmt.Sprintf("%d KB/s", s.Config.DownloadSpeedLimitKBps)
	}
	sess.WriteLine("%s GUARD/QUOTA PauseOnGaming=%v (%s) · Idle=%s (%ds) · Quota %.1f / %s · Speed=%s (0=unlimited)",
		dot, s.Config.PauseOnGaming, s.Config.GamingParseBehavior, s.Config.IdlePolicy,
		s.Config.IdleThresholdSeconds, usedMB, dailyStr, speedStr)
	sess.WriteLine("%s SALT KEYS   Mode=%s · STRATZ=%v · OpenDota=%v (%d/day left)",
		dot, s.SaltQuota.Mode, s.SaltQuota.StratzConfigured, s.SaltQuota.OpenDotaConfigured, s.SaltQuota.OpenDotaRemainingDay)
}

func handleServerCmd(sess *session, cfgMgr *config.Manager, orch *orchestrator.Orchestrator, args []string) error {
	if len(args) == 0 {
		snap := orch.GetStatusSnapshot()
		url := snap.ServerURL
		if url == "" {
			url = "(none configured)"
		}
		sess.WriteLine("Server: %s (connected=%v, latency=%dms, error=%q)", url, snap.Connected, snap.ServerLatencyMs, snap.LastError)
		return nil
	}
	if len(args) != 1 {
		return errors.New("usage: server [url|clear]")
	}
	u := strings.TrimRight(strings.TrimSpace(args[0]), "/")
	if strings.EqualFold(u, "clear") {
		u = ""
	}
	cfgMgr.Update(func(c *config.WorkerConfig) {
		c.ServerURL = u
	})
	orch.ProbeServerNow()
	snap := orch.GetStatusSnapshot()
	sess.WriteLine("Server set to %q (connected=%v)", u, snap.Connected)
	return nil
}

func handleRegionCmd(sess *session, cfgMgr *config.Manager, orch *orchestrator.Orchestrator, args []string) error {
	if len(args) == 0 {
		snap := orch.GetStatusSnapshot()
		sess.WriteLine("Preferred region: %s (priority=%v)", snap.TargetRegion, snap.RegionPriority)
		return nil
	}
	if len(args) != 1 {
		return errors.New("usage: region [AUTO|EU|NA|SEA|SA|CN|OCE]")
	}
	r, err := normalizeRegion(args[0])
	if err != nil {
		return err
	}
	cfgMgr.Update(func(c *config.WorkerConfig) {
		c.PreferredRegion = r
	})
	sess.WriteLine("Preferred region saved: %s", r)
	return nil
}

func handleKeysCmd(sess *session, cfgMgr *config.Manager, orch *orchestrator.Orchestrator, args []string) error {
	if len(args) == 0 || strings.EqualFold(args[0], "show") {
		snap := orch.GetStatusSnapshot()
		sess.WriteLine("Local Salt Keys: stratz=%v opendota=%v (mode=%s, remaining_today=%d)",
			snap.Config.StratzAPIKey != "", snap.Config.OpenDotaAPIKey != "",
			snap.SaltQuota.Mode, snap.SaltQuota.OpenDotaRemainingDay)
		return nil
	}
	if len(args) != 2 {
		return errors.New("usage: keys [show | <stratz|opendota> <key|clear>]")
	}
	val := strings.TrimSpace(args[1])
	if strings.EqualFold(val, "clear") {
		val = ""
	}
	switch strings.ToLower(args[0]) {
	case "stratz":
		cfgMgr.Update(func(c *config.WorkerConfig) { c.StratzAPIKey = val })
		if val != "" {
			orch.ResetSaltQuota()
		}
		sess.WriteLine("Saved STRATZ key (set=%v)", val != "")
		return nil
	case "opendota":
		cfgMgr.Update(func(c *config.WorkerConfig) { c.OpenDotaAPIKey = val })
		sess.WriteLine("Saved OpenDota key (set=%v)", val != "")
		return nil
	default:
		return fmt.Errorf("unknown key provider %q (use stratz or opendota)", args[0])
	}
}

func handleGuardCmd(sess *session, cfgMgr *config.Manager, args []string) error {
	if len(args) == 0 {
		cfg := cfgMgr.Get()
		sess.WriteLine("Gamer Guard: enabled=%v behavior=%s watched=%v", cfg.PauseOnGaming, cfg.GamingParseBehavior, cfg.WatchedProcesses)
		return nil
	}
	if len(args) > 2 {
		return errors.New("usage: guard [on|off] [HOLD_IN_RAM|ECO_SLOWDOWN]")
	}
	on, err := parseToggle(args[0])
	if err != nil {
		return err
	}
	var beh config.GamingParseBehavior
	if len(args) == 2 {
		beh, err = normalizeGamingBehavior(args[1])
		if err != nil {
			return err
		}
	}
	updated := cfgMgr.Update(func(c *config.WorkerConfig) {
		c.PauseOnGaming = on
		if beh != "" {
			c.GamingParseBehavior = beh
		}
	})
	sess.WriteLine("Gamer Guard saved: enabled=%v behavior=%s", updated.PauseOnGaming, updated.GamingParseBehavior)
	return nil
}

func handleIdleCmd(sess *session, cfgMgr *config.Manager, orch *orchestrator.Orchestrator, args []string) error {
	if len(args) == 0 {
		snap := orch.GetStatusSnapshot()
		sess.WriteLine("Idle policy: %s (threshold=%ds, current_idle=%ds)",
			snap.Config.IdlePolicy, snap.Config.IdleThresholdSeconds, snap.UserIdleSeconds)
		return nil
	}
	if len(args) > 2 {
		return errors.New("usage: idle [ALWAYS_RUN|TURBO_WHEN_IDLE|ONLY_WHEN_IDLE] [threshold_sec]")
	}
	pol, err := normalizeIdlePolicy(args[0])
	if err != nil {
		return err
	}
	sec := -1
	if len(args) == 2 {
		n, convErr := strconv.Atoi(args[1])
		if convErr != nil || n <= 0 {
			return fmt.Errorf("invalid idle threshold %q", args[1])
		}
		sec = n
	}
	updated := cfgMgr.Update(func(c *config.WorkerConfig) {
		c.IdlePolicy = pol
		if sec > 0 {
			c.IdleThresholdSeconds = sec
		}
	})
	sess.WriteLine("Idle policy saved: %s (threshold=%ds)", updated.IdlePolicy, updated.IdleThresholdSeconds)
	return nil
}

func handleLimitsCmd(sess *session, cfgMgr *config.Manager, args []string) error {
	if len(args) == 0 {
		cfg := cfgMgr.Get()
		usedMB := float64(cfg.UsedBandwidthTodayBytes) / (1024.0 * 1024.0)
		dailyStr := "unlimited"
		if cfg.MaxDailyBandwidthMB > 0 {
			dailyStr = fmt.Sprintf("%d MB/day", cfg.MaxDailyBandwidthMB)
		}
		speedStr := "unlimited"
		if cfg.DownloadSpeedLimitKBps > 0 {
			speedStr = fmt.Sprintf("%d KB/s", cfg.DownloadSpeedLimitKBps)
		}
		sess.WriteLine("Bandwidth limits: %.1f MB used / %s, speed=%s (0=unlimited)", usedMB, dailyStr, speedStr)
		return nil
	}
	if len(args) > 2 {
		return errors.New("usage: limits [daily_mb|unlimited] [speed_kbps|unlimited]")
	}
	dailyMB, err := parseLimitArg(args[0])
	if err != nil {
		return fmt.Errorf("invalid daily_mb %q: %w", args[0], err)
	}
	speedKBps := -1
	if len(args) == 2 {
		s, err := parseLimitArg(args[1])
		if err != nil {
			return fmt.Errorf("invalid speed_kbps %q: %w", args[1], err)
		}
		speedKBps = s
	}
	updated := cfgMgr.Update(func(c *config.WorkerConfig) {
		c.MaxDailyBandwidthMB = dailyMB
		if speedKBps >= 0 {
			c.DownloadSpeedLimitKBps = speedKBps
		}
	})
	dailyOut := fmt.Sprintf("%d MB/day", updated.MaxDailyBandwidthMB)
	if updated.MaxDailyBandwidthMB == 0 {
		dailyOut = "0 MB/day (unlimited)"
	}
	speedOut := fmt.Sprintf("%d KB/s", updated.DownloadSpeedLimitKBps)
	if updated.DownloadSpeedLimitKBps == 0 {
		speedOut = "0 KB/s (unlimited)"
	}
	sess.WriteLine("Bandwidth limits saved: %s, %s", dailyOut, speedOut)
	return nil
}

func parseLimitArg(raw string) (int, error) {
	clean := strings.ToLower(strings.TrimSpace(raw))
	if clean == "unlimited" || clean == "inf" {
		return 0, nil
	}
	n, err := strconv.Atoi(clean)
	if err != nil || n < 0 {
		return 0, errors.New("expected >= 0 or 'unlimited'")
	}
	return n, nil
}

func handleLoginCmd(ctx context.Context, sess *session, orch *orchestrator.Orchestrator, args []string) error {
	if len(args) < 1 || len(args) > 2 {
		return errors.New("usage: login <nickname> [password]")
	}
	nick := strings.TrimSpace(args[0])
	pass := ""
	if len(args) == 2 {
		pass = args[1]
	} else {
		p, err := sess.ReadLine("Password: ", true)
		if err != nil {
			return err
		}
		pass = p
	}
	if err := orch.LoginAccount(ctx, nick, pass); err != nil {
		return err
	}
	snap := orch.GetStatusSnapshot()
	sess.WriteLine("Signed in as %s (%s, %d XP)", snap.AccountNickname, snap.AccountFaction, snap.ContributorXP)
	return nil
}

func handleRegisterCmd(ctx context.Context, sess *session, orch *orchestrator.Orchestrator, args []string) error {
	if len(args) < 1 || len(args) > 3 {
		return errors.New("usage: register <nickname> [RADIANT|DIRE] | register <nickname> <password> <RADIANT|DIRE>")
	}
	nick := strings.TrimSpace(args[0])
	var pass, faction string
	switch len(args) {
	case 3:
		pass = args[1]
		faction = args[2]
	case 2:
		p, err := sess.ReadLine("Password: ", true)
		if err != nil {
			return err
		}
		pass = p
		faction = args[1]
	case 1:
		p, err := sess.ReadLine("Password: ", true)
		if err != nil {
			return err
		}
		pass = p
		f, err := sess.ReadLine("Faction (RADIANT/DIRE): ", false)
		if err != nil {
			return err
		}
		faction = f
	}
	faction = strings.ToLower(strings.TrimSpace(faction))
	if faction != "radiant" && faction != "dire" {
		return errors.New("faction must be RADIANT or DIRE")
	}
	if err := orch.RegisterAccount(ctx, nick, pass, faction); err != nil {
		return err
	}
	snap := orch.GetStatusSnapshot()
	sess.WriteLine("Registered %s on %s", snap.AccountNickname, snap.AccountFaction)
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

func handleWatchCmd(ctx context.Context, sess *session, orch *orchestrator.Orchestrator) error {
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
		s := orch.GetStatusSnapshot()
		stage := s.StateLabel
		if s.CurrentMatchID > 0 {
			stage = fmt.Sprintf("#%d %s %s %.0f%%", s.CurrentMatchID, s.CurrentStage, progressBar(s.ProgressPercent, 14), s.ProgressPercent)
		}
		return fmt.Sprintf("[WATCH] %s | %.1f MB/s | %d ticks/s | Parsed: %d (press Enter to exit)",
			stage, s.DownloadMBps, s.TicksPerSecond, s.YourParsedToday)
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

func hasConfigFlag(set map[string]bool) bool {
	for _, k := range []string{"server", "region", "stratz-key", "opendota-key", "pause-on-gaming", "gaming-behavior", "idle-policy", "idle-threshold", "bandwidth-limit", "speed-limit"} {
		if set[k] {
			return true
		}
	}
	return false
}

func normalizeRegion(raw string) (string, error) {
	r := strings.ToUpper(strings.TrimSpace(raw))
	switch r {
	case "AUTO", "EU", "NA", "SEA", "SA", "CN", "OCE":
		return r, nil
	default:
		return "", fmt.Errorf("invalid region %q (use AUTO, EU, NA, SEA, SA, CN, OCE)", raw)
	}
}

func normalizeGamingBehavior(raw string) (config.GamingParseBehavior, error) {
	switch strings.ToUpper(strings.TrimSpace(raw)) {
	case string(config.BehaviorHoldInRAM):
		return config.BehaviorHoldInRAM, nil
	case string(config.BehaviorEcoSlowdown):
		return config.BehaviorEcoSlowdown, nil
	default:
		return "", fmt.Errorf("invalid gaming behavior %q (use HOLD_IN_RAM or ECO_SLOWDOWN)", raw)
	}
}

func normalizeIdlePolicy(raw string) (config.IdlePolicy, error) {
	switch strings.ToUpper(strings.TrimSpace(raw)) {
	case string(config.IdlePolicyAlwaysRun), "ALWAYS_ACTIVE":
		return config.IdlePolicyAlwaysRun, nil
	case string(config.IdlePolicyTurboWhenIdle):
		return config.IdlePolicyTurboWhenIdle, nil
	case string(config.IdlePolicyOnlyWhenIdle):
		return config.IdlePolicyOnlyWhenIdle, nil
	default:
		return "", fmt.Errorf("invalid idle policy %q (use ALWAYS_RUN, TURBO_WHEN_IDLE, or ONLY_WHEN_IDLE)", raw)
	}
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
