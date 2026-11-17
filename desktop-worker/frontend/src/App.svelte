<script>
  import { onMount } from "svelte";

  let activeTab = "miner";
  let showAuthModal = false;
  let authMode = "register";
  let authFaction = "radiant";
  let authNickname = "";
  let authPassword = "";
  let authError = "";
  let authBusy = false;

  let status = {
    state: "ACTIVE",
    state_label: "Active",
    connected: false,
    target_region: "AUTO",
    region_priority: [],
    region_latencies: [],
    probing_regions: false,
    account_nickname: "",
    account_faction: "",
    authenticated: false,
    prompted_auth: true,
    factions: { radiant: { xp: 0, members: 0 }, dire: { xp: 0, members: 0 } },
    current_match_id: 0,
    current_cluster: 0,
    current_stage: "IDLE",
    progress_percent: 0,
    ticks_per_second: 0,
    ram_buffer_bytes: 0,
    your_parsed_today: 0,
    total_parsed_all_time: 0,
    wards_extracted_total: 0,
    contributor_xp: 0,
    contributor_level: 1,
    contributor_rank: "Herald",
    contributor_rank_icon: "I",
    next_rank_xp: 150,
    rank_progress_percent: 0,
    streak_days: 1,
    global_parsed_total: 0,
    used_bandwidth_today_bytes: 0,
    max_daily_bandwidth_mb: 10240,
    user_paused: false,
    game_running: false,
    detected_game: "",
    user_idle_seconds: 0,
    active_alert_banner: "",
    active_parser_name: "default (2.0.0)",
    activity_feed: []
  };

  let prefs = {
    server_url: "http://localhost:8080",
    preferred_region: "AUTO",
    pause_on_gaming: true,
    gaming_parse_behavior: "HOLD_IN_RAM",
    idle_policy: "TURBO_WHEN_IDLE",
    idle_threshold_seconds: 300,
    watched_processes_str: "dota2.exe, dota2, cs2.exe, cs2, valorant.exe",
    max_daily_bandwidth_mb: 10240,
    download_speed_limit_kbps: 0
  };

  let prefsLoaded = false;
  let authModalChecked = false;
  let saveNotice = "";

  const RANK_TIERS = [
    { name: "Herald", tier: "I", minXP: 0 },
    { name: "Guardian", tier: "II", minXP: 150 },
    { name: "Crusader", tier: "III", minXP: 400 },
    { name: "Archon", tier: "IV", minXP: 800 },
    { name: "Legend", tier: "V", minXP: 1500 },
    { name: "Ancient", tier: "VI", minXP: 3000 },
    { name: "Divine", tier: "VII", minXP: 6000 },
    { name: "Immortal", tier: "VIII", minXP: 12000 }
  ];

  async function callGetStatus() {
    if (window.go && window.go.main && window.go.main.App) {
      return await window.go.main.App.GetStatus();
    }
    const res = await fetch("/api/local/status");
    return await res.json();
  }

  async function togglePause() {
    if (window.go && window.go.main && window.go.main.App) {
      status = await window.go.main.App.TogglePause();
      return;
    }
    const res = await fetch("/api/local/toggle-pause", { method: "POST" });
    status = await res.json();
  }

  async function triggerBenchmark() {
    if (window.go && window.go.main && window.go.main.App) {
      status = await window.go.main.App.RunBenchmark();
    } else {
      const res = await fetch("/api/local/benchmark", { method: "POST" });
      status = await res.json();
    }
    activeTab = "miner";
  }

  async function reprobeRegions() {
    if (window.go && window.go.main && window.go.main.App && window.go.main.App.ReprobeRegions) {
      status = await window.go.main.App.ReprobeRegions();
    } else {
      const res = await fetch("/api/local/reprobe-regions", { method: "POST" });
      status = await res.json();
    }
  }

  async function submitAuthForm() {
    authError = "";
    authBusy = true;
    try {
      if (window.go && window.go.main && window.go.main.App) {
        status = authMode === "register"
          ? await window.go.main.App.AuthRegister(authNickname.trim(), authPassword, authFaction)
          : await window.go.main.App.AuthLogin(authNickname.trim(), authPassword);
      } else {
        const endpoint = authMode === "register" ? "/api/local/auth/register" : "/api/local/auth/login";
        const body = authMode === "register"
          ? { nickname: authNickname.trim(), password: authPassword, faction: authFaction }
          : { nickname: authNickname.trim(), password: authPassword };
        const res = await fetch(endpoint, {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify(body)
        });
        const data = await res.json();
        if (!res.ok) throw new Error(data.error || "Authentication failed");
        status = data;
      }
      authPassword = "";
      showAuthModal = false;
    } catch (err) {
      authError = err && err.message ? err.message : String(err);
    } finally {
      authBusy = false;
    }
  }

  async function dismissAuthPrompt() {
    if (window.go && window.go.main && window.go.main.App && window.go.main.App.DismissAuthPrompt) {
      status = await window.go.main.App.DismissAuthPrompt();
    } else {
      const res = await fetch("/api/local/auth/dismiss", { method: "POST" });
      status = await res.json();
    }
    showAuthModal = false;
  }

  async function logoutAccount() {
    if (window.go && window.go.main && window.go.main.App && window.go.main.App.AuthLogout) {
      status = await window.go.main.App.AuthLogout();
    } else {
      const res = await fetch("/api/local/auth/logout", { method: "POST" });
      status = await res.json();
    }
  }

  async function savePreferences() {
    const payload = {
      server_url: prefs.server_url,
      preferred_region: prefs.preferred_region,
      pause_on_gaming: prefs.pause_on_gaming,
      gaming_parse_behavior: prefs.gaming_parse_behavior,
      idle_policy: prefs.idle_policy,
      idle_threshold_seconds: Number(prefs.idle_threshold_seconds) || 300,
      watched_processes: prefs.watched_processes_str.split(",").map(s => s.trim()).filter(Boolean),
      max_daily_bandwidth_mb: Number(prefs.max_daily_bandwidth_mb),
      download_speed_limit_kbps: Number(prefs.download_speed_limit_kbps) || 0
    };

    if (window.go && window.go.main && window.go.main.App) {
      status = await window.go.main.App.SavePreferences(payload);
    } else {
      const res = await fetch("/api/local/preferences", {
        method: "PUT",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(payload)
      });
      status = await res.json();
    }
    saveNotice = "Configuration saved";
    setTimeout(() => (saveNotice = ""), 2500);
  }

  function formatMB(bytes) {
    return (Number(bytes || 0) / (1024 * 1024)).toFixed(1) + " MB";
  }

  onMount(() => {
    const poll = async () => {
      try {
        const s = await callGetStatus();
        status = s;
        if (!authModalChecked) {
          authModalChecked = true;
          if (!s.authenticated && !s.prompted_auth) {
            showAuthModal = true;
          }
        }
        if (!prefsLoaded && s.config) {
          prefs.server_url = s.config.server_url;
          prefs.preferred_region = s.config.preferred_region || "AUTO";
          prefs.pause_on_gaming = s.config.pause_on_gaming;
          prefs.gaming_parse_behavior = s.config.gaming_parse_behavior;
          prefs.idle_policy = s.config.idle_policy;
          prefs.idle_threshold_seconds = s.config.idle_threshold_seconds;
          prefs.watched_processes_str = (s.config.watched_processes || []).join(", ");
          prefs.max_daily_bandwidth_mb = s.config.max_daily_bandwidth_mb;
          prefs.download_speed_limit_kbps = s.config.download_speed_limit_kbps;
          prefsLoaded = true;
        }
      } catch (e) {}
    };
    poll();
    const timer = setInterval(poll, 450);
    return () => clearInterval(timer);
  });
</script>

<div class="h-screen w-screen flex bg-[#0a0d14] text-[#f1f5f9] select-none overflow-hidden">
  <aside class="w-60 h-full bg-[#0d111a] border-r border-[#1e2638] flex flex-col justify-between p-4 shrink-0">
    <div class="space-y-5">
      <nav class="space-y-1">
        <button on:click={() => (activeTab = "miner")} class="w-full text-left px-3 py-2 rounded-md text-xs font-medium {activeTab === 'miner' ? 'bg-[#161f30] text-white border border-[#26334d]' : 'text-[#94a3b8] hover:text-white'}">
          Node Monitor
        </button>
        <button on:click={() => (activeTab = "settings")} class="w-full text-left px-3 py-2 rounded-md text-xs font-medium {activeTab === 'settings' ? 'bg-[#161f30] text-white border border-[#26334d]' : 'text-[#94a3b8] hover:text-white'}">
          Configuration
        </button>
      </nav>

      <div class="bg-[#111622] border border-[#1e2638] rounded-lg p-3.5 space-y-3">
        <div class="flex items-center justify-between gap-2">
          <div class="min-w-0">
            <div class="text-[10px] uppercase tracking-wider text-[#64748b]">Account</div>
            <div class="text-xs font-semibold text-white truncate mt-0.5">
              {status.authenticated ? status.account_nickname : "Anonymous"}
            </div>
          </div>
          <span class="text-[10px] font-medium px-1.5 py-0.5 rounded border shrink-0 {status.authenticated ? 'bg-emerald-500/10 text-emerald-300 border-emerald-500/30' : 'bg-[#161f30] text-[#94a3b8] border-[#26334d]'}">
            {status.authenticated ? "Synced" : "Local"}
          </span>
        </div>

        <div class="pt-2.5 border-t border-[#1e2638] space-y-2">
          <div class="flex items-center justify-between">
            <div class="flex items-center gap-2">
              <span class="font-mono text-[11px] font-semibold px-1.5 py-0.5 rounded bg-[#161f30] text-blue-400 border border-[#26334d]">{status.contributor_rank_icon}</span>
              <span class="text-xs font-semibold text-white">{status.contributor_rank}</span>
            </div>
            <span class="font-mono text-[11px] text-[#94a3b8]">Level {status.contributor_level}</span>
          </div>
          <div class="w-full h-1.5 bg-[#090c12] rounded-full overflow-hidden">
            <div class="h-full bg-blue-500 transition-all duration-300" style="width: {Math.min(100, Math.max(4, status.rank_progress_percent))}%"></div>
          </div>
          <div class="flex items-center justify-between text-[11px] text-[#64748b] font-mono">
            <span>{status.contributor_xp} / {status.next_rank_xp} XP</span>
            <span>{status.streak_days}d streak</span>
          </div>
        </div>

        <div class="flex gap-1.5 pt-0.5">
          {#if status.authenticated}
            <button on:click={logoutAccount} class="flex-1 py-1.5 px-2.5 rounded bg-[#161f30] hover:bg-[#1e293b] text-[#cbd5e1] border border-[#26334d] text-[11px] font-medium transition">
              Sign Out
            </button>
          {:else}
            <button on:click={() => { authError = ""; authMode = "register"; showAuthModal = true; }} class="flex-1 py-1.5 px-2.5 rounded bg-[#161f30] hover:bg-[#1e293b] text-blue-300 border border-[#26334d] text-[11px] font-medium transition">
              Sign In / Register
            </button>
          {/if}
          <button on:click={() => (activeTab = "ranks")} class="py-1.5 px-2.5 rounded bg-[#090c12] hover:bg-[#161f30] text-[#94a3b8] hover:text-white border border-[#1e2638] text-[11px] font-medium transition">
            Tiers
          </button>
        </div>
      </div>
    </div>

    <div class="space-y-2 pt-4 border-t border-[#1e2638]">
      <button on:click={togglePause} class="w-full py-2 px-3 rounded-md font-medium text-xs transition {status.user_paused ? 'bg-emerald-600 hover:bg-emerald-500 text-white' : 'bg-[#161f30] hover:bg-[#1e293b] text-[#e2e8f0] border border-[#26334d]'}">
        {status.user_paused ? "Resume Worker" : "Pause Worker"}
      </button>
    </div>
  </aside>

  <main class="flex-1 h-full flex flex-col overflow-hidden">
    <header class="h-14 border-b border-[#1e2638] bg-[#0d111a] px-6 flex items-center justify-between shrink-0">
      <div class="flex items-center gap-3">
        <span class="inline-flex items-center gap-2 px-2.5 py-1 rounded text-xs font-medium font-mono border {status.state === 'SERVER_DISCONNECTED' || (!status.connected && !status.user_paused && !(status.game_running && prefs.pause_on_gaming)) ? 'bg-rose-500/10 text-rose-400 border-rose-500/25' : (status.state === 'ACTIVE' || status.state === 'ACTIVE_TURBO_IDLE' || status.state === 'READY' || status.state === 'GAMING_ECO_SLOWDOWN' ? 'bg-emerald-500/10 text-emerald-400 border-emerald-500/25' : 'bg-amber-500/10 text-amber-400 border-amber-500/25')}">
          <span>{status.state_label}</span>
        </span>
        {#if status.active_alert_banner}
          <span class="text-xs font-medium text-amber-300 bg-amber-500/10 border border-amber-500/25 px-2.5 py-1 rounded">{status.active_alert_banner}</span>
        {/if}
      </div>
      <div class="flex items-center gap-5 text-xs text-[#94a3b8]">
        <div>Region: <span class="text-white font-medium font-mono">{status.target_region === "AUTO" && status.region_priority && status.region_priority.length ? `AUTO (${status.region_priority[0]})` : status.target_region}</span></div>
        <div>Idle: <span class="text-white font-medium font-mono">{status.user_idle_seconds}s</span></div>
      </div>
    </header>

    <div class="flex-1 overflow-y-auto p-6">
      {#if activeTab === "miner"}
        <div class="grid grid-cols-2 gap-4 mb-5">
          <div class="bg-[#111622] border border-[#1e2638] rounded-lg p-4">
            <div class="text-xs text-[#94a3b8]">Parser Throughput</div>
            <div class="mt-1.5 flex items-baseline gap-1.5">
              <span class="text-2xl font-semibold text-white font-mono">{status.ticks_per_second.toLocaleString()}</span>
              <span class="text-xs text-[#64748b] font-mono">ticks/s</span>
            </div>
          </div>
          <div class="bg-[#111622] border border-[#1e2638] rounded-lg p-4">
            <div class="text-xs text-[#94a3b8]">Parsed Today</div>
            <div class="mt-1.5 flex items-baseline justify-between">
              <span class="text-2xl font-semibold text-white font-mono">{status.your_parsed_today.toLocaleString()}</span>
              <span class="text-xs text-[#64748b] font-mono">Total: {status.total_parsed_all_time.toLocaleString()}</span>
            </div>
          </div>
        </div>
      {:else if activeTab === "ranks"}
        <div class="space-y-4">
          <button on:click={() => (activeTab = "miner")} class="px-3 py-1.5 rounded bg-[#161f30] text-[#e2e8f0] border border-[#26334d] text-xs font-medium">
            Back to Monitor
          </button>
          <div class="grid grid-cols-4 gap-3">
            {#each RANK_TIERS as r}
              <div class="p-3.5 rounded-md border {status.contributor_xp >= r.minXP ? 'bg-[#161f30] border-blue-500/40 text-white' : 'bg-[#090c12] border-[#1e2638] text-[#64748b]'}">
                <div class="flex items-center justify-between">
                  <span class="font-mono text-xs font-semibold">Tier {r.tier}</span>
                  <span class="text-[11px] font-mono">{r.minXP} XP</span>
                </div>
                <div class="text-sm font-semibold mt-1.5">{r.name}</div>
              </div>
            {/each}
          </div>
        </div>
      {:else}
        <div class="bg-[#111622] border border-[#1e2638] rounded-lg p-5 space-y-4 max-w-2xl text-xs">
          <div class="grid grid-cols-3 gap-4">
            <div class="col-span-2">
              <label for="serverUrlInput" class="block text-[#94a3b8] mb-1">Central Server URL</label>
              <input id="serverUrlInput" bind:value={prefs.server_url} class="w-full bg-[#090c12] border border-[#1e2638] rounded px-3 py-1.5 text-white font-mono" />
            </div>
            <div>
              <label for="prefRegionSelect" class="block text-[#94a3b8] mb-1">Preferred Region</label>
              <select id="prefRegionSelect" bind:value={prefs.preferred_region} class="w-full bg-[#090c12] border border-[#1e2638] rounded px-3 py-1.5 text-white">
                <option value="AUTO">Auto (Closest by Latency)</option>
                <option value="EU">EU</option>
                <option value="NA">NA</option>
                <option value="SEA">SEA</option>
                <option value="SA">SA</option>
                <option value="CN">CN</option>
                <option value="OCE">OCE</option>
              </select>
            </div>
          </div>
          <div class="p-3 rounded border border-[#1e2638] bg-[#090c12] space-y-2">
            <div class="flex items-center justify-between">
              <span class="font-medium text-white">Regional Relay Latencies</span>
              <button on:click={reprobeRegions} disabled={status.probing_regions} class="px-2.5 py-1 rounded bg-[#161f30] hover:bg-[#1e293b] text-blue-300 border border-[#26334d] font-medium">
                {status.probing_regions ? "Probing..." : "Re-probe Latency"}
              </button>
            </div>
            <div class="grid grid-cols-3 gap-2 font-mono text-[11px]">
              {#each (status.region_latencies || []) as item, idx}
                <div class="px-2 py-1 rounded border border-[#1e2638] bg-[#0d111a] flex items-center justify-between">
                  <span>{idx + 1}. {item.region}</span>
                  <span>{item.reachable ? `${item.latency_ms}ms` : "timeout"}</span>
                </div>
              {/each}
            </div>
          </div>
          <button on:click={savePreferences} class="px-4 py-2 rounded-md bg-blue-600 hover:bg-blue-500 text-white font-medium">
            Save Configuration
          </button>
          {#if saveNotice}
            <span class="text-emerald-400 ml-3">{saveNotice}</span>
          {/if}
        </div>
      {/if}
    </div>
  </main>

  {#if showAuthModal}
    <div class="fixed inset-0 z-50 bg-black/70 flex items-center justify-center p-4">
      <div class="w-full max-w-md bg-[#111622] border border-[#1e2638] rounded-lg p-6 space-y-4">
        <div>
          <h2 class="text-base font-semibold text-white">
            {authMode === "register" ? "Create Contributor Account" : "Sign In to Account"}
          </h2>
          <p class="text-xs text-[#94a3b8] mt-1">
            Accounts track your XP and contributor rank across workers and sessions. You can also run anonymously with local-only XP.
          </p>
        </div>
        <div class="flex gap-2 bg-[#090c12] p-1 rounded border border-[#1e2638] text-xs">
          <button on:click={() => { authMode = "register"; authError = ""; }} class="flex-1 py-1.5 rounded {authMode === 'register' ? 'bg-[#161f30] text-white font-medium' : 'text-[#94a3b8]'}">Create Account</button>
          <button on:click={() => { authMode = "login"; authError = ""; }} class="flex-1 py-1.5 rounded {authMode === 'login' ? 'bg-[#161f30] text-white font-medium' : 'text-[#94a3b8]'}">Sign In</button>
        </div>
        <div class="space-y-3 text-xs">
          <div>
            <label class="block text-[#94a3b8] mb-1">Nickname</label>
            <input bind:value={authNickname} type="text" placeholder="3-24 chars (letters, digits, _, -)" class="w-full bg-[#090c12] border border-[#1e2638] rounded px-3 py-2 text-white" />
          </div>
          <div>
            <label class="block text-[#94a3b8] mb-1">Password</label>
            <input bind:value={authPassword} type="password" placeholder="At least 6 characters" class="w-full bg-[#090c12] border border-[#1e2638] rounded px-3 py-2 text-white" />
          </div>
        </div>
        {#if authError}
          <div class="text-xs text-red-300 bg-red-500/10 border border-red-500/30 rounded px-3 py-2">{authError}</div>
        {/if}
        <div class="flex items-center justify-between gap-2 pt-1">
          <button on:click={dismissAuthPrompt} class="px-3 py-2 rounded-md bg-[#161f30] hover:bg-[#1e293b] text-[#94a3b8] border border-[#26334d] text-xs font-medium">
            Use Anonymously
          </button>
          <button on:click={submitAuthForm} disabled={authBusy} class="px-4 py-2 rounded-md bg-blue-600 hover:bg-blue-500 text-white text-xs font-medium">
            {authBusy ? "Please wait..." : (authMode === "register" ? "Create Account" : "Sign In")}
          </button>
        </div>
      </div>
    </div>
  {/if}
</div>
