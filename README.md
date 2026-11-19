# Dota2ReplayCourier

Volunteers run a desktop worker that downloads and parses Dota 2 replays. A central server finds matches, hands them out, and collects the parsed JSON. Neither side needs Docker or an external database.

The repository has two Go programs:

- `central-server/` finds matches, filters them, queues them by region, and stores or forwards the results. It also serves the admin page at `/admin`.
- `desktop-worker/` is a Wails desktop app for Windows, macOS, and Linux. It leases replays from the server, downloads each `.dem.bz2` into memory, parses it, and uploads the result.

Both modules target Go 1.26.

## How it fits together

1. The server discovers recent matches through the Steam Web API. Without a Steam key it falls back to the OpenDota `publicMatches` endpoint.
2. Matches that pass your filters (lobby type, game mode, region, human players, duration) go into regional queues: `EU`, `NA`, `SEA`, `SA`, `CN`, `OCE`. The queues live in an embedded Redis, so there is nothing to install.
3. A worker leases a batch of jobs. It resolves each replay salt itself, trying STRATZ first and OpenDota second. If the worker runs out of quota, the server resolves salts for it.
4. The worker downloads the replay into RAM, decompresses it, runs the active parser, and submits gzip-compressed JSON.
5. The server stores the match (optionally as a file), pushes it to subscribers, and credits XP to the worker's account.

## Run the central server

```powershell
cd central-server
go build -o central-server.exe ./cmd/server
.\central-server.exe
```

Open `http://localhost:8080/admin`. The first visit starts a short tutorial that walks through each section. The `? Tutorial` button in the top bar replays it.

Run the server from the same directory every time. It keeps its state in `./data`, relative to the working directory, so starting it from a different folder creates a second, empty `data` directory.

### Configure it in the admin page

Before signing in at `/admin` for the first time, set the admin password in the server terminal:

```text
server> password <your-password>
```

Passwords are stored as `bcrypt` hashes in `data/admin_auth.json` and cannot be created or changed from the browser. Failed login attempts are rate-limited per IP (five wrong attempts in a minute lock out logins for 60 seconds). The harvester always starts off on launch, even if you saved filters before restarting.

- **API Keys.** The Steam Web API key controls match discovery. STRATZ (priority 1) and OpenDota (priority 2) keys are used for replay salts. Keys apply immediately and are saved to `data/api_keys.json`. After saving, `/admin` shows only masked previews (`••••••••1234`), never the full key. Leave an input blank to keep the stored key, or type `clear` to remove it. There is no `.env` file support.
- **Match Ingestion Filters.** Choose lobby types, game modes, regions, whether to require ten human players, and duration bounds. Rules are saved to `data/filter_config.json`; the harvester on/off switch is not persisted.
- **Active Replay Parser.** Switch between `default` (built-in Manta parser), `raw` (omit parsing and have workers upload the raw downloaded `.dem.bz2`), or custom parsers you upload. See [Custom parsers](#custom-parsers).
- **Integrations and Storage.** Set an outbound webhook and turn local file saving on or off. Saved to `data/sink_config.json`.

### Getting parsed matches out

There are three ways to read results, and they work whether or not file saving is on.

| Method | Details |
| :--- | :--- |
| Webhook | The server sends a `POST` with the full match record to the URL you set in `/admin` or in `EXTERNAL_WEBHOOK_URL`. Each request carries an `X-Match-ID` header and is retried up to three times. |
| Live stream | `GET /api/v1/stream/matches` is a Server-Sent Events feed of every newly parsed match. |
| REST | `GET /api/v1/matches/<match_id>` returns one match. `GET /api/v1/matches/<match_id>.dem.bz2` (or `?raw=1`) downloads the raw replay when `raw` mode was used. `GET /api/v1/matches?limit=50` lists recent ones. |

With file saving on, each match is written to `data/matches/<match_id>.json`, and `raw` submissions also write `data/matches/<match_id>.dem.bz2`. With file saving off, nothing is written to disk. In that case the REST endpoints only see the 200 most recent match records and the 8 most recent raw replays in memory, which are lost on restart. If you want every match in your own database, use the webhook or the stream.

A match record contains `match_id`, `cluster`, `region`, `parser_name`, `parser_version`, `worker_id`, `payload`, and `created_at`.

### Data directory

| Path | Contents |
| :--- | :--- |
| `data/accounts.db` | SQLite database of worker accounts and XP |
| `data/admin_auth.json` | `bcrypt` hash of the `/admin` console password |
| `data/admin_ui_state.json` | Whether the first-run tutorial has been completed |
| `data/api_keys.json` | Steam, STRATZ, and OpenDota keys saved from `/admin` or the CLI |
| `data/filter_config.json` | Saved match filters (harvester enabled state always resets to off) |
| `data/sink_config.json` | Webhook URL, webhook toggle, and file saving toggle |
| `data/matches/` | Parsed `.json` records (and `.dem.bz2` files in `raw` mode) when file saving is on |
| `data/parsers/` | Uploaded parsers and their build artifacts |
| `data/runtimes/` | Cached runtimes, such as the Python WASI module |

### Environment variables

All are optional. Settings saved to `data/` take precedence over environment variables on startup.

| Variable | Default | Purpose |
| :--- | :--- | :--- |
| `PORT` | `8080` | HTTP port for the worker API and `/admin` |
| `ADMIN_SECRET` | empty | Fallback bearer token or `X-Admin-Secret` header for `/api/v1/admin/*` |
| `STORAGE_DIR` | `data/matches` | Where match files are written |
| `ACCOUNTS_DB_PATH` | `data/accounts.db` | SQLite accounts database |
| `PARSERS_DIR` | `data/parsers` | Uploaded parser storage |
| `EXTERNAL_WEBHOOK_URL` | empty | Initial webhook target |
| `STEAM_API_KEY` | empty | Steam Web API key |
| `STRATZ_API_TOKEN` | empty | STRATZ bearer token |
| `OPENDOTA_API_KEY` | empty | OpenDota API key |
| `REDIS_ADDR`, `REDIS_PASSWORD` | empty | External Redis. The embedded one is used when unset or unreachable. |

## Run the desktop worker

```powershell
cd desktop-worker
go build -tags "desktop,production" -ldflags "-w -s -H windowsgui" -o dota-analytics-worker.exe .
.\dota-analytics-worker.exe
```

On macOS and Linux, run `go build` with the same tags and drop the `-H windowsgui` linker flag, which is Windows-only.

The worker starts with no server configured. Open the Configuration tab, enter your server URL, and click Test Connection. Creating an account or signing in needs a connected server, so the account dialog has its own server field and Connect button.

### Configuration

- Preferred region, either automatic (lowest latency) or fixed.
- STRATZ and OpenDota API keys for replay salt resolution.
- Daily bandwidth cap (MB) and download speed limit (KB/s); `0` or `unlimited` disables either cap.
- System idle policy and the PC inactivity time before the worker speeds up.
- Gamer Guard, described below.

If the worker loses its connection to the coordinator, it cancels any in-flight batch immediately until the server is reachable again. The worker stores its settings, session token, and counters in `%AppData%\Dota2ReplayCourier\config.json` on Windows, and in the equivalent user config directory on other systems.

### Gamer Guard

The worker watches for the games you list, which default to `dota2`, `cs2`, and `valorant`. Type the process name without `.exe`.

- If a watched game starts during a download, the worker finishes the current file, sends a desktop notification, and stops starting new downloads.
- If a watched game starts during a parse, the worker either holds the compressed replay in RAM with no CPU use, or lowers its process priority to idle. You pick which in Configuration.

Turning Gamer Guard off in Configuration disables these checks.

## Custom parsers

The built-in parser is based on Manta. To replace it for every worker, upload a parser in `/admin` under Active Replay Parser. Workers pick up the active parser on their next heartbeat. Results parsed with an older parser are dismissed and re-queued.

A parser reads the decompressed `.dem` bytes and writes JSON. How it receives them depends on the language:

| Extension | How it runs | Input |
| :--- | :--- | :--- |
| `.go` | Compiled to WASI on the server, run in a `wazero` sandbox | `stdin` |
| `.lua` | Bundled with `gopher-lua`, compiled to WASI, run in `wazero` | Globals `stdin_len()`, `stdin_slice(start, len)`, `read_stdin()` |
| `.py` | Bundled with a CPython WASI runtime, run in `wazero` | `sys.stdin.buffer` |
| `.wasm` | Run as uploaded in `wazero` | `stdin` |
| `.js` | Run in a `goja` VM | Globals `stdin_len()`, `stdin_slice(start, len)`, `read_stdin()`, `stdin_bytes()` |

Every parser also gets `DOTA_MATCH_ID`, `DOTA_CLUSTER`, and `DOTA_REGION`, as environment variables for WASI parsers and as globals for Lua and JavaScript. Output should be a JSON document on `stdout`. For JavaScript, `console.log` or the final expression works. Output that is not valid JSON is wrapped as `{"match_id": ..., "raw_output": "..."}`.

The `.go` and `.lua` uploads need the Go toolchain installed on the server, because the server compiles them. The `.py` upload downloads a Python WASI runtime the first time and caches it under `data/runtimes/`. Set `PYTHON_WASM_PATH` to use your own copy instead.

### Sandbox limits

- No network access.
- No access to the host filesystem. A Python parser sees only an in-memory read-only copy of its own script.
- Lua exposes only the base, table, string, and math libraries, with `dofile` and `loadfile` removed.
- JavaScript has no `require`, `process`, filesystem, or network APIs.
- A 120-second execution limit, a 512 MiB memory ceiling for WASI modules, and a 50 MiB cap on `stdout`.

### Embedding a parser in the worker

If you build the worker yourself, you can skip uploads and attach a Go parser directly. Implement [`parser.BlackBoxParser`](desktop-worker/internal/parser/interface.go) and register it:

```go
orch.SetCustomBlackBoxParser(myParser)
```

## Headless CLI mode

Both binaries support terminal operation without the browser or desktop window. Both interactive prompts support `↑`/`↓` command history (`history`, `!!`, `!<n>`), `Tab` completion, unambiguous prefix matching, typo suggestions, bare inspect-by-default commands, and a 1-line `watch` ticker (`w`). Live logs redraw the prompt automatically and can be controlled with `logs <on|off|errors|N>` (`l`).

### Central server CLI

Passing `--headless` (or any configuration flag) disables `/admin` and `/api/v1/admin/*`. Startup flags apply in memory for that run only. Interactive commands typed on `stdin` save changes to `data/` (except `harvester on|off`, which is always runtime-only). Type `save` to persist session flags to disk.

```powershell
.\central-server.exe --headless --harvester --regions=EU,NA --min-duration=1200 --file-storage=true
```

Startup flags: `--headless`, `--port`, `--harvester`, `--steam-key`, `--stratz-key`, `--opendota-key`, `--regions`, `--lobby-types`, `--game-modes`, `--min-duration`, `--max-duration`, `--require-10-humans`, `--file-storage`, `--webhook-url`, `--webhook`, `--parser`, `--admin-password`.

Interactive `stdin` commands (aliases `s`, `h`, `i`, `w`, `l`, `q`): `help`, `status`, `harvester [on|off|toggle]`, `keys [show|<steam|stratz|opendota> <key|clear>]`, `filter [show|regions|lobbies|modes|min-duration|max-duration|humans]`, `file-storage [on|off|toggle]`, `webhook [url|on|off|clear]`, `parser [list|activate <default|raw|id>|upload|delete]`, `inject [count|match_id] [cluster]`, `password [new_password]`, `watch`, `logs [on|off|errors|N]`, `history`, `save`, `quit`.

### Worker CLI

Build the standalone pure-Go CLI binary (no Wails or WebView needed; cross-compiles to Linux and macOS cleanly) or pass `--headless` / flags to `dota-analytics-worker`:

```powershell
cd desktop-worker
go build -o dota-worker-cli.exe ./cmd/worker
.\dota-worker-cli.exe --server=http://localhost:8080 --region=EU
```

Startup flags apply in memory for that session (`config.json` keeps its saved settings while still saving match counters/XP). Headless mode disables the local UI HTTP server on port `34115`. When started interactively with no server configured, a first-run prompt lets you enter the Coordinator URL and optionally sign in or register (pass `--no-wizard` to skip).

Startup flags: `--headless`, `--server`, `--region`, `--stratz-key`, `--opendota-key`, `--pause-on-gaming`, `--gaming-behavior`, `--idle-policy`, `--idle-threshold`, `--bandwidth-limit`, `--speed-limit`, `--paused`, `--no-wizard`.

Interactive `stdin` commands (aliases `s`, `p`, `r`, `w`, `l`, `q`): `help`, `status`, `pause`, `resume`, `server [url|clear]`, `region [REGION]`, `regions`, `keys [show|stratz|opendota]`, `guard [on|off] [HOLD_IN_RAM|ECO_SLOWDOWN]`, `idle [POLICY] [sec]`, `limits [daily_mb|unlimited] [speed_kbps|unlimited]` (`0=unlimited`), `login <nick> [pass]`, `register <nick> [RADIANT|DIRE]`, `logout`, `watch`, `logs [on|off|errors|N]`, `setup`, `history`, `save`, `quit`. Omitting the password on `login`/`register` prompts for it with masked input. Closing `stdin` exits only the command reader so background daemons stay running until interrupted.

## Tests

```powershell
cd central-server
go test ./...

cd ../desktop-worker
go test ./...
```

The parser tests compile small Go programs to WASI, so they need the Go toolchain on your `PATH`.
