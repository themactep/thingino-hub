# Thingino Hub

`thinginohub` is a small Telegram-to-MQTT bridge for Thingino cameras.

It solves the Telegram limitation where only one process can poll `getUpdates` for a given bot token. Instead of running one Telegram bot daemon on every camera, you run one hub and let it fan out commands to cameras over MQTT.

For snapshots, the hub can fetch the camera image directly from a registered `snapshot_url`. That URL can be an ONVIF-exposed snapshot URI or any direct image endpoint on the camera.

The hub can also talk to the new Thingino-native camera API. In the current
implementation, that API is typically exposed under `/api/v1`.

## What It Does

- Long-polls Telegram using one shared bot token
- Accepts Telegram commands and routes them to a target camera over MQTT
- Tracks the originating Telegram chat for each request
- Listens for MQTT replies from cameras and forwards them back to Telegram
- Exposes a small web UI for editing hub configuration and previewing camera snapshots

## Command Format

The hub currently supports these Telegram commands:

- `/help`
- `/cam list`
- `/cam <camera_id> <command> [args...]`
- `/cam <camera_name> <command> [args...]`

Examples:

```text
/cam list
/cam aabbccddeeff snap
/cam front-door arm
/cam garage clip 10
```

## Quick Start

### 1. Create a Telegram Bot

Use `@BotFather` in Telegram:

1. Run `/newbot`
2. Pick a display name and username
3. Copy the bot token

### 2. Configure the Hub

Copy the example config if needed:

```sh
cp config.example.yaml config.yaml
```

Edit `config.yaml` and set:

- `telegram.token`
- `mqtt.host`
- `mqtt.username` and `mqtt.password` if your broker requires them
- optional static `cameras` aliases if you want friendly names before auto-registration happens

If the broker runs on the same machine as the hub and the hub is started with Podman, prefer:

```yaml
mqtt:
  host: "host.containers.internal"
```

Using the host's LAN IP from a rootless Podman container can fail even when the broker is reachable from the host itself.

Example:

```yaml
telegram:
  token: "123456789:replace-me"
  api_url: "https://api.telegram.org"
  polling_timeout: 30
  allowed_chat_ids: []
  allowed_usernames: []

mqtt:
  host: "192.168.1.10"
  port: 1883
  username: ""
  password: ""
  keepalive: 60
  use_tls: false

routing:
  command_topic: "thingino/cam/{camera_id}/cmd"
  reply_topic: "thingino/cam/+/reply"
  registration_topic: "thingino/cam/+/hello"

ui:
  username: ""
  password: ""
  registration_stale_after_seconds: 0
  snapshot_heartbeat_interval_seconds: 60
  snapshot_heartbeat_timeout_seconds: 5
  snapshot_cache_stale_after_seconds: 3600

history:
  enabled: true
  path: ""
  recent_actions_limit: 20

defaults:
  onvif_username: "thingino"
  onvif_password: "thingino"

cameras:
  - id: "aabbccddeeff"
    name: "front-door"
    api_base_url: "http://192.168.1.50/api/v1"
    api_token: ""
    snapshot_url: "http://192.168.1.50/x/ch0.jpg"
    api_key: ""
    onvif_endpoint: "http://192.168.1.50/onvif/device_service"
    onvif_username: "thingino"
    onvif_password: "thingino"
  - id: "112233445566"
    name: "garage"
    onvif_username: "thingino"
    onvif_password: "thingino"
```

The `cameras` section is optional. If cameras publish retained registration messages on MQTT, the hub can discover them automatically.

If a camera exposes the new Thingino-native API, you can also configure:

- `api_base_url`
- `api_token`

When `api_base_url` is set, the hub probes `GET /device`, `GET /capabilities`,
and `GET /state`, and it tries the native snapshot action before falling back to
the older `snapshot_url`.

## Running With Podman

### Build the image

```sh
podman build -t telegrambothub -f Containerfile .
```

### Run the container

```sh
sh run-podman.sh
```

That launcher:

- rebuilds `localhost/telegrambothub:latest` by default before running
- mounts `config.yaml` at `/config/config.yaml` with write access so the web UI can save changes
- mounts `./data` at `/data` so the discovered camera registry survives container restarts
- publishes the web UI on `http://127.0.0.1:8080`
- runs the hub in the foreground

If you want to skip the rebuild and use the existing local image:

```sh
SKIP_BUILD=1 sh run-podman.sh
```

You can change the UI bind address with:

```sh
UI_BIND=0.0.0.0:8080:8080 sh run-podman.sh
```

### Run with Podman Compose

```sh
podman compose up --build
```

Then open:

```text
http://127.0.0.1:8080
```

## Web UI

The hub now includes a small server-rendered admin UI.

Pages:

- `/` dashboard with the camera roster, preview snapshots, and fleet actions
- `/status` dedicated status page for Telegram, MQTT, polling, registration, history, and related runtime cards
- `/events` dedicated live event feed page for hub actions, registrations, and camera-agent events
- `/enroll` enrollment page with a primary credentials-first connect flow plus advanced probe and pairing helpers
- `/camera/<camera_id>` individual camera page with metadata, ONVIF device details, snapshot preview, rescan, connect/pair/delete actions, OTA rebuild command, and local override fields such as display name
- `/camera/<camera_id>/history` database-backed timeline of native actions and stored probe/state samples
- `/config` editor for Telegram, MQTT, routing, and static camera overrides
- `/snapshot/<camera_id>` preview route that serves the latest cached snapshot or a fallback image

Notes:

- the UI writes back to `config.yaml`
- the runtime camera registry is stored separately in `data/camera-state.yaml` by default when using the provided container launchers
- `Save and Reload` applies the new config to the running process
- `Rescan Cameras` asks known cameras to publish fresh registration metadata over MQTT
- roster cards no longer carry their own action buttons; use the preview or camera name to open the detail page
- `Delete` lives on the camera detail page and removes saved overrides, asks the camera to revoke its own registration, drops the camera from the current roster, and tries to clear its retained MQTT registration topic
- auto-registered cameras remain runtime-discovered; camera pages can save per-camera overrides such as name, IP, snapshot URL, API key, native API base URL, native API token, ONVIF endpoint, and ONVIF credentials
- by default the UI is bound to localhost through Podman port mapping
- `ui.registration_stale_after_seconds` controls how long a retained `online` registration stays fresh before the dashboard shows that camera as `offline`; the default is `0` because many camera setups only publish registration on boot, not as a heartbeat
- `ui.snapshot_heartbeat_interval_seconds` controls background snapshot refresh for live status and local preview caching; the default is `60` seconds and keeps cameras in the roster while updating the online badge from a fresh snapshot check
- `ui.snapshot_heartbeat_timeout_seconds` controls how long each snapshot probe waits before marking a camera offline
- `ui.snapshot_cache_stale_after_seconds` controls how long an old cached preview can still be shown after a camera goes offline; recent stale snapshots are tinted, older ones fall back to the placeholder image
- `history.enabled` enables a local SQLite-backed history store for native actions and coarse state samples
- `history.path` overrides the SQLite database path; when empty, the hub uses a default database next to `config.yaml`
- `history.recent_actions_limit` controls how many recent database-backed native actions are shown on each camera detail page
- the live event feed merges hub actions, MQTT registrations, and native camera-agent `/events` activity into one stream on the dedicated `/events` page
- bulk actions currently support queued API/ONVIF/snapshot refreshes, MQTT rescan requests, and first-pass streaming service start/stop/restart operations across selected cameras
- the primary enrollment path is `Connect Camera`: provide the camera IP and valid ONVIF credentials, and the hub resolves the discovered camera identity, installs the pairing bootstrap over MQTT, stores the generated bearer token, and hydrates the camera state in one step
- the enrollment page still exposes advanced helpers for probe, pairing repair, and showing the generated pairing details when you need to inspect the bootstrap payload
- camera detail pages include a `Connect to Hub` form that reuses the discovered roster entry and only asks for credentials, a `Pair` button for direct MQTT bootstrap repair, and a copyable OTA rebuild command in the form `CAMERA=<camera_image_id> IP=<camera_ip> make cleanbuild upgrade_ota`
- partial override saves preserve existing auth and token values, so editing a display name or another single field no longer clears unrelated credentials
- ONVIF device info refreshes on startup, on registration refresh, and on demand from the camera detail page; by default the hub tries `http://<camera-ip>/onvif/device_service` when no explicit endpoint is configured
- camera detail pages render from cached supported-controls data first, then hydrate native API and ONVIF details in the background so the page stays responsive while fresher state arrives
- quick controls and camera-page refresh buttons use narrow JSON responses or queued acknowledgements rather than full camera payloads where possible
- camera-page action feedback is shown as a floating toast instead of shifting the page layout

If camera-side revoke succeeds, the camera stays unregistered until it is explicitly registered again. If the revoke command does not reach the camera, it can still reappear later after publishing a fresh registration heartbeat.

### Camera State Persistence

Known cameras and their last saved metadata are persisted outside `config.yaml`.

- default path outside containers: next to `config.yaml` as `camera-state.yaml`
- default path in the provided Podman and compose flows: `data/camera-state.yaml`
- override with `HUB_STATE_PATH` if you want the registry elsewhere

This keeps the editable hub config separate from the discovered camera registry while allowing auto-registered cameras to survive restarts.

### History Database

The hub can also keep a local SQLite history database for native API activity.

- default path outside containers: next to `config.yaml` as `hub-history.sqlite3`
- override with `history.path` in YAML or `HUB_HISTORY_DB` in the environment
- current first-pass contents: native action events and coarse probe/state samples

This database is used for timelines and later analysis. It is not a source of
truth for live camera control.

## Testing

The hub now has a small unittest-based regression module for the optimized camera-page routes.

Run it with:

```sh
python -m unittest -v tests.test_web_routes
```

Current coverage focuses on route behavior that should stay small and non-blocking:

- dashboard rendering, event feed snapshots, bulk-action summaries, and enrollment responses
- camera detail hydration payloads
- full cached camera payload fetches
- minimal quick-action deltas for day/night and privacy
- queued refresh acknowledgements without full camera blobs

### Web UI Authentication

The web UI supports optional browser-session login.

You can store the credentials directly in `config.yaml`:

```yaml
ui:
  username: admin
  password: change-me
```

This matches the rest of the hub configuration model and keeps all hub secrets in one place.

Set both environment variables before starting the container:

```sh
export HUB_UI_USERNAME=admin
export HUB_UI_PASSWORD=change-me
sh run-podman.sh
```

Notes:

- both username and password must be set, otherwise UI auth stays disabled
- `HUB_UI_USERNAME` and `HUB_UI_PASSWORD` override the YAML values when present
- when enabled, the UI redirects unauthenticated browser requests to a sign-in page and keeps the session until the browser session is closed or the user signs out
- valid `Authorization` headers are still accepted and upgraded into a browser session
- snapshot preview routes are protected too

### ONVIF Device Details

The hub can query each camera's ONVIF device service for basic device identity fields.

Currently it stores and displays:

- manufacturer
- model
- firmware version
- serial number
- hardware ID

Per-camera ONVIF settings can be saved either in the camera detail page or in the `cameras` YAML list with these keys:

- `onvif_endpoint`
- `onvif_username`
- `onvif_password`

The native API fields can also be saved per camera:

- `api_base_url`
- `api_token`

If `onvif_endpoint` is omitted, the hub derives `http://<camera-ip>/onvif/device_service` when the camera IP is known.
If ONVIF credentials are omitted, the hub falls back to Thingino's default `thingino:thingino` credentials.

## Camera-Side Setup

Each camera needs an MQTT subscription that hands incoming payloads to the local Telegram camera agent. This subscription is configured in the camera firmware and does not require any manual setup from the hub.

On Thingino cameras with the firmware-side agent installed, the required subscription is pre-configured at build time:

```text
Topic:  thingino/cam/%id/cmd
Action: telegram-cam-agent "$MQTT_PAYLOAD"
```

The `%id` shorthand resolves to the camera SoC serial number first, with MAC-based fallback, which matches the `camera_id` used by the hub. The hub communicates with cameras exclusively via MQTT commands, ONVIF, and the native camera agent API — it does not access any camera-side web UI or CGI endpoints.

### Pairing and Native Agent Bootstrap

When you connect a camera to the hub, the hub sends an `install-agent-bootstrap` command over MQTT. That command delivers:

- a generated bearer token for the native camera agent API
- the hub's MQTT broker host, port, username, and password

The camera-side `telegram-cam-agent` script writes these values into `/etc/thingino-agent-bootstrap.json` and restarts the agent service. On restart the agent merges the bootstrap config into `/etc/thingino.json` using the `jct import` tool, enabling TLS, setting the listen address to `0.0.0.0`, and configuring the MQTT subscription broker so the camera reconnects to the hub's broker automatically after a reboot.

After bootstrap the hub probes the agent on `https://<camera-ip>:1998/api/v1`, stores the bearer token, and marks the camera connected. No manual broker or agent configuration on the camera is required.

### Auto-registration

With the camera-side registration helper enabled, each camera publishes a retained JSON registration to:

```text
thingino/cam/<camera_id>/hello
```

The hub subscribes to `thingino/cam/+/hello` and uses those retained messages to populate `/cam list` without any manual camera entries in `config.yaml`.

For dashboard status, an `online` registration is only treated as stale when `ui.registration_stale_after_seconds` is set above `0`. Enable that only if your cameras publish regular registration heartbeats; otherwise retained boot-time registrations will make healthy cameras appear offline.

When `ui.snapshot_heartbeat_interval_seconds` is above `0`, the hub refreshes each camera's effective snapshot URL in the background on that interval. A successful image fetch marks the camera online and updates the locally cached preview image. If a later probe fails, the most recent cached preview can still be shown with a muted stale treatment until `ui.snapshot_cache_stale_after_seconds` is exceeded, after which the placeholder image is used instead.

If a camera was registered before newer metadata such as `snapshot_url` was added, use the dashboard `Rescan Cameras` button after updating the camera-side agent. The hub sends a `register` command over MQTT and the camera republishes its retained registration payload.

Once a camera is visible in the roster, the preferred next step is to open its detail page or visit `/enroll` and use the connect flow with valid credentials. You do not need to type a camera ID manually.

Example registration payload:

```json
{
  "camera_id": "aabbccddeeff",
  "name": "front-door",
  "hostname": "front-door",

You can combine that with auth:

```sh
export HUB_UI_USERNAME=admin
export HUB_UI_PASSWORD=change-me
UI_BIND=0.0.0.0:8080:8080 sh run-podman.sh
```
  "ip": "192.168.88.128",
  "snapshot_url": "http://192.168.88.128/x/ch0.jpg",
  "status": "online",
  "timestamp": 1710000000
}
```

The hub uses `snapshot_url` for `/cam <id> snap`. If your camera already exposes a stable ONVIF snapshot URI, publish or configure that URL instead of the default snapshot path.

### Media Commands

`snap` is hub-driven:

- the hub fetches the image from `snapshot_url`
- the hub sends the photo to Telegram with the shared hub bot token
- the camera does not need the shared hub bot token for snapshots

If the snapshot endpoint requires authentication, set a per-camera `api_key` in the hub config or use a pre-authenticated ONVIF snapshot URL.

`clip` is still camera-driven:

The camera agent uses `send2telegram` for clip delivery. Configure `send2telegram` on each camera if you want `/cam <id> clip` to work.

The camera agent overrides the destination chat ID per request, so one shared bot can still deliver photos and clips back to the Telegram user who issued the command.

If `send2telegram` is not configured on a camera:

- `ping`, `help`, `arm`, and `disarm` still work
- `snap` still works if `snapshot_url` is reachable by the hub
- `clip` will fail with a reply message explaining the missing setup

## Running Without Containers

If you want to test the service directly on the host:

```sh
python3 -m venv .venv
. .venv/bin/activate
pip install -r requirements.txt
python3 -m app.main
```

The service reads `HUB_CONFIG` if set, otherwise it defaults to `/config/config.yaml` in the container and `config.yaml` can be used by setting:

```sh
HUB_CONFIG=$(pwd)/config.yaml python3 -m app.main
```

## MQTT Protocol

The hub publishes commands to:

```text
thingino/cam/<camera_id>/cmd
```

with a JSON payload like:

```json
{
  "request_id": "9b4a4aa8e4e6402f934e50c33402af90",
  "chat_id": 123456789,
  "username": "alice",
  "camera_id": "aabbccddeeff",
  "command": "snap",
  "args": [],
  "raw_text": "snap",
  "sent_at": 1710000000
}
```

The hub listens for replies on:

```text
thingino/cam/+/reply
```

It also listens for retained registrations on:

```text
thingino/cam/+/hello
```

Preferred reply payload:

```json
{
  "request_id": "9b4a4aa8e4e6402f934e50c33402af90",
  "message": "Snapshot queued"
}
```

The hub also accepts:

- JSON with `chat_id` and `message`
- Plain text payloads, routed using the most recent Telegram chat for that camera

## Camera IDs

The hub uses the camera identity from MQTT registration and command topics:

- registration: `thingino/cam/<camera_id>/hello`
- commands: `thingino/cam/<camera_id>/cmd`

For current Thingino agent builds, that ID is the agent-style `%id` value exposed by `mqtt-sub`, which resolves to the SoC serial style identifier used by the camera agent and the hub roster.

Do not use these values for hub enrollment or pairing identity:

- native API `/api/v1/device.id`
- legacy CGI `camera_id`

The connect and enroll flows resolve the authoritative hub camera ID from the current roster automatically, so the user only needs the camera IP and valid credentials.

## Access Control

You can restrict who may use the bot:

- `telegram.allowed_chat_ids`
- `telegram.allowed_usernames`

If both lists are empty, the hub allows any Telegram user who can message the bot.

## Logging

Set `LOG_LEVEL` when starting the container or process:

```sh
LOG_LEVEL=DEBUG sh run-podman.sh
```

Valid values depend on Python logging levels, for example:

- `DEBUG`
- `INFO`
- `WARNING`
- `ERROR`

## Current Scope

This project is intended to work with a small allowlisted camera-side MQTT agent in Thingino firmware.

That camera-side agent currently supports:

- `ping`
- `help`
- `arm`
- `disarm`
- `snap`
- `clip`

## Troubleshooting

### Bot does not reply

- Verify `telegram.token`
- Make sure no other process is polling the same bot token
- Send `/help` directly to the bot in Telegram

### Commands are acknowledged but cameras do nothing

- Verify the MQTT broker settings
- Confirm the camera is subscribed to `thingino/cam/<camera_id>/cmd`
- Check that the camera-side agent understands the JSON payload format
- If the camera is auto-discovered but not fully connected yet, use `Connect to Hub` on the camera page or `/enroll`; the hub sends the pairing bootstrap command over MQTT and installs the bearer token when credentials are valid

### Replies never come back to Telegram

- Confirm the camera publishes to `thingino/cam/<camera_id>/reply`
- Prefer JSON replies with the original `request_id`
- Check the hub logs for dropped replies without chat mapping

### MQTT works on the host but not inside the hub container

If Mosquitto is running on the same machine as Podman:

- use `host.containers.internal` as `mqtt.host`
- do not assume the host's LAN IP is reachable from a rootless Podman container
