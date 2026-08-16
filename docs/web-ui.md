# Web UI Guide

The Thingino Hub dashboard lets you monitor and control all your cameras from one place.

---

## Main Pages

**Dashboard** (`/`)
- See all connected cameras at a glance
- View latest snapshot from each camera
- Check online/offline status
- Quick access to camera details and actions

**Status** (`/status`)
- Connection status of MQTT broker
- Telegram bot connection status
- System health and runtime info

**Events** (`/events`)
- Log of recent actions (snapshots, commands, errors)
- Useful for troubleshooting and monitoring activity

**Enroll** (`/enroll`)
- Connect new cameras to the hub
- Enter camera IP and ONVIF credentials
- One-click pairing

**Camera Details** (`/camera/<camera_id>`)
- View camera metadata and settings
- See ONVIF information
- Connect/disconnect camera
- Access snapshot and live preview
- Send commands (arm, disarm, snapshot, etc.)

**Camera History** (`/camera/<camera_id>/history`)
- Timeline of actions for this specific camera
- Useful for auditing and debugging

**Configuration** (`/config`)
- Edit hub settings
- Save and reload without restarting

---

## Key Features

### Live Previews

- **Snapshot preview** – Still image refreshed at regular intervals
- **Live preview (Raptor cameras)** – Real-time stream using native WebRTC (no iframe)
- **Controls** – Manually refresh snapshot or view MJPEG stream

### Camera Information

Each camera card shows:
- 📷 Latest snapshot
- 🟢 Online/Offline status
- 📍 IP address
- 🎯 Camera ID
- 🔧 ONVIF details (model, firmware, etc.)

### Quick Actions

- **Snapshot** – Capture and display latest image
- **Clip** – Record a video clip
- **Arm/Disarm** – Control camera recording
- **Refresh** – Update camera metadata
- **Delete** – Remove camera from roster

### Telegram Integration

Send commands via Telegram:
- `/help` – List available commands
- `/cam list` – Show all cameras
- `/cam <name> snap` – Get snapshot for a camera
- `/cam <name> arm` – Arm recording

---

## Configuration Options

You can adjust how the dashboard behaves:

**Preview Settings:**
- How often snapshots refresh
- How long before a camera is marked "offline"
- How long to cache snapshots

**Example configuration:**
```yaml
ui:
  snapshot_heartbeat_interval_seconds: 30
  snapshot_heartbeat_timeout_seconds: 60
  snapshot_cache_stale_after_seconds: 120
  registration_stale_after_seconds: 300
```

---

## UI Authentication

If you enabled dashboard protection, you'll be asked to log in:

```yaml
ui:
  username: "admin"
  password: "your-password"
```

The hub supports:
- **Browser login** – Username/password form
- **API header** – `Authorization: Bearer <token>` for programmatic access

---

## Important Behaviors

### Save and Reload
- Click "Save and Reload" to apply config changes without restarting the container
- All cameras remain connected; connection is transparent

### Rescan Cameras
- Click "Rescan Cameras" to request fresh metadata from all cameras
- Useful if a camera was offline and is now back online

### Camera State Persistence
- Camera roster is stored in `data/camera-state.yaml`
- Persists across restarts
- Manual settings in `config.yaml` override auto-discovered values

### Online/Offline Status
- Determined by MQTT heartbeats from cameras
- "Stale after" setting controls timeout
- Camera marked offline if no heartbeat for X seconds

### Config backups and restore

**What is stored:** each backup is a full native `GET /config` snapshot (plus capabilities when available). That usually includes stream settings and nested OSD objects when the camera agent returns them. Pairing secrets are stripped on restore/clone, not from the stored JSON download.

**What restore/clone actually writes:** the hub peels known fields into native `/settings/...` leaf patches, then sends any remaining groups via omnibus `PATCH /config`.

This is a **split limitation**:

| Layer | Role |
| --- | --- |
| **Raptor / native API (firmware)** | Omnibus `PATCH /config` only applies a subset of config (mainly image/motion/daynight-style groups). It **ignores stream and OSD** (and some image leaves such as anti-flicker). Those must be written through `/settings/...` paths. |
| **Hub** | Peels stream/OSD (and some image leaves) onto `/settings` via a shared writable-settings catalog — resolution, fps, bitrate, format, mode, audio, OSD enable/time/usertext/privacy (including **position**), fonts/colors/logo where present in the backup. Restore, clone, and Settings share that catalog so UI and apply stay aligned. |

So: Raptor forces leaf writes for stream/OSD; the hub catalog is the map of which leaves Settings/restore/clone will write. Extending coverage means adding catalog entries (and optional Settings UI), not expecting omnibus `/config` to round-trip OSD.

Restore also refuses to apply while live capabilities/config cannot be read (empty/non-JSON `/config` after OTA). Apply is staged: the hub writes one block at a time (imaging, streams, …), confirms each over narrow `/settings` leaves with backoff, shows a progress checklist, then captures a post-restore backup. Config clone (pull/push) uses the same staged confirm + progress flow.

---

## Troubleshooting

**Dashboard won't load?**
- Verify the hub is running: `http://127.0.0.1:8080`
- Check that MQTT broker is connected
- Look at hub logs for errors

**Cameras appear offline?**
- Check MQTT broker connection
- Verify cameras can reach the broker
- Restart the camera app

**Snapshots not updating?**
- Confirm camera IP is reachable
- Check snapshot URL is correct
- Verify ONVIF credentials if using HTTPS

**Can't add a camera?**
- Check camera is on the same network
- Verify ONVIF endpoint URL (`/onvif/device_service`)
- Confirm credentials are correct (usually `thingino`/`thingino`)

---

## Next Steps

- **Need to adjust settings?** See [Configuration Reference](configuration.md)
- **Pairing not working?** Read [Camera Setup Guide](camera-setup-and-pairing.md)
- **Using Telegram?** Check [Operations Guide](operations.md)
