import json
import tempfile
import unittest
from unittest import mock

from app.web import create_web_app


class FakeUpstreamResponse:
    def __init__(
        self,
        body: bytes = b"",
        headers: dict[str, str] | None = None,
        lines: list[bytes] | None = None,
        status: int = 200,
    ) -> None:
        self._body = body
        self.headers = headers or {}
        self._lines = list(lines or [])
        self.status = status

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return None

    def read(self) -> bytes:
        return self._body

    def readline(self) -> bytes:
        if self._lines:
            return self._lines.pop(0)
        return b""

    def close(self) -> None:
        return None


class FakeHub:
    def __init__(self) -> None:
        self.recent_events = [
            {
                "sequence": 3,
                "camera_id": "cam1",
                "camera_name": "Test Camera",
                "timestamp": "1700000000",
                "at": "now",
                "source": "camera_event",
                "name": "motion started",
                "status": "success",
                "detail": "Motion detected",
            }
        ]
        self.camera = {
            "camera_id": "cam1",
            "name": "Test Camera",
            "status": "online",
            "camera_image_id": "wyze_cam3_t31x_gc2053_atbm6031",
            "ota_upgrade_command": "CAMERA=wyze_cam3_t31x_gc2053_atbm6031 IP=192.168.1.2 make cleanbuild upgrade_ota",
            "api_status": "online",
            "api_last_ok_at": "now",
            "api_last_error": "",
            "api_device_name": "test-cam",
            "api_device_model": "wyze",
            "api_streamer": "prudynt",
            "api_version": "0-local",
            "hostname": "test-cam",
            "ip": "192.168.1.2",
            "snapshot_url": "http://192.168.1.2/x/ch0.jpg",
            "snapshot_ch1_url": "http://192.168.1.2/x/ch1.jpg",
            "mjpeg_ch0_url": "http://192.168.1.2/x/ch0.mjpg",
            "mjpeg_ch1_url": "http://192.168.1.2/x/ch1.mjpg",
            "rtsp_ch0_url": "rtsp://192.168.1.2:554/ch0",
            "rtsp_ch1_url": "rtsp://192.168.1.2:554/ch1",
            "web_ui_url": "http://192.168.1.2/",
            "api_base_url": "https://192.168.1.2:1998/api/v1",
            "api_token": "",
            "api_key": "",
            "last_registration_at": "now",
            "last_probe_at": "now",
            "last_snapshot_ok_at": "now",
            "last_probe_error": "",
            "identity_conflict_error": "",
            "onvif_manufacturer": "Thingino",
            "onvif_model": "wyze",
            "onvif_firmware_version": "master",
            "onvif_serial_number": "123",
            "onvif_hardware_id": "t31",
            "onvif_last_ok_at": "now",
            "onvif_last_error": "",
            "preview_version": "1",
            "mqtt_command_status": "online",
            "mqtt_command_capable": True,
            "mqtt_command_last_error": "",
            "present_on_mqtt_broker": True,
            "has_agent": True,
            "hub_connected": False,
            "registered_on_hub": False,
            "is_paired": False,
            "setup_status": "connect",
            "override_name": "",
            "override_ip": "",
            "override_snapshot_url": "",
            "override_api_key": "",
            "override_api_base_url": "",
            "override_api_token": "",
            "override_onvif_endpoint": "",
            "override_onvif_username": "",
            "override_onvif_password": "",
            "native_action_history": [],
        }
        self.controls = {
            "native_controls_available": True,
            "native_record_supported": True,
            "native_daynight_supported": True,
            "native_daynight_enabled": True,
            "native_daynight_force_mode": "",
            "native_daynight_total_gain_night_threshold": "3000",
            "native_daynight_total_gain_day_threshold": "300",
            "native_daynight_controls_color": True,
            "native_daynight_controls_ircut": True,
            "native_daynight_controls_ir850": True,
            "native_daynight_controls_ir940": True,
            "native_daynight_controls_white": False,
            "native_daynight_schedule_enabled": False,
            "native_daynight_schedule_start_at": "18:00",
            "native_daynight_schedule_stop_at": "07:00",
            "native_daynight_running_mode": "day",
            "native_daynight_action_supported": True,
            "native_daynight_requested_mode": "auto",
            "native_privacy_supported": True,
            "native_privacy_enabled": False,
            "native_image_anti_flicker": "60hz",
            "native_image_anti_flicker_supported": True,
            "native_image_hflip": False,
            "native_image_hflip_supported": True,
            "native_image_vflip": False,
            "native_image_vflip_supported": True,
            "native_stream_controls": [
                {
                    "audio_enabled": True,
                    "audio_enabled_supported": True,
                    "bitrate": "3000",
                    "bitrate_supported": True,
                    "enabled": True,
                    "enabled_supported": True,
                    "format": "H264",
                    "format_supported": True,
                    "fps": "30",
                    "fps_supported": True,
                    "height": "1080",
                    "height_supported": True,
                    "label": "Main Stream",
                    "mode": "CBR",
                    "mode_supported": True,
                    "name": "stream0",
                    "osd_enabled": True,
                    "osd_enabled_supported": True,
                    "osd_time_enabled": True,
                    "osd_time_enabled_supported": True,
                    "osd_usertext_enabled": True,
                    "osd_usertext_enabled_supported": True,
                    "osd_usertext_format": "%hostname",
                    "osd_usertext_format_supported": True,
                    "osd_privacy_enabled": True,
                    "osd_privacy_enabled_supported": True,
                    "osd_privacy_fill_alpha": "255",
                    "osd_privacy_fill_color": "#000000FF",
                    "osd_privacy_fill_color_supported": True,
                    "osd_privacy_fill_color_value": "#000000",
                    "osd_privacy_stroke_alpha": "255",
                    "osd_privacy_stroke_color": "#FFFFFFFF",
                    "osd_privacy_stroke_color_supported": True,
                    "osd_privacy_stroke_color_value": "#FFFFFF",
                    "osd_privacy_text": "PRIVACY ENABLED",
                    "osd_privacy_text_supported": True,
                    "stream_id": 0,
                    "width": "1920",
                    "width_supported": True,
                },
                {
                    "audio_enabled": False,
                    "audio_enabled_supported": True,
                    "bitrate": "640",
                    "bitrate_supported": True,
                    "enabled": True,
                    "enabled_supported": True,
                    "format": "H264",
                    "format_supported": True,
                    "fps": "15",
                    "fps_supported": True,
                    "height": "360",
                    "height_supported": True,
                    "label": "Substream",
                    "mode": "VBR",
                    "mode_supported": True,
                    "name": "stream1",
                    "osd_enabled": True,
                    "osd_enabled_supported": True,
                    "osd_time_enabled": False,
                    "osd_time_enabled_supported": True,
                    "osd_usertext_enabled": False,
                    "osd_usertext_enabled_supported": True,
                    "osd_usertext_format": "",
                    "osd_usertext_format_supported": True,
                    "osd_privacy_enabled": False,
                    "osd_privacy_enabled_supported": True,
                    "osd_privacy_fill_alpha": "255",
                    "osd_privacy_fill_color": "#111111FF",
                    "osd_privacy_fill_color_supported": True,
                    "osd_privacy_fill_color_value": "#111111",
                    "osd_privacy_stroke_alpha": "255",
                    "osd_privacy_stroke_color": "#EEEEEEFF",
                    "osd_privacy_stroke_color_supported": True,
                    "osd_privacy_stroke_color_value": "#EEEEEE",
                    "osd_privacy_text": "PRIVACY ENABLED",
                    "osd_privacy_text_supported": True,
                    "stream_id": 1,
                    "width": "640",
                    "width_supported": True,
                }
            ],
        }
        self.config = {
            "telegram": {
                "token": "123456:token",
                "api_url": "https://api.telegram.org",
                "polling_timeout": 30,
                "allowed_chat_ids": [],
                "allowed_usernames": [],
            },
            "mqtt": {
                "host": "mqtt.local",
                "port": 1883,
                "username": "",
                "password": "",
                "keepalive": 60,
                "use_tls": False,
            },
            "routing": {
                "command_topic": "thingino/cam/{camera_id}/cmd",
                "reply_topic": "thingino/cam/+/reply",
                "registration_topic": "thingino/cam/+/hello",
                "event_topic": "thingino/cam/+/event",
                "state_topic": "thingino/cam/+/state",
            },
            "ui": {
                "username": "",
                "password": "",
                "competency_level": "basic",
                "registration_stale_after_seconds": 0,
                "snapshot_heartbeat_interval_seconds": 0,
                "snapshot_heartbeat_timeout_seconds": 5,
                "snapshot_cache_stale_after_seconds": 3600,
                "api_probe_interval_seconds": 0,
            },
            "history": {
                "enabled": True,
                "path": "",
                "recent_actions_limit": 20,
                "max_action_events_per_camera": 1000,
                "max_state_samples_per_camera": 5000,
            },
            "cameras": [],
        }
        self.last_override_update = None
        self.last_patch_payload = None
        self.last_send2_payload = None
        self.saved_config = None
        self.reload_called = False

    def get_camera_for_ui(self, camera_id: str):
        if camera_id != "cam1":
            raise RuntimeError("Unknown camera")
        self._sync_setup_status()
        return dict(self.camera)

    def get_camera_snapshot_url_for_ui(self, camera_id: str, stream_name: str = "ch0") -> str:
        if camera_id != "cam1":
            raise RuntimeError("Unknown camera")
        if stream_name == "ch1":
            return str(self.camera.get("snapshot_ch1_url") or "")
        return str(self.camera.get("snapshot_url") or "")

    def get_cached_snapshot_for_ui(self, camera_id: str):
        if camera_id != "cam1":
            raise RuntimeError("Unknown camera")
        return None

    def get_camera_webrtc_url_for_ui(self, camera_id: str) -> str:
        if camera_id != "cam1":
            raise RuntimeError("Unknown camera")
        return str(self.camera.get("webrtc_url") or "")

    def get_camera_login_credentials_for_ui(self, camera_id: str) -> tuple[str, str]:
        if camera_id != "cam1":
            raise RuntimeError("Unknown camera")
        return ("thingino", "thingino")

    def get_camera_supported_controls_for_ui(self, camera_id: str):
        if camera_id != "cam1":
            raise RuntimeError("Unknown camera")
        return dict(self.controls)

    def _sync_setup_status(self) -> None:
        self.camera["present_on_mqtt_broker"] = bool(self.camera.get("present_on_mqtt_broker", True))
        self.camera["is_paired"] = bool(self.camera.get("is_paired") or str(self.camera.get("api_token") or "").strip())
        self.camera["hub_connected"] = bool(self.camera.get("hub_connected", self.camera.get("registered_on_hub", False))) or self.camera["is_paired"]
        self.camera["registered_on_hub"] = self.camera["hub_connected"]
        self.camera["has_agent"] = bool(self.camera.get("has_agent", self.camera.get("mqtt_command_status") == "online")) or self.camera["is_paired"]
        self.camera["mqtt_command_capable"] = bool(self.camera["has_agent"])
        if self.camera.get("is_paired"):
            self.camera["setup_status"] = "paired"
            return
        if not self.camera.get("present_on_mqtt_broker"):
            self.camera["setup_status"] = "unavailable"
            return
        if self.camera.get("mqtt_command_status") == "unknown":
            self.camera["setup_status"] = "verifying"
        elif not self.camera.get("has_agent"):
            self.camera["setup_status"] = "unavailable"
        elif not self.camera.get("registered_on_hub"):
            self.camera["setup_status"] = "connect"
        else:
            self.camera["setup_status"] = "pair"

    def export_config(self):
        return {
            "telegram": dict(self.config["telegram"]),
            "mqtt": dict(self.config["mqtt"]),
            "routing": dict(self.config["routing"]),
            "ui": dict(self.config["ui"]),
            "history": dict(self.config["history"]),
            "cameras": list(self.config["cameras"]),
        }

    def save_config(self, config):
        self.saved_config = config

    def reload_config(self):
        self.reload_called = True
    def update_camera_override(self, camera_id: str, override: dict[str, str]) -> None:
        if camera_id != "cam1":
            raise RuntimeError("Unknown camera")
        self.last_override_update = dict(override)
        for key, value in override.items():
            self.camera[f"override_{key}"] = value

    def list_recent_events_for_ui(self, limit: int = 40):
        return list(self.recent_events[:limit])

    def live_events_since(self, last_sequence: int, limit: int = 20):
        events = [entry for entry in self.recent_events if int(entry["sequence"]) > int(last_sequence)]
        return 3, events[:limit]

    def queue_camera_api_refresh(self, camera_id: str) -> str:
        if camera_id != "cam1":
            raise RuntimeError("Unknown camera")
        return "scheduled"

    def queue_camera_onvif_refresh(self, camera_id: str) -> str:
        if camera_id != "cam1":
            raise RuntimeError("Unknown camera")
        return "already_running"

    def set_camera_daynight_mode(self, camera_id: str, mode: str, *, refresh_after: bool = True):
        if camera_id != "cam1":
            raise RuntimeError("Unknown camera")
        if refresh_after:
            raise AssertionError("day/night route should be non-blocking")
        return {"status": "accepted", "mode": mode}

    def set_camera_privacy(self, camera_id: str, enabled: bool, channel: str = "all", *, refresh_after: bool = True):
        if camera_id != "cam1":
            raise RuntimeError("Unknown camera")
        if refresh_after:
            raise AssertionError("privacy route should be non-blocking")
        return {"status": "accepted", "enabled": enabled, "channel": channel}

    def patch_camera_config(self, camera_id: str, payload, *, refresh_after: bool = True):
        if camera_id != "cam1":
            raise RuntimeError("Unknown camera")
        if refresh_after:
            raise AssertionError("patch config route should be non-blocking")
        self.last_patch_payload = payload
        if "motion" in payload:
            motion = payload.get("motion") or {}
            self.controls["native_motion_enabled"] = bool(motion.get("enabled"))
        if "daynight" in payload:
            daynight = payload.get("daynight") or {}
            if "enabled" in daynight:
                self.controls["native_daynight_enabled"] = bool(daynight.get("enabled"))
            if "force_mode" in daynight:
                self.controls["native_daynight_force_mode"] = daynight.get("force_mode") or ""
            for field in ("total_gain_night_threshold", "total_gain_day_threshold"):
                if field in daynight:
                    self.controls[f"native_daynight_{field}"] = str(daynight.get(field) or "")
            controls = daynight.get("controls") or {}
            if isinstance(controls, dict):
                for field in ("color", "ircut", "ir850", "ir940", "white"):
                    if field in controls:
                        self.controls[f"native_daynight_controls_{field}"] = bool(controls.get(field))
            schedule = daynight.get("schedule") or {}
            if isinstance(schedule, dict):
                if "enabled" in schedule:
                    self.controls["native_daynight_schedule_enabled"] = bool(schedule.get("enabled"))
                if "start_at" in schedule:
                    self.controls["native_daynight_schedule_start_at"] = schedule.get("start_at") or ""
                if "stop_at" in schedule:
                    self.controls["native_daynight_schedule_stop_at"] = schedule.get("stop_at") or ""
        for stream_name, stream_payload in payload.items():
            if not str(stream_name).startswith("stream") or not isinstance(stream_payload, dict):
                continue
            for stream in self.controls.get("native_stream_controls", []):
                if stream.get("name") != stream_name:
                    continue
                for key, value in stream_payload.items():
                    if key == "osd" and isinstance(value, dict):
                        if "enabled" in value:
                            stream["osd_enabled"] = bool(value.get("enabled"))
                        if isinstance(value.get("time"), dict) and "enabled" in value["time"]:
                            stream["osd_time_enabled"] = bool(value["time"].get("enabled"))
                        if isinstance(value.get("usertext"), dict):
                            if "enabled" in value["usertext"]:
                                stream["osd_usertext_enabled"] = bool(value["usertext"].get("enabled"))
                            if "format" in value["usertext"]:
                                stream["osd_usertext_format"] = value["usertext"].get("format")
                        if isinstance(value.get("privacy"), dict):
                            privacy = value["privacy"]
                            if "enabled" in privacy:
                                stream["osd_privacy_enabled"] = bool(privacy.get("enabled"))
                            if "text" in privacy:
                                stream["osd_privacy_text"] = privacy.get("text") or ""
                            if "fill_color" in privacy:
                                stream["osd_privacy_fill_color"] = privacy.get("fill_color") or ""
                                stream["osd_privacy_fill_color_value"] = str(privacy.get("fill_color") or "")[:7] or "#000000"
                                stream["osd_privacy_fill_alpha"] = str(int(str(privacy.get("fill_color") or "#000000FF")[-2:], 16))
                            if "stroke_color" in privacy:
                                stream["osd_privacy_stroke_color"] = privacy.get("stroke_color") or ""
                                stream["osd_privacy_stroke_color_value"] = str(privacy.get("stroke_color") or "")[:7] or "#000000"
                                stream["osd_privacy_stroke_alpha"] = str(int(str(privacy.get("stroke_color") or "#000000FF")[-2:], 16))
                        continue
                    stream[key] = value
        history = list(self.camera.get("native_action_history") or [])
        history.insert(
            0,
            {
                "action": "patch config",
                "at": "now",
                "detail": ", ".join(sorted(payload.keys())) or "accepted",
                "source": "native_api",
                "status": "success",
            },
        )
        self.camera["native_action_history"] = history[:8]
        return {"status": "accepted"}

    def update_camera_send2_config(self, camera_id: str, payload):
        if camera_id != "cam1":
            raise RuntimeError("Unknown camera")
        if self.camera.get("api_status") == "offline":
            raise RuntimeError("Native API is offline for this camera.")
        self.last_send2_payload = payload
        return {"result": "accepted"}

    def test_camera_send2_service(self, camera_id: str, service_name: str, *, verbose: bool = True, send_type: str = ""):
        if camera_id != "cam1":
            raise RuntimeError("Unknown camera")
        return {
            "message": {
                "status": "success",
                "output": f"send2 {service_name} {send_type or 'default'} verbose={verbose}",
            }
        }

    def perform_bulk_action(self, camera_ids, action: str):
        if action == "restart-streaming":
            return {
                "action": action,
                "total": len(camera_ids),
                "success_count": 0,
                "error_count": len(camera_ids),
                "results": [{"camera_id": camera_id, "status": "error", "detail": "camera offline"} for camera_id in camera_ids],
            }
        return {
            "action": action,
            "total": len(camera_ids),
            "success_count": len(camera_ids),
            "error_count": 0,
            "results": [{"camera_id": camera_id, "status": "success", "detail": "scheduled"} for camera_id in camera_ids],
        }

    def unregister_camera(self, camera_id: str):
        if camera_id != "cam1":
            raise RuntimeError("Unknown camera")
        return {
            "config_removed": True,
            "retained_cleared": True,
            "command_published": True,
            "retained_error": "",
        }

    def connect_camera(self, enrollment):
        if self.camera.get("mqtt_command_status") != "online":
            raise RuntimeError(self.camera.get("mqtt_command_last_error") or "Camera did not respond to hub MQTT commands.")
        camera_id = enrollment.get("camera_id") or enrollment.get("id") or "cam2"
        self.camera["hub_connected"] = True
        self.camera["registered_on_hub"] = True
        self.camera["is_paired"] = False
        self.camera["override_onvif_username"] = enrollment.get("onvif_username") or ""
        self.camera["override_onvif_password"] = enrollment.get("onvif_password") or ""
        self._sync_setup_status()
        return {
            "camera_id": camera_id,
            "status": "success",
            "status_detail": f"Connected {camera_id} to the hub.",
            "api_base_url": f"https://{enrollment.get('ip') or '192.168.1.2'}:1998/api/v1",
            "api_token": "",
        }

    def enroll_camera(self, enrollment):
        return {
            "camera_id": "cam2",
            "updated_existing": False,
            "rescan_requested": True,
            "api_refresh": "scheduled",
            "onvif_refresh": "not_configured",
            "controls_refresh": "scheduled",
        }

    def probe_camera_enrollment(self, enrollment):
        return {
            "camera_id": "cam2",
            "name": "Back Door",
            "ip": enrollment.get("ip") or "192.168.1.3",
            "conflicts": {},
            "snapshot_url": "http://192.168.1.3/x/ch0.jpg",
            "can_save": True,
            "api": {
                "configured": True,
                "ok": True,
                "base_url": "https://192.168.1.3:1998/api/v1",
                "error": "",
                "device_name": "Back Door",
                "device_model": "wyze",
                "streamer": "prudynt",
                "version": "1.0",
            },
            "onvif": {
                "configured": True,
                "ok": False,
                "endpoint": "http://192.168.1.3/onvif/device_service",
                "error": "authentication failed",
                "manufacturer": "",
                "model": "",
                "firmware_version": "",
            },
        }

    def generate_pairing_bundle(self, enrollment):
        return {
            "camera_id": "cam2",
            "name": "Back Door",
            "ip": enrollment.get("ip") or "192.168.1.3",
            "api_base_url": "https://192.168.1.3:1998/api/v1",
            "api_token": "pairing-token-123",
            "bootstrap_payload": {
                "agent": {
                    "enabled": True,
                    "tls": True,
                    "listen": "0.0.0.0",
                    "port": 1998,
                    "token": "pairing-token-123",
                }
            },
            "bootstrap_json": '{\n  "agent": {\n    "enabled": true,\n    "listen": "0.0.0.0",\n    "port": 1998,\n    "tls": true,\n    "token": "pairing-token-123"\n  }\n}',
            "commands": [
                "jct /etc/thingino.json set agent.enabled true",
                "jct /etc/thingino.json set agent.tls true",
            ],
            "bootstrap_install_commands": [
                "printf '%s\\n' '{\"agent\":{}}' > /etc/thingino-agent-bootstrap.json",
                "/etc/init.d/S95thingino-agent restart",
            ],
            "save_entry": {
                "id": "cam2",
                "api_base_url": "https://192.168.1.3:1998/api/v1",
                "api_token": "pairing-token-123",
            },
        }

    def install_pairing_bundle_via_mqtt(self, enrollment):
        if self.camera.get("mqtt_command_status") != "online":
            raise RuntimeError(self.camera.get("mqtt_command_last_error") or "Camera did not respond to hub MQTT commands.")
        camera_id = enrollment.get("camera_id") or enrollment.get("id") or "cam2"
        self.camera["hub_connected"] = True
        self.camera["registered_on_hub"] = True
        self.camera["api_status"] = "online"
        result = self.generate_pairing_bundle(enrollment)
        self.camera["api_token"] = result["api_token"]
        self.camera["is_paired"] = True
        self._sync_setup_status()
        result.update(
            {
                "status": "success",
                "status_detail": "Agent bootstrap installed via MQTT",
                "mqtt": {
                    "camera_id": camera_id,
                    "published": True,
                    "reply_received": True,
                    "reply_ok": True,
                    "reply_text": "Agent bootstrap installed",
                    "request_id": "request-123",
                },
            }
        )
        return result

    def snapshot_status(self):
        return {
            "telegram_ok": True,
            "telegram_last_ok": "now",
            "telegram_last_error": "",
            "mqtt_connected": True,
            "mqtt_host": "mqtt.local",
            "mqtt_port": 1883,
            "mqtt_last_error": "",
            "config_path": "/tmp/config.yaml",
            "history_enabled": True,
            "history_db_path": "/tmp/history.sqlite3",
            "history_recent_actions_limit": 20,
            "api_known": 1,
            "api_ready": 1,
            "api_errors": 0,
            "api_last_ok": "now",
            "onvif_known": 0,
            "onvif_ready": 0,
            "onvif_errors": 0,
            "onvif_last_ok": "",
            "last_reload_at": "now",
        }

    def list_cameras_for_ui(self):
        return [
            {
                "camera_id": "cam1",
                "name": "Test Camera",
                "ip": "192.168.1.2",
                "snapshot_url": "http://192.168.1.2/x/ch0.jpg",
                "status": "online",
                "api_status": "online",
                "mqtt_command_status": self.camera.get("mqtt_command_status", "online"),
                "mqtt_command_capable": self.camera.get("mqtt_command_capable", True),
                "mqtt_command_last_error": self.camera.get("mqtt_command_last_error", ""),
                "api_streamer": "prudynt",
                "preview_state": "placeholder",
                "hostname": "test-cam",
                "onvif_label": "",
                "last_registration_at": "now",
                "last_probe_at": "now",
                "last_snapshot_ok_at": "now",
                "last_probe_error": "",
                "preview_version": "1",
            }
        ]

    def get_camera_history_for_ui(self, camera_id: str, limit: int = 100, kind_filter: str = "all", sample_type_filter: str = "all"):
        if camera_id != "cam1":
            raise RuntimeError("Unknown camera")
        self._sync_setup_status()
        return {
            "camera_id": "cam1",
            "name": "Test Camera",
            "ip": "192.168.1.2",
            "status": "online",
            "api_status": self.camera.get("api_status", "online"),
            "setup_status": self.camera.get("setup_status", "connect"),
            "present_on_mqtt_broker": self.camera.get("present_on_mqtt_broker", True),
            "has_agent": self.camera.get("has_agent", True),
            "registered_on_hub": self.camera.get("registered_on_hub", False),
            "is_paired": self.camera.get("is_paired", False),
            "hub_connected": self.camera.get("hub_connected", False),
            "mqtt_command_status": self.camera.get("mqtt_command_status", "online"),
            "mqtt_command_capable": self.camera.get("mqtt_command_capable", True),
            "mqtt_command_last_error": self.camera.get("mqtt_command_last_error", ""),
            "history_enabled": True,
            "history_db_path": "/tmp/history.sqlite3",
            "history_limit": limit,
            "history_kind_filter": kind_filter,
            "history_sample_type_filter": sample_type_filter,
            "available_sample_types": ["all", "api_probe"],
            "timeline_action_count": 1,
            "timeline_state_count": 0,
            "timeline_config_count": 1,
            "latest_api_probe": None,
            "latest_snapshot_probe": None,
            "charts": [
                {
                    "title": "Network Reachability",
                    "summary": "100% positive across 1 known samples",
                    "latest": "online",
                    "width": 320,
                    "height": 44,
                    "bars": [
                        {"x": 0, "y": 6, "width": 319, "height": 32, "fill": "#198754", "title": "now: online"},
                    ],
                    "range_start": "now",
                    "range_end": "now",
                    "sample_count": 1,
                    "legend": [
                        {"label": "online", "fill": "#198754"},
                    ],
                }
            ],
            "timeline": [
                {
                    "at": "now",
                    "timestamp": "1700000000",
                    "kind": "config",
                    "name": "enrollment create",
                    "status": "info",
                    "source": "hub",
                    "detail": "/hub/enrollment/ip: null -> \"192.168.1.2\" (Camera enrollment)",
                }
            ],
        }


class SensorDataProxyRouteTests(unittest.TestCase):
    def setUp(self) -> None:
        self.hub = FakeHub()
        self.hub.camera["api_token"] = "test-token"
        self.app = create_web_app(self.hub)
        self.client = self.app.test_client()

    def test_sensor_data_history_uses_agent_runtime_payload(self) -> None:
        history_payload = {
            "history": [
                {
                    "time_now": 1700000000,
                    "total_gain": 256,
                    "ae_luma": 36,
                    "daynight_mode": "day",
                }
            ]
        }

        with mock.patch("urllib.request.urlopen", return_value=FakeUpstreamResponse(json.dumps(history_payload).encode("utf-8"), {"Content-Type": "application/json"})) as urlopen:
            response = self.client.post("/camera/cam1/sensor-data/history")

        self.assertEqual(response.status_code, 200)
        payload = response.get_json()
        self.assertEqual(payload, {"daynight": {"history": history_payload["history"]}})
        request_arg = urlopen.call_args.args[0]
        self.assertEqual(request_arg.full_url, "https://192.168.1.2:1998/api/v1/runtime/sensor-data")
        self.assertEqual(request_arg.get_header("Authorization"), "Bearer test-token")

    def test_sensor_data_stream_uses_agent_event_stream(self) -> None:
        upstream = FakeUpstreamResponse(
            headers={"Content-Type": "text/event-stream"},
            lines=[
                b"retry: 2000\n",
                b"\n",
                b"data: {\"time_now\":1700000001,\"total_gain\":256}\n",
                b"\n",
                b"",
            ],
        )

        with mock.patch("urllib.request.urlopen", return_value=upstream) as urlopen:
            response = self.client.get("/camera/cam1/sensor-data/stream")

        self.assertEqual(response.status_code, 200)
        self.assertIn(b'data: {"time_now":1700000001,"total_gain":256}', response.data)
        request_arg = urlopen.call_args.args[0]
        self.assertEqual(request_arg.full_url, "https://192.168.1.2:1998/api/v1/events/sensor-data")
        self.assertEqual(request_arg.get_header("Authorization"), "Bearer test-token")


class WebRouteTests(unittest.TestCase):
    def setUp(self) -> None:
        self.hub = FakeHub()
        self.app = create_web_app(self.hub)
        self.client = self.app.test_client()
        self.json_headers = {
            "Accept": "application/json",
            "X-Requested-With": "fetch",
        }

    def test_hydrate_returns_camera_payload_and_refresh_state(self) -> None:
        response = self.client.post("/camera/cam1/hydrate", headers=self.json_headers)

        self.assertEqual(response.status_code, 200)
        payload = response.get_json()
        self.assertTrue(payload["ok"])
        self.assertEqual(payload["refreshes"], {"api": "scheduled", "onvif": "already_running"})
        self.assertEqual(payload["camera"]["camera_id"], "cam1")
        self.assertTrue(payload["camera"]["native_controls_available"])

    def test_payload_route_returns_full_camera_payload(self) -> None:
        response = self.client.get("/camera/cam1/payload", headers=self.json_headers)

        self.assertEqual(response.status_code, 200)
        payload = response.get_json()
        self.assertTrue(payload["ok"])
        self.assertEqual(payload["camera"]["camera_id"], "cam1")
        self.assertIn("native_daynight_requested_mode", payload["camera"])

    def test_camera_detail_links_to_secondary_pages(self) -> None:
        response = self.client.get("/camera/cam1")

        self.assertEqual(response.status_code, 200)
        body = response.get_data(as_text=True)
        self.assertIn('href="http://192.168.1.2/"', body)
        self.assertIn('>Camera Web UI<', body)
        self.assertNotIn('data-async-action="/pair/cam1"', body)
        self.assertNotIn(">Pair<", body)
        self.assertIn("/connect/cam1", body)
        self.assertIn("Connect to Hub", body)
        self.assertIn("Step 1 of 2", body)
        self.assertIn('href="/camera/cam1/info"', body)
        self.assertIn('href="/camera/cam1/settings"', body)
        self.assertIn('href="/camera/cam1/sensor-data"', body)
        self.assertIn('href="/camera/cam1/send2"', body)
        self.assertNotIn('href="/camera/cam1/overrides"', body)
        self.assertNotIn('href="/camera/cam1/native-actions"', body)
        self.assertNotIn('href="/camera/cam1/expert-config"', body)
        self.assertIn("Camera Endpoints", body)
        self.assertIn('data-copy-text="rtsp://192.168.1.2:554/ch0"', body)
        self.assertIn('data-copy-text="rtsp://192.168.1.2:554/ch1"', body)
        self.assertIn('data-copy-text="http://192.168.1.2/x/ch0.jpg"', body)
        self.assertIn('data-copy-text="http://192.168.1.2/x/ch1.jpg"', body)
        self.assertIn('data-copy-text="http://192.168.1.2/x/ch0.mjpg"', body)
        self.assertIn('data-copy-text="http://192.168.1.2/x/ch1.mjpg"', body)

    def test_camera_detail_uses_webrtc_preview_for_raptor(self) -> None:
        self.hub.camera["api_streamer"] = "raptor"
        self.hub.camera["webrtc_url"] = "https://192.168.1.2:8554/webrtc"

        response = self.client.get("/camera/cam1")

        self.assertEqual(response.status_code, 200)
        body = response.get_data(as_text=True)
        self.assertIn('src="/preview-webrtc/cam1"', body)
        self.assertIn("Preview uses WebRTC for this camera.", body)
        self.assertIn('data-copy-text="https://192.168.1.2:8554/webrtc"', body)

    def test_camera_detail_uses_mjpeg_even_when_placeholder(self) -> None:
        self.hub.camera["preview_state"] = "placeholder"
        self.hub.camera["status"] = "offline"
        self.hub.camera["mjpeg_ch0_url"] = "http://192.168.1.2/x/ch0.mjpg"

        response = self.client.get("/camera/cam1")

        self.assertEqual(response.status_code, 200)
        body = response.get_data(as_text=True)
        self.assertIn('src="/preview-live/cam1"', body)
        self.assertIn('data-preview-online="true"', body)
        self.assertIn('data-allow-offline-live="true"', body)
        self.assertNotIn('alt="No stream available"', body)

    def test_preview_webrtc_proxy_rewrites_whip_path(self) -> None:
        self.hub.camera["api_streamer"] = "raptor"
        self.hub.camera["webrtc_url"] = "https://192.168.1.2:8554/webrtc"
        upstream_html = b"<script>fetch('/whip?stream='+stream,{method:'POST'})</script>"
        with mock.patch(
            "app.web.urllib.request.urlopen",
            return_value=FakeUpstreamResponse(upstream_html, {"Content-Type": "text/html; charset=utf-8"}),
        ) as mocked_urlopen:
            response = self.client.get("/preview-webrtc/cam1")

        self.assertEqual(response.status_code, 200)
        body = response.get_data(as_text=True)
        self.assertIn("fetch('/preview-webrtc/cam1/whip?stream='", body)
        request_to_camera = mocked_urlopen.call_args[0][0]
        self.assertEqual(request_to_camera.full_url, "https://192.168.1.2:8554/webrtc")
        self.assertTrue(str(request_to_camera.get_header("Authorization")).startswith("Basic "))

    def test_preview_webrtc_whip_rewrites_location_header(self) -> None:
        self.hub.camera["api_streamer"] = "raptor"
        self.hub.camera["webrtc_url"] = "https://192.168.1.2:8554/webrtc"
        upstream = FakeUpstreamResponse(
            b"v=0\r\n",
            {"Content-Type": "application/sdp", "Location": "/whip/session-1"},
            status=201,
        )
        with mock.patch("app.web.urllib.request.urlopen", return_value=upstream) as mocked_urlopen:
            response = self.client.post(
                "/preview-webrtc/cam1/whip?stream=1",
                data="v=0\r\n",
                content_type="application/sdp",
            )

        self.assertEqual(response.status_code, 201)
        self.assertEqual(response.headers["Location"], "/preview-webrtc/cam1/whip/session-1")
        request_to_camera = mocked_urlopen.call_args[0][0]
        self.assertEqual(request_to_camera.full_url, "https://192.168.1.2:8554/whip?stream=1")

    def test_camera_detail_shows_advanced_links_for_advanced_users(self) -> None:
        self.hub.config["ui"]["competency_level"] = "advanced"

        response = self.client.get("/camera/cam1")

        self.assertEqual(response.status_code, 200)
        body = response.get_data(as_text=True)
        self.assertIn('href="/camera/cam1/overrides"', body)
        self.assertIn('href="/camera/cam1/native-actions"', body)
        self.assertNotIn('href="/camera/cam1/expert-config"', body)

    def test_camera_detail_shows_expert_link_for_expert_users(self) -> None:
        self.hub.config["ui"]["competency_level"] = "expert"

        response = self.client.get("/camera/cam1")

        self.assertEqual(response.status_code, 200)
        body = response.get_data(as_text=True)
        self.assertIn('href="/camera/cam1/overrides"', body)
        self.assertIn('href="/camera/cam1/native-actions"', body)
        self.assertIn('href="/camera/cam1/expert-config"', body)

    def test_camera_info_page_renders_copyable_ota_command(self) -> None:
        response = self.client.get("/camera/cam1/info")

        self.assertEqual(response.status_code, 200)
        body = response.get_data(as_text=True)
        self.assertIn("Info &amp; ONVIF", body)
        self.assertIn('href="http://192.168.1.2/"', body)
        self.assertIn('>Camera Web UI<', body)
        self.assertIn("Finish Camera Setup", body)
        self.assertIn('action="/connect/cam1"', body)
        self.assertIn('name="redirect_page" value="info"', body)
        self.assertNotIn('action="/pair/cam1"', body)
        self.assertIn('href="/camera/cam1"', body)
        self.assertIn('href="/camera/cam1/settings"', body)
        self.assertIn('href="/camera/cam1/sensor-data"', body)
        self.assertIn('href="/camera/cam1/history"', body)
        self.assertIn("Camera Info", body)
        self.assertIn("ONVIF", body)
        self.assertIn("Firmware Rebuild and OTA Command", body)
        self.assertIn('class="form-control font-monospace cb"', body)
        self.assertIn('data-copy-text="CAMERA=wyze_cam3_t31x_gc2053_atbm6031 IP=192.168.1.2 make cleanbuild upgrade_ota"', body)
        self.assertIn("CAMERA=wyze_cam3_t31x_gc2053_atbm6031 IP=192.168.1.2 make cleanbuild upgrade_ota", body)

    def test_camera_overrides_page_saves_changes(self) -> None:
        self.hub.config["ui"]["competency_level"] = "advanced"
        response = self.client.post(
            "/camera/cam1/overrides",
            data={"name": "Front Door", "onvif_username": "thingino"},
            follow_redirects=True,
        )

        self.assertEqual(response.status_code, 200)
        body = response.get_data(as_text=True)
        self.assertIn("Camera Overrides", body)
        self.assertIn("Saved camera overrides for cam1.", body)
        self.assertEqual(self.hub.last_override_update, {"name": "Front Door", "onvif_username": "thingino"})

    def test_camera_overrides_redirects_without_advanced_access(self) -> None:
        response = self.client.get("/camera/cam1/overrides", follow_redirects=True)

        self.assertEqual(response.status_code, 200)
        body = response.get_data(as_text=True)
        self.assertIn("Advanced access is required for this camera maintenance page.", body)
        self.assertIn("Long-Term Camera Settings", body)

    def test_camera_settings_page_renders_long_term_controls(self) -> None:
        self.hub.camera["native_motion_supported"] = True
        self.hub.camera["native_motion_enabled"] = True
        self.hub.camera["native_send2_available"] = True
        self.hub.camera["native_send2_motion_sensitivity"] = 4
        self.hub.camera["native_send2_motion_cooldown"] = 12
        response = self.client.get("/camera/cam1/settings")

        self.assertEqual(response.status_code, 200)
        body = response.get_data(as_text=True)
        self.assertIn("Long-Term Camera Settings", body)
        self.assertIn("Anti-flicker mode", body)
        self.assertIn("Horizontal flip", body)
        self.assertIn("Brightness", body)
        self.assertIn("Contrast", body)
        self.assertIn("Saturation", body)
        self.assertIn("Sharpness", body)
        self.assertNotIn("Motion sensitivity for notifications", body)
        self.assertIn("Day/Night", body)
        self.assertIn("Automatic switching enabled", body)
        self.assertIn("Switch to night mode above", body)
        self.assertIn("Switch to day mode below", body)
        self.assertIn("Change color mode", body)
        self.assertIn("Flip IR cut filter", body)
        self.assertIn("Use time-based schedule", body)
        self.assertIn("Privacy Screen Overlays", body)
        self.assertIn("Overlay text", body)
        self.assertIn("Fill color", body)
        self.assertIn("Stroke color", body)
        self.assertIn('name="stream0_osd_privacy_text"', body)
        self.assertIn('name="stream1_osd_privacy_text"', body)
        self.assertIn("Stream Parameters", body)
        self.assertIn("stream0_width", body)

    def test_camera_sensor_data_page_renders_proxy_endpoints(self) -> None:
        response = self.client.get("/camera/cam1/sensor-data")

        self.assertEqual(response.status_code, 200)
        body = response.get_data(as_text=True)
        self.assertIn("Raw Sensor Data Collection", body)
        self.assertIn('data-history-url="/camera/cam1/sensor-data/history"', body)
        self.assertIn('data-stream-url="/camera/cam1/sensor-data/stream"', body)
        self.assertIn('/preview-live/cam1?stream=ch1', body)
        self.assertIn("new SensorDataCollector", body)

    def test_camera_sensor_data_history_proxies_camera_payload(self) -> None:
        class FakeUpstreamResponse:
            def __init__(self, body: bytes, content_type: str) -> None:
                self._body = body
                self.headers = {"Content-Type": content_type}

            def __enter__(self):
                return self

            def __exit__(self, exc_type, exc, tb):
                return False

            def read(self) -> bytes:
                return self._body

        upstream_body = json.dumps({"history": [{"time_now": 1, "ev": 4}]}).encode("utf-8")
        with mock.patch("app.web.urllib.request.urlopen", return_value=FakeUpstreamResponse(upstream_body, "application/json")) as mocked_urlopen:
            response = self.client.post(
                "/camera/cam1/sensor-data/history",
                data=json.dumps({"daynight": {"history": None}}),
                content_type="application/json",
            )

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.get_json(), {"daynight": {"history": [{"time_now": 1, "ev": 4}]}})
        request_to_camera = mocked_urlopen.call_args[0][0]
        self.assertEqual(request_to_camera.full_url, "https://192.168.1.2:1998/api/v1/runtime/sensor-data")
        self.assertEqual(request_to_camera.get_method(), "GET")

    def test_camera_sensor_data_stream_proxies_event_stream(self) -> None:
        class FakeStreamResponse:
            def __init__(self) -> None:
                self.headers = {"Content-Type": "text/event-stream"}
                self._lines = [b"retry: 2000\n", b"\n", b"data: {\"time_now\":1}\n", b"\n", b""]

            def readline(self) -> bytes:
                return self._lines.pop(0)

            def close(self) -> None:
                return None

        with mock.patch("app.web.urllib.request.urlopen", return_value=FakeStreamResponse()) as mocked_urlopen:
            response = self.client.get("/camera/cam1/sensor-data/stream")

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.headers["Content-Type"], "text/event-stream")
        self.assertEqual(response.headers["X-Accel-Buffering"], "no")
        self.assertIn("retry: 2000", response.get_data(as_text=True))
        request_to_camera = mocked_urlopen.call_args[0][0]
        self.assertEqual(request_to_camera.full_url, "https://192.168.1.2:1998/api/v1/events/sensor-data")
        self.assertEqual(request_to_camera.get_method(), "GET")

    def test_camera_send2_page_renders_services_and_output(self) -> None:
        self.hub.controls["native_send2_available"] = True
        self.hub.controls["native_send2_services"] = [
            {
                "name": "telegram",
                "label": "Telegram",
                "photo_supported": True,
                "video_supported": False,
                "motion_enabled": True,
                "photo_enabled": True,
                "video_enabled": False,
            }
        ]

        response = self.client.get("/camera/cam1/send2")

        self.assertEqual(response.status_code, 200)
        body = response.get_data(as_text=True)
        self.assertIn("Send to Services", body)
        self.assertIn("Motion Detection", body)
        self.assertIn("Start motion detection on boot", body)
        self.assertIn("Sensitivity", body)
        self.assertIn("Cooldown", body)
        self.assertIn("Available Services", body)
        self.assertIn("Finish Camera Setup", body)
        self.assertIn('name="redirect_page" value="send2"', body)
        self.assertNotIn('action="/pair/cam1"', body)
        self.assertIn("Send2 Test Output", body)
        self.assertIn("Save Motion &amp; Send2 Settings", body)
        self.assertIn('href="/camera/cam1/settings"', body)
        self.assertIn('href="/camera/cam1/sensor-data"', body)
        self.assertIn('href="/camera/cam1/send2"', body)
        self.assertIn("Telegram", body)
        self.assertNotIn('name="send2telegram_video"', body)
        self.assertNotIn("Test video", body)

    def test_camera_history_page_renders_pairing_card(self) -> None:
        response = self.client.get("/camera/cam1/history")

        self.assertEqual(response.status_code, 200)
        body = response.get_data(as_text=True)
        self.assertIn("Finish Camera Setup", body)
        self.assertIn('name="redirect_page" value="history"', body)
        self.assertIn('action="/connect/cam1"', body)
        self.assertNotIn('action="/pair/cam1"', body)

    def test_camera_history_page_shows_pair_step_only_after_hub_connection_exists(self) -> None:
        self.hub.camera["hub_connected"] = True
        self.hub.camera["registered_on_hub"] = True
        self.hub.camera["api_token"] = ""
        self.hub.camera["is_paired"] = False
        self.hub.camera["api_status"] = "offline"

        response = self.client.get("/camera/cam1/history")

        self.assertEqual(response.status_code, 200)
        body = response.get_data(as_text=True)
        self.assertIn("Finish Camera Setup", body)
        self.assertIn("Step 2 of 2", body)
        self.assertNotIn('action="/connect/cam1"', body)
        self.assertIn('action="/pair/cam1"', body)
        self.assertIn("Install Pairing Bundle", body)

    def test_camera_pages_hide_setup_actions_for_legacy_registration_false_positive(self) -> None:
        self.hub.camera["present_on_mqtt_broker"] = True
        self.hub.camera["has_agent"] = False
        self.hub.camera["mqtt_command_status"] = "offline"
        self.hub.camera["mqtt_command_capable"] = False
        self.hub.camera["mqtt_command_last_error"] = "Camera did not respond to hub MQTT commands."

        response = self.client.get("/camera/cam1")

        self.assertEqual(response.status_code, 200)
        body = response.get_data(as_text=True)
        self.assertIn("Automatic setup is unavailable for this camera.", body)
        self.assertNotIn("Finish Camera Setup", body)
        self.assertNotIn("Step 1 of 2", body)
        self.assertNotIn('action="/connect/cam1"', body)
        self.assertNotIn('action="/pair/cam1"', body)
        self.assertNotIn('data-async-action="/pair/cam1"', body)

    def test_camera_history_page_hides_pair_step_for_legacy_registration_false_positive(self) -> None:
        self.hub.camera["hub_connected"] = True
        self.hub.camera["registered_on_hub"] = True
        self.hub.camera["api_token"] = ""
        self.hub.camera["is_paired"] = False
        self.hub.camera["api_status"] = "offline"
        self.hub.camera["has_agent"] = False
        self.hub.camera["mqtt_command_status"] = "offline"
        self.hub.camera["mqtt_command_capable"] = False
        self.hub.camera["mqtt_command_last_error"] = "Camera did not respond to hub MQTT commands."

        response = self.client.get("/camera/cam1/history")

        self.assertEqual(response.status_code, 200)
        body = response.get_data(as_text=True)
        self.assertIn("Automatic setup is unavailable for this camera.", body)
        self.assertNotIn("Finish Camera Setup", body)
        self.assertNotIn("Step 2 of 2", body)
        self.assertNotIn('action="/pair/cam1"', body)

    def test_camera_detail_shows_pair_quick_action_only_after_connection_step(self) -> None:
        response = self.client.get("/camera/cam1")

        self.assertEqual(response.status_code, 200)
        body = response.get_data(as_text=True)
        self.assertNotIn('data-async-action="/pair/cam1"', body)

        self.hub.camera["hub_connected"] = True
        self.hub.camera["registered_on_hub"] = True
        self.hub.camera["api_token"] = ""
        self.hub.camera["is_paired"] = False
        self.hub.camera["api_status"] = "offline"

        response = self.client.get("/camera/cam1")

        self.assertEqual(response.status_code, 200)
        body = response.get_data(as_text=True)
        self.assertIn('data-async-action="/pair/cam1"', body)

    def test_camera_history_page_hides_pairing_card_when_camera_is_paired(self) -> None:
        self.hub.camera["hub_connected"] = True
        self.hub.camera["registered_on_hub"] = True
        self.hub.camera["api_token"] = "paired-token"
        self.hub.camera["is_paired"] = True
        self.hub.camera["api_status"] = "online"

        response = self.client.get("/camera/cam1/history")

        self.assertEqual(response.status_code, 200)
        body = response.get_data(as_text=True)
        self.assertNotIn("Complete Pairing", body)
        self.assertNotIn('action="/connect/cam1"', body)
        self.assertNotIn('action="/pair/cam1"', body)

    def test_camera_send2_page_disables_controls_when_api_is_offline(self) -> None:
        self.hub.camera["api_status"] = "offline"
        self.hub.controls["native_controls_available"] = False
        self.hub.controls["native_controls_error"] = "GET /config failed: connection refused"
        self.hub.controls["native_send2_available"] = True
        self.hub.controls["native_send2_services"] = [
            {
                "name": "telegram",
                "label": "Telegram",
                "photo_supported": True,
                "video_supported": True,
                "motion_enabled": True,
                "photo_enabled": True,
                "video_enabled": True,
            }
        ]

        response = self.client.get("/camera/cam1/send2")

        self.assertEqual(response.status_code, 200)
        body = response.get_data(as_text=True)
        self.assertIn("Send2 settings are unavailable while the camera's Native API is offline.", body)
        self.assertIn("<fieldset disabled>", body)
        self.assertIn("Save Motion &amp; Send2 Settings", body)

    def test_camera_send2_page_ignores_stale_api_probe_when_live_controls_are_available(self) -> None:
        self.hub.camera["hub_connected"] = True
        self.hub.camera["registered_on_hub"] = True
        self.hub.camera["api_token"] = "paired-token"
        self.hub.camera["is_paired"] = True
        self.hub.camera["api_status"] = "offline"
        self.hub.camera["api_last_error"] = "IncompleteRead(589736 bytes read, 42117 more expected)"
        self.hub.controls["native_controls_available"] = True
        self.hub.controls["native_controls_error"] = ""
        self.hub.controls["native_send2_available"] = True
        self.hub.controls["native_send2_services"] = [
            {
                "name": "telegram",
                "label": "Telegram",
                "photo_supported": True,
                "video_supported": True,
                "motion_enabled": True,
                "photo_enabled": True,
                "video_enabled": True,
            }
        ]

        response = self.client.get("/camera/cam1/send2")

        self.assertEqual(response.status_code, 200)
        body = response.get_data(as_text=True)
        self.assertNotIn("Send2 settings are unavailable while the camera's Native API is offline.", body)
        self.assertNotIn("<fieldset disabled>", body)
        self.assertIn("Save Motion &amp; Send2 Settings", body)

    def test_camera_detail_renders_send_and_motion_shortcuts_when_camera_is_paired(self) -> None:
        self.hub.camera["hub_connected"] = True
        self.hub.camera["registered_on_hub"] = True
        self.hub.camera["api_token"] = "paired-token"
        self.hub.camera["is_paired"] = True
        self.hub.controls["native_motion_supported"] = True
        self.hub.controls["native_motion_enabled"] = True
        self.hub.controls["native_record_supported"] = True
        self.hub.controls["native_send2_available"] = True
        self.hub.controls["native_send2_services"] = [
            {
                "name": "telegram",
                "label": "Telegram",
                "photo_supported": True,
                "video_supported": False,
                "motion_enabled": True,
                "photo_enabled": True,
                "video_enabled": False,
                "photo_test_supported": True,
                "video_test_supported": False,
                "default_test_supported": False,
            }
        ]

        response = self.client.get("/camera/cam1")

        self.assertEqual(response.status_code, 200)
        body = response.get_data(as_text=True)
        self.assertIn('id="camera-control-bar"', body)
        self.assertIn('data-cb-motion-toggle', body)
        self.assertIn('href="/camera/cam1/send2#motion-detection-settings"', body)
        self.assertIn('href="#clip-recording"', body)
        self.assertIn('href="#timelapse-sources"', body)
        self.assertIn('href="/camera/cam1/settings#daynight-settings"', body)
        self.assertIn('href="/camera/cam1/settings#stream-audio-settings"', body)
        self.assertIn('id="cb-audio-btn"', body)
        self.assertIn('data-audio-stream-target="stream0"', body)
        self.assertIn('href="/camera/cam1/send2"', body)
        self.assertIn('data-send2-test-action="/send2-test/cam1/telegram"', body)
        self.assertIn('data-send2-service-label="Telegram photo"', body)
        self.assertIn('title="Run the default Send2 action"', body)

    def test_apply_supported_config_returns_stream_audio_delta(self) -> None:
        response = self.client.post(
            "/apply-supported-config/cam1",
            data={
                "stream0_audio_enabled_present": "1",
            },
            headers=self.json_headers,
        )

        self.assertEqual(response.status_code, 200)
        payload = response.get_json()
        self.assertEqual(payload["message"], "Settings applied: native config: accepted")
        self.assertIn("native_stream_controls", payload["camera"])
        self.assertEqual(payload["camera"]["native_stream_controls"], [{"name": "stream0", "audio_enabled": False}])
        self.assertFalse(self.hub.controls["native_stream_controls"][0]["audio_enabled"])

    def test_apply_supported_config_returns_daynight_threshold_and_schedule_delta(self) -> None:
        response = self.client.post(
            "/apply-supported-config/cam1",
            data={
                "daynight_enabled_present": "1",
                "daynight_enabled": "on",
                "daynight_force_mode": "",
                "daynight_total_gain_night_threshold": "2800",
                "daynight_total_gain_day_threshold": "250",
                "daynight_controls_color_present": "1",
                "daynight_controls_color": "on",
                "daynight_controls_ircut_present": "1",
                "daynight_controls_ir850_present": "1",
                "daynight_controls_ir940_present": "1",
                "daynight_controls_ir940": "on",
                "daynight_controls_white_present": "1",
                "daynight_schedule_enabled_present": "1",
                "daynight_schedule_enabled": "on",
                "daynight_schedule_start_at": "19:00",
                "daynight_schedule_stop_at": "06:30",
            },
            headers=self.json_headers,
        )

        self.assertEqual(response.status_code, 200)
        payload = response.get_json()
        self.assertEqual(payload["camera"]["native_daynight_total_gain_night_threshold"], "2800")
        self.assertEqual(payload["camera"]["native_daynight_total_gain_day_threshold"], "250")
        self.assertTrue(payload["camera"]["native_daynight_controls_color"])
        self.assertFalse(payload["camera"]["native_daynight_controls_ircut"])
        self.assertTrue(payload["camera"]["native_daynight_controls_ir940"])
        self.assertTrue(payload["camera"]["native_daynight_schedule_enabled"])
        self.assertEqual(payload["camera"]["native_daynight_schedule_start_at"], "19:00")
        self.assertEqual(payload["camera"]["native_daynight_schedule_stop_at"], "06:30")

    def test_apply_supported_config_returns_privacy_overlay_delta(self) -> None:
        response = self.client.post(
            "/apply-supported-config/cam1",
            data={
                "stream0_osd_privacy_enabled_present": "1",
                "stream0_osd_privacy_enabled": "on",
                "stream0_osd_privacy_text": "PRIVATE MODE",
                "stream0_osd_privacy_fill_color": "#112233",
                "stream0_osd_privacy_fill_alpha": "128",
                "stream0_osd_privacy_stroke_color": "#AABBCC",
                "stream0_osd_privacy_stroke_alpha": "64",
            },
            headers=self.json_headers,
        )

        self.assertEqual(response.status_code, 200)
        payload = response.get_json()
        self.assertEqual(payload["message"], "Settings applied: native config: accepted")
        self.assertIn("native_stream_controls", payload["camera"])
        self.assertEqual(
            payload["camera"]["native_stream_controls"],
            [
                {
                    "name": "stream0",
                    "osd_privacy_enabled": True,
                    "osd_privacy_text": "PRIVATE MODE",
                    "osd_privacy_fill_color_value": "#112233",
                    "osd_privacy_fill_alpha": "128",
                    "osd_privacy_stroke_color_value": "#AABBCC",
                    "osd_privacy_stroke_alpha": "64",
                }
            ],
        )
        self.assertEqual(
            self.hub.last_patch_payload["stream0"]["osd"]["privacy"],
            {
                "enabled": True,
                "text": "PRIVATE MODE",
                "fill_color": "#11223380",
                "stroke_color": "#AABBCC40",
            },
        )

    def test_camera_detail_removes_send2_sections(self) -> None:
        self.hub.camera["hub_connected"] = True
        self.hub.camera["registered_on_hub"] = True
        self.hub.camera["api_token"] = "paired-token"
        self.hub.camera["is_paired"] = True
        self.hub.controls["native_send2_available"] = True
        self.hub.controls["native_send2_services"] = [
            {
                "name": "telegram",
                "label": "Telegram",
                "photo_supported": True,
                "video_supported": True,
                "motion_enabled": True,
                "photo_enabled": True,
                "video_enabled": True,
                "photo_test_supported": True,
                "video_test_supported": True,
                "default_test_supported": False,
            }
        ]

        response = self.client.get("/camera/cam1")

        self.assertEqual(response.status_code, 200)
        body = response.get_data(as_text=True)
        self.assertNotIn("Send2 Services", body)
        self.assertNotIn("Send2 Test Output", body)
        self.assertIn("<i class=\"bi bi-send\"></i> Send", body)
        self.assertIn('data-send2-test-action="/send2-test/cam1/telegram"', body)
        self.assertIn("Send2 services and on-demand send tests live on the Send2 page.", body)

    def test_camera_detail_removes_long_term_imaging_and_motion_fields(self) -> None:
        response = self.client.get("/camera/cam1")

        self.assertEqual(response.status_code, 200)
        body = response.get_data(as_text=True)
        self.assertNotIn('for="image_brightness"', body)
        self.assertNotIn('for="image_contrast"', body)
        self.assertNotIn('for="image_saturation"', body)
        self.assertNotIn('for="image_sharpness"', body)
        self.assertNotIn('for="motion_enabled"', body)
        self.assertNotIn('for="send2_motion_sensitivity"', body)
        self.assertNotIn('for="send2_motion_cooldown"', body)
        self.assertNotIn("Supported Camera Controls", body)
        self.assertNotIn("Apply Supported Settings", body)
        self.assertNotIn("Back to Roster", body)
        self.assertIn("Persistent imaging, motion, and stream settings moved to the Settings page.", body)

    def test_camera_settings_post_redirects_back_to_settings(self) -> None:
        response = self.client.post(
            "/apply-supported-config/cam1",
            data={
                "redirect_page": "settings",
                "image_anti_flicker": "50hz",
                "image_hflip_present": "1",
                "image_hflip": "on",
                "image_vflip_present": "1",
                "stream0_enabled_present": "1",
                "stream0_enabled": "on",
                "stream0_width": "1280",
            },
            follow_redirects=False,
        )

        self.assertEqual(response.status_code, 302)
        self.assertEqual(response.headers["Location"], "/camera/cam1/settings")
        self.assertIsInstance(self.hub.last_patch_payload, dict)
        self.assertEqual(self.hub.last_patch_payload["image"]["anti_flicker"], "1")
        self.assertTrue(self.hub.last_patch_payload["image"]["hflip"])
        self.assertFalse(self.hub.last_patch_payload["image"]["vflip"])
        self.assertEqual(self.hub.last_patch_payload["stream0"]["width"], 1280)

    def test_camera_send2_post_redirects_back_to_send2(self) -> None:
        response = self.client.post(
            "/apply-supported-config/cam1",
            data={
                "redirect_page": "send2",
                "motion_send2telegram_present": "1",
                "motion_send2telegram": "on",
                "send2telegram_photo_present": "1",
                "send2telegram_photo": "on",
                "send2telegram_video_present": "1",
            },
            follow_redirects=False,
        )

        self.assertEqual(response.status_code, 302)
        self.assertEqual(response.headers["Location"], "/camera/cam1/send2")
        self.assertEqual(
            self.hub.last_send2_payload,
            {
                "motion": {"send2telegram": True},
                "telegram": {"send_photo": True, "send_video": False},
            },
        )

    def test_camera_send2_post_returns_clear_error_when_api_is_offline(self) -> None:
        self.hub.camera["api_status"] = "offline"

        response = self.client.post(
            "/apply-supported-config/cam1",
            data={
                "redirect_page": "send2",
                "motion_send2telegram_present": "1",
                "motion_send2telegram": "on",
            },
            headers=self.json_headers,
        )

        self.assertEqual(response.status_code, 500)
        payload = response.get_json()
        self.assertFalse(payload["ok"])
        self.assertEqual(payload["redirect_url"], "/camera/cam1/send2")
        self.assertIn("Native API is offline for this camera.", payload["message"])

    def test_camera_expert_config_redirects_without_expert_access(self) -> None:
        response = self.client.get("/camera/cam1/expert-config", follow_redirects=True)

        self.assertEqual(response.status_code, 200)
        body = response.get_data(as_text=True)
        self.assertIn("Expert access is required for the Native API Config Patch page.", body)
        self.assertIn("Long-Term Camera Settings", body)

    def test_camera_expert_config_redirects_for_advanced_users(self) -> None:
        self.hub.config["ui"]["competency_level"] = "advanced"

        response = self.client.get("/camera/cam1/expert-config", follow_redirects=True)

        self.assertEqual(response.status_code, 200)
        body = response.get_data(as_text=True)
        self.assertIn("Expert access is required for the Native API Config Patch page.", body)

    def test_camera_expert_config_renders_for_expert_users(self) -> None:
        self.hub.config["ui"]["competency_level"] = "expert"

        response = self.client.get("/camera/cam1/expert-config")

        self.assertEqual(response.status_code, 200)
        body = response.get_data(as_text=True)
        self.assertIn("Expert Config", body)
        self.assertIn('href="/camera/cam1/native-actions"', body)
        self.assertIn('href="/camera/cam1/history"', body)
        self.assertIn("Native API Config Patch", body)
        self.assertIn("Expert-only tool", body)
        self.assertIn("name=\"config_patch\"", body)

    def test_patch_config_requires_expert_access(self) -> None:
        response = self.client.post(
            "/patch-config/cam1",
            data={"config_patch": '{"image": {"brightness": 64}}'},
            headers=self.json_headers,
        )

        self.assertEqual(response.status_code, 403)
        payload = response.get_json()
        self.assertFalse(payload["ok"])
        self.assertEqual(payload["redirect_url"], "/camera/cam1/settings")

    def test_patch_config_redirects_back_to_expert_page(self) -> None:
        self.hub.config["ui"]["competency_level"] = "expert"

        response = self.client.post(
            "/patch-config/cam1",
            data={
                "redirect_page": "expert",
                "config_patch": '{"image": {"brightness": 64}}',
            },
            follow_redirects=False,
        )

        self.assertEqual(response.status_code, 302)
        self.assertEqual(response.headers["Location"], "/camera/cam1/expert-config")
        self.assertEqual(self.hub.last_patch_payload, {"image": {"brightness": 64}})

    def test_config_page_renders_user_competency_selector(self) -> None:
        response = self.client.get("/config")

        self.assertEqual(response.status_code, 200)
        body = response.get_data(as_text=True)
        self.assertIn("User competency", body)
        self.assertIn('name="ui_competency_level"', body)

    def test_config_page_saves_user_competency_level(self) -> None:
        response = self.client.post(
            "/config",
            data={
                "telegram_token": "123456:token",
                "telegram_api_url": "https://api.telegram.org",
                "telegram_polling_timeout": "30",
                "telegram_allowed_chat_ids": "",
                "telegram_allowed_usernames": "",
                "mqtt_host": "mqtt.local",
                "mqtt_port": "1883",
                "mqtt_username": "",
                "mqtt_password": "",
                "mqtt_keepalive": "60",
                "routing_command_topic": "thingino/cam/{camera_id}/cmd",
                "routing_reply_topic": "thingino/cam/+/reply",
                "routing_registration_topic": "thingino/cam/+/hello",
                "routing_event_topic": "thingino/cam/+/event",
                "routing_state_topic": "thingino/cam/+/state",
                "ui_username": "",
                "ui_password": "",
                "ui_competency_level": "expert",
                "ui_registration_stale_after_seconds": "0",
                "ui_snapshot_heartbeat_interval_seconds": "0",
                "ui_api_probe_interval_seconds": "0",
                "ui_snapshot_heartbeat_timeout_seconds": "5",
                "ui_snapshot_cache_stale_after_seconds": "3600",
                "history_path": "",
                "history_recent_actions_limit": "20",
                "history_max_action_events_per_camera": "1000",
                "history_max_state_samples_per_camera": "5000",
                "cameras_yaml": "",
                "action": "save",
            },
            follow_redirects=False,
        )

        self.assertEqual(response.status_code, 302)
        self.assertEqual(response.headers["Location"], "/config")
        self.assertIsNotNone(self.hub.saved_config)
        self.assertEqual(self.hub.saved_config["ui"]["competency_level"], "expert")

    def test_camera_native_actions_page_renders_history(self) -> None:
        self.hub.config["ui"]["competency_level"] = "advanced"
        self.hub.camera["native_action_history"] = [
            {
                "at": "now",
                "action": "snapshot",
                "status": "success",
                "source": "hub",
                "detail": "queued",
            }
        ]

        response = self.client.get("/camera/cam1/native-actions")

        self.assertEqual(response.status_code, 200)
        body = response.get_data(as_text=True)
        self.assertIn("Recent Native Actions", body)
        self.assertIn("snapshot", body)
        self.assertIn("queued", body)

    def test_camera_native_actions_redirects_without_advanced_access(self) -> None:
        response = self.client.get("/camera/cam1/native-actions", follow_redirects=True)

        self.assertEqual(response.status_code, 200)
        body = response.get_data(as_text=True)
        self.assertIn("Advanced access is required for this camera maintenance page.", body)
        self.assertIn("Long-Term Camera Settings", body)

    def test_pair_camera_returns_success_summary(self) -> None:
        response = self.client.post("/pair/cam1", headers=self.json_headers)

        self.assertEqual(response.status_code, 200)
        payload = response.get_json()
        self.assertTrue(payload["ok"])
        self.assertIn("Pairing installed for cam1", payload["message"])
        self.assertEqual(payload["redirect_url"], "/camera/cam1")

    def test_pair_camera_rejects_false_positive_registration(self) -> None:
        self.hub.camera["mqtt_command_status"] = "offline"
        self.hub.camera["mqtt_command_capable"] = False
        self.hub.camera["mqtt_command_last_error"] = "Camera did not respond to hub MQTT commands."

        response = self.client.post("/pair/cam1", headers=self.json_headers)

        self.assertEqual(response.status_code, 500)
        payload = response.get_json()
        self.assertFalse(payload["ok"])
        self.assertIn("Camera did not respond to hub MQTT commands.", payload["message"])

    def test_pair_camera_preserves_requested_subpage_redirect(self) -> None:
        response = self.client.post(
            "/pair/cam1",
            data={"redirect_page": "history"},
            headers=self.json_headers,
        )

        self.assertEqual(response.status_code, 200)
        payload = response.get_json()
        self.assertTrue(payload["ok"])
        self.assertEqual(payload["redirect_url"], "/camera/cam1/history")

    def test_connect_camera_returns_success_summary(self) -> None:
        response = self.client.post(
            "/connect/cam1",
            data={"onvif_username": "thingino", "onvif_password": "thingino"},
            headers=self.json_headers,
        )

        self.assertEqual(response.status_code, 200)
        payload = response.get_json()
        self.assertTrue(payload["ok"])
        self.assertIn("Connected cam1 to the hub.", payload["message"])
        self.assertEqual(payload["redirect_url"], "/camera/cam1")

    def test_connect_camera_leaves_camera_in_pair_step(self) -> None:
        self.hub.camera["api_status"] = "offline"
        response = self.client.post(
            "/connect/cam1",
            data={"onvif_username": "thingino", "onvif_password": "thingino"},
            headers=self.json_headers,
        )

        self.assertEqual(response.status_code, 200)

        response = self.client.get("/camera/cam1")

        self.assertEqual(response.status_code, 200)
        body = response.get_data(as_text=True)
        self.assertIn("Step 2 of 2", body)
        self.assertIn("Install Pairing Bundle", body)
        self.assertNotIn("Step 1 of 2", body)
        self.assertNotIn("Connect to Hub", body)

    def test_connect_camera_rejects_false_positive_registration(self) -> None:
        self.hub.camera["mqtt_command_status"] = "offline"
        self.hub.camera["mqtt_command_capable"] = False
        self.hub.camera["mqtt_command_last_error"] = "Camera did not respond to hub MQTT commands."

        response = self.client.post(
            "/connect/cam1",
            data={"onvif_username": "thingino", "onvif_password": "thingino"},
            headers=self.json_headers,
        )

        self.assertEqual(response.status_code, 500)
        payload = response.get_json()
        self.assertFalse(payload["ok"])
        self.assertIn("Camera did not respond to hub MQTT commands.", payload["message"])

    def test_connect_camera_preserves_requested_subpage_redirect(self) -> None:
        response = self.client.post(
            "/connect/cam1",
            data={
                "redirect_page": "info",
                "onvif_username": "thingino",
                "onvif_password": "thingino",
            },
            headers=self.json_headers,
        )

        self.assertEqual(response.status_code, 200)
        payload = response.get_json()
        self.assertTrue(payload["ok"])
        self.assertEqual(payload["redirect_url"], "/camera/cam1/info")

    def test_hydrate_payload_route_returns_delta_payload(self) -> None:
        response = self.client.get("/camera/cam1/hydrate-payload", headers=self.json_headers)

        self.assertEqual(response.status_code, 200)
        payload = response.get_json()
        self.assertTrue(payload["ok"])
        self.assertEqual(payload["camera"]["api_status"], "online")
        self.assertIn("native_daynight_requested_mode", payload["camera"])
        self.assertNotIn("camera_id", payload["camera"])

    def test_daynight_returns_minimal_delta(self) -> None:
        response = self.client.post(
            "/daynight/cam1",
            data={"daynight_mode": "day"},
            headers=self.json_headers,
        )

        self.assertEqual(response.status_code, 200)
        payload = response.get_json()
        self.assertEqual(payload["message"], "Day/night set to day.")
        self.assertEqual(payload["camera"], {"native_daynight_requested_mode": "day"})

    def test_privacy_returns_minimal_delta(self) -> None:
        response = self.client.post(
            "/privacy/cam1",
            data={"privacy_enabled": "true", "privacy_channel": "all"},
            headers=self.json_headers,
        )

        self.assertEqual(response.status_code, 200)
        payload = response.get_json()
        self.assertEqual(payload["message"], "Privacy enabled.")
        self.assertEqual(payload["camera"], {"native_privacy_enabled": True})

    def test_apply_supported_config_returns_latest_action_delta(self) -> None:
        response = self.client.post(
            "/apply-supported-config/cam1",
            data={"motion_enabled_present": "1", "motion_enabled": "on"},
            headers=self.json_headers,
        )

        self.assertEqual(response.status_code, 200)
        payload = response.get_json()
        self.assertEqual(payload["message"], "Settings applied: native config: accepted")
        self.assertTrue(payload["camera"]["native_motion_enabled"])
        self.assertIn("native_action_history_latest", payload["camera"])
        self.assertNotIn("native_action_history", payload["camera"])
        self.assertEqual(payload["camera"]["native_action_history_latest"]["detail"], "motion")

    def test_refresh_api_is_queued_and_has_no_camera_blob(self) -> None:
        response = self.client.post("/refresh-api/cam1", headers=self.json_headers)

        self.assertEqual(response.status_code, 200)
        payload = response.get_json()
        self.assertTrue(payload["ok"])
        self.assertEqual(payload["message"], "Native API refresh queued.")
        self.assertNotIn("camera", payload)

    def test_event_feed_returns_recent_entries(self) -> None:
        response = self.client.get("/events/feed", headers=self.json_headers)

        self.assertEqual(response.status_code, 200)
        payload = response.get_json()
        self.assertTrue(payload["ok"])
        self.assertEqual(len(payload["events"]), 1)
        self.assertEqual(payload["events"][0]["name"], "motion started")

    def test_dashboard_links_to_status_events_and_enroll_pages(self) -> None:
        response = self.client.get("/")

        self.assertEqual(response.status_code, 200)
        body = response.get_data(as_text=True)
        self.assertIn("href=\"/status\"", body)
        self.assertIn("href=\"/events\"", body)
        self.assertIn("href=\"/enroll\"", body)
        self.assertIn("bulk-action-form", body)
        self.assertNotIn("action=\"/delete/cam1\"", body)
        self.assertNotIn(">Delete<", body)
        self.assertNotIn("enroll-camera-form", body)
        self.assertNotIn("dashboard-event-feed", body)
        self.assertNotIn("Polling OK", body)
        self.assertNotIn("Connected", body)
        self.assertIn('src="/snapshot/cam1?stream=ch1&amp;v=1"', body)

    def test_snapshot_route_fetches_live_when_cache_missing(self) -> None:
        upstream = FakeUpstreamResponse(b"\xff\xd8\xff\xe0", {"Content-Type": "image/jpeg"})
        with mock.patch("app.web.urllib.request.urlopen", return_value=upstream) as mocked_urlopen:
            response = self.client.get("/snapshot/cam1?stream=ch0")

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.headers["Content-Type"], "image/jpeg")
        request_to_camera = mocked_urlopen.call_args[0][0]
        self.assertEqual(request_to_camera.full_url, "http://192.168.1.2/x/ch0.jpg")

    def test_snapshot_route_falls_back_to_ch0_when_ch1_unavailable(self) -> None:
        self.hub.camera["snapshot_ch1_url"] = ""
        upstream = FakeUpstreamResponse(b"\xff\xd8\xff\xe0", {"Content-Type": "image/jpeg"})
        with mock.patch("app.web.urllib.request.urlopen", return_value=upstream) as mocked_urlopen:
            response = self.client.get("/snapshot/cam1?stream=ch1")

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.headers["Content-Type"], "image/jpeg")
        request_to_camera = mocked_urlopen.call_args[0][0]
        self.assertEqual(request_to_camera.full_url, "http://192.168.1.2/x/ch0.jpg")

    def test_snapshot_route_returns_stub_when_ch1_and_ch0_fetch_fail(self) -> None:
        with mock.patch("app.web.urllib.request.urlopen", side_effect=Exception("offline")):
            response = self.client.get("/snapshot/cam1?stream=ch1")

        self.assertEqual(response.status_code, 200)
        self.assertIn("image/svg+xml", response.headers["Content-Type"])

    def test_snapshot_route_falls_back_when_ch1_response_is_empty(self) -> None:
        side_effect = [
            FakeUpstreamResponse(b"", {"Content-Type": "image/jpeg"}),
            FakeUpstreamResponse(b"\xff\xd8\xff\xe0", {"Content-Type": "image/jpeg"}),
        ]
        with mock.patch("app.web.urllib.request.urlopen", side_effect=side_effect) as mocked_urlopen:
            response = self.client.get("/snapshot/cam1?stream=ch1")

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.headers["Content-Type"], "image/jpeg")
        first_request = mocked_urlopen.call_args_list[0][0][0]
        second_request = mocked_urlopen.call_args_list[1][0][0]
        self.assertEqual(first_request.full_url, "http://192.168.1.2/x/ch1.jpg")
        self.assertEqual(second_request.full_url, "http://192.168.1.2/x/ch0.jpg")

    def test_snapshot_route_ignores_empty_cached_snapshot(self) -> None:
        with tempfile.NamedTemporaryFile(suffix=".jpg") as cached_file:
            with mock.patch.object(self.hub, "get_cached_snapshot_for_ui", return_value=cached_file.name):
                with mock.patch("app.web.urllib.request.urlopen", side_effect=Exception("offline")):
                    response = self.client.get("/snapshot/cam1?stream=ch1")

        self.assertEqual(response.status_code, 200)
        self.assertIn("image/svg+xml", response.headers["Content-Type"])

    def test_delete_camera_returns_success_summary(self) -> None:
        response = self.client.post("/delete/cam1", headers=self.json_headers)

        self.assertEqual(response.status_code, 200)
        payload = response.get_json()
        self.assertTrue(payload["ok"])
        self.assertIn("Removed cam1 from the roster", payload["message"])
        self.assertEqual(payload["redirect_url"], "/")

    def test_status_page_renders_status_cards(self) -> None:
        response = self.client.get("/status")

        self.assertEqual(response.status_code, 200)
        body = response.get_data(as_text=True)
        self.assertIn("System Status", body)
        self.assertIn("Telegram", body)
        self.assertIn("MQTT", body)
        self.assertIn("Native API", body)
        self.assertIn("History", body)
        self.assertIn("Reload Config", body)

    def test_events_page_renders_live_event_feed(self) -> None:
        response = self.client.get("/events")

        self.assertEqual(response.status_code, 200)
        body = response.get_data(as_text=True)
        self.assertIn("Live Event Feed", body)
        self.assertIn("dashboard-event-feed", body)
        self.assertIn("motion started", body)
        self.assertIn("new EventSource", body)

    def test_enroll_page_renders_enrollment_form(self) -> None:
        response = self.client.get("/enroll")

        self.assertEqual(response.status_code, 200)
        body = response.get_data(as_text=True)
        self.assertIn("Enroll Camera", body)
        self.assertIn("enroll-camera-form", body)
        self.assertIn("enroll-pairing-button", body)
        self.assertIn("enroll-pairing-install-button", body)
        self.assertIn("enroll-probe-button", body)
        self.assertNotIn("enroll_camera_id", body)
        self.assertNotIn("enroll_name", body)

    def test_dashboard_renders_and_clears_bulk_action_results(self) -> None:
        with self.client.session_transaction() as session:
            session["dashboard_bulk_action_result"] = {
                "action": "restart-streaming",
                "total": 1,
                "success_count": 0,
                "error_count": 1,
                "results": [{"camera_id": "cam1", "status": "error", "detail": "camera offline"}],
            }

        response = self.client.get("/")

        self.assertEqual(response.status_code, 200)
        body = response.get_data(as_text=True)
        self.assertIn("Bulk Results", body)
        self.assertIn("camera offline", body)
        self.assertIn("Retry", body)

        second_response = self.client.get("/")
        second_body = second_response.get_data(as_text=True)
        self.assertNotIn("Bulk Results", second_body)

    def test_camera_history_renders_config_change_entries(self) -> None:
        response = self.client.get("/camera/cam1/history")

        self.assertEqual(response.status_code, 200)
        body = response.get_data(as_text=True)
        self.assertIn("Network Reachability", body)
        self.assertIn("Config changes only", body)
        self.assertIn("enrollment create", body)
        self.assertIn("/hub/enrollment/ip", body)

    def test_bulk_action_returns_summary(self) -> None:
        response = self.client.post(
            "/bulk-action",
            data={"bulk_action": "refresh-api", "camera_ids": ["cam1"]},
            headers=self.json_headers,
        )

        self.assertEqual(response.status_code, 200)
        payload = response.get_json()
        self.assertTrue(payload["ok"])
        self.assertEqual(payload["result"]["success_count"], 1)
        self.assertEqual(payload["result"]["action"], "refresh-api")

    def test_bulk_action_redirect_sets_dashboard_results(self) -> None:
        response = self.client.post(
            "/bulk-action",
            data={"bulk_action": "restart-streaming", "camera_ids": ["cam1"]},
            follow_redirects=True,
        )

        self.assertEqual(response.status_code, 200)
        body = response.get_data(as_text=True)
        self.assertIn("Bulk Results", body)
        self.assertIn("camera offline", body)
        self.assertIn("Retry", body)

    def test_enroll_camera_returns_new_camera_summary(self) -> None:
        response = self.client.post(
            "/enroll",
            data={
                "ip": "192.168.1.3",
            },
            headers=self.json_headers,
        )

        self.assertEqual(response.status_code, 200)
        payload = response.get_json()
        self.assertTrue(payload["ok"])
        self.assertEqual(payload["result"]["camera_id"], "cam2")
        self.assertIn("Connected cam2 to the hub.", payload["message"])

    def test_enroll_probe_returns_probe_summary(self) -> None:
        response = self.client.post(
            "/enroll/probe",
            data={
                "ip": "192.168.1.3",
            },
            headers=self.json_headers,
        )

        self.assertEqual(response.status_code, 200)
        payload = response.get_json()
        self.assertTrue(payload["ok"])
        self.assertEqual(payload["result"]["camera_id"], "cam2")
        self.assertTrue(payload["result"]["api"]["ok"])
        self.assertFalse(payload["result"]["onvif"]["ok"])

    def test_pairing_bundle_returns_bootstrap_payload(self) -> None:
        response = self.client.post(
            "/enroll/pairing-bundle",
            data={
                "ip": "192.168.1.3",
            },
            headers=self.json_headers,
        )

        self.assertEqual(response.status_code, 200)
        payload = response.get_json()
        self.assertTrue(payload["ok"])
        self.assertEqual(payload["result"]["camera_id"], "cam2")
        self.assertEqual(payload["result"]["api_token"], "pairing-token-123")
        self.assertEqual(payload["result"]["bootstrap_payload"]["agent"]["listen"], "0.0.0.0")

    def test_pairing_install_returns_mqtt_confirmation(self) -> None:
        response = self.client.post(
            "/enroll/pairing-install",
            data={
                "ip": "192.168.1.3",
            },
            headers=self.json_headers,
        )

        self.assertEqual(response.status_code, 200)
        payload = response.get_json()
        self.assertTrue(payload["ok"])
        self.assertEqual(payload["result"]["status"], "success")
        self.assertTrue(payload["result"]["mqtt"]["reply_received"])
        self.assertEqual(payload["result"]["mqtt"]["reply_text"], "Agent bootstrap installed")


if __name__ == "__main__":
    unittest.main()
