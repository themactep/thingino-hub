import copy
import http.client
import json
import logging
import mimetypes
import os
import ipaddress
import base64
import hashlib
import signal
import secrets
import ssl
import sys
import threading
import time
import urllib.parse
import urllib.request
import uuid
import json as json_module
from dataclasses import dataclass, replace
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from xml.etree import ElementTree
from xml.sax.saxutils import escape as xml_escape

import paho.mqtt.client as mqtt
import yaml

from .camera_api import CameraApiClient, CameraApiError
from .history_store import HistoryStore
from .web import WebServer, create_web_app

LOG = logging.getLogger("telegrambothub")
DEFAULT_CONFIG = os.environ.get("HUB_CONFIG", "/config/config.yaml")
DEFAULT_STATE_FILENAME = "camera-state.yaml"
DEFAULT_THINGINO_USERNAME = "thingino"
DEFAULT_THINGINO_PASSWORD = "thingino"
SEND2_SERVICES = [
    ("email", "Email"),
    ("ftp", "FTP"),
    ("telegram", "Telegram"),
    ("mqtt", "MQTT"),
    ("webhook", "Webhook"),
    ("storage", "Storage"),
    ("ntfy", "Ntfy"),
    ("gphotos", "Google Photos"),
]
SEND2_REQUIRED_FIELDS = {
    "email": ["host", "port", "from_address", "to_address"],
    "ftp": ["host", "port"],
    "telegram": ["token", "channel"],
    "mqtt": ["host", "port", "topic"],
    "webhook": ["url"],
    "storage": ["mount"],
    "ntfy": ["topic"],
    "gphotos": ["client_id", "client_secret", "refresh_token"],
}


@dataclass(frozen=True)
class Camera:
    camera_id: str
    name: str
    ip: str = ""
    snapshot_url: str = ""
    api_key: str = ""
    api_base_url: str = ""
    api_token: str = ""
    onvif_endpoint: str = ""
    onvif_username: str = ""
    onvif_password: str = ""
    hostname: str = ""
    status: str = "unknown"
    last_registration_at: int | None = None
    probe_status: str = "unknown"
    last_probe_at: int | None = None
    last_snapshot_ok_at: int | None = None
    last_probe_error: str = ""
    snapshot_cache_path: str = ""
    onvif_manufacturer: str = ""
    onvif_model: str = ""
    onvif_firmware_version: str = ""
    onvif_serial_number: str = ""
    onvif_hardware_id: str = ""
    onvif_last_ok_at: int | None = None
    onvif_last_error: str = ""
    api_status: str = "unknown"
    api_last_ok_at: int | None = None
    api_last_error: str = ""
    api_device_name: str = ""
    api_device_model: str = ""
    api_streamer: str = ""
    api_version: str = ""


class TelegramApi:
    def __init__(self, token: str, api_url: str) -> None:
        self.token = token
        self.api_url = api_url.rstrip("/")

    def _request(self, method: str, payload: dict[str, Any] | None = None) -> dict[str, Any]:
        url = f"{self.api_url}/bot{self.token}/{method}"
        data = None
        headers = {}
        if payload is not None:
            data = json.dumps(payload).encode("utf-8")
            headers["Content-Type"] = "application/json"
        request = urllib.request.Request(url, data=data, headers=headers, method="POST" if data else "GET")
        with urllib.request.urlopen(request, timeout=60) as response:
            body = response.read().decode("utf-8")
        parsed = json.loads(body)
        if not parsed.get("ok"):
            raise RuntimeError(parsed.get("description", f"Telegram API call failed: {method}"))
        return parsed

    def get_updates(self, offset: int | None, timeout: int) -> list[dict[str, Any]]:
        query = {"timeout": timeout}
        if offset is not None:
            query["offset"] = offset
        url = f"{self.api_url}/bot{self.token}/getUpdates?{urllib.parse.urlencode(query)}"
        request = urllib.request.Request(url, method="GET")
        with urllib.request.urlopen(request, timeout=timeout + 10) as response:
            body = response.read().decode("utf-8")
        parsed = json.loads(body)
        if not parsed.get("ok"):
            raise RuntimeError(parsed.get("description", "getUpdates failed"))
        return parsed.get("result", [])

    def send_message(self, chat_id: int, text: str) -> None:
        self._request("sendMessage", {"chat_id": chat_id, "text": text})

    def send_photo(self, chat_id: int, photo: bytes, filename: str, caption: str = "") -> None:
        boundary = f"telegrambothub-{uuid.uuid4().hex}"
        parts = []
        fields = [("chat_id", str(chat_id))]
        if caption:
            fields.append(("caption", caption))
        for name, value in fields:
            parts.append(f"--{boundary}\r\n".encode("utf-8"))
            parts.append(f'Content-Disposition: form-data; name="{name}"\r\n\r\n'.encode("utf-8"))
            parts.append(value.encode("utf-8"))
            parts.append(b"\r\n")

        content_type = mimetypes.guess_type(filename)[0] or "application/octet-stream"
        parts.append(f"--{boundary}\r\n".encode("utf-8"))
        parts.append(
            f'Content-Disposition: form-data; name="photo"; filename="{filename}"\r\n'.encode("utf-8")
        )
        parts.append(f"Content-Type: {content_type}\r\n\r\n".encode("utf-8"))
        parts.append(photo)
        parts.append(b"\r\n")
        parts.append(f"--{boundary}--\r\n".encode("utf-8"))
        body = b"".join(parts)

        url = f"{self.api_url}/bot{self.token}/sendPhoto"
        request = urllib.request.Request(
            url,
            data=body,
            headers={"Content-Type": f"multipart/form-data; boundary={boundary}"},
            method="POST",
        )
        with urllib.request.urlopen(request, timeout=60) as response:
            parsed = json.loads(response.read().decode("utf-8"))
        if not parsed.get("ok"):
            raise RuntimeError(parsed.get("description", "Telegram API call failed: sendPhoto"))


class Hub:
    def __init__(self, config: dict[str, Any], config_path: str) -> None:
        self.config_path = config_path
        self.state_path = Path(
            os.environ.get("HUB_STATE_PATH")
            or (Path(config_path).resolve().parent / DEFAULT_STATE_FILENAME)
        )
        self.stop_event = threading.Event()
        self.state_lock = threading.RLock()
        self.reply_lock = threading.Lock()
        self.snapshot_cache_dir = Path(os.environ.get("HUB_SNAPSHOT_CACHE_DIR", "/tmp/thinginohub-snapshot-cache"))
        self.onvif_refreshing: set[str] = set()
        self.api_refreshing: set[str] = set()
        self.pending_by_request: dict[str, dict[str, Any]] = {}
        self.last_chat_by_camera: dict[str, int] = {}
        self.native_action_history_by_camera: dict[str, list[dict[str, Any]]] = {}
        self.optimistic_supported_controls_by_camera: dict[str, tuple[float, dict[str, Any]]] = {}
        self.history_store: HistoryStore | None = None
        self.history_db_path = ""
        self.history_enabled = True
        self.history_recent_actions_limit = 20
        self.history_max_action_events_per_camera = 1000
        self.history_max_state_samples_per_camera = 5000
        self.cameras, self.static_camera_ids = self._load_state()
        self.last_telegram_ok_at: float | None = None
        self.last_telegram_error = ""
        self.last_mqtt_error = ""
        self.last_reload_at: float | None = None
        self.mqtt_connected = False
        self.mqtt_client: mqtt.Client | None = None
        self._apply_config(config)
        self.update_offset: int | None = None

    def _camera_api_base_url(self, camera: Camera) -> str:
        base_url = camera.api_base_url.strip()
        if base_url:
            return base_url
        if camera.ip:
            return f"http://{camera.ip}/api/v1"

        snapshot_url = camera.snapshot_url.strip()
        if not snapshot_url:
            return ""

        parsed = urllib.parse.urlsplit(snapshot_url)
        if not parsed.netloc:
            return ""
        scheme = parsed.scheme or "http"
        return urllib.parse.urlunsplit((scheme, parsed.netloc, "/api/v1", "", ""))

    def _camera_api_token(self, camera: Camera) -> str:
        return camera.api_token.strip()

    def _camera_api_client(self, camera: Camera) -> CameraApiClient:
        base_url = self._camera_api_base_url(camera)
        if not base_url:
            raise RuntimeError(f"Native API base URL is not configured for {camera.name}")
        return CameraApiClient(base_url, token=self._camera_api_token(camera), timeout=self.snapshot_heartbeat_timeout_seconds)

    def _load_cameras(self) -> dict[str, Camera]:
        cameras = {}
        for entry in self.config.get("cameras", []):
            camera_id = str(entry["id"]).strip().lower()
            existing = self.cameras.get(camera_id)
            cameras[camera_id] = self._camera_with_runtime_state(
                Camera(
                camera_id=camera_id,
                name=str(entry.get("name") or camera_id),
                ip=str(entry.get("ip") or "").strip(),
                snapshot_url=str(entry.get("snapshot_url") or "").strip(),
                api_key=str(entry.get("api_key") or "").strip(),
                api_base_url=str(entry.get("api_base_url") or "").strip(),
                api_token=str(entry.get("api_token") or "").strip(),
                onvif_endpoint=str(entry.get("onvif_endpoint") or "").strip(),
                onvif_username=str(entry.get("onvif_username") or "").strip(),
                onvif_password=str(entry.get("onvif_password") or "").strip(),
                hostname=str(entry.get("hostname") or "").strip(),
                status=str(entry.get("status") or "static").strip() or "static",
                last_registration_at=entry.get("last_registration_at"),
                ),
                existing,
            )
        return cameras

    def _configured_camera_name(self, camera_id: str) -> str:
        resolved = str(camera_id or "").strip().lower()
        if not resolved:
            return ""

        with self.state_lock:
            configured_cameras = list(self.config.get("cameras", []))

        for entry in configured_cameras:
            if str(entry.get("id") or "").strip().lower() != resolved:
                continue
            return str(entry.get("name") or "").strip()

        return ""

    def _load_state(self) -> tuple[dict[str, Camera], set[str]]:
        if not self.state_path.exists():
            return {}, set()

        try:
            with self.state_path.open("r", encoding="utf-8") as handle:
                payload = yaml.safe_load(handle) or {}
        except FileNotFoundError:
            return {}, set()
        except Exception:
            LOG.warning("Failed to load camera state from %s", self.state_path, exc_info=True)
            return {}, set()

        if not isinstance(payload, dict):
            LOG.warning("Ignoring invalid camera state payload in %s", self.state_path)
            return {}, set()

        cameras: dict[str, Camera] = {}
        for entry in payload.get("cameras") or []:
            if not isinstance(entry, dict):
                continue
            camera_id = str(entry.get("camera_id") or entry.get("id") or "").strip().lower()
            if not camera_id:
                continue
            cameras[camera_id] = Camera(
                camera_id=camera_id,
                name=str(entry.get("name") or camera_id).strip() or camera_id,
                ip=str(entry.get("ip") or "").strip(),
                snapshot_url=str(entry.get("snapshot_url") or "").strip(),
                api_key=str(entry.get("api_key") or "").strip(),
                api_base_url=str(entry.get("api_base_url") or "").strip(),
                api_token=str(entry.get("api_token") or "").strip(),
                onvif_endpoint=str(entry.get("onvif_endpoint") or "").strip(),
                onvif_username=str(entry.get("onvif_username") or "").strip(),
                hostname=str(entry.get("hostname") or "").strip(),
                status=str(entry.get("status") or "unknown").strip() or "unknown",
                last_registration_at=self._coerce_int(entry.get("last_registration_at")),
                probe_status=str(entry.get("probe_status") or "unknown").strip() or "unknown",
                last_probe_at=self._coerce_int(entry.get("last_probe_at")),
                last_snapshot_ok_at=self._coerce_int(entry.get("last_snapshot_ok_at")),
                last_probe_error=str(entry.get("last_probe_error") or "").strip(),
                snapshot_cache_path=str(entry.get("snapshot_cache_path") or "").strip(),
                onvif_manufacturer=str(entry.get("onvif_manufacturer") or "").strip(),
                onvif_model=str(entry.get("onvif_model") or "").strip(),
                onvif_firmware_version=str(entry.get("onvif_firmware_version") or "").strip(),
                onvif_serial_number=str(entry.get("onvif_serial_number") or "").strip(),
                onvif_hardware_id=str(entry.get("onvif_hardware_id") or "").strip(),
                onvif_last_ok_at=self._coerce_int(entry.get("onvif_last_ok_at")),
                onvif_last_error=str(entry.get("onvif_last_error") or "").strip(),
                api_status=str(entry.get("api_status") or "unknown").strip() or "unknown",
                api_last_ok_at=self._coerce_int(entry.get("api_last_ok_at")),
                api_last_error=str(entry.get("api_last_error") or "").strip(),
                api_device_name=str(entry.get("api_device_name") or "").strip(),
                api_device_model=str(entry.get("api_device_model") or "").strip(),
                api_streamer=str(entry.get("api_streamer") or "").strip(),
                api_version=str(entry.get("api_version") or "").strip(),
            )

        static_camera_ids = {
            str(camera_id).strip().lower()
            for camera_id in payload.get("static_camera_ids") or []
            if str(camera_id).strip()
        }
        return cameras, static_camera_ids

    def _serialize_camera(self, camera: Camera) -> dict[str, Any]:
        return {
            "camera_id": camera.camera_id,
            "name": camera.name,
            "ip": camera.ip,
            "snapshot_url": camera.snapshot_url,
            "api_key": camera.api_key,
            "api_base_url": camera.api_base_url,
            "api_token": camera.api_token,
            "onvif_endpoint": camera.onvif_endpoint,
            "onvif_username": camera.onvif_username,
            "hostname": camera.hostname,
            "status": camera.status,
            "last_registration_at": camera.last_registration_at,
            "probe_status": camera.probe_status,
            "last_probe_at": camera.last_probe_at,
            "last_snapshot_ok_at": camera.last_snapshot_ok_at,
            "last_probe_error": camera.last_probe_error,
            "snapshot_cache_path": camera.snapshot_cache_path,
            "onvif_manufacturer": camera.onvif_manufacturer,
            "onvif_model": camera.onvif_model,
            "onvif_firmware_version": camera.onvif_firmware_version,
            "onvif_serial_number": camera.onvif_serial_number,
            "onvif_hardware_id": camera.onvif_hardware_id,
            "onvif_last_ok_at": camera.onvif_last_ok_at,
            "onvif_last_error": camera.onvif_last_error,
            "api_status": camera.api_status,
            "api_last_ok_at": camera.api_last_ok_at,
            "api_last_error": camera.api_last_error,
            "api_device_name": camera.api_device_name,
            "api_device_model": camera.api_device_model,
            "api_streamer": camera.api_streamer,
            "api_version": camera.api_version,
        }

    def _persist_state(self) -> None:
        with self.state_lock:
            payload = {
                "version": 1,
                "static_camera_ids": sorted(self.static_camera_ids),
                "cameras": [
                    self._serialize_camera(self.cameras[camera_id])
                    for camera_id in sorted(self.cameras)
                ],
            }

        try:
            self.state_path.parent.mkdir(parents=True, exist_ok=True)
            temp_path = self.state_path.with_name(f"{self.state_path.name}.{uuid.uuid4().hex}.tmp")
            with temp_path.open("w", encoding="utf-8") as handle:
                yaml.safe_dump(payload, handle, sort_keys=False)
            temp_path.replace(self.state_path)
        except Exception:
            LOG.warning("Failed to persist camera state to %s", self.state_path, exc_info=True)

    def _apply_config(self, config: dict[str, Any]) -> None:
        with self.state_lock:
            self.snapshot_cache_dir.mkdir(parents=True, exist_ok=True)
            self.config = config
            static_cameras = self._load_cameras()
            dynamic_cameras = {camera_id: camera for camera_id, camera in self.cameras.items() if camera_id not in self.static_camera_ids}
            dynamic_cameras.update(static_cameras)
            self.cameras = dynamic_cameras
            self.static_camera_ids = set(static_cameras)

            telegram_cfg = config["telegram"]
            self.telegram = TelegramApi(telegram_cfg["token"], telegram_cfg.get("api_url", "https://api.telegram.org"))
            self.allowed_chat_ids = set(int(value) for value in telegram_cfg.get("allowed_chat_ids", []))
            self.allowed_usernames = set(str(value) for value in telegram_cfg.get("allowed_usernames", []))
            self.polling_timeout = int(telegram_cfg.get("polling_timeout", 30))
            self.command_topic_template = config["routing"]["command_topic"]
            self.reply_topic = config["routing"]["reply_topic"]
            self.registration_topic = config["routing"].get("registration_topic", "thingino/cam/+/hello")
            self.registration_stale_after_seconds = max(0, int(config.get("ui", {}).get("registration_stale_after_seconds", 0)))
            self.snapshot_heartbeat_interval_seconds = max(0, int(config.get("ui", {}).get("snapshot_heartbeat_interval_seconds", 60)))
            self.snapshot_heartbeat_timeout_seconds = max(1, int(config.get("ui", {}).get("snapshot_heartbeat_timeout_seconds", 5)))
            self.snapshot_cache_stale_after_seconds = max(0, int(config.get("ui", {}).get("snapshot_cache_stale_after_seconds", 3600)))
            self._configure_history_store(config)

            if self.mqtt_client is not None:
                try:
                    self.mqtt_client.loop_stop()
                finally:
                    try:
                        self.mqtt_client.disconnect()
                    except Exception:
                        LOG.debug("MQTT disconnect during reconfigure failed", exc_info=True)
            self.mqtt_client = self._build_mqtt_client()
            self.mqtt_connected = False
            self.last_reload_at = time.time()
        self._persist_state()
        self._schedule_onvif_refresh_for_all()
        self._schedule_api_refresh_for_all()

    def _configure_history_store(self, config: dict[str, Any]) -> None:
        history_cfg = config.get("history") or {}
        enabled = bool(history_cfg.get("enabled", True))
        recent_actions_limit = max(1, int(history_cfg.get("recent_actions_limit", 20)))
        max_action_events_per_camera = max(1, int(history_cfg.get("max_action_events_per_camera", 1000)))
        max_state_samples_per_camera = max(1, int(history_cfg.get("max_state_samples_per_camera", 5000)))
        configured_path = str(history_cfg.get("path") or "").strip()
        db_path = configured_path or os.environ.get("HUB_HISTORY_DB") or str(
            Path(self.config_path).resolve().parent / "hub-history.sqlite3"
        )

        current_path = self.history_db_path
        current_store = self.history_store
        self.history_enabled = enabled
        self.history_recent_actions_limit = recent_actions_limit
        self.history_max_action_events_per_camera = max_action_events_per_camera
        self.history_max_state_samples_per_camera = max_state_samples_per_camera
        self.history_db_path = db_path if enabled else ""

        if not enabled:
            if current_store is not None:
                current_store.close()
            self.history_store = None
            return

        if (
            current_store is not None
            and current_path == db_path
            and current_store.max_action_events_per_camera == max_action_events_per_camera
            and current_store.max_state_samples_per_camera == max_state_samples_per_camera
        ):
            return

        try:
            new_store = HistoryStore(
                db_path,
                max_action_events_per_camera=max_action_events_per_camera,
                max_state_samples_per_camera=max_state_samples_per_camera,
            )
        except Exception:
            LOG.warning("Failed to initialize history store at %s", db_path, exc_info=True)
            if current_store is not None:
                current_store.close()
            self.history_store = None
            self.history_enabled = False
            self.history_db_path = ""
            return

        if current_store is not None:
            current_store.close()
        self.history_store = new_store

    def _build_mqtt_client(self) -> mqtt.Client:
        client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id=f"telegrambothub-{uuid.uuid4().hex[:8]}")
        mqtt_cfg = self.config["mqtt"]
        if mqtt_cfg.get("username"):
            client.username_pw_set(mqtt_cfg["username"], mqtt_cfg.get("password") or "")
        if mqtt_cfg.get("use_tls"):
            client.tls_set(cert_reqs=ssl.CERT_REQUIRED)
        client.on_connect = self._on_mqtt_connect
        client.on_message = self._on_mqtt_message
        client.on_disconnect = self._on_mqtt_disconnect
        return client

    def _on_mqtt_connect(self, client: mqtt.Client, _userdata: Any, _flags: Any, reason_code: Any, _properties: Any) -> None:
        LOG.info("MQTT connected: %s", reason_code)
        self.mqtt_connected = True
        self.last_mqtt_error = ""
        client.subscribe(self.reply_topic)
        client.subscribe(self.registration_topic)

    def _on_mqtt_disconnect(self, _client: mqtt.Client, _userdata: Any, disconnect_flags: Any, reason_code: Any, _properties: Any) -> None:
        self.mqtt_connected = False
        if self.stop_event.is_set():
            return
        self.last_mqtt_error = f"flags={disconnect_flags} reason={reason_code}"
        LOG.warning("MQTT disconnected: flags=%s reason=%s", disconnect_flags, reason_code)

    def _on_mqtt_message(self, _client: mqtt.Client, _userdata: Any, message: mqtt.MQTTMessage) -> None:
        topic = message.topic
        payload = message.payload.decode("utf-8", errors="replace")
        if topic.endswith("/hello"):
            self._handle_registration(topic, payload)
            return
        LOG.info("MQTT reply %s %s", topic, payload)
        chat_id, text = self._format_reply(topic, payload)
        if chat_id is None or not text:
            return
        try:
            self.telegram.send_message(chat_id, text)
        except Exception:
            LOG.exception("Failed to forward MQTT reply to Telegram")

    def _handle_registration(self, topic: str, payload: str) -> None:
        camera_id = self._camera_id_from_topic(topic)
        try:
            decoded = json.loads(payload)
        except json.JSONDecodeError:
            decoded = {}
        if not isinstance(decoded, dict):
            decoded = {}
        name = str(decoded.get("name") or decoded.get("hostname") or camera_id).strip() or camera_id
        status = str(decoded.get("status") or "online").strip().lower() or "online"
        existing = self.cameras.get(camera_id)
        configured_name = self._configured_camera_name(camera_id)
        hostname = str(decoded.get("hostname") or (existing.hostname if existing else "")).strip()
        ip = str(decoded.get("ip") or (existing.ip if existing else "")).strip()
        snapshot_url = str(decoded.get("snapshot_url") or (existing.snapshot_url if existing else "")).strip()
        api_key = str(decoded.get("api_key") or (existing.api_key if existing else "")).strip()
        api_base_url = str(decoded.get("api_base_url") or (existing.api_base_url if existing else "")).strip()
        api_token = str(decoded.get("api_token") or (existing.api_token if existing else "")).strip()
        onvif_endpoint = str(decoded.get("onvif_endpoint") or decoded.get("onvif_url") or (existing.onvif_endpoint if existing else "")).strip()
        onvif_username = str(decoded.get("onvif_username") or (existing.onvif_username if existing else "")).strip()
        onvif_password = str(decoded.get("onvif_password") or (existing.onvif_password if existing else "")).strip()
        last_registration_at = self._coerce_int(decoded.get("timestamp")) or int(time.time())
        resolved_name = configured_name or name
        if not configured_name and existing is not None and existing.name and existing.name != existing.camera_id and name == camera_id:
            resolved_name = existing.name
        with self.state_lock:
            self.cameras[camera_id] = self._camera_with_runtime_state(
                Camera(
                    camera_id=camera_id,
                    name=resolved_name,
                    ip=ip,
                    snapshot_url=snapshot_url,
                    api_key=api_key,
                    api_base_url=api_base_url,
                    api_token=api_token,
                    onvif_endpoint=onvif_endpoint,
                    onvif_username=onvif_username,
                    onvif_password=onvif_password,
                    hostname=hostname,
                    status=status,
                    last_registration_at=last_registration_at,
                ),
                existing,
            )
        self._persist_state()
        LOG.info("Camera registration: id=%s name=%s status=%s", camera_id, self.cameras[camera_id].name, status)
        self._schedule_api_refresh(camera_id)
        self._schedule_onvif_refresh(camera_id)

    def _camera_with_runtime_state(self, camera: Camera, existing: Camera | None) -> Camera:
        if existing is None:
            return camera
        return replace(
            camera,
            probe_status=existing.probe_status,
            last_probe_at=existing.last_probe_at,
            last_snapshot_ok_at=existing.last_snapshot_ok_at,
            last_probe_error=existing.last_probe_error,
            snapshot_cache_path=existing.snapshot_cache_path,
            onvif_manufacturer=existing.onvif_manufacturer,
            onvif_model=existing.onvif_model,
            onvif_firmware_version=existing.onvif_firmware_version,
            onvif_serial_number=existing.onvif_serial_number,
            onvif_hardware_id=existing.onvif_hardware_id,
            onvif_last_ok_at=existing.onvif_last_ok_at,
            onvif_last_error=existing.onvif_last_error,
            api_status=existing.api_status,
            api_last_ok_at=existing.api_last_ok_at,
            api_last_error=existing.api_last_error,
            api_device_name=existing.api_device_name,
            api_device_model=existing.api_device_model,
            api_streamer=existing.api_streamer,
            api_version=existing.api_version,
        )

    def _schedule_api_refresh_for_all(self) -> None:
        with self.state_lock:
            camera_ids = list(self.cameras)
        for camera_id in camera_ids:
            self._schedule_api_refresh(camera_id)

    def _schedule_api_refresh(self, camera_id: str) -> bool:
        resolved = self._resolve_camera_id(camera_id) or camera_id.strip().lower()
        with self.state_lock:
            camera = self.cameras.get(resolved)
            if camera is None:
                return False
            if not self._camera_api_base_url(camera):
                return False
            if resolved in self.api_refreshing:
                return False
            self.api_refreshing.add(resolved)
        worker = threading.Thread(
            target=self._refresh_api_worker,
            args=(resolved,),
            name=f"telegrambothub-api-{resolved[:8]}",
            daemon=True,
        )
        worker.start()
        return True

    def _refresh_api_worker(self, camera_id: str) -> None:
        try:
            self.refresh_camera_api_details(camera_id)
        finally:
            with self.state_lock:
                self.api_refreshing.discard(camera_id)

    def refresh_camera_api_details(self, camera_id: str) -> bool:
        resolved = self._resolve_camera_id(camera_id) or camera_id.strip().lower()
        with self.state_lock:
            camera = self.cameras.get(resolved)
        if camera is None:
            raise RuntimeError(f"Unknown camera: {camera_id}")

        conflict = self._camera_ip_conflict(camera)
        if conflict is not None:
            self._record_api_result(resolved, None, self._camera_ip_conflict_error(camera, conflict))
            return False

        try:
            info = self._fetch_camera_api_details(camera)
        except Exception as error:
            self._record_api_result(resolved, None, self._normalize_native_api_error(error))
            return False

        self._record_api_result(resolved, info, "")
        return True

    def _fetch_camera_api_details(self, camera: Camera) -> dict[str, Any]:
        client = self._camera_api_client(camera)
        payload = client.probe()
        device = payload.get("device") or {}
        software = device.get("software") or {}
        state = payload.get("state") or {}
        system = state.get("system") or {}
        network = state.get("network") or {}
        motion = state.get("motion") or {}
        privacy = state.get("privacy") or {}
        daynight = state.get("daynight") or {}
        return {
            "device_name": str(device.get("name") or camera.name).strip(),
            "device_model": str(device.get("model") or "").strip(),
            "streamer": str(software.get("streamer") or "").strip(),
            "version": str(software.get("api_version") or "").strip(),
            "streamer_running": bool(system.get("streamer_running")),
            "network_online": bool(network.get("online")),
            "ip": str(network.get("ip") or "").strip(),
            "motion_enabled": self._coerce_bool(motion.get("enabled")),
            "privacy_enabled": self._coerce_bool(privacy.get("enabled")),
            "daynight_target_mode": str(daynight.get("target_mode") or "").strip(),
            "daynight_running_mode": str(daynight.get("running_mode") or "").strip(),
        }

    def _record_api_result(self, camera_id: str, info: dict[str, Any] | None, error: str) -> None:
        now = int(time.time())
        updated = False
        api_status = "online" if info is not None else self._native_api_status_for_error(error)
        with self.state_lock:
            current = self.cameras.get(camera_id)
            if current is None:
                return
            values = info or {}
            self.cameras[camera_id] = replace(
                current,
                api_status=api_status,
                api_last_ok_at=now if info is not None else current.api_last_ok_at,
                api_last_error=error,
                api_device_name=values.get("device_name", current.api_device_name) if info is not None else current.api_device_name,
                api_device_model=values.get("device_model", current.api_device_model) if info is not None else current.api_device_model,
                api_streamer=values.get("streamer", current.api_streamer) if info is not None else current.api_streamer,
                api_version=values.get("version", current.api_version) if info is not None else current.api_version,
            )
            updated = True
        if updated:
            self._persist_state()
        self._record_history_state_sample(
            camera_id,
            "api_probe",
            {
                "api_status": api_status,
                "api_error": error,
                "api_streamer": (info or {}).get("streamer", ""),
                "api_version": (info or {}).get("version", ""),
                "streamer_running": (info or {}).get("streamer_running"),
                "network_online": (info or {}).get("network_online"),
                "motion_enabled": (info or {}).get("motion_enabled"),
                "privacy_enabled": (info or {}).get("privacy_enabled"),
                "daynight_running_mode": (info or {}).get("daynight_running_mode", ""),
                "ip": (info or {}).get("ip", ""),
            },
            normalized={
                "api_status": api_status,
                "streamer_running": (info or {}).get("streamer_running"),
                "network_online": (info or {}).get("network_online"),
                "motion_enabled": (info or {}).get("motion_enabled"),
                "privacy_enabled": (info or {}).get("privacy_enabled"),
                "daynight_running_mode": (info or {}).get("daynight_running_mode", ""),
                "ip": (info or {}).get("ip", ""),
            },
            recorded_at=now,
        )

    def _record_optimistic_supported_controls(self, camera_id: str, payload: dict[str, Any]) -> None:
        values: dict[str, Any] = {}

        image = payload.get("image") or {}
        if isinstance(image, dict):
            for field in ("brightness", "contrast", "saturation", "sharpness"):
                if field in image:
                    values[f"native_image_{field}"] = str(image.get(field) or "")
            if "anti_flicker" in image:
                values["native_image_anti_flicker"] = self._normalize_anti_flicker_mode(image.get("anti_flicker"))
            if "hflip" in image:
                values["native_image_hflip"] = bool(self._coerce_bool(image.get("hflip")))
            if "vflip" in image:
                values["native_image_vflip"] = bool(self._coerce_bool(image.get("vflip")))

        motion = payload.get("motion") or {}
        if isinstance(motion, dict) and "enabled" in motion:
            values["native_motion_enabled"] = bool(self._coerce_bool(motion.get("enabled")))

        daynight = payload.get("daynight") or {}
        if isinstance(daynight, dict):
            if "enabled" in daynight:
                values["native_daynight_enabled"] = bool(self._coerce_bool(daynight.get("enabled")))
            if "force_mode" in daynight:
                force_mode = str(daynight.get("force_mode") or "").strip()
                values["native_daynight_force_mode"] = force_mode
                values["native_daynight_requested_mode"] = force_mode or "auto"
            if "target_mode" in daynight:
                values["native_daynight_requested_mode"] = str(daynight.get("target_mode") or "auto").strip() or "auto"

        if not values:
            return

        now = time.monotonic()
        expires_at = now + 10.0
        with self.state_lock:
            existing = self.optimistic_supported_controls_by_camera.get(camera_id)
            merged = dict(existing[1]) if existing is not None and existing[0] > now else {}
            merged.update(values)
            self.optimistic_supported_controls_by_camera[camera_id] = (expires_at, merged)

    def _optimistic_supported_controls_overlay(self, camera_id: str) -> dict[str, Any]:
        now = time.monotonic()
        with self.state_lock:
            existing = self.optimistic_supported_controls_by_camera.get(camera_id)
            if existing is None:
                return {}
            expires_at, values = existing
            if expires_at <= now:
                self.optimistic_supported_controls_by_camera.pop(camera_id, None)
                return {}
            return dict(values)

    def control_camera_service(self, camera_id: str, service: str, operation: str) -> dict[str, Any]:
        resolved = self._resolve_camera_id(camera_id) or camera_id.strip().lower()
        with self.state_lock:
            camera = self.cameras.get(resolved)
        if camera is None:
            raise RuntimeError(f"Unknown camera: {camera_id}")

        normalized_service = str(service or "").strip().lower()
        normalized_operation = str(operation or "").strip().lower()
        if not normalized_service:
            raise RuntimeError("Service name is required")
        if not normalized_operation:
            raise RuntimeError("Service operation is required")

        try:
            result = self._camera_api_client(camera).control_service(normalized_service, normalized_operation)
        except Exception as error:
            self._record_native_action(
                resolved,
                "service_control",
                "error",
                f"{normalized_service}.{normalized_operation}: {error}",
            )
            raise

        self._record_native_action(
            resolved,
            "service_control",
            "success",
            f"{normalized_service}.{normalized_operation}: {result.get('status') or 'accepted'}",
        )
        self.refresh_camera_api_details(resolved)
        return result

    def restart_camera_streaming_service(self, camera_id: str) -> dict[str, Any]:
        return self.control_camera_service(camera_id, "streaming", "restart")

    def start_camera_streaming_service(self, camera_id: str) -> dict[str, Any]:
        return self.control_camera_service(camera_id, "streaming", "start")

    def stop_camera_streaming_service(self, camera_id: str) -> dict[str, Any]:
        return self.control_camera_service(camera_id, "streaming", "stop")

    def restart_camera_streamer(self, camera_id: str) -> dict[str, Any]:
        return self.restart_camera_streaming_service(camera_id)

    def patch_camera_config(self, camera_id: str, payload: dict[str, Any], *, refresh_after: bool = True) -> dict[str, Any]:
        resolved = self._resolve_camera_id(camera_id) or camera_id.strip().lower()
        with self.state_lock:
            camera = self.cameras.get(resolved)
        if camera is None:
            raise RuntimeError(f"Unknown camera: {camera_id}")

        try:
            result = self._camera_api_client(camera).patch_config(payload)
        except Exception as error:
            self._record_native_action(resolved, "patch_config", "error", str(error))
            raise

        self._record_native_action(
            resolved,
            "patch_config",
            "success",
            ", ".join(sorted(payload.keys())) or str(result.get("status") or "accepted"),
        )
        if refresh_after:
            self.refresh_camera_api_details(resolved)
        else:
            self._record_optimistic_supported_controls(resolved, payload)
            self._schedule_api_refresh(resolved)
        return result

    def update_camera_send2_config(self, camera_id: str, payload: dict[str, Any]) -> dict[str, Any]:
        resolved = self._resolve_camera_id(camera_id) or camera_id.strip().lower()
        with self.state_lock:
            camera = self.cameras.get(resolved)
        if camera is None:
            raise RuntimeError(f"Unknown camera: {camera_id}")

        try:
            current_payload = self._camera_send2_request(camera, "/x/json-send2.cgi", method="GET")
            merged_payload = self._merge_send2_payload(current_payload, payload)
            result = self._camera_send2_request(camera, "/x/json-send2.cgi", method="POST", json_payload=merged_payload)
        except Exception as error:
            self._record_native_action(resolved, "send2_config", "error", str(error))
            raise

        detail = ", ".join(sorted(payload.keys())) or "accepted"
        self._record_native_action(resolved, "send2_config", "success", detail)
        return result

    def _merge_send2_payload(self, current: dict[str, Any], patch: dict[str, Any]) -> dict[str, Any]:
        merged = copy.deepcopy(current)

        def merge_dict(target: dict[str, Any], updates: dict[str, Any]) -> dict[str, Any]:
            for key, value in updates.items():
                if isinstance(value, dict):
                    existing = target.get(key)
                    if not isinstance(existing, dict):
                        existing = {}
                    target[key] = merge_dict(dict(existing), value)
                else:
                    target[key] = value
            return target

        return merge_dict(merged, patch)

    def _is_timeout_error(self, error: Exception) -> bool:
        message = str(error or "").strip().lower()
        return "timed out" in message or "timeout" in message

    def test_camera_send2_service(
        self,
        camera_id: str,
        service_name: str,
        *,
        verbose: bool = True,
        send_type: str = "",
    ) -> dict[str, Any]:
        resolved = self._resolve_camera_id(camera_id) or camera_id.strip().lower()
        with self.state_lock:
            camera = self.cameras.get(resolved)
        if camera is None:
            raise RuntimeError(f"Unknown camera: {camera_id}")

        normalized_service = str(service_name or "").strip().lower()
        if normalized_service not in {name for name, _label in SEND2_SERVICES}:
            raise RuntimeError(f"Unknown send2 service: {service_name}")

        normalized_type = str(send_type or "").strip().lower()
        if normalized_type not in {"", "photo", "video"}:
            raise RuntimeError("Send2 test type must be empty, photo, or video")

        query = {"to": normalized_service}
        if verbose:
            query["verbose"] = "1"
        if normalized_type:
            query["type"] = normalized_type

        path = f"/x/send.cgi?{urllib.parse.urlencode(query)}"
        try:
            result = self._camera_send2_request(camera, path, method="GET", timeout_seconds=max(self.snapshot_heartbeat_timeout_seconds, 30))
        except Exception as error:
            if self._is_timeout_error(error):
                detail = normalized_service
                if normalized_type:
                    detail = f"{detail}.{normalized_type}"
                timeout_detail = f"{detail}: timed out waiting for response"
                self._record_native_action(resolved, "send2_test", "success", timeout_detail)
                return {
                    "status": "accepted",
                    "timeout_waiting_for_response": True,
                    "message": timeout_detail,
                }
            self._record_native_action(resolved, "send2_test", "error", f"{normalized_service}: {error}")
            raise

        detail = normalized_service
        if normalized_type:
            detail = f"{detail}.{normalized_type}"
        self._record_native_action(resolved, "send2_test", "success", detail)
        return result

    def set_camera_privacy(self, camera_id: str, enabled: bool, channel: str = "all") -> dict[str, Any]:
        resolved = self._resolve_camera_id(camera_id) or camera_id.strip().lower()
        with self.state_lock:
            camera = self.cameras.get(resolved)
        if camera is None:
            raise RuntimeError(f"Unknown camera: {camera_id}")

        try:
            result = self._camera_api_client(camera).set_privacy(enabled=enabled, channel=channel)
        except Exception as error:
            self._record_native_action(resolved, "privacy", "error", str(error))
            raise

        state = "enabled" if enabled else "disabled"
        detail = f"{state} on {channel}"
        self._record_native_action(resolved, "privacy", "success", detail)
        self.refresh_camera_api_details(resolved)
        return result

    def set_camera_daynight_mode(self, camera_id: str, mode: str, *, refresh_after: bool = True) -> dict[str, Any]:
        resolved = self._resolve_camera_id(camera_id) or camera_id.strip().lower()
        with self.state_lock:
            camera = self.cameras.get(resolved)
        if camera is None:
            raise RuntimeError(f"Unknown camera: {camera_id}")

        normalized_mode = str(mode or "").strip().lower()
        if normalized_mode not in {"auto", "day", "night"}:
            raise RuntimeError("Day/night mode must be auto, day, or night")

        try:
            result = self._camera_api_client(camera).set_daynight_mode(normalized_mode)
        except Exception as error:
            self._record_native_action(resolved, "daynight", "error", str(error))
            raise

        self._record_native_action(resolved, "daynight", "success", normalized_mode)
        if refresh_after:
            self.refresh_camera_api_details(resolved)
        else:
            self._record_optimistic_supported_controls(
                resolved,
                {"daynight": {"target_mode": normalized_mode}},
            )
            self._schedule_api_refresh(resolved)
        return result

    def record_camera_clip(
        self,
        camera_id: str,
        duration_seconds: int = 10,
        stream_id: int = 0,
        path: str = "",
    ) -> dict[str, Any]:
        resolved = self._resolve_camera_id(camera_id) or camera_id.strip().lower()
        with self.state_lock:
            camera = self.cameras.get(resolved)
        if camera is None:
            raise RuntimeError(f"Unknown camera: {camera_id}")

        try:
            result = self._camera_api_client(camera).record_clip(
                duration_seconds=duration_seconds,
                stream_id=stream_id,
                path=path,
            )
        except Exception as error:
            self._record_native_action(resolved, "record", "error", str(error))
            raise

        result_path = str(((result.get("result") or {}).get("path") or "")).strip()
        detail = f"{duration_seconds}s stream {stream_id}"
        if result_path:
            detail = f"{detail} -> {result_path}"
        self._record_native_action(resolved, "record", "success", detail)
        return result

    def _record_native_action(self, camera_id: str, action: str, status: str, detail: str) -> None:
        recorded_at = int(time.time())
        entry = {
            "at": recorded_at,
            "action": action,
            "status": status,
            "detail": detail.strip(),
        }
        with self.state_lock:
            history = list(self.native_action_history_by_camera.get(camera_id, []))
            history.insert(0, entry)
            self.native_action_history_by_camera[camera_id] = history[:8]
        self._record_history_action(
            camera_id,
            action,
            status,
            detail.strip(),
            recorded_at=recorded_at,
        )

    def _native_action_history_for_ui(self, camera_id: str) -> list[dict[str, str]]:
        if self.history_store is not None:
            rows = self.history_store.recent_action_events(camera_id, self.history_recent_actions_limit)
            return [
                {
                    "at": self._format_timestamp(entry.get("recorded_at")),
                    "action": str(entry.get("action") or "").replace("_", " "),
                    "status": str(entry.get("status") or "unknown"),
                    "detail": str(entry.get("detail") or ""),
                    "source": str(entry.get("source") or "native_api"),
                }
                for entry in rows
            ]
        with self.state_lock:
            history = list(self.native_action_history_by_camera.get(camera_id, []))
        return [
            {
                "at": self._format_timestamp(entry.get("at")),
                "action": str(entry.get("action") or "").replace("_", " "),
                "status": str(entry.get("status") or "unknown"),
                "detail": str(entry.get("detail") or ""),
                "source": "memory",
            }
            for entry in history
        ]

    def _record_history_action(
        self,
        camera_id: str,
        action: str,
        status: str,
        detail: str,
        recorded_at: int | None = None,
    ) -> None:
        if self.history_store is None:
            return
        try:
            self.history_store.record_action_event(
                recorded_at=recorded_at or int(time.time()),
                camera_id=camera_id,
                source="native_api",
                action=action,
                status=status,
                detail=detail,
            )
        except Exception:
            LOG.warning("Failed to record action history for %s", camera_id, exc_info=True)

    def _record_history_state_sample(
        self,
        camera_id: str,
        sample_type: str,
        sample: dict[str, Any],
        normalized: dict[str, Any] | None = None,
        recorded_at: int | None = None,
    ) -> None:
        if self.history_store is None:
            return
        try:
            self.history_store.record_state_sample(
                recorded_at=recorded_at or int(time.time()),
                camera_id=camera_id,
                sample_type=sample_type,
                sample=sample,
                normalized=normalized,
            )
        except Exception:
            LOG.warning("Failed to record state sample for %s", camera_id, exc_info=True)

    def get_camera_history_for_ui(
        self,
        camera_id: str,
        limit: int = 100,
        kind_filter: str = "all",
        sample_type_filter: str = "all",
    ) -> dict[str, Any]:
        resolved = self._resolve_camera_id(camera_id) or camera_id.strip().lower()
        with self.state_lock:
            camera = self.cameras.get(resolved)
        if camera is None:
            raise RuntimeError(f"Unknown camera: {camera_id}")

        timeline: list[dict[str, str]] = []
        available_sample_types = ["all"]
        charts: list[dict[str, Any]] = []
        kind_filter = kind_filter if kind_filter in {"all", "action", "state"} else "all"
        sample_type_filter = sample_type_filter.strip() or "all"
        if self.history_store is not None:
            action_rows = self.history_store.recent_action_events(resolved, max(limit * 3, 100))
            state_rows = self.history_store.recent_state_samples(resolved, max(limit * 3, 100))
            charts = self._history_charts_for_ui(state_rows)
            sample_types = sorted(
                {
                    str(entry.get("sample_type") or "").strip()
                    for entry in state_rows
                    if str(entry.get("sample_type") or "").strip()
                }
            )
            available_sample_types.extend(sample_types)

            if kind_filter in {"all", "action"}:
                for entry in action_rows:
                    timeline.append(
                        {
                            "at": self._format_timestamp(entry.get("recorded_at")),
                            "timestamp": str(entry.get("recorded_at") or 0),
                            "kind": "action",
                            "name": str(entry.get("action") or "").replace("_", " "),
                            "status": str(entry.get("status") or "unknown"),
                            "source": str(entry.get("source") or "native_api"),
                            "detail": str(entry.get("detail") or ""),
                        }
                    )

            if kind_filter in {"all", "state"}:
                for entry in state_rows:
                    row_sample_type = str(entry.get("sample_type") or "").strip() or "sample"
                    if sample_type_filter != "all" and row_sample_type != sample_type_filter:
                        continue
                    timeline.append(self._state_sample_entry_for_ui(entry))

            timeline.sort(key=lambda item: int(item.get("timestamp") or 0), reverse=True)
            timeline = timeline[:limit]
        else:
            timeline = [
                {
                    "at": item["at"],
                    "timestamp": "0",
                    "kind": "action",
                    "name": item["action"],
                    "status": item["status"],
                    "source": item.get("source", "memory"),
                    "detail": item["detail"],
                }
                for item in self._native_action_history_for_ui(resolved)
            ]

        action_count = sum(1 for item in timeline if item.get("kind") == "action")
        state_count = sum(1 for item in timeline if item.get("kind") == "state")
        latest_api_probe = next(
            (
                item
                for item in timeline
                if item.get("kind") == "state" and item.get("name") == "api probe"
            ),
            None,
        )
        latest_snapshot_probe = next(
            (
                item
                for item in timeline
                if item.get("kind") == "state" and item.get("name") == "snapshot probe"
            ),
            None,
        )

        return {
            "camera_id": camera.camera_id,
            "name": camera.name,
            "ip": camera.ip or "",
            "status": self._camera_status_for_ui(camera),
            "api_status": camera.api_status,
            "history_enabled": self.history_store is not None,
            "history_db_path": self.history_db_path,
            "history_limit": limit,
            "history_kind_filter": kind_filter,
            "history_sample_type_filter": sample_type_filter,
            "available_sample_types": available_sample_types,
            "timeline_action_count": action_count,
            "timeline_state_count": state_count,
            "latest_api_probe": latest_api_probe,
            "latest_snapshot_probe": latest_snapshot_probe,
            "charts": charts,
            "timeline": timeline,
        }

    def _history_charts_for_ui(self, state_rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
        api_rows = [
            row for row in reversed(state_rows)
            if str(row.get("sample_type") or "") == "api_probe"
        ]
        if not api_rows:
            return []

        charts = [
            self._binary_history_chart(
                title="API Reachability",
                rows=api_rows,
                extractor=lambda row: self._status_flag(row.get("api_status"), "online"),
                on_label="online",
                off_label="offline",
                legend_items=[
                    {"label": "online", "fill": "#198754"},
                    {"label": "offline", "fill": "#dc3545"},
                    {"label": "unknown", "fill": "#6c757d"},
                ],
            ),
            self._binary_history_chart(
                title="Streamer Running",
                rows=api_rows,
                extractor=lambda row: self._db_bool(row.get("streamer_running")),
                on_label="running",
                off_label="stopped",
                legend_items=[
                    {"label": "running", "fill": "#198754"},
                    {"label": "stopped", "fill": "#dc3545"},
                    {"label": "unknown", "fill": "#6c757d"},
                ],
            ),
            self._binary_history_chart(
                title="Motion Enabled",
                rows=api_rows,
                extractor=lambda row: self._db_bool(row.get("motion_enabled")),
                on_label="enabled",
                off_label="disabled",
                legend_items=[
                    {"label": "enabled", "fill": "#198754"},
                    {"label": "disabled", "fill": "#dc3545"},
                    {"label": "unknown", "fill": "#6c757d"},
                ],
            ),
            self._binary_history_chart(
                title="Privacy Enabled",
                rows=api_rows,
                extractor=lambda row: self._db_bool(row.get("privacy_enabled")),
                on_label="enabled",
                off_label="disabled",
                legend_items=[
                    {"label": "enabled", "fill": "#198754"},
                    {"label": "disabled", "fill": "#dc3545"},
                    {"label": "unknown", "fill": "#6c757d"},
                ],
            ),
            self._categorical_history_chart(
                title="Day/Night Mode",
                rows=api_rows,
                extractor=lambda row: str(row.get("daynight_running_mode") or "").strip().lower() or "unknown",
            ),
        ]
        snapshot_rows = [
            row for row in reversed(state_rows)
            if str(row.get("sample_type") or "") == "snapshot_probe"
        ]
        if snapshot_rows:
            charts.extend(
                [
                    self._binary_history_chart(
                        title="Snapshot Reachability",
                        rows=snapshot_rows,
                        extractor=lambda row: self._status_flag(self._sample_json_field(row, "probe_status"), "online"),
                        on_label="online",
                        off_label="offline",
                        legend_items=[
                            {"label": "online", "fill": "#198754"},
                            {"label": "offline", "fill": "#dc3545"},
                            {"label": "unknown", "fill": "#6c757d"},
                        ],
                    ),
                    self._binary_history_chart(
                        title="Snapshot Cache",
                        rows=snapshot_rows,
                        extractor=lambda row: self._db_bool(row.get("has_cached_snapshot")),
                        on_label="cached",
                        off_label="empty",
                        legend_items=[
                            {"label": "cached", "fill": "#198754"},
                            {"label": "empty", "fill": "#dc3545"},
                            {"label": "unknown", "fill": "#6c757d"},
                        ],
                    ),
                ]
            )
        return [chart for chart in charts if chart.get("bars")]

    def _binary_history_chart(
        self,
        *,
        title: str,
        rows: list[dict[str, Any]],
        extractor: Any,
        on_label: str,
        off_label: str,
        legend_items: list[dict[str, str]],
    ) -> dict[str, Any]:
        bars = []
        known = 0
        positive = 0
        latest_label = "unknown"
        width = 320
        height = 44
        count = max(1, len(rows))
        gap = 1
        raw_bar_width = max(2, width // count)
        bar_width = max(2, raw_bar_width - gap)
        for index, row in enumerate(rows):
            state = extractor(row)
            x = index * raw_bar_width
            y = 6
            bar_height = 32
            timestamp_label = self._format_timestamp(row.get("recorded_at"))
            if state is True:
                fill = "#198754"
                positive += 1
                known += 1
                latest_label = on_label if index == len(rows) - 1 else latest_label
                state_label = on_label
            elif state is False:
                fill = "#dc3545"
                known += 1
                latest_label = off_label if index == len(rows) - 1 else latest_label
                state_label = off_label
            else:
                fill = "#6c757d"
                latest_label = "unknown" if index == len(rows) - 1 else latest_label
                state_label = "unknown"
            bars.append(
                {
                    "x": x,
                    "y": y,
                    "width": bar_width,
                    "height": bar_height,
                    "fill": fill,
                    "title": f"{timestamp_label}: {state_label}",
                }
            )

        if rows:
            latest_state = extractor(rows[-1])
            latest_label = on_label if latest_state is True else off_label if latest_state is False else "unknown"
        percentage = int(round((positive / known) * 100)) if known else 0
        range_start = self._format_timestamp(rows[0].get("recorded_at") if rows else None)
        range_end = self._format_timestamp(rows[-1].get("recorded_at") if rows else None)
        return {
            "title": title,
            "type": "binary",
            "width": width,
            "height": height,
            "bars": bars,
            "latest": latest_label,
            "range_start": range_start,
            "range_end": range_end,
            "sample_count": len(rows),
            "legend": legend_items,
            "summary": f"{percentage}% positive across {known} known samples" if known else "No known samples yet",
        }

    def _categorical_history_chart(
        self,
        *,
        title: str,
        rows: list[dict[str, Any]],
        extractor: Any,
    ) -> dict[str, Any]:
        palette = {
            "day": "#f59f00",
            "night": "#0d6efd",
            "auto": "#20c997",
            "unknown": "#6c757d",
        }
        bars = []
        width = 320
        height = 44
        count = max(1, len(rows))
        gap = 1
        raw_bar_width = max(2, width // count)
        bar_width = max(2, raw_bar_width - gap)
        latest = "unknown"
        counts: dict[str, int] = {}
        for index, row in enumerate(rows):
            value = extractor(row)
            latest = value if index == len(rows) - 1 else latest
            counts[value] = counts.get(value, 0) + 1
            timestamp_label = self._format_timestamp(row.get("recorded_at"))
            bars.append(
                {
                    "x": index * raw_bar_width,
                    "y": 6,
                    "width": bar_width,
                    "height": 32,
                    "fill": palette.get(value, palette["unknown"]),
                    "title": f"{timestamp_label}: {value}",
                }
            )
        summary = ", ".join(f"{name}:{count}" for name, count in sorted(counts.items()))
        range_start = self._format_timestamp(rows[0].get("recorded_at") if rows else None)
        range_end = self._format_timestamp(rows[-1].get("recorded_at") if rows else None)
        return {
            "title": title,
            "type": "categorical",
            "width": width,
            "height": height,
            "bars": bars,
            "latest": latest,
            "range_start": range_start,
            "range_end": range_end,
            "sample_count": len(rows),
            "legend": [
                {"label": "day", "fill": palette["day"]},
                {"label": "night", "fill": palette["night"]},
                {"label": "auto", "fill": palette["auto"]},
                {"label": "unknown", "fill": palette["unknown"]},
            ],
            "summary": summary,
        }

    def _db_bool(self, value: Any) -> bool | None:
        if value is None or value == "":
            return None
        try:
            return bool(int(value))
        except (TypeError, ValueError):
            return None

    def _status_flag(self, value: Any, expected: str) -> bool | None:
        text = str(value or "").strip().lower()
        if not text:
            return None
        return text == expected

    def _sample_json_field(self, row: dict[str, Any], field_name: str) -> Any:
        raw_payload = str(row.get("sample_json") or "{}")
        try:
            sample = json_module.loads(raw_payload)
        except json_module.JSONDecodeError:
            return None
        return sample.get(field_name)

    def _state_sample_entry_for_ui(self, entry: dict[str, Any]) -> dict[str, str]:
        recorded_at = int(entry.get("recorded_at") or 0)
        sample_type = str(entry.get("sample_type") or "sample")
        raw_payload = str(entry.get("sample_json") or "{}")
        try:
            sample = json_module.loads(raw_payload)
        except json_module.JSONDecodeError:
            sample = {"raw": raw_payload}

        if sample_type == "api_probe":
            status = str(sample.get("api_status") or "unknown")
            detail = f"API {status}"
            streamer = str(sample.get("api_streamer") or "").strip()
            version = str(sample.get("api_version") or "").strip()
            error = str(sample.get("api_error") or "").strip()
            extras = ", ".join(part for part in (streamer, version) if part)
            if extras:
                detail = f"{detail} ({extras})"
            if error:
                detail = f"{detail}: {error}"
            state_status = "success" if status == "online" else "error"
        elif sample_type == "snapshot_probe":
            status = str(sample.get("probe_status") or "unknown")
            cached = "cached snapshot" if sample.get("has_cached_snapshot") else "no cache"
            error = str(sample.get("probe_error") or "").strip()
            detail = f"Snapshot {status} ({cached})"
            if error:
                detail = f"{detail}: {error}"
            state_status = "success" if status == "online" else "error"
        else:
            detail = json_module.dumps(sample, sort_keys=True)
            state_status = "info"

        if sample_type == "api_probe":
            flags = []
            if entry.get("streamer_running") is not None:
                flags.append("streamer running" if int(entry["streamer_running"]) else "streamer stopped")
            if entry.get("network_online") is not None:
                flags.append("network online" if int(entry["network_online"]) else "network offline")
            if entry.get("motion_enabled") is not None:
                flags.append("motion on" if int(entry["motion_enabled"]) else "motion off")
            if entry.get("privacy_enabled") is not None:
                flags.append("privacy on" if int(entry["privacy_enabled"]) else "privacy off")
            if entry.get("daynight_running_mode"):
                flags.append(f"day/night {entry['daynight_running_mode']}")
            if entry.get("ip"):
                flags.append(f"ip {entry['ip']}")
            if flags:
                detail = f"{detail} | {'; '.join(flags)}"

        return {
            "at": self._format_timestamp(recorded_at),
            "timestamp": str(recorded_at),
            "kind": "state",
            "name": sample_type.replace("_", " "),
            "status": state_status,
            "source": "history",
            "detail": detail,
        }

    def get_camera_supported_controls_for_ui(self, camera_id: str) -> dict[str, Any]:
        defaults = {
            "native_controls_available": False,
            "native_controls_error": "",
            "native_service_controls": [],
            "native_image_controls_supported": False,
            "native_image_anti_flicker_supported": False,
            "native_image_hflip_supported": False,
            "native_image_vflip_supported": False,
            "native_motion_supported": False,
            "native_daynight_supported": False,
            "native_daynight_action_supported": False,
            "native_streaming_start_supported": False,
            "native_streaming_stop_supported": False,
            "native_streaming_restart_supported": False,
            "native_stream_controls": [],
            "native_daynight_modes": ["auto", "day", "night"],
            "native_daynight_requested_mode": "auto",
            "native_daynight_running_mode": "",
            "native_image_brightness": "",
            "native_image_contrast": "",
            "native_image_saturation": "",
            "native_image_sharpness": "",
            "native_image_anti_flicker": "",
            "native_image_hflip": False,
            "native_image_vflip": False,
            "native_motion_enabled": False,
            "native_daynight_enabled": False,
            "native_daynight_force_mode": "",
            "native_privacy_supported": False,
            "native_privacy_enabled": False,
            "native_record_supported": False,
            "native_send2_available": False,
            "native_send2_error": "",
            "native_send2_overview_url": "",
            "native_send2_motion_sensitivity": "",
            "native_send2_motion_cooldown": "",
            "native_send2_services": [],
            "config_patch_example": json.dumps({"image": {"brightness": 128}}, indent=2),
        }

        resolved = self._resolve_camera_id(camera_id) or camera_id.strip().lower()
        with self.state_lock:
            camera = self.cameras.get(resolved)
        if camera is None:
            raise RuntimeError(f"Unknown camera: {camera_id}")

        capabilities: dict[str, Any] = {}
        config_payload: dict[str, Any] = {}
        state_payload: dict[str, Any] = {}
        native_controls_ok = False
        try:
            client = self._camera_api_client(camera)
            capabilities = client.get_capabilities()
            config_payload = client.get_config()
            state_payload = client.get_state()
            native_controls_ok = True
        except Exception as error:
            defaults["native_controls_error"] = self._normalize_native_api_error(error)

        backend_config = config_payload.get("backend") or {}
        prudynt_config = (backend_config.get("raw") or backend_config.get("prudynt") or {})
        image = prudynt_config.get("image") or {}
        motion = prudynt_config.get("motion") or {}
        daynight = prudynt_config.get("daynight") or {}
        state_daynight = (state_payload.get("daynight") or {})
        state_privacy = (state_payload.get("privacy") or {})
        config_caps = capabilities.get("config") or {}
        control_caps = config_caps.get("controls") or {}
        image_caps = capabilities.get("image") or control_caps.get("image") or {}
        motion_caps = capabilities.get("motion") or control_caps.get("motion") or {}
        daynight_caps = capabilities.get("daynight") or control_caps.get("daynight") or {}
        privacy_caps = capabilities.get("privacy") or {}
        services_caps = capabilities.get("services") or {}
        streaming_service_caps = (services_caps.get("streaming") or {})
        streams_caps = capabilities.get("streams") or {}
        stream_count = self._coerce_int(streams_caps.get("count"))

        stream_controls: list[dict[str, Any]] = []
        if native_controls_ok:
            for stream_name in sorted(prudynt_config):
                if not stream_name.startswith("stream"):
                    continue
                stream_config = prudynt_config.get(stream_name) or {}
                if not isinstance(stream_config, dict):
                    continue
                stream_suffix = stream_name[6:]
                if not stream_suffix.isdigit():
                    continue
                stream_index = int(stream_suffix)
                if stream_count is not None and stream_count >= 0 and stream_index >= stream_count:
                    continue
                stream_controls.append(
                    {
                        "name": stream_name,
                        "stream_id": stream_index,
                        "label": "Main Stream" if stream_index == 0 else ("Substream" if stream_index == 1 else f"Stream {stream_index}"),
                        "enabled_supported": "enabled" in stream_config,
                        "audio_enabled_supported": "audio_enabled" in stream_config,
                        "width_supported": "width" in stream_config,
                        "height_supported": "height" in stream_config,
                        "fps_supported": "fps" in stream_config,
                        "bitrate_supported": "bitrate" in stream_config,
                        "format_supported": "format" in stream_config,
                        "mode_supported": "mode" in stream_config,
                        "enabled": bool(self._coerce_bool(stream_config.get("enabled"))),
                        "audio_enabled": bool(self._coerce_bool(stream_config.get("audio_enabled"))),
                        "width": "" if "width" not in stream_config else str(stream_config.get("width") or 0),
                        "height": "" if "height" not in stream_config else str(stream_config.get("height") or 0),
                        "fps": "" if "fps" not in stream_config else str(stream_config.get("fps") or 0),
                        "bitrate": "" if "bitrate" not in stream_config else str(stream_config.get("bitrate") or 0),
                        "format": str(stream_config.get("format") or "").strip(),
                        "mode": str(stream_config.get("mode") or "").strip(),
                        "osd_enabled_supported": isinstance(stream_config.get("osd"), dict) and "enabled" in (stream_config.get("osd") or {}),
                        "osd_enabled": bool(self._coerce_bool(((stream_config.get("osd") or {}).get("enabled")))),
                        "osd_time_enabled_supported": isinstance(((stream_config.get("osd") or {}).get("time")), dict) and "enabled" in (((stream_config.get("osd") or {}).get("time")) or {}),
                        "osd_time_enabled": bool(self._coerce_bool((((stream_config.get("osd") or {}).get("time") or {}).get("enabled")))),
                        "osd_usertext_enabled_supported": isinstance(((stream_config.get("osd") or {}).get("usertext")), dict) and "enabled" in (((stream_config.get("osd") or {}).get("usertext")) or {}),
                        "osd_usertext_enabled": bool(self._coerce_bool((((stream_config.get("osd") or {}).get("usertext") or {}).get("enabled")))),
                        "osd_usertext_format_supported": isinstance(((stream_config.get("osd") or {}).get("usertext")), dict) and "format" in (((stream_config.get("osd") or {}).get("usertext")) or {}),
                        "osd_usertext_format": str((((stream_config.get("osd") or {}).get("usertext") or {}).get("format") or "")).strip(),
                    }
                )

        operation_order = {"start": 0, "stop": 1, "restart": 2}
        service_controls: list[dict[str, Any]] = []
        if native_controls_ok:
            for service_name in sorted(services_caps):
                service_caps = services_caps.get(service_name) or {}
                if not isinstance(service_caps, dict):
                    continue
                operations = [
                    operation
                    for operation, supported in service_caps.items()
                    if self._coerce_bool(supported)
                ]
                if not operations:
                    continue
                operations.sort(key=lambda operation: (operation_order.get(operation, 99), operation))
                service_controls.append(
                    {
                        "service": str(service_name),
                        "label": str(service_name).replace("_", " ").replace("-", " ").title(),
                        "operations": [
                            {
                                "name": operation,
                                "label": str(operation).replace("_", " ").replace("-", " ").title(),
                            }
                            for operation in operations
                        ],
                    }
                )

        if native_controls_ok:
            patch_example = {
                "image": {
                    "brightness": self._coerce_int(image.get("brightness")) or 128,
                    "contrast": self._coerce_int(image.get("contrast")) or 128,
                    "saturation": self._coerce_int(image.get("saturation")) or 128,
                    "sharpness": self._coerce_int(image.get("sharpness")) or 128,
                    "hflip": self._coerce_bool(image.get("hflip")),
                    "vflip": self._coerce_bool(image.get("vflip")),
                },
                "motion": {
                    "enabled": self._coerce_bool(motion.get("enabled")),
                },
                "daynight": {
                    "enabled": self._coerce_bool(daynight.get("enabled")),
                    "force_mode": str(daynight.get("force_mode") or "").strip(),
                },
            }
            anti_flicker = str(image.get("anti_flicker") or "").strip()
            normalized_anti_flicker = self._normalize_anti_flicker_mode(anti_flicker)
            if anti_flicker:
                patch_example["image"]["anti_flicker"] = anti_flicker

            if stream_controls:
                first_stream = stream_controls[0]
                patch_example[first_stream["name"]] = {
                    "osd": {
                        "enabled": first_stream["osd_enabled"],
                        "time": {"enabled": first_stream["osd_time_enabled"]},
                        "usertext": {
                            "enabled": first_stream["osd_usertext_enabled"],
                            "format": first_stream["osd_usertext_format"] or "%hostname",
                        },
                    }
                }

            defaults.update(
                {
                    "native_controls_available": True,
                    "native_service_controls": service_controls,
                    "native_motion_supported": bool(motion_caps.get("enabled")),
                    "native_daynight_supported": bool(daynight_caps.get("enabled")),
                    "native_daynight_action_supported": bool(daynight_caps.get("force_mode")) or bool(daynight_caps.get("modes")),
                    "native_image_controls_supported": bool(image_caps),
                    "native_image_anti_flicker_supported": bool(image_caps.get("anti_flicker")),
                    "native_image_hflip_supported": "hflip" in image,
                    "native_image_vflip_supported": "vflip" in image,
                    "native_privacy_supported": bool(privacy_caps.get("enabled")),
                    "native_privacy_enabled": bool(self._coerce_bool(state_privacy.get("enabled"))),
                    "native_streaming_start_supported": bool(streaming_service_caps.get("start")),
                    "native_streaming_stop_supported": bool(streaming_service_caps.get("stop")),
                    "native_streaming_restart_supported": bool(streaming_service_caps.get("restart")),
                    "native_stream_controls": stream_controls,
                    "native_record_supported": bool(streams_caps.get("clip_recording")),
                    "native_image_brightness": str(image.get("brightness") or ""),
                    "native_image_contrast": str(image.get("contrast") or ""),
                    "native_image_saturation": str(image.get("saturation") or ""),
                    "native_image_sharpness": str(image.get("sharpness") or ""),
                    "native_image_anti_flicker": normalized_anti_flicker,
                    "native_image_hflip": bool(self._coerce_bool(image.get("hflip"))),
                    "native_image_vflip": bool(self._coerce_bool(image.get("vflip"))),
                    "native_motion_enabled": bool(self._coerce_bool(motion.get("enabled"))),
                    "native_daynight_enabled": bool(self._coerce_bool(daynight.get("enabled"))),
                    "native_daynight_force_mode": str(daynight.get("force_mode") or "").strip(),
                    "native_daynight_modes": self._normalize_daynight_modes(daynight_caps.get("modes")),
                    "native_daynight_requested_mode": str(state_daynight.get("target_mode") or ("auto" if self._coerce_bool(daynight.get("enabled")) else str(daynight.get("force_mode") or "").strip()) or "auto").strip(),
                    "native_daynight_running_mode": str(state_daynight.get("running_mode") or "").strip(),
                    "config_patch_example": json.dumps(patch_example, indent=2),
                }
            )

        defaults.update(self._optimistic_supported_controls_overlay(resolved))
        defaults.update(self._camera_send2_controls_for_ui(camera))
        return defaults

    def _normalize_native_api_error(self, error: Exception | str) -> str:
        message = str(error).strip()
        lowered = message.lower()
        if (
            "not found" in lowered
            and (
                "requested url /api/v1" in lowered
                or "requested url /x/api/v1" in lowered
            )
        ):
            return "Native API is not available on this camera build."
        return message

    def _native_api_status_for_error(self, error: str) -> str:
        normalized = self._normalize_native_api_error(error)
        if normalized == "Native API is not available on this camera build.":
            return "unsupported"
        return "offline"


    def _format_reply(self, topic: str, payload: str) -> tuple[int | None, str | None]:
        camera_id = self._camera_id_from_topic(topic)
        self._ensure_camera(camera_id)
        text = None
        chat_id = None
        try:
            decoded = json.loads(payload)
        except json.JSONDecodeError:
            decoded = None

        if isinstance(decoded, dict):
            request_id = decoded.get("request_id")
            if request_id:
                with self.reply_lock:
                    pending = self.pending_by_request.pop(str(request_id), None)
                if pending:
                    chat_id = pending["chat_id"]
            if chat_id is None and decoded.get("chat_id") is not None:
                decoded_chat_id = int(decoded["chat_id"])
                if decoded_chat_id > 0:
                    chat_id = decoded_chat_id
            if chat_id is None and camera_id in self.last_chat_by_camera:
                chat_id = self.last_chat_by_camera[camera_id]
            text = str(decoded.get("message") or decoded.get("text") or json.dumps(decoded, sort_keys=True))
        else:
            if camera_id in self.last_chat_by_camera:
                chat_id = self.last_chat_by_camera[camera_id]
            text = payload.strip()

        if chat_id is None:
            LOG.info("Dropping MQTT reply without Telegram chat mapping: %s", payload)
            return None, None

        prefix = camera_id
        if camera_id in self.cameras:
            prefix = self.cameras[camera_id].name
        return chat_id, f"[{prefix}] {text}"

    def _camera_id_from_topic(self, topic: str) -> str:
        parts = topic.split("/")
        if len(parts) >= 3:
            return parts[2].lower()
        return "unknown"

    def _registration_topic_for_camera(self, camera_id: str) -> str:
        topic = str(self.registration_topic or "").strip()
        if not topic:
            raise RuntimeError("Registration topic is not configured")
        if "{camera_id}" in topic:
            return topic.format(camera_id=camera_id)
        if "+" in topic:
            return topic.replace("+", camera_id, 1)
        if "#" in topic:
            return topic.replace("#", camera_id, 1)
        raise RuntimeError(f"Registration topic does not identify individual cameras: {topic}")

    def _ensure_camera(self, camera_id: str) -> None:
        created = False
        with self.state_lock:
            if camera_id not in self.cameras:
                self.cameras[camera_id] = Camera(camera_id=camera_id, name=camera_id)
                created = True
        if created:
            self._persist_state()

    def start(self) -> None:
        while not self.stop_event.is_set():
            try:
                if not self._connect_mqtt():
                    time.sleep(5)
                    continue
                updates = self.telegram.get_updates(self.update_offset, self.polling_timeout)
                self.last_telegram_ok_at = time.time()
                self.last_telegram_error = ""
                for update in updates:
                    self.update_offset = int(update["update_id"]) + 1
                    self._handle_update(update)
            except Exception:
                if self.stop_event.is_set():
                    break
                self.last_telegram_error = str(sys.exc_info()[1] or "unknown error")
                LOG.exception("Telegram polling failed")
                time.sleep(5)

    def stop(self) -> None:
        self.stop_event.set()
        try:
            if self.mqtt_client is not None:
                self.mqtt_client.loop_stop()
        finally:
            try:
                if self.mqtt_client is not None:
                    self.mqtt_client.disconnect()
            except Exception:
                LOG.debug("MQTT disconnect during shutdown failed", exc_info=True)
        if self.history_store is not None:
            self.history_store.close()
            self.history_store = None

    def _connect_mqtt(self) -> bool:
        if self.mqtt_client is None:
            return False
        if self.mqtt_client.is_connected():
            return True
        mqtt_cfg = self.config["mqtt"]
        try:
            self.mqtt_client.connect(
                mqtt_cfg["host"],
                int(mqtt_cfg.get("port", 1883)),
                int(mqtt_cfg.get("keepalive", 60)),
            )
            self.mqtt_client.loop_start()
            return True
        except OSError as error:
            LOG.warning(
                "MQTT connect failed to %s:%s: %s",
                mqtt_cfg["host"],
                mqtt_cfg.get("port", 1883),
                error,
            )
            self.last_mqtt_error = str(error)
            return False

    def snapshot_probe_loop(self) -> None:
        while not self.stop_event.is_set():
            if self.snapshot_heartbeat_interval_seconds <= 0:
                self.stop_event.wait(5)
                continue
            probe_count = self._probe_cameras()
            wait_seconds = self.snapshot_heartbeat_interval_seconds if probe_count else min(self.snapshot_heartbeat_interval_seconds, 5)
            if self.stop_event.wait(wait_seconds):
                return

    def _probe_cameras(self) -> int:
        with self.state_lock:
            cameras = list(self.cameras.values())
        attempted = 0
        for camera in cameras:
            if self.stop_event.is_set():
                return attempted
            snapshot_url = self._camera_snapshot_url(camera)
            if not snapshot_url:
                continue
            conflict = self._camera_ip_conflict(camera, cameras)
            if conflict is not None:
                self._record_probe_result(camera.camera_id, ok=False, error=self._camera_ip_conflict_error(camera, conflict))
                continue
            attempted += 1
            try:
                photo, filename = self._probe_snapshot(camera)
            except Exception as error:
                self._record_probe_result(camera.camera_id, ok=False, error=str(error))
            else:
                cache_path = self._store_cached_snapshot(camera.camera_id, photo, filename)
                self._record_probe_result(camera.camera_id, ok=True, error="", cache_path=cache_path)
        return attempted

    def _probe_snapshot(self, camera: Camera) -> tuple[bytes, str]:
        return self._fetch_snapshot(camera)

    def _record_probe_result(self, camera_id: str, ok: bool, error: str, cache_path: str = "") -> None:
        now = int(time.time())
        updated = False
        with self.state_lock:
            current = self.cameras.get(camera_id)
            if current is None:
                return
            self.cameras[camera_id] = replace(
                current,
                probe_status="online" if ok else "offline",
                last_probe_at=now,
                last_snapshot_ok_at=now if ok else current.last_snapshot_ok_at,
                last_probe_error="" if ok else error,
                snapshot_cache_path=cache_path or current.snapshot_cache_path,
            )
            updated = True
        if updated:
            self._persist_state()
        self._record_history_state_sample(
            camera_id,
            "snapshot_probe",
            {
                "probe_status": "online" if ok else "offline",
                "probe_error": error,
                "has_cached_snapshot": bool(cache_path),
            },
            normalized={
                "has_cached_snapshot": bool(cache_path),
            },
            recorded_at=now,
        )

    def _handle_update(self, update: dict[str, Any]) -> None:
        message = update.get("message") or update.get("edited_message")
        if not isinstance(message, dict):
            return
        text = message.get("text")
        if not text:
            return
        chat = message.get("chat") or {}
        chat_id = chat.get("id")
        if chat_id is None:
            return
        username = ((message.get("from") or {}).get("username") or "").strip()
        if not self._is_allowed(int(chat_id), username):
            LOG.info("Ignoring unauthorized Telegram message from chat=%s user=%s", chat_id, username)
            return
        LOG.info("Telegram command from chat=%s user=%s text=%s", chat_id, username, text)
        response = self._dispatch_command(int(chat_id), username, text)
        if response:
            self.telegram.send_message(int(chat_id), response)

    def _is_allowed(self, chat_id: int, username: str) -> bool:
        chat_allowed = not self.allowed_chat_ids or chat_id in self.allowed_chat_ids
        user_allowed = not self.allowed_usernames or username in self.allowed_usernames
        return chat_allowed and user_allowed

    def _dispatch_command(self, chat_id: int, username: str, text: str) -> str:
        parts = text.strip().split()
        if not parts:
            return ""
        head = parts[0].split("@", 1)[0].lower()
        if head in {"/help", "/start"}:
            return self._help_text()
        if head in {"/cam", "/camera"}:
            return self._handle_camera_command(chat_id, username, parts[1:])
        if head in {"/cams", "/list"}:
            return self._list_cameras()
        return "Unknown command. Try /help"

    def _help_text(self) -> str:
        return (
            "Commands:\n"
            "/cam list\n"
            "/cam <camera_id> <command> [args...]\n"
            "\n"
            "Examples:\n"
            "/cam aabbccddeeff snap\n"
            "/cam front-door arm"
        )

    def _list_cameras(self) -> str:
        if not self.cameras:
            return "No cameras known yet. Wait for MQTT auto-registration or add static aliases in config.yaml."
        lines = ["Configured cameras:"]
        for camera in sorted(self.cameras.values(), key=lambda item: item.name):
            details = []
            if camera.ip:
                details.append(camera.ip)
            if camera.snapshot_url:
                details.append("snap")
            suffix = f" ({', '.join(details)})" if details else ""
            lines.append(f"- {camera.name}: {camera.camera_id}{suffix}")
        return "\n".join(lines)

    def _resolve_camera_id(self, token: str) -> str | None:
        normalized = token.strip().lower()
        if normalized in self.cameras:
            return normalized
        for camera in self.cameras.values():
            if camera.name.lower() == normalized:
                return camera.camera_id
        return None

    def _camera_snapshot_url(self, camera: Camera, stream_name: str = "ch0") -> str | None:
        normalized_stream = str(stream_name or "ch0").strip().lower()
        normalized_stream = normalized_stream.split("?", 1)[0].split("&", 1)[0] or "ch0"

        if camera.ip:
            return f"http://{camera.ip}/x/{normalized_stream}.jpg"

        snapshot_url = camera.snapshot_url.strip()
        if not snapshot_url:
            return None
        parsed = urllib.parse.urlsplit(snapshot_url)
        if not parsed.netloc:
            return snapshot_url if normalized_stream == "ch0" else None
        scheme = parsed.scheme or "http"
        return urllib.parse.urlunsplit((scheme, parsed.netloc, f"/x/{normalized_stream}.jpg", "", ""))

    def _camera_mjpeg_url(self, camera: Camera, stream_name: str = "ch0") -> str:
        normalized_stream = str(stream_name or "ch0").strip().lower()
        normalized_stream = normalized_stream.split("?", 1)[0].split("&", 1)[0] or "ch0"
        if camera.ip:
            return f"http://{camera.ip}/x/{normalized_stream}.mjpg"

        snapshot_url = self._camera_snapshot_url(camera)
        if not snapshot_url:
            return ""

        parsed = urllib.parse.urlsplit(snapshot_url)
        if not parsed.netloc:
            return ""
        scheme = parsed.scheme or "http"
        return urllib.parse.urlunsplit((scheme, parsed.netloc, f"/x/{normalized_stream}.mjpg", "", ""))

    def _camera_web_ui_url(self, camera: Camera) -> str:
        if camera.ip:
            return f"http://{camera.ip}/"

        snapshot_url = self._camera_snapshot_url(camera)
        if not snapshot_url:
            return ""

        parsed = urllib.parse.urlsplit(snapshot_url)
        if not parsed.scheme or not parsed.netloc:
            return ""
        return urllib.parse.urlunsplit((parsed.scheme, parsed.netloc, "/", "", ""))

    def _camera_send2_request(
        self,
        camera: Camera,
        path: str,
        *,
        method: str = "GET",
        json_payload: dict[str, Any] | None = None,
        timeout_seconds: int | None = None,
    ) -> dict[str, Any]:
        base_url = self._camera_web_ui_url(camera)
        if not base_url:
            raise RuntimeError(f"Web UI URL is not configured for {camera.name}")

        url = urllib.parse.urljoin(base_url, path.lstrip("/"))
        headers = {"Accept": "application/json"}
        body = None
        if json_payload is not None:
            headers["Content-Type"] = "application/json"
            body = json.dumps(json_payload).encode("utf-8")

        request = urllib.request.Request(url, data=body, method=method.upper(), headers=headers)
        username, password = self._camera_onvif_credentials(camera)
        if username or password:
            auth_token = base64.b64encode(f"{username}:{password}".encode("utf-8")).decode("ascii")
            request.add_header("Authorization", f"Basic {auth_token}")

        try:
            timeout = max(int(timeout_seconds or 0), self.snapshot_heartbeat_timeout_seconds, 5)
            with urllib.request.urlopen(request, timeout=timeout) as response:
                payload = response.read().decode("utf-8", errors="replace")
        except urllib.error.HTTPError as error:
            detail = error.read().decode("utf-8", errors="replace").strip() or error.reason or f"HTTP {error.code}"
            raise RuntimeError(detail) from error
        except urllib.error.URLError as error:
            raise RuntimeError(str(error.reason or error)) from error

        try:
            decoded = json.loads(payload)
        except json.JSONDecodeError as error:
            raise RuntimeError("Camera send2 endpoint returned invalid JSON") from error
        if not isinstance(decoded, dict):
            raise RuntimeError("Camera send2 endpoint returned an invalid response")
        return decoded

    def _camera_send2_controls_for_ui(self, camera: Camera) -> dict[str, Any]:
        defaults = {
            "native_send2_available": False,
            "native_send2_error": "",
            "native_send2_overview_url": "",
            "native_send2_motion_sensitivity": "",
            "native_send2_motion_cooldown": "",
            "native_send2_services": [],
        }

        overview_url = urllib.parse.urljoin(self._camera_web_ui_url(camera), "tool-send2.html") if self._camera_web_ui_url(camera) else ""
        defaults["native_send2_overview_url"] = overview_url

        try:
            payload = self._camera_send2_request(camera, "/x/json-send2.cgi", method="GET")
        except Exception as error:
            defaults["native_send2_error"] = str(error)
            return defaults

        motion = payload.get("motion") or {}
        services: list[dict[str, Any]] = []
        for service_name, service_label in SEND2_SERVICES:
            service_data = payload.get(service_name) or {}
            if not isinstance(service_data, dict):
                service_data = {}
            required_fields = SEND2_REQUIRED_FIELDS.get(service_name, [])
            missing_fields = [field for field in required_fields if not self._send2_value_is_present(service_data.get(field))]
            filled_fields = [field for field in required_fields if field not in missing_fields]
            has_any_config = any(
                self._send2_value_is_present(value)
                for key, value in service_data.items()
                if key not in {"send_photo", "send_video"}
            )
            if not required_fields:
                readiness = "ready" if has_any_config else "unknown"
                readiness_summary = "Ready" if has_any_config else "Open camera page to configure"
            elif not missing_fields:
                readiness = "ready"
                readiness_summary = "Ready"
            elif filled_fields:
                readiness = "partial"
                readiness_summary = f"Missing: {', '.join(missing_fields)}"
            else:
                readiness = "not_configured"
                readiness_summary = "Not configured"

            photo_enabled = self._coerce_bool(service_data.get("send_photo")) is not False
            video_enabled = self._coerce_bool(service_data.get("send_video")) is True
            services.append(
                {
                    "name": service_name,
                    "label": service_label,
                    "motion_key": f"send2{service_name}",
                    "motion_enabled": bool(self._coerce_bool(motion.get(f"send2{service_name}"))),
                    "photo_enabled": photo_enabled,
                    "video_enabled": video_enabled,
                    "config_url": urllib.parse.urljoin(self._camera_web_ui_url(camera), f"tool-send2-{service_name}.html") if self._camera_web_ui_url(camera) else "",
                    "configured": has_any_config,
                    "readiness": readiness,
                    "readiness_summary": readiness_summary,
                    "missing_fields": missing_fields,
                    "photo_test_supported": photo_enabled,
                    "video_test_supported": video_enabled,
                    "default_test_supported": not photo_enabled and not video_enabled,
                }
            )

        defaults.update(
            {
                "native_send2_available": True,
                "native_send2_motion_sensitivity": "" if motion.get("sensitivity") in (None, "") else str(motion.get("sensitivity")),
                "native_send2_motion_cooldown": "" if motion.get("cooldown_time") in (None, "") else str(motion.get("cooldown_time")),
                "native_send2_services": services,
            }
        )
        return defaults

    def _send2_value_is_present(self, value: Any) -> bool:
        if value is None:
            return False
        if value is False:
            return False
        if isinstance(value, str):
            return bool(value.strip())
        if isinstance(value, (list, dict, tuple, set)):
            return bool(value)
        return True

    def _camera_onvif_endpoint(self, camera: Camera) -> str:
        endpoint = camera.onvif_endpoint.strip()
        if endpoint:
            return endpoint
        if camera.ip:
            return f"http://{camera.ip}/onvif/device_service"
        snapshot_url = camera.snapshot_url.strip()
        if not snapshot_url:
            return ""
        parsed = urllib.parse.urlsplit(snapshot_url)
        if not parsed.netloc:
            return ""
        scheme = parsed.scheme or "http"
        return urllib.parse.urlunsplit((scheme, parsed.netloc, "/onvif/device_service", "", ""))

    def _camera_onvif_credentials(self, camera: Camera) -> tuple[str, str]:
        username = camera.onvif_username.strip() or DEFAULT_THINGINO_USERNAME
        password = camera.onvif_password or DEFAULT_THINGINO_PASSWORD
        return username, password

    def _schedule_onvif_refresh_for_all(self) -> None:
        with self.state_lock:
            camera_ids = list(self.cameras)
        for camera_id in camera_ids:
            self._schedule_onvif_refresh(camera_id)

    def _schedule_onvif_refresh(self, camera_id: str) -> bool:
        resolved = self._resolve_camera_id(camera_id) or camera_id.strip().lower()
        with self.state_lock:
            camera = self.cameras.get(resolved)
            if camera is None:
                return False
            if not self._camera_onvif_endpoint(camera):
                return False
            if resolved in self.onvif_refreshing:
                return False
            self.onvif_refreshing.add(resolved)
        worker = threading.Thread(
            target=self._refresh_onvif_worker,
            args=(resolved,),
            name=f"telegrambothub-onvif-{resolved[:8]}",
            daemon=True,
        )
        worker.start()
        return True

    def _refresh_onvif_worker(self, camera_id: str) -> None:
        try:
            self.refresh_onvif_details(camera_id)
        finally:
            with self.state_lock:
                self.onvif_refreshing.discard(camera_id)

    def refresh_onvif_details(self, camera_id: str) -> bool:
        resolved = self._resolve_camera_id(camera_id) or camera_id.strip().lower()
        with self.state_lock:
            camera = self.cameras.get(resolved)
        if camera is None:
            raise RuntimeError(f"Unknown camera: {camera_id}")

        conflict = self._camera_ip_conflict(camera)
        if conflict is not None:
            self._record_onvif_result(resolved, None, self._camera_ip_conflict_error(camera, conflict))
            return False

        endpoint = self._camera_onvif_endpoint(camera)
        if not endpoint:
            raise RuntimeError(f"ONVIF endpoint is not configured for {camera.name}")

        try:
            info = self._fetch_onvif_device_information(camera)
        except Exception as error:
            self._record_onvif_result(resolved, None, str(error))
            return False

        self._record_onvif_result(resolved, info, "")
        return True

    def _record_onvif_result(self, camera_id: str, info: dict[str, str] | None, error: str) -> None:
        now = int(time.time())
        updated = False
        with self.state_lock:
            current = self.cameras.get(camera_id)
            if current is None:
                return
            values = info or {}
            self.cameras[camera_id] = replace(
                current,
                onvif_manufacturer=values.get("manufacturer", current.onvif_manufacturer) if info is not None else current.onvif_manufacturer,
                onvif_model=values.get("model", current.onvif_model) if info is not None else current.onvif_model,
                onvif_firmware_version=values.get("firmware_version", current.onvif_firmware_version) if info is not None else current.onvif_firmware_version,
                onvif_serial_number=values.get("serial_number", current.onvif_serial_number) if info is not None else current.onvif_serial_number,
                onvif_hardware_id=values.get("hardware_id", current.onvif_hardware_id) if info is not None else current.onvif_hardware_id,
                onvif_last_ok_at=now if info is not None else current.onvif_last_ok_at,
                onvif_last_error=error,
            )
            updated = True
        if updated:
            self._persist_state()

    def _fetch_onvif_device_information(self, camera: Camera) -> dict[str, str]:
        endpoint = self._camera_onvif_endpoint(camera)
        if not endpoint:
            raise RuntimeError("camera ONVIF endpoint is not configured")

        last_error = ""
        for soap_version in ("1.2", "1.1"):
            try:
                response = self._perform_onvif_device_request(camera, endpoint, soap_version)
                return self._parse_onvif_device_information(response)
            except Exception as error:
                last_error = str(error)
        raise RuntimeError(last_error or "ONVIF device information request failed")

    def _perform_onvif_device_request(self, camera: Camera, endpoint: str, soap_version: str) -> bytes:
        action = "http://www.onvif.org/ver10/device/wsdl/GetDeviceInformation"
        body = self._build_onvif_device_information_envelope(camera, soap_version)
        request = urllib.request.Request(endpoint, data=body, method="POST")
        request.add_header("Accept", "application/soap+xml, text/xml")
        username, password = self._camera_onvif_credentials(camera)
        auth_token = base64.b64encode(f"{username}:{password}".encode("utf-8")).decode("ascii")
        request.add_header("Authorization", f"Basic {auth_token}")
        if soap_version == "1.2":
            request.add_header(
                "Content-Type",
                f'application/soap+xml; charset=utf-8; action="{action}"',
            )
        else:
            request.add_header("Content-Type", "text/xml; charset=utf-8")
            request.add_header("SOAPAction", f'"{action}"')
        with urllib.request.urlopen(request, timeout=self.snapshot_heartbeat_timeout_seconds) as response:
            return response.read()

    def _build_onvif_device_information_envelope(self, camera: Camera, soap_version: str) -> bytes:
        envelope_ns = (
            "http://www.w3.org/2003/05/soap-envelope"
            if soap_version == "1.2"
            else "http://schemas.xmlsoap.org/soap/envelope/"
        )
        header = self._onvif_security_header(camera, envelope_ns)
        envelope = (
            f'<s:Envelope xmlns:s="{envelope_ns}" '
            'xmlns:tds="http://www.onvif.org/ver10/device/wsdl" '
            'xmlns:wsse="http://docs.oasis-open.org/wss/2004/01/oasis-200401-wss-wssecurity-secext-1.0.xsd" '
            'xmlns:wsu="http://docs.oasis-open.org/wss/2004/01/oasis-200401-wss-wssecurity-utility-1.0.xsd">'
            f"{header}"
            "<s:Body><tds:GetDeviceInformation/></s:Body>"
            "</s:Envelope>"
        )
        return envelope.encode("utf-8")

    def _onvif_security_header(self, camera: Camera, envelope_ns: str) -> str:
        username, password = self._camera_onvif_credentials(camera)
        if not username or not password:
            return ""
        nonce = secrets.token_bytes(16)
        created = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        digest = hashlib.sha1(nonce + created.encode("utf-8") + password.encode("utf-8")).digest()
        nonce_b64 = base64.b64encode(nonce).decode("ascii")
        digest_b64 = base64.b64encode(digest).decode("ascii")
        return (
            f'<s:Header><wsse:Security s:mustUnderstand="1" xmlns:s="{envelope_ns}">'
            "<wsse:UsernameToken>"
            f"<wsse:Username>{xml_escape(username)}</wsse:Username>"
            '<wsse:Password Type="http://docs.oasis-open.org/wss/2004/01/oasis-200401-wss-username-token-profile-1.0#PasswordDigest">'
            f"{digest_b64}</wsse:Password>"
            '<wsse:Nonce EncodingType="http://docs.oasis-open.org/wss/2004/01/oasis-200401-wss-soap-message-security-1.0#Base64Binary">'
            f"{nonce_b64}</wsse:Nonce>"
            f"<wsu:Created>{created}</wsu:Created>"
            "</wsse:UsernameToken></wsse:Security></s:Header>"
        )

    def _parse_onvif_device_information(self, payload: bytes) -> dict[str, str]:
        try:
            root = ElementTree.fromstring(payload)
        except ElementTree.ParseError as error:
            raise RuntimeError(f"Invalid ONVIF XML response: {error}") from error

        response = next(
            (element for element in root.iter() if self._xml_local_name(element.tag) == "GetDeviceInformationResponse"),
            None,
        )
        if response is None:
            raise RuntimeError(self._extract_onvif_fault(root) or "Missing ONVIF device information response")

        return {
            "manufacturer": self._find_xml_child_text(response, "Manufacturer"),
            "model": self._find_xml_child_text(response, "Model"),
            "firmware_version": self._find_xml_child_text(response, "FirmwareVersion"),
            "serial_number": self._find_xml_child_text(response, "SerialNumber"),
            "hardware_id": self._find_xml_child_text(response, "HardwareId"),
        }

    def _extract_onvif_fault(self, root: ElementTree.Element) -> str:
        for element in root.iter():
            if self._xml_local_name(element.tag) not in {"Text", "faultstring", "Reason"}:
                continue
            text = " ".join(part.strip() for part in element.itertext() if part.strip())
            if text:
                return text
        return ""

    def _find_xml_child_text(self, parent: ElementTree.Element, local_name: str) -> str:
        for child in list(parent):
            if self._xml_local_name(child.tag) != local_name:
                continue
            text = "".join(child.itertext()).strip()
            if text:
                return text
        return ""

    def _xml_local_name(self, tag: str) -> str:
        if tag.startswith("{"):
            return tag.split("}", 1)[1]
        return tag

    def _build_snapshot_request(self, camera: Camera) -> urllib.request.Request:
        snapshot_url = self._camera_snapshot_url(camera)
        if not snapshot_url:
            raise RuntimeError("camera snapshot endpoint is not configured")
        request = urllib.request.Request(snapshot_url, method="GET")
        if camera.api_key:
            request.add_header("X-API-Key", camera.api_key)
        return request

    def _fetch_snapshot_via_api(self, camera: Camera) -> tuple[bytes, str]:
        client = self._camera_api_client(camera)
        return client.fetch_snapshot(0)

    def _snapshot_cache_file_for(self, camera_id: str, filename: str) -> Path:
        suffix = Path(filename).suffix.lower() or ".jpg"
        return self.snapshot_cache_dir / f"{camera_id}{suffix}"

    def _store_cached_snapshot(self, camera_id: str, photo: bytes, filename: str) -> str:
        cache_path = self._snapshot_cache_file_for(camera_id, filename)
        temp_path = cache_path.with_name(f"{cache_path.name}.{uuid.uuid4().hex}.tmp")

        with self.state_lock:
            current = self.cameras.get(camera_id)
            previous_cache_path = current.snapshot_cache_path if current is not None else ""

        temp_path.write_bytes(photo)
        temp_path.replace(cache_path)

        if previous_cache_path and previous_cache_path != str(cache_path):
            previous_path = Path(previous_cache_path)
            if previous_path.exists():
                try:
                    previous_path.unlink()
                except OSError:
                    LOG.debug("Failed to remove previous snapshot cache %s", previous_path, exc_info=True)

        return str(cache_path)

    def _cached_snapshot_path(self, camera: Camera) -> Path | None:
        cache_path = camera.snapshot_cache_path.strip()
        if not cache_path:
            return None
        resolved = Path(cache_path)
        if resolved.is_file():
            return resolved
        return None

    def _clear_cached_snapshot(self, camera_id: str, cache_path: Path | None = None) -> None:
        path_to_remove = cache_path
        cleared = False
        with self.state_lock:
            current = self.cameras.get(camera_id)
            if current is None:
                return
            if path_to_remove is None and current.snapshot_cache_path:
                path_to_remove = Path(current.snapshot_cache_path)
            self.cameras[camera_id] = replace(current, snapshot_cache_path="")
            cleared = True

        if path_to_remove is not None and path_to_remove.exists():
            try:
                path_to_remove.unlink()
            except OSError:
                LOG.debug("Failed to remove expired snapshot cache %s", path_to_remove, exc_info=True)
        if cleared:
            self._persist_state()

    def _fetch_snapshot(self, camera: Camera) -> tuple[bytes, str]:
        if self._camera_api_base_url(camera):
            try:
                photo, filename = self._fetch_snapshot_via_api(camera)
                return photo, f"{camera.camera_id}{Path(filename).suffix or '.jpg'}"
            except (CameraApiError, urllib.error.URLError, http.client.IncompleteRead) as error:
                self._record_api_result(camera.camera_id, None, str(error))
                if not self._camera_snapshot_url(camera):
                    raise

        request = self._build_snapshot_request(camera)
        with urllib.request.urlopen(request, timeout=self.snapshot_heartbeat_timeout_seconds) as response:
            content_type = response.headers.get("Content-Type", "image/jpeg")
            photo = response.read()
        extension = mimetypes.guess_extension(content_type.split(";", 1)[0].strip()) or ".jpg"
        filename = f"{camera.camera_id}{extension}"
        return photo, filename

    def _handle_camera_command(self, chat_id: int, username: str, args: list[str]) -> str:
        if not args or args[0].lower() == "list":
            return self._list_cameras()
        if len(args) < 2:
            return "Usage: /cam <camera_id> <command> [args...]"
        camera_id = self._resolve_camera_id(args[0])
        if camera_id is None:
            return f"Unknown camera: {args[0]}"
        command = args[1]
        command_args = args[2:]
        camera = self.cameras.get(camera_id, Camera(camera_id, camera_id))
        if command.lower() == "snap":
            try:
                photo, filename = self._fetch_snapshot(camera)
                self.telegram.send_photo(chat_id, photo, filename, caption=camera.name)
                self.last_chat_by_camera[camera_id] = chat_id
                return ""
            except Exception as error:
                LOG.exception("Failed to fetch snapshot for %s", camera_id)
                return f"Snapshot failed for {camera.name}: {error}"

        request_id = uuid.uuid4().hex
        payload = {
            "request_id": request_id,
            "chat_id": chat_id,
            "username": username,
            "camera_id": camera_id,
            "command": command,
            "args": command_args,
            "raw_text": " ".join(args[1:]),
            "sent_at": int(time.time()),
        }
        topic = self.command_topic_template.format(camera_id=camera_id)
        with self.reply_lock:
            self.pending_by_request[request_id] = {
                "chat_id": chat_id,
                "camera_id": camera_id,
                "command": command,
                "created_at": time.time(),
            }
        self.last_chat_by_camera[camera_id] = chat_id
        info = self.mqtt_client.publish(topic, json.dumps(payload), qos=1)
        info.wait_for_publish()
        if info.rc != mqtt.MQTT_ERR_SUCCESS:
            with self.reply_lock:
                self.pending_by_request.pop(request_id, None)
            return "Failed to publish MQTT command"
        display_name = camera.name
        joined_args = " ".join(command_args)
        summary = command if not joined_args else f"{command} {joined_args}"
        return f"Queued for {display_name}: {summary}"

    def export_config(self) -> dict[str, Any]:
        with self.state_lock:
            return json.loads(json.dumps(self.config))

    def export_camera_override(self, camera_id: str) -> dict[str, Any]:
        resolved = self._resolve_camera_id(camera_id) or camera_id.strip().lower()
        camera = self.cameras.get(resolved)
        if camera is None:
            raise RuntimeError(f"Unknown camera: {camera_id}")

        config = self.export_config()
        entry = None
        for item in config.get("cameras", []):
            if str(item.get("id") or "").strip().lower() == resolved:
                entry = item
                break
        if entry is None:
            entry = {"id": resolved}

        return {
            "id": resolved,
            "name": str(entry.get("name") or camera.name),
            "ip": str(entry.get("ip") or camera.ip),
            "snapshot_url": str(entry.get("snapshot_url") or camera.snapshot_url),
            "api_key": str(entry.get("api_key") or camera.api_key),
            "api_base_url": str(entry.get("api_base_url") or camera.api_base_url),
            "api_token": str(entry.get("api_token") or camera.api_token),
            "onvif_endpoint": str(entry.get("onvif_endpoint") or camera.onvif_endpoint),
            "onvif_username": str(entry.get("onvif_username") or camera.onvif_username),
            "onvif_password": str(entry.get("onvif_password") or camera.onvif_password),
        }

    def update_camera_override(self, camera_id: str, override: dict[str, str]) -> None:
        resolved = self._resolve_camera_id(camera_id) or camera_id.strip().lower()
        camera = self.cameras.get(resolved)
        if camera is None:
            raise RuntimeError(f"Unknown camera: {camera_id}")

        config = self.export_config()
        cameras = list(config.get("cameras", []))
        index = None
        for idx, item in enumerate(cameras):
            if str(item.get("id") or "").strip().lower() == resolved:
                index = idx
                break

        entry = {
            "id": resolved,
            "name": override.get("name", "").strip() or camera.name,
            "ip": override.get("ip", "").strip(),
            "snapshot_url": override.get("snapshot_url", "").strip(),
            "api_key": override.get("api_key", "").strip(),
            "api_base_url": override.get("api_base_url", "").strip(),
            "api_token": override.get("api_token", "").strip(),
            "onvif_endpoint": override.get("onvif_endpoint", "").strip(),
            "onvif_username": override.get("onvif_username", "").strip(),
            "onvif_password": override.get("onvif_password", ""),
        }
        if index is None:
            cameras.append(entry)
        else:
            cameras[index] = entry
        config["cameras"] = cameras
        self.save_config(config)
        self.reload_config()

    def unregister_camera(self, camera_id: str) -> dict[str, Any]:
        resolved = self._resolve_camera_id(camera_id) or camera_id.strip().lower()
        if resolved not in self.cameras:
            raise RuntimeError(f"Unknown camera: {camera_id}")

        command_published = self._publish_control_command(resolved, "unregister")

        config = self.export_config()
        original_cameras = list(config.get("cameras", []))
        filtered_cameras = [
            item
            for item in original_cameras
            if str(item.get("id") or "").strip().lower() != resolved
        ]
        config_removed = len(filtered_cameras) != len(original_cameras)
        if config_removed:
            config["cameras"] = filtered_cameras
            self.save_config(config)
            self.reload_config()

        retained_cleared = False
        retained_error = ""
        try:
            topic = self._registration_topic_for_camera(resolved)
            if self._connect_mqtt() and self.mqtt_client is not None:
                info = self.mqtt_client.publish(topic, b"", qos=1, retain=True)
                info.wait_for_publish()
                if info.rc == mqtt.MQTT_ERR_SUCCESS:
                    retained_cleared = True
                else:
                    retained_error = f"publish rc={info.rc}"
                    LOG.warning("Failed to clear retained registration for %s: rc=%s", resolved, info.rc)
            else:
                retained_error = "MQTT is not connected"
        except Exception as error:
            retained_error = str(error)
            LOG.warning("Failed to clear retained registration for %s: %s", resolved, error)

        cached_snapshot_path = ""
        with self.state_lock:
            current = self.cameras.pop(resolved, None)
            if current is not None:
                cached_snapshot_path = current.snapshot_cache_path
            self.last_chat_by_camera.pop(resolved, None)
            self.native_action_history_by_camera.pop(resolved, None)
        with self.reply_lock:
            stale_request_ids = [
                request_id
                for request_id, pending in self.pending_by_request.items()
                if pending.get("camera_id") == resolved
            ]
            for request_id in stale_request_ids:
                self.pending_by_request.pop(request_id, None)

        if cached_snapshot_path:
            cache_file = Path(cached_snapshot_path)
            if cache_file.exists():
                try:
                    cache_file.unlink()
                except OSError:
                    LOG.debug("Failed to remove snapshot cache %s", cache_file, exc_info=True)

        self._persist_state()

        return {
            "camera_id": resolved,
            "command_published": command_published,
            "config_removed": config_removed,
            "retained_cleared": retained_cleared,
            "retained_error": retained_error,
        }

    def save_config(self, config: dict[str, Any]) -> None:
        config_path = Path(self.config_path)
        with config_path.open("w", encoding="utf-8") as handle:
            yaml.safe_dump(config, handle, sort_keys=False)

    def reload_config(self) -> None:
        config = load_config(self.config_path)
        self._apply_config(config)

    def snapshot_status(self) -> dict[str, Any]:
        mqtt_cfg = self.config["mqtt"]
        return {
            "telegram_ok": self.last_telegram_error == "",
            "telegram_last_ok": self._format_timestamp(self.last_telegram_ok_at),
            "telegram_last_error": self.last_telegram_error,
            "mqtt_connected": self.mqtt_connected,
            "mqtt_host": mqtt_cfg["host"],
            "mqtt_port": mqtt_cfg.get("port", 1883),
            "mqtt_last_error": self.last_mqtt_error,
            "config_path": self.config_path,
            "history_enabled": self.history_enabled,
            "history_db_path": self.history_db_path,
            "history_recent_actions_limit": self.history_recent_actions_limit,
            "history_max_action_events_per_camera": self.history_max_action_events_per_camera,
            "history_max_state_samples_per_camera": self.history_max_state_samples_per_camera,
            "last_reload_at": self._format_timestamp(self.last_reload_at),
            **self._api_summary(),
            **self._onvif_summary(),
        }

    def _api_summary(self) -> dict[str, Any]:
        with self.state_lock:
            cameras = list(self.cameras.values())

        eligible = 0
        refreshed = 0
        errors = 0
        last_ok_at: int | None = None
        for camera in cameras:
            if not self._camera_api_base_url(camera):
                continue
            eligible += 1
            if camera.api_last_ok_at is not None:
                refreshed += 1
                last_ok_at = max(last_ok_at or camera.api_last_ok_at, camera.api_last_ok_at)
            if camera.api_last_error:
                errors += 1

        return {
            "api_known": eligible,
            "api_ready": refreshed,
            "api_errors": errors,
            "api_last_ok": self._format_timestamp(last_ok_at),
        }

    def _onvif_summary(self) -> dict[str, Any]:
        with self.state_lock:
            cameras = list(self.cameras.values())

        eligible = 0
        refreshed = 0
        errors = 0
        last_ok_at: int | None = None
        for camera in cameras:
            if not self._camera_onvif_endpoint(camera):
                continue
            eligible += 1
            if camera.onvif_last_ok_at is not None:
                refreshed += 1
                last_ok_at = max(last_ok_at or camera.onvif_last_ok_at, camera.onvif_last_ok_at)
            if camera.onvif_last_error:
                errors += 1

        return {
            "onvif_known": eligible,
            "onvif_ready": refreshed,
            "onvif_errors": errors,
            "onvif_last_ok": self._format_timestamp(last_ok_at),
        }

    def list_cameras_for_ui(self) -> list[dict[str, str]]:
        cameras = []
        with self.state_lock:
            snapshot = sorted(self.cameras.values(), key=self._camera_ui_sort_key)
        for camera in snapshot:
            conflict = self._camera_ip_conflict(camera, snapshot)
            api_error = self._normalize_native_api_error(camera.api_last_error)
            api_status = camera.api_status
            if camera.api_status == "offline" and api_error == "Native API is not available on this camera build.":
                api_status = "unsupported"
            preview_state = self._camera_preview_state(camera)
            cameras.append(
                {
                    "camera_id": camera.camera_id,
                    "name": camera.name,
                    "ip": camera.ip,
                    "snapshot_url": "" if conflict is not None else (self._camera_snapshot_url(camera) or ""),
                    "status": self._camera_status_for_ui(camera),
                    "api_status": api_status,
                    "api_streamer": camera.api_streamer,
                    "preview_state": preview_state,
                    "hostname": camera.hostname,
                    "onvif_label": " ".join(part for part in (camera.onvif_manufacturer, camera.onvif_model) if part).strip(),
                    "last_registration_at": self._format_timestamp(camera.last_registration_at),
                    "last_probe_at": self._format_timestamp(camera.last_probe_at),
                    "last_snapshot_ok_at": self._format_timestamp(camera.last_snapshot_ok_at),
                    "last_probe_error": camera.last_probe_error,
                    "preview_version": str(camera.last_snapshot_ok_at or camera.last_probe_at or 0),
                }
            )
        return cameras

    def _camera_ui_sort_key(self, camera: Camera) -> tuple[int, bytes | str, str]:
        try:
            parsed_ip = ipaddress.ip_address(camera.ip.strip()) if camera.ip.strip() else None
        except ValueError:
            parsed_ip = None

        if parsed_ip is None:
            return (1, camera.ip.lower(), camera.name.lower())
        return (0, parsed_ip.packed, camera.name.lower())

    def get_camera_for_ui(self, camera_id: str) -> dict[str, str]:
        resolved = self._resolve_camera_id(camera_id) or camera_id.strip().lower()
        with self.state_lock:
            camera = self.cameras.get(resolved)
        if camera is None:
            raise RuntimeError(f"Unknown camera: {camera_id}")
        override = self.export_camera_override(resolved)
        conflict = self._camera_ip_conflict(camera)
        live_links_available = conflict is None
        api_error = self._normalize_native_api_error(camera.api_last_error)
        api_status = camera.api_status
        if camera.api_status == "offline" and api_error == "Native API is not available on this camera build.":
            api_status = "unsupported"
        preview_state = self._camera_preview_state(camera)
        return {
            "camera_id": camera.camera_id,
            "name": camera.name,
            "hostname": camera.hostname or "n/a",
            "ip": camera.ip or "",
            "snapshot_url": (self._camera_snapshot_url(camera) or "") if live_links_available else "",
            "web_ui_url": self._camera_web_ui_url(camera) if live_links_available else "",
            "api_base_url": self._camera_api_base_url(camera) if live_links_available else "",
            "api_status": api_status,
            "api_last_ok_at": self._format_timestamp(camera.api_last_ok_at),
            "api_last_error": api_error,
            "api_device_name": camera.api_device_name,
            "api_device_model": camera.api_device_model,
            "api_streamer": camera.api_streamer,
            "api_version": camera.api_version,
            "onvif_endpoint": self._camera_onvif_endpoint(camera) if live_links_available else "",
            "api_key": camera.api_key,
            "status": self._camera_status_for_ui(camera),
            "preview_state": preview_state,
            "last_registration_at": self._format_timestamp(camera.last_registration_at),
            "last_probe_at": self._format_timestamp(camera.last_probe_at),
            "last_snapshot_ok_at": self._format_timestamp(camera.last_snapshot_ok_at),
            "last_probe_error": camera.last_probe_error,
            "identity_conflict_error": self._camera_ip_conflict_error(camera, conflict) if conflict is not None else "",
            "onvif_manufacturer": camera.onvif_manufacturer,
            "onvif_model": camera.onvif_model,
            "onvif_firmware_version": camera.onvif_firmware_version,
            "onvif_serial_number": camera.onvif_serial_number,
            "onvif_hardware_id": camera.onvif_hardware_id,
            "onvif_last_ok_at": self._format_timestamp(camera.onvif_last_ok_at),
            "onvif_last_error": camera.onvif_last_error,
            "preview_version": str(camera.last_snapshot_ok_at or camera.last_probe_at or 0),
            "override_name": override.get("name", ""),
            "override_ip": override.get("ip", ""),
            "override_snapshot_url": override.get("snapshot_url", ""),
            "override_api_key": override.get("api_key", ""),
            "override_api_base_url": override.get("api_base_url", ""),
            "override_api_token": override.get("api_token", ""),
            "override_onvif_endpoint": override.get("onvif_endpoint", ""),
            "override_onvif_username": override.get("onvif_username", ""),
            "override_onvif_password": override.get("onvif_password", ""),
            "native_action_history": self._native_action_history_for_ui(resolved),
        }

    def _timestamp_is_recent(self, timestamp: int | None, window_seconds: int) -> bool:
        if timestamp is None or window_seconds <= 0:
            return False
        return (time.time() - float(timestamp)) <= float(window_seconds)

    def _camera_probe_grace_seconds(self) -> int:
        interval = max(0, int(self.snapshot_heartbeat_interval_seconds))
        timeout = max(1, int(self.snapshot_heartbeat_timeout_seconds))
        return max(interval + max(timeout * 2, 15), timeout + 15)

    def _camera_registration_status_for_ui(self, camera: Camera) -> str:
        if camera.status != "online":
            return camera.status
        if camera.last_registration_at is None:
            return camera.status
        if self.registration_stale_after_seconds <= 0:
            return camera.status
        if time.time() - float(camera.last_registration_at) > self.registration_stale_after_seconds:
            return "offline"
        return camera.status

    def _camera_has_recent_live_signal(self, camera: Camera) -> bool:
        if self._timestamp_is_recent(camera.last_snapshot_ok_at, self._camera_probe_grace_seconds()):
            return True

        if camera.api_status == "online":
            api_window = max(self.registration_stale_after_seconds, self._camera_probe_grace_seconds())
            if self._timestamp_is_recent(camera.api_last_ok_at, api_window):
                return True

        return False

    def _camera_status_for_ui(self, camera: Camera) -> str:
        if self._camera_ip_conflict(camera) is not None:
            return "offline"
        if self._camera_has_recent_live_signal(camera):
            return "online"
        if self.snapshot_heartbeat_interval_seconds > 0 and self._camera_snapshot_url(camera):
            if camera.probe_status in {"online", "offline"}:
                return camera.probe_status
        return self._camera_registration_status_for_ui(camera)

    def _camera_preview_state(self, camera: Camera) -> str:
        if not self._camera_snapshot_url(camera):
            return "placeholder"

        cached_snapshot = self._cached_snapshot_path(camera)
        if cached_snapshot is None or camera.last_snapshot_ok_at is None:
            return "placeholder"

        if self.snapshot_cache_stale_after_seconds > 0:
            snapshot_age = time.time() - float(camera.last_snapshot_ok_at)
            if snapshot_age > self.snapshot_cache_stale_after_seconds:
                self._clear_cached_snapshot(camera.camera_id, cached_snapshot)
                return "placeholder"

        if self._camera_status_for_ui(camera) != "online":
            return "stale"
        return "live"

    def get_cached_snapshot_for_ui(self, camera_id: str) -> Path | None:
        resolved = self._resolve_camera_id(camera_id) or camera_id.strip().lower()
        with self.state_lock:
            camera = self.cameras.get(resolved)
        if camera is None:
            raise RuntimeError(f"Unknown camera: {camera_id}")
        if self._camera_preview_state(camera) == "placeholder":
            return None
        return self._cached_snapshot_path(camera)

    def get_camera_snapshot_url_for_ui(self, camera_id: str, stream_name: str = "ch0") -> str:
        resolved = self._resolve_camera_id(camera_id) or camera_id.strip().lower()
        with self.state_lock:
            camera = self.cameras.get(resolved)
        if camera is None:
            raise RuntimeError(f"Unknown camera: {camera_id}")
        conflict = self._camera_ip_conflict(camera)
        if conflict is not None:
            raise RuntimeError(self._camera_ip_conflict_error(camera, conflict))
        return self._camera_snapshot_url(camera, stream_name) or ""

    def get_camera_mjpeg_url_for_ui(self, camera_id: str, stream_name: str = "ch0") -> str:
        resolved = self._resolve_camera_id(camera_id) or camera_id.strip().lower()
        with self.state_lock:
            camera = self.cameras.get(resolved)
        if camera is None:
            raise RuntimeError(f"Unknown camera: {camera_id}")
        conflict = self._camera_ip_conflict(camera)
        if conflict is not None:
            raise RuntimeError(self._camera_ip_conflict_error(camera, conflict))
        return self._camera_mjpeg_url(camera, stream_name)

    def _camera_ip_conflict(self, camera: Camera, cameras_snapshot: list[Camera] | None = None) -> Camera | None:
        ip = str(camera.ip or "").strip()
        if not ip:
            return None

        if cameras_snapshot is None:
            with self.state_lock:
                cameras_snapshot = list(self.cameras.values())

        current_registered_at = self._coerce_int(camera.last_registration_at) or 0
        conflict: Camera | None = None
        for other in cameras_snapshot:
            if other.camera_id == camera.camera_id:
                continue
            if str(other.ip or "").strip() != ip:
                continue
            other_registered_at = self._coerce_int(other.last_registration_at) or 0
            if other_registered_at <= current_registered_at or other_registered_at <= 0:
                continue
            if conflict is None or other_registered_at > (self._coerce_int(conflict.last_registration_at) or 0):
                conflict = other
        return conflict

    def _camera_ip_conflict_error(self, camera: Camera, conflict: Camera | None = None) -> str:
        resolved_conflict = conflict or self._camera_ip_conflict(camera)
        if resolved_conflict is None:
            return ""
        return f"IP {camera.ip} is now registered by {resolved_conflict.camera_id}; refusing to mix camera identities"

    def refresh_snapshot_cache(self, camera_id: str) -> bool:
        resolved = self._resolve_camera_id(camera_id) or camera_id.strip().lower()
        with self.state_lock:
            camera = self.cameras.get(resolved)
        if camera is None:
            raise RuntimeError(f"Unknown camera: {camera_id}")
        conflict = self._camera_ip_conflict(camera)
        if conflict is not None:
            self._record_probe_result(resolved, ok=False, error=self._camera_ip_conflict_error(camera, conflict))
            return False
        if not self._camera_snapshot_url(camera):
            raise RuntimeError(f"Snapshot URL is not configured for {camera.name}")

        try:
            photo, filename = self._fetch_snapshot(camera)
        except Exception as error:
            self._record_probe_result(resolved, ok=False, error=str(error))
            return False

        cache_path = self._store_cached_snapshot(resolved, photo, filename)
        self._record_probe_result(resolved, ok=True, error="", cache_path=cache_path)
        return True

    def rescan_cameras(self, camera_id: str | None = None) -> tuple[int, int]:
        if camera_id is None:
            target_ids = sorted(self.cameras)
        else:
            resolved = self._resolve_camera_id(camera_id) or camera_id.strip().lower()
            if resolved not in self.cameras:
                raise RuntimeError(f"Unknown camera: {camera_id}")
            target_ids = [resolved]

        if not target_ids:
            return 0, 0

        published = 0
        for target_id in target_ids:
            if self._publish_control_command(target_id, "register"):
                published += 1
            self._schedule_onvif_refresh(target_id)
        return len(target_ids), published

    def _publish_control_command(self, camera_id: str, command: str) -> bool:
        if not self._connect_mqtt() or self.mqtt_client is None:
            return False

        payload = {
            "request_id": uuid.uuid4().hex,
            "chat_id": 0,
            "username": "",
            "camera_id": camera_id,
            "command": command,
            "args": [],
            "raw_text": command,
            "sent_at": int(time.time()),
        }
        topic = self.command_topic_template.format(camera_id=camera_id)
        info = self.mqtt_client.publish(topic, json.dumps(payload), qos=1)
        info.wait_for_publish()
        if info.rc == mqtt.MQTT_ERR_SUCCESS:
            return True
        LOG.warning("Failed to publish %s request for %s: rc=%s", command, camera_id, info.rc)
        return False

    def _format_timestamp(self, timestamp: float | None) -> str:
        if timestamp is None:
            return ""
        return datetime.fromtimestamp(timestamp, tz=timezone.utc).astimezone().strftime("%Y-%m-%d %H:%M:%S %Z")

    def _coerce_int(self, value: Any) -> int | None:
        try:
            if value is None or value == "":
                return None
            return int(value)
        except (TypeError, ValueError):
            return None

    def _coerce_bool(self, value: Any) -> bool | None:
        if isinstance(value, bool):
            return value
        if value is None or value == "":
            return None
        lowered = str(value).strip().lower()
        if lowered in {"1", "true", "yes", "on", "enabled"}:
            return True
        if lowered in {"0", "false", "no", "off", "disabled"}:
            return False
        return None

    def _normalize_daynight_modes(self, value: Any) -> list[str]:
        if not isinstance(value, list):
            return ["auto", "day", "night"]
        modes = []
        for item in value:
            mode = str(item or "").strip().lower()
            if mode and mode not in modes:
                modes.append(mode)
        return modes or ["auto", "day", "night"]

    def _normalize_anti_flicker_mode(self, value: Any) -> str:
        mode = str(value or "").strip().lower()
        if not mode:
            return "off"
        aliases = {
            "0": "off",
            "off": "off",
            "disable": "off",
            "disabled": "off",
            "1": "50hz",
            "50": "50hz",
            "50hz": "50hz",
            "2": "60hz",
            "60": "60hz",
            "60hz": "60hz",
        }
        return aliases.get(mode, mode)


def load_config_dict(config: dict[str, Any]) -> dict[str, Any]:
    for section in ("telegram", "mqtt", "routing"):
        if section not in config:
            raise ValueError(f"Missing config section: {section}")
    config.setdefault("ui", {})
    config.setdefault("history", {})
    config["ui"].setdefault("username", "")
    config["ui"].setdefault("password", "")
    config["ui"].setdefault("registration_stale_after_seconds", 0)
    config["ui"].setdefault("snapshot_heartbeat_interval_seconds", 60)
    config["ui"].setdefault("snapshot_heartbeat_timeout_seconds", 5)
    config["ui"].setdefault("snapshot_cache_stale_after_seconds", 3600)
    config["history"].setdefault("enabled", True)
    config["history"].setdefault("path", "")
    config["history"].setdefault("recent_actions_limit", 20)
    config["history"].setdefault("max_action_events_per_camera", 1000)
    config["history"].setdefault("max_state_samples_per_camera", 5000)
    if not config["telegram"].get("token"):
        raise ValueError("telegram.token is required")
    if not config["mqtt"].get("host"):
        raise ValueError("mqtt.host is required")
    if not config["routing"].get("command_topic"):
        raise ValueError("routing.command_topic is required")
    if not config["routing"].get("reply_topic"):
        raise ValueError("routing.reply_topic is required")
    return config


def load_config(path: str) -> dict[str, Any]:
    config_path = Path(path)
    if not config_path.exists():
        raise FileNotFoundError(f"Config file not found: {config_path}")
    with config_path.open("r", encoding="utf-8") as handle:
        config = yaml.safe_load(handle) or {}
    return load_config_dict(config)


def configure_logging() -> None:
    level_name = os.environ.get("LOG_LEVEL", "INFO").upper()
    level = getattr(logging, level_name, logging.INFO)
    logging.basicConfig(
        level=level,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
        stream=sys.stdout,
    )


def main() -> int:
    configure_logging()
    config_path = os.environ.get("HUB_CONFIG", DEFAULT_CONFIG)
    config = load_config(config_path)
    hub = Hub(config, config_path)
    ui_host = os.environ.get("HUB_UI_HOST", "127.0.0.1")
    ui_port = int(os.environ.get("HUB_UI_PORT", "8080"))
    ui_config = config.get("ui") or {}
    ui_username = os.environ.get("HUB_UI_USERNAME") or str(ui_config.get("username") or "")
    ui_password = os.environ.get("HUB_UI_PASSWORD") or str(ui_config.get("password") or "")
    if (ui_username and not ui_password) or (ui_password and not ui_username):
        LOG.warning("Web UI auth is disabled because both HUB_UI_USERNAME and HUB_UI_PASSWORD are required")
    web_server = WebServer(create_web_app(hub, ui_username=ui_username, ui_password=ui_password), ui_host, ui_port)
    hub_thread = threading.Thread(target=hub.start, name="telegrambothub-main", daemon=True)
    probe_thread = threading.Thread(target=hub.snapshot_probe_loop, name="telegrambothub-probe", daemon=True)

    def handle_signal(_signum: int, _frame: Any) -> None:
        LOG.info("Stopping hub")
        hub.stop()
        web_server.stop()

    signal.signal(signal.SIGINT, handle_signal)
    signal.signal(signal.SIGTERM, handle_signal)

    LOG.info("Starting telegrambothub")
    hub_thread.start()
    probe_thread.start()
    web_server.start()
    while (hub_thread.is_alive() or probe_thread.is_alive()) and not hub.stop_event.wait(0.5):
        pass
    hub.stop()
    web_server.stop()
    hub_thread.join(timeout=5)
    probe_thread.join(timeout=5)
    LOG.info("Stopped telegrambothub")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
