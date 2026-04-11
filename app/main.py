import copy
import http.client
import json
import logging
import mimetypes
import os
import ipaddress
import base64
import hashlib
import re
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
from .config_model import load_config_dict as _load_config_dict
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
    mqtt_command_status: str = "unknown"
    mqtt_command_last_ok_at: int | None = None
    mqtt_command_last_error: str = ""


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
        self.snapshot_refreshing: set[str] = set()
        self.supported_controls_refreshing: set[str] = set()
        self.mqtt_command_refreshing: set[str] = set()
        self.auto_pairing_in_progress: set[str] = set()
        self.auto_pairing_next_retry_at: dict[str, float] = {}
        self.pending_by_request: dict[str, dict[str, Any]] = {}
        self.command_reply_timeout_seconds = 5.0
        self.last_chat_by_camera: dict[str, int] = {}
        self.native_action_history_by_camera: dict[str, list[dict[str, Any]]] = {}
        self.optimistic_supported_controls_by_camera: dict[str, tuple[float, dict[str, Any]]] = {}
        self.supported_controls_cache_by_camera: dict[str, dict[str, Any]] = {}
        self.live_events: list[dict[str, Any]] = []
        self.live_event_limit = 200
        self.live_event_sequence = 0
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
        ip = self._normalized_camera_ip(camera.ip)
        if ip:
            return f"http://{ip}/api/v1"

        snapshot_url = camera.snapshot_url.strip()
        if not snapshot_url:
            return ""

        parsed = urllib.parse.urlsplit(snapshot_url)
        if not parsed.netloc:
            return ""
        scheme = parsed.scheme or "http"
        return urllib.parse.urlunsplit((scheme, parsed.netloc, "/api/v1", "", ""))

    def _normalized_camera_ip(self, value: str) -> str:
        candidate = str(value or "").strip()
        if not candidate:
            return ""
        try:
            ipaddress.ip_address(candidate)
        except ValueError:
            return ""
        return candidate

    def _camera_public_host(self, camera: Camera) -> str:
        ip = self._normalized_camera_ip(camera.ip)
        if ip:
            return ip

        api_base_url = camera.api_base_url.strip()
        if api_base_url:
            parsed = urllib.parse.urlsplit(api_base_url)
            if parsed.hostname:
                return parsed.hostname

        snapshot_url = camera.snapshot_url.strip()
        if snapshot_url:
            parsed = urllib.parse.urlsplit(snapshot_url)
            if parsed.hostname:
                return parsed.hostname

        return ""

    def _camera_web_ui_url(self, camera: Camera) -> str:
        host = self._camera_public_host(camera)
        if host:
            scheme = "https" if camera.api_base_url.strip().startswith("https://") else "http"
            return urllib.parse.urlunsplit((scheme, host, "/", "", ""))

        return ""

    def _camera_api_token(self, camera: Camera) -> str:
        return camera.api_token.strip()

    def _camera_login_credentials(self, camera: Camera) -> tuple[str, str]:
        username = str(camera.onvif_username or "").strip() or self.default_onvif_username
        password = str(camera.onvif_password or "")
        if password == "":
            password = self.default_onvif_password
        return username, password

    def _camera_token_endpoint_urls(self, camera: Camera) -> list[str]:
        host = self._camera_public_host(camera)
        if not host:
            return []

        urls: list[str] = []
        snapshot_url = str(camera.snapshot_url or "").strip()
        if snapshot_url:
            parsed = urllib.parse.urlsplit(snapshot_url)
            if parsed.scheme in {"http", "https"} and parsed.netloc:
                urls.append(urllib.parse.urlunsplit((parsed.scheme, parsed.netloc, "/x/json-agent-token.cgi", "", "")))

        preferred_scheme = "https" if str(camera.api_base_url or "").strip().startswith("https://") else "http"
        for scheme in (preferred_scheme, "https", "http"):
            candidate = f"{scheme}://{host}/x/json-agent-token.cgi"
            if candidate not in urls:
                urls.append(candidate)

        return urls

    def _fetch_camera_api_token_via_login(self, camera: Camera) -> str:
        username, password = self._camera_login_credentials(camera)
        if not username or password == "":
            raise RuntimeError("Camera login credentials are not configured")

        auth_payload = {
            "username": username,
            "password": base64.b64encode(password.encode("utf-8")).decode("ascii"),
            "encoding": "base64",
        }

        last_error = ""
        for token_url in self._camera_token_endpoint_urls(camera):
            login_url = urllib.parse.urlunsplit(
                (
                    urllib.parse.urlsplit(token_url).scheme,
                    urllib.parse.urlsplit(token_url).netloc,
                    "/x/login.cgi",
                    "",
                    "",
                )
            )
            open_kwargs: dict[str, Any] = {"timeout": max(self.snapshot_heartbeat_timeout_seconds, 8)}
            if token_url.startswith("https://"):
                open_kwargs["context"] = ssl._create_unverified_context()
            try:
                login_request = urllib.request.Request(
                    login_url,
                    data=json.dumps(auth_payload, separators=(",", ":")).encode("utf-8"),
                    headers={
                        "Content-Type": "application/json",
                        "Accept": "application/json",
                    },
                    method="POST",
                )
                with urllib.request.urlopen(login_request, **open_kwargs) as login_response:
                    set_cookie = str(login_response.headers.get("Set-Cookie") or "")

                session_cookie = set_cookie.split(";", 1)[0].strip()
                if not session_cookie:
                    raise RuntimeError("Camera login did not return a session cookie")

                token_request = urllib.request.Request(
                    token_url,
                    headers={
                        "Accept": "application/json",
                        "Cookie": session_cookie,
                    },
                    method="GET",
                )
                with urllib.request.urlopen(token_request, **open_kwargs) as token_response:
                    token_payload = json.loads(token_response.read().decode("utf-8", errors="replace"))

                logout_request = urllib.request.Request(
                    urllib.parse.urlunsplit(
                        (
                            urllib.parse.urlsplit(token_url).scheme,
                            urllib.parse.urlsplit(token_url).netloc,
                            "/x/logout.cgi",
                            "",
                            "",
                        )
                    ),
                    headers={"Cookie": session_cookie},
                    method="GET",
                )
                try:
                    urllib.request.urlopen(logout_request, **open_kwargs).read()
                except Exception:
                    pass

                api_token = str((token_payload or {}).get("api_token") or "").strip()
                if not api_token:
                    raise RuntimeError("Camera token endpoint returned empty token")
                return api_token
            except Exception as error:
                last_error = str(error)
                continue

        raise RuntimeError(last_error or "Unable to fetch camera token via login")

    def _fetch_snapshot_with_camera_login(self, camera: Camera, snapshot_url: str) -> tuple[bytes, str]:
        username, password = self._camera_login_credentials(camera)
        if not username or password == "":
            raise RuntimeError("Camera snapshot endpoint requires authentication and camera credentials are not configured")

        parsed_snapshot = urllib.parse.urlsplit(snapshot_url)
        snapshot_origin = urllib.parse.urlunsplit((parsed_snapshot.scheme, parsed_snapshot.netloc, "/", "", ""))
        login_url = urllib.request.urljoin(snapshot_origin, "/x/login.cgi")
        login_payload = json.dumps(
            {
                "username": username,
                "password": password,
            },
            separators=(",", ":"),
        ).encode("utf-8")

        handlers: list[Any] = [urllib.request.HTTPCookieProcessor(http.cookiejar.CookieJar())]
        if login_url.startswith("https://") or snapshot_url.startswith("https://"):
            handlers.append(urllib.request.HTTPSHandler(context=ssl._create_unverified_context()))
        opener = urllib.request.build_opener(*handlers)

        login_request = urllib.request.Request(login_url, data=login_payload, method="POST")
        login_request.add_header("Content-Type", "application/json")
        login_request.add_header("Accept", "application/json")
        with opener.open(login_request, timeout=self.snapshot_heartbeat_timeout_seconds):
            pass

        snapshot_request = urllib.request.Request(snapshot_url, method="GET")
        snapshot_request.add_header("Accept", "image/jpeg, */*")
        with opener.open(snapshot_request, timeout=self.snapshot_heartbeat_timeout_seconds) as response:
            content_type = response.headers.get("Content-Type", "image/jpeg")
            photo = response.read()
        return photo, content_type

    def _store_camera_api_token(self, camera_id: str, api_token: str) -> Camera | None:
        resolved = self._resolve_camera_id(camera_id) or str(camera_id or "").strip().lower()
        if not resolved or not api_token:
            return None

        with self.state_lock:
            current = self.cameras.get(resolved)
        if current is None:
            return None
        if str(current.api_token or "").strip() == api_token:
            return current

        config = self.export_config()
        updated = False
        for entry in config.get("cameras", []):
            if str(entry.get("id") or "").strip().lower() != resolved:
                continue
            entry["api_token"] = api_token
            updated = True
            break

        if updated:
            self.save_config(config)
            self.reload_config()

        with self.state_lock:
            return self.cameras.get(resolved)

    def _refresh_camera_api_token_from_camera(self, camera_id: str, camera: Camera) -> Camera | None:
        api_token = self._fetch_camera_api_token_via_login(camera)
        return self._store_camera_api_token(camera_id, api_token)

    def _camera_api_client(self, camera: Camera) -> CameraApiClient:
        base_url = self._camera_api_base_url(camera)
        if not base_url:
            raise RuntimeError(f"Native API base URL is not configured for {camera.name}")
        return CameraApiClient(
            base_url,
            token=self._camera_api_token(camera),
            timeout=max(self.snapshot_heartbeat_timeout_seconds, 10),
        )

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
                mqtt_command_status=str(entry.get("mqtt_command_status") or "unknown").strip() or "unknown",
                mqtt_command_last_ok_at=self._coerce_int(entry.get("mqtt_command_last_ok_at")),
                mqtt_command_last_error=str(entry.get("mqtt_command_last_error") or "").strip(),
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
            "mqtt_command_status": camera.mqtt_command_status,
            "mqtt_command_last_ok_at": camera.mqtt_command_last_ok_at,
            "mqtt_command_last_error": camera.mqtt_command_last_error,
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
            self.event_topic = config["routing"].get("event_topic", "thingino/cam/+/event")
            self.state_topic = config["routing"].get("state_topic", "thingino/cam/+/state")
            self.registration_stale_after_seconds = max(0, int(config.get("ui", {}).get("registration_stale_after_seconds", 0)))
            self.snapshot_heartbeat_interval_seconds = max(0, int(config.get("ui", {}).get("snapshot_heartbeat_interval_seconds", 60)))
            self.snapshot_heartbeat_timeout_seconds = max(1, int(config.get("ui", {}).get("snapshot_heartbeat_timeout_seconds", 5)))
            self.api_probe_interval_seconds = max(0, int(config.get("ui", {}).get("api_probe_interval_seconds", 300)))
            self.snapshot_cache_stale_after_seconds = max(0, int(config.get("ui", {}).get("snapshot_cache_stale_after_seconds", 3600)))
            defaults_cfg = config.get("defaults") or {}
            pairing_cfg = config.get("pairing") or {}
            self.default_onvif_username = str(defaults_cfg.get("onvif_username") or DEFAULT_THINGINO_USERNAME).strip()
            self.default_onvif_password = str(defaults_cfg.get("onvif_password") or DEFAULT_THINGINO_PASSWORD)
            self.auto_pairing_enabled = bool(pairing_cfg.get("auto_install_on_registration", True))
            self.auto_pairing_retry_seconds = max(0, int(pairing_cfg.get("auto_install_retry_seconds", 300)))
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
        if self.api_probe_interval_seconds > 0:
            self._schedule_onvif_refresh_for_all()
            self._schedule_api_refresh_for_all()
            self._schedule_supported_controls_refresh_for_all()

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
        client.subscribe(self.event_topic)
        client.subscribe(self.state_topic)

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
        if topic.endswith("/event"):
            self._handle_mqtt_event(topic, payload)
            return
        if topic.endswith("/state"):
            self._handle_mqtt_state(topic, payload)
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
        incoming_ip = str(decoded.get("ip") or "").strip()
        ip = self._normalized_camera_ip(incoming_ip) or self._normalized_camera_ip(existing.ip if existing else "")
        snapshot_url = str(decoded.get("snapshot_url") or (existing.snapshot_url if existing else "")).strip()
        api_key = str(decoded.get("api_key") or (existing.api_key if existing else "")).strip()
        incoming_api_base_url = str(decoded.get("api_base_url") or "").strip()
        existing_api_base_url = str(existing.api_base_url if existing else "").strip()
        # Don't let a camera re-registration downgrade https:// to http:// — the
        # registration may carry a pre-bootstrap URL (TLS was off at last boot).
        if existing_api_base_url.startswith("https://") and incoming_api_base_url.startswith("http://"):
            api_base_url = existing_api_base_url
        else:
            api_base_url = incoming_api_base_url or existing_api_base_url
        if snapshot_url:
            parsed_snapshot = urllib.parse.urlsplit(snapshot_url)
            parsed_api_base = urllib.parse.urlsplit(api_base_url) if api_base_url else None
            if parsed_snapshot.netloc and parsed_snapshot.path.startswith("/api/") and parsed_api_base and parsed_api_base.hostname:
                scheme = parsed_snapshot.scheme or (parsed_api_base.scheme or "http")
                port = parsed_snapshot.port
                netloc = f"{parsed_api_base.hostname}:{port}" if port else parsed_api_base.hostname
                snapshot_url = urllib.parse.urlunsplit(
                    (scheme, netloc, parsed_snapshot.path, parsed_snapshot.query, "")
                )
        api_token = str((existing.api_token if existing else "") or "").strip()
        onvif_endpoint = str(decoded.get("onvif_endpoint") or decoded.get("onvif_url") or (existing.onvif_endpoint if existing else "")).strip()
        onvif_username = str(decoded.get("onvif_username") or (existing.onvif_username if existing else "")).strip()
        onvif_password = str(decoded.get("onvif_password") or (existing.onvif_password if existing else "")).strip()
        last_registration_at = self._coerce_int(decoded.get("timestamp")) or int(time.time())
        resolved_name = configured_name or name
        if not configured_name and existing is not None and existing.name and existing.name != existing.camera_id and name == camera_id:
            resolved_name = existing.name
        previous_status = str(existing.status or "").strip().lower() if existing is not None else ""
        registration_changed = existing is None
        api_refresh_needed = False
        onvif_refresh_needed = False
        supported_controls_refresh_needed = False
        mqtt_command_refresh_needed = existing is None
        if existing is not None:
            registration_changed = any(
                (
                    existing.name != resolved_name,
                    existing.hostname != hostname,
                    existing.ip != ip,
                    existing.status != status,
                    existing.snapshot_url != snapshot_url,
                    existing.api_key != api_key,
                    existing.api_base_url != api_base_url,
                    existing.api_token != api_token,
                    existing.onvif_endpoint != onvif_endpoint,
                    existing.onvif_username != onvif_username,
                    existing.onvif_password != onvif_password,
                )
            )
            became_online = status == "online" and previous_status != "online"
            api_connection_changed = any(
                (
                    existing.ip != ip,
                    existing.api_key != api_key,
                    existing.api_base_url != api_base_url,
                    existing.api_token != api_token,
                )
            )
            onvif_connection_changed = any(
                (
                    existing.ip != ip,
                    existing.onvif_endpoint != onvif_endpoint,
                    existing.onvif_username != onvif_username,
                    existing.onvif_password != onvif_password,
                )
            )
            api_refresh_needed = became_online or api_connection_changed
            onvif_refresh_needed = became_online or onvif_connection_changed
            supported_controls_refresh_needed = became_online or api_connection_changed
            mqtt_command_refresh_needed = became_online
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
        if registration_changed:
            detail = resolved_name if not ip else f"{resolved_name} @ {ip}"
            self._record_history_action(
                camera_id,
                "registration",
                "success",
                detail,
                recorded_at=last_registration_at,
                source="mqtt_registration",
            )
        if api_refresh_needed:
            self._schedule_api_refresh(camera_id)
        if onvif_refresh_needed:
            self._schedule_onvif_refresh(camera_id)
        if supported_controls_refresh_needed:
            self._schedule_supported_controls_refresh(camera_id)
        if mqtt_command_refresh_needed:
            self._schedule_mqtt_command_refresh(camera_id)
        self._schedule_auto_pairing(camera_id)

    def _handle_mqtt_event(self, topic: str, payload: str) -> None:
        camera_id = self._camera_id_from_topic(topic)
        try:
            decoded = json.loads(payload)
        except json.JSONDecodeError:
            LOG.warning("Ignoring invalid MQTT event payload from %s: %s", topic, payload)
            return
        if not isinstance(decoded, dict):
            LOG.warning("Ignoring non-object MQTT event payload from %s", topic)
            return

        event_name = str(decoded.get("event") or decoded.get("type") or "").strip()
        event_payload = decoded.get("data")
        if not event_name:
            LOG.warning("Ignoring MQTT event payload without event name from %s", topic)
            return
        if not isinstance(event_payload, dict):
            if "data" not in decoded:
                event_payload = dict(decoded)
            else:
                event_payload = {"message": str(event_payload or "").strip()}

        self._ensure_camera(camera_id)
        self._handle_camera_stream_event(camera_id, {"event": event_name, "data": event_payload})

    def _handle_mqtt_state(self, topic: str, payload: str) -> None:
        camera_id = self._camera_id_from_topic(topic)
        try:
            decoded = json.loads(payload)
        except json.JSONDecodeError:
            LOG.warning("Ignoring invalid MQTT state payload from %s", topic)
            return
        if not isinstance(decoded, dict):
            LOG.warning("Ignoring non-object MQTT state payload from %s", topic)
            return

        device = decoded.get("device")
        state = decoded.get("state")
        if not isinstance(device, dict) or not isinstance(state, dict):
            LOG.warning("Ignoring MQTT state payload without device/state objects from %s", topic)
            return

        system = state.get("system") if isinstance(state.get("system"), dict) else {}
        network = state.get("network") if isinstance(state.get("network"), dict) else {}
        motion = state.get("motion") if isinstance(state.get("motion"), dict) else {}
        privacy = state.get("privacy") if isinstance(state.get("privacy"), dict) else {}
        daynight = state.get("daynight") if isinstance(state.get("daynight"), dict) else {}
        timestamp = self._coerce_int(decoded.get("timestamp")) or int(time.time())

        with self.state_lock:
            current = self.cameras.get(camera_id)
            if current is None:
                current = Camera(camera_id=camera_id, name=camera_id)
            name = str(device.get("name") or current.name or camera_id).strip() or camera_id
            hostname = str(device.get("hostname") or current.hostname or "").strip()
            ip = self._normalized_camera_ip(str(network.get("ip") or current.ip or ""))
            updated = replace(
                current,
                name=name,
                hostname=hostname,
                ip=ip or current.ip,
                status="online",
                last_registration_at=timestamp,
                probe_status="online",
                last_probe_at=timestamp,
                last_probe_error="",
                api_status="online",
                api_last_ok_at=timestamp,
                api_last_error="",
                api_device_name=str(device.get("name") or current.api_device_name or name).strip(),
                api_device_model=str(device.get("model") or current.api_device_model).strip(),
                api_streamer=str(device.get("streamer") or current.api_streamer).strip(),
                api_version=str(device.get("firmware_version") or current.api_version).strip(),
            )
            self.cameras[camera_id] = updated
        self._persist_state()
        self._record_history_state_sample(
            camera_id,
            "mqtt_state",
            {
                "api_status": "online",
                "streamer_running": system.get("streamer_running"),
                "network_online": network.get("online"),
                "motion_enabled": motion.get("enabled"),
                "privacy_enabled": privacy.get("enabled"),
                "daynight_target_mode": str(daynight.get("target_mode") or "").strip(),
                "daynight_running_mode": str(daynight.get("running_mode") or "").strip(),
                "ip": ip or current.ip,
                "raw": decoded,
            },
            normalized={
                "api_status": "online",
                "streamer_running": system.get("streamer_running"),
                "network_online": network.get("online"),
                "motion_enabled": motion.get("enabled"),
                "privacy_enabled": privacy.get("enabled"),
                "daynight_target_mode": str(daynight.get("target_mode") or "").strip(),
                "daynight_running_mode": str(daynight.get("running_mode") or "").strip(),
                "ip": ip or current.ip,
            },
            recorded_at=timestamp,
        )

    def _auto_pairing_enrollment(self, camera: Camera) -> dict[str, str]:
        return {
            "camera_id": camera.camera_id,
            "id": camera.camera_id,
            "name": camera.name,
            "ip": self._normalized_camera_ip(camera.ip),
            "snapshot_url": camera.snapshot_url,
            "api_key": camera.api_key,
            "api_base_url": camera.api_base_url,
            "api_token": camera.api_token,
            "onvif_endpoint": camera.onvif_endpoint,
            "onvif_username": camera.onvif_username.strip() or self.default_onvif_username,
            "onvif_password": camera.onvif_password or self.default_onvif_password,
        }

    def _auto_pairing_is_ready(self, camera_id: str, *, now: float | None = None) -> bool:
        if not getattr(self, "auto_pairing_enabled", True):
            return False
        resolved = self._resolve_camera_id(camera_id) or camera_id.strip().lower()
        current_time = time.monotonic() if now is None else now
        retry_map = getattr(self, "auto_pairing_next_retry_at", {})
        in_progress_set = getattr(self, "auto_pairing_in_progress", set())
        with self.state_lock:
            camera = self.cameras.get(resolved)
            retry_at = retry_map.get(resolved, 0.0)
            in_progress = resolved in in_progress_set
        if camera is None or in_progress:
            return False
        if retry_at > current_time:
            return False
        if str(camera.status or "").strip().lower() != "online":
            return False
        if self._camera_api_token(camera):
            return False
        if not (self._normalized_camera_ip(camera.ip) or self._camera_api_base_url(camera)):
            return False
        return True

    def _schedule_auto_pairing(self, camera_id: str) -> bool:
        resolved = self._resolve_camera_id(camera_id) or camera_id.strip().lower()
        if not self._auto_pairing_is_ready(resolved):
            return False
        if not hasattr(self, "auto_pairing_in_progress"):
            self.auto_pairing_in_progress = set()
        with self.state_lock:
            if resolved in self.auto_pairing_in_progress:
                return False
            self.auto_pairing_in_progress.add(resolved)
        worker = threading.Thread(
            target=self._auto_pairing_worker,
            args=(resolved,),
            name=f"telegrambothub-auto-pair-{resolved[:8]}",
            daemon=True,
        )
        worker.start()
        return True

    def _auto_pairing_worker(self, camera_id: str) -> None:
        try:
            self._run_auto_pairing(camera_id)
        finally:
            with self.state_lock:
                self.auto_pairing_in_progress.discard(camera_id)

    def _run_auto_pairing(self, camera_id: str) -> None:
        resolved = self._resolve_camera_id(camera_id) or camera_id.strip().lower()
        if not self._auto_pairing_is_ready(resolved):
            return
        if not hasattr(self, "auto_pairing_next_retry_at"):
            self.auto_pairing_next_retry_at = {}
        with self.state_lock:
            camera = self.cameras.get(resolved)
        if camera is None:
            return

        enrollment = self._auto_pairing_enrollment(camera)
        try:
            self.connect_camera(enrollment)
            result = self.install_pairing_bundle_via_mqtt(enrollment)
        except Exception as error:
            retry_seconds = max(0, int(getattr(self, "auto_pairing_retry_seconds", 300)))
            with self.state_lock:
                self.auto_pairing_next_retry_at[resolved] = time.monotonic() + retry_seconds
            self._record_history_action(
                resolved,
                "auto_pairing",
                "error",
                str(error),
                source="hub",
            )
            LOG.info("Automatic pairing failed for %s: %s", resolved, error)
            return

        with self.state_lock:
            self.auto_pairing_next_retry_at.pop(resolved, None)
        self._record_history_action(
            resolved,
            "auto_pairing",
            "success",
            str(result.get("status_detail") or "Camera paired automatically"),
            source="hub",
        )

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
            mqtt_command_status=existing.mqtt_command_status,
            mqtt_command_last_ok_at=existing.mqtt_command_last_ok_at,
            mqtt_command_last_error=existing.mqtt_command_last_error,
        )

    def _schedule_api_refresh_for_all(self) -> None:
        with self.state_lock:
            camera_ids = list(self.cameras)
        for camera_id in camera_ids:
            self._schedule_api_refresh(camera_id)

    def _schedule_supported_controls_refresh_for_all(self) -> None:
        with self.state_lock:
            camera_ids = list(self.cameras)
        for camera_id in camera_ids:
            self._schedule_supported_controls_refresh(camera_id)

    def _schedule_mqtt_command_refresh_for_all(self) -> None:
        with self.state_lock:
            camera_ids = list(self.cameras)
        for camera_id in camera_ids:
            self._schedule_mqtt_command_refresh(camera_id)

    def _schedule_mqtt_command_refresh(self, camera_id: str) -> bool:
        resolved = self._resolve_camera_id(camera_id) or camera_id.strip().lower()
        with self.state_lock:
            camera = self.cameras.get(resolved)
            if camera is None:
                return False
            if resolved in self.mqtt_command_refreshing:
                return False
            self.mqtt_command_refreshing.add(resolved)
        worker = threading.Thread(
            target=self._refresh_mqtt_command_worker,
            args=(resolved,),
            name=f"telegrambothub-mqtt-{resolved[:8]}",
            daemon=True,
        )
        worker.start()
        return True

    def _refresh_mqtt_command_worker(self, camera_id: str) -> None:
        try:
            self.refresh_camera_mqtt_command_status(camera_id)
        finally:
            with self.state_lock:
                self.mqtt_command_refreshing.discard(camera_id)

    def _handle_camera_stream_event(self, camera_id: str, stream_event: dict[str, Any]) -> None:
        event_name = str(stream_event.get("event") or "message").strip() or "message"
        if event_name == "keepalive":
            return

        payload = stream_event.get("data")
        if not isinstance(payload, dict):
            payload = {"message": str(payload or "").strip()}
        recorded_at = self._coerce_int(payload.get("timestamp")) or int(time.time())
        status = self._camera_stream_event_status(event_name, payload)
        detail = self._camera_stream_event_detail(event_name, payload)
        payload_summary = json.dumps(payload, sort_keys=True)[:500]
        self._record_history_action(
            camera_id,
            event_name.replace(".", "_"),
            status,
            detail,
            recorded_at=recorded_at,
            source="camera_event",
            payload_summary=payload_summary,
        )

    def _camera_stream_event_status(self, event_name: str, payload: dict[str, Any]) -> str:
        status = str(payload.get("status") or "").strip().lower()
        if status in {"success", "error", "warning", "info", "offline", "online", "degraded"}:
            if status == "degraded":
                return "warning"
            if status in {"online", "offline"}:
                return "info"
            return status
        if event_name == "error":
            return "error"
        if event_name.endswith("warning"):
            return "warning"
        if event_name in {"motion.started", "motion.stopped", "streamer.restarted", "record.completed", "hello"}:
            return "success"
        return "info"

    def _camera_stream_event_detail(self, event_name: str, payload: dict[str, Any]) -> str:
        if event_name == "hello":
            parts = [str(payload.get("backend") or "").strip(), str(payload.get("stream") or "").strip()]
            detail = " / ".join(part for part in parts if part)
            return detail or "Camera event stream connected"
        if event_name == "motion.started":
            return "Motion detected"
        if event_name == "motion.stopped":
            return "Motion cleared"
        if event_name == "streamer.restarted":
            service = str(payload.get("service") or "streaming").strip()
            return f"{service} restarted"
        if event_name == "record.completed":
            path = str(payload.get("path") or "").strip()
            duration = self._coerce_int(payload.get("duration_seconds"))
            if path and duration is not None:
                return f"{duration}s clip -> {path}"
            if path:
                return path
            if duration is not None:
                return f"{duration}s clip completed"
            return "Clip recording completed"
        if event_name == "firmware.progress":
            phase = str(payload.get("phase") or payload.get("step") or "update").strip()
            progress = payload.get("progress")
            if progress not in {None, ""}:
                return f"{phase}: {progress}%"
            return phase or "Firmware update in progress"
        if event_name == "health.warning":
            message = str(payload.get("message") or payload.get("reason") or "").strip()
            status = str(payload.get("status") or "").strip()
            return message or status or "Camera reported a health warning"
        if event_name == "state.changed":
            paths = payload.get("paths") or []
            if isinstance(paths, list) and paths:
                return ", ".join(str(path).strip() for path in paths if str(path).strip())
            return "Camera state changed"
        return str(payload.get("message") or payload.get("detail") or event_name.replace(".", " ")).strip()

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

    def queue_camera_api_refresh(self, camera_id: str) -> str:
        resolved = self._resolve_camera_id(camera_id) or camera_id.strip().lower()
        with self.state_lock:
            camera = self.cameras.get(resolved)
        if camera is None:
            raise RuntimeError(f"Unknown camera: {camera_id}")
        if not self._camera_api_base_url(camera):
            raise RuntimeError(f"Native API is not configured for {camera.name}")
        api_scheduled = self._schedule_api_refresh(resolved)
        controls_scheduled = self._schedule_supported_controls_refresh(resolved)
        return "scheduled" if api_scheduled or controls_scheduled else "already_running"

    def _schedule_supported_controls_refresh(self, camera_id: str) -> bool:
        resolved = self._resolve_camera_id(camera_id) or camera_id.strip().lower()
        with self.state_lock:
            camera = self.cameras.get(resolved)
            if camera is None:
                return False
            if resolved in self.supported_controls_refreshing:
                return False
            self.supported_controls_refreshing.add(resolved)
        worker = threading.Thread(
            target=self._refresh_supported_controls_worker,
            args=(resolved,),
            name=f"telegrambothub-controls-{resolved[:8]}",
            daemon=True,
        )
        worker.start()
        return True

    def _refresh_supported_controls_worker(self, camera_id: str) -> None:
        try:
            self.refresh_camera_supported_controls_for_ui(camera_id)
        finally:
            with self.state_lock:
                self.supported_controls_refreshing.discard(camera_id)

    def refresh_camera_supported_controls_for_ui(self, camera_id: str) -> dict[str, Any]:
        resolved = self._resolve_camera_id(camera_id) or camera_id.strip().lower()
        controls = self._get_camera_supported_controls_for_ui_live(resolved)
        with self.state_lock:
            self.supported_controls_cache_by_camera[resolved] = dict(controls)
        return dict(controls)

    def get_cached_camera_supported_controls_for_ui(self, camera_id: str) -> dict[str, Any]:
        resolved = self._resolve_camera_id(camera_id) or camera_id.strip().lower()
        with self.state_lock:
            camera = self.cameras.get(resolved)
            cached = self.supported_controls_cache_by_camera.get(resolved)
        if camera is None:
            raise RuntimeError(f"Unknown camera: {camera_id}")
        needs_live_refresh = False
        if cached is None:
            needs_live_refresh = camera.api_status == "online"
        elif camera.api_status == "online":
            needs_live_refresh = not bool(cached.get("native_controls_available")) or bool(cached.get("native_controls_error"))

        if needs_live_refresh:
            try:
                controls = self.refresh_camera_supported_controls_for_ui(resolved)
            except Exception:
                LOG.debug("Supported controls refresh failed for %s while API looked online", resolved, exc_info=True)
                controls = dict(cached) if cached is not None else self._default_camera_supported_controls_for_ui(camera)
                self._schedule_supported_controls_refresh(resolved)
        elif cached is None:
            self._schedule_supported_controls_refresh(resolved)
            controls = self._default_camera_supported_controls_for_ui(camera)
        else:
            controls = dict(cached)
        optimistic_overlay = self._optimistic_supported_controls_overlay(resolved)
        optimistic_stream_updates = optimistic_overlay.pop("native_stream_controls_updates", None)
        controls.update(optimistic_overlay)
        if isinstance(optimistic_stream_updates, list) and optimistic_stream_updates:
            merged_streams = [dict(stream) for stream in (controls.get("native_stream_controls") or []) if isinstance(stream, dict)]
            stream_indexes = {
                str(stream.get("name") or ""): index
                for index, stream in enumerate(merged_streams)
                if str(stream.get("name") or "")
            }
            for stream_update in optimistic_stream_updates:
                if not isinstance(stream_update, dict):
                    continue
                stream_name = str(stream_update.get("name") or "")
                if not stream_name:
                    continue
                update_values = dict(stream_update)
                if stream_name in stream_indexes:
                    merged_streams[stream_indexes[stream_name]].update(update_values)
                else:
                    stream_indexes[stream_name] = len(merged_streams)
                    merged_streams.append(update_values)
            controls["native_stream_controls"] = merged_streams
        return controls

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
                "daynight_target_mode": (info or {}).get("daynight_target_mode", ""),
                "daynight_running_mode": (info or {}).get("daynight_running_mode", ""),
                "ip": (info or {}).get("ip", ""),
            },
            normalized={
                "api_status": api_status,
                "streamer_running": (info or {}).get("streamer_running"),
                "network_online": (info or {}).get("network_online"),
                "motion_enabled": (info or {}).get("motion_enabled"),
                "privacy_enabled": (info or {}).get("privacy_enabled"),
                "daynight_target_mode": (info or {}).get("daynight_target_mode", ""),
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
            for field in ("total_gain_night_threshold", "total_gain_day_threshold"):
                if field in daynight and daynight.get(field) not in (None, ""):
                    values[f"native_daynight_{field}"] = str(daynight.get(field))
            controls = daynight.get("controls") or {}
            if isinstance(controls, dict):
                for field in ("color", "ircut", "ir850", "ir940", "white"):
                    if field in controls:
                        values[f"native_daynight_controls_{field}"] = bool(self._coerce_bool(controls.get(field)))
            schedule = daynight.get("schedule") or {}
            if isinstance(schedule, dict):
                if "enabled" in schedule:
                    values["native_daynight_schedule_enabled"] = bool(self._coerce_bool(schedule.get("enabled")))
                if "start_at" in schedule:
                    values["native_daynight_schedule_start_at"] = str(schedule.get("start_at") or "")
                if "stop_at" in schedule:
                    values["native_daynight_schedule_stop_at"] = str(schedule.get("stop_at") or "")

        privacy = payload.get("privacy") or {}
        if isinstance(privacy, dict) and "enabled" in privacy:
            values["native_privacy_enabled"] = bool(self._coerce_bool(privacy.get("enabled")))

        stream_updates: list[dict[str, Any]] = []
        for stream_name, stream_payload in payload.items():
            if not str(stream_name).startswith("stream") or not isinstance(stream_payload, dict):
                continue
            stream_update: dict[str, Any] = {"name": str(stream_name)}
            for field_name in ("enabled", "audio_enabled", "width", "height", "fps", "bitrate", "format", "mode"):
                if field_name in stream_payload:
                    field_value = stream_payload.get(field_name)
                    if field_name in {"enabled", "audio_enabled"}:
                        stream_update[field_name] = bool(self._coerce_bool(field_value))
                    else:
                        stream_update[field_name] = "" if field_value in (None, "") else str(field_value)

            osd_payload = stream_payload.get("osd") or {}
            if isinstance(osd_payload, dict):
                if "enabled" in osd_payload:
                    stream_update["osd_enabled"] = bool(self._coerce_bool(osd_payload.get("enabled")))
                time_payload = osd_payload.get("time") or {}
                if isinstance(time_payload, dict) and "enabled" in time_payload:
                    stream_update["osd_time_enabled"] = bool(self._coerce_bool(time_payload.get("enabled")))
                usertext_payload = osd_payload.get("usertext") or {}
                if isinstance(usertext_payload, dict):
                    if "enabled" in usertext_payload:
                        stream_update["osd_usertext_enabled"] = bool(self._coerce_bool(usertext_payload.get("enabled")))
                    if "format" in usertext_payload:
                        stream_update["osd_usertext_format"] = str(usertext_payload.get("format") or "")
                privacy_payload = osd_payload.get("privacy") or {}
                if isinstance(privacy_payload, dict):
                    if "enabled" in privacy_payload:
                        stream_update["osd_privacy_enabled"] = bool(self._coerce_bool(privacy_payload.get("enabled")))
                    if "text" in privacy_payload:
                        stream_update["osd_privacy_text"] = str(privacy_payload.get("text") or "")
                    if "fill_color" in privacy_payload:
                        fill_color_value, fill_alpha = self._split_hex_color_alpha(privacy_payload.get("fill_color"))
                        stream_update["osd_privacy_fill_color"] = str(privacy_payload.get("fill_color") or "")
                        stream_update["osd_privacy_fill_color_value"] = fill_color_value
                        stream_update["osd_privacy_fill_alpha"] = fill_alpha
                    if "stroke_color" in privacy_payload:
                        stroke_color_value, stroke_alpha = self._split_hex_color_alpha(privacy_payload.get("stroke_color"))
                        stream_update["osd_privacy_stroke_color"] = str(privacy_payload.get("stroke_color") or "")
                        stream_update["osd_privacy_stroke_color_value"] = stroke_color_value
                        stream_update["osd_privacy_stroke_alpha"] = stroke_alpha

            if len(stream_update) > 1:
                stream_updates.append(stream_update)

        if stream_updates:
            values["native_stream_controls_updates"] = stream_updates

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

    def control_camera_service(self, camera_id: str, service: str, operation: str, *, refresh_after: bool = True) -> dict[str, Any]:
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
        if refresh_after:
            self.refresh_camera_api_details(resolved)
        else:
            self._schedule_api_refresh(resolved)
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
        self._record_history_config_changes(
            resolved,
            self._config_changes_from_patch(payload),
            source="native_api",
            change_type="native_patch",
        )
        if refresh_after:
            self.refresh_camera_api_details(resolved)
        else:
            self._record_optimistic_supported_controls(resolved, payload)
            self._schedule_api_refresh(resolved)
            self._schedule_supported_controls_refresh(resolved)
        return result

    def update_camera_send2_config(self, camera_id: str, payload: dict[str, Any]) -> dict[str, Any]:
        resolved = self._resolve_camera_id(camera_id) or camera_id.strip().lower()
        with self.state_lock:
            camera = self.cameras.get(resolved)
        if camera is None:
            raise RuntimeError(f"Unknown camera: {camera_id}")
        if not self._camera_api_base_url(camera):
            raise RuntimeError("Native API is not configured for this camera.")
        if str(camera.api_status or "").strip().lower() == "offline":
            raise RuntimeError("Native API is offline for this camera.")
        if str(camera.api_status or "").strip().lower() == "unsupported":
            raise RuntimeError("Native API is not available on this camera build.")

        try:
            client = self._camera_api_client(camera)
            results: list[str] = []
            send2_capabilities = client.get_capabilities().get("send2") or {}

            motion = payload.get("motion") or {}
            if "sensitivity" in motion:
                client.patch_setting("motion/sensitivity", {"sensitivity": motion["sensitivity"]})
                results.append("motion.sensitivity")
            if "cooldown_time" in motion:
                client.patch_setting("motion/cooldown-time", {"cooldown_time": motion["cooldown_time"]})
                results.append("motion.cooldown_time")
            for service_name, _label in SEND2_SERVICES:
                motion_key = f"send2{service_name}"
                if motion_key in motion:
                    client.patch_setting(f"motion/outputs/send2/{service_name}", {"enabled": bool(motion[motion_key])})
                    results.append(f"motion.{motion_key}")
                service_data = payload.get(service_name)
                if isinstance(service_data, dict):
                    service_cap = send2_capabilities.get(service_name) or {}
                    photo_supported = bool(service_cap.get("send_photo")) if isinstance(service_cap, dict) else False
                    video_supported = bool(service_cap.get("send_video")) if isinstance(service_cap, dict) else False
                    if "send_photo" in service_data and photo_supported:
                        client.patch_setting(f"send2/services/{service_name}/send-photo", {"send_photo": bool(service_data["send_photo"])})
                        results.append(f"{service_name}.send_photo")
                    if "send_video" in service_data and video_supported:
                        client.patch_setting(f"send2/services/{service_name}/send-video", {"send_video": bool(service_data["send_video"])})
                        results.append(f"{service_name}.send_video")
        except Exception as error:
            self._record_native_action(resolved, "send2_config", "error", str(error))
            raise

        detail = ", ".join(results) or "accepted"
        self._record_native_action(resolved, "send2_config", "success", detail)
        self._record_history_config_changes(
            resolved,
            self._config_changes_from_patch(payload),
            source="native_api",
            change_type="send2_patch",
        )
        return {"status": "accepted", "applied": results}

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

        action_path = f"send2/{normalized_service}/test"
        if normalized_type:
            action_path = f"{action_path}-{normalized_type}"

        try:
            client = self._camera_api_client(camera)
            result = client.post_action(
                action_path,
                {"verbose": verbose} if verbose else None,
                timeout=max(self.snapshot_heartbeat_timeout_seconds, 30),
            )
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
        if result.get("status") == "error":
            error_msg = result.get("message") or f"{detail}: send2 test failed"
            self._record_native_action(resolved, "send2_test", "error", error_msg)
            raise RuntimeError(error_msg)
        self._record_native_action(resolved, "send2_test", "success", detail)
        return result

    def set_camera_privacy(self, camera_id: str, enabled: bool, channel: str = "all", *, refresh_after: bool = True) -> dict[str, Any]:
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
        if refresh_after:
            self.refresh_camera_api_details(resolved)
        else:
            self._record_optimistic_supported_controls(
                resolved,
                {"privacy": {"enabled": enabled}},
            )
            self._schedule_api_refresh(resolved)
            self._schedule_supported_controls_refresh(resolved)
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
            self._schedule_supported_controls_refresh(resolved)
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
            rows = self.history_store.recent_action_events(
                camera_id,
                self.history_recent_actions_limit,
                sources=["native_api"],
            )
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
        source: str = "native_api",
        payload_summary: str = "",
    ) -> None:
        resolved = self._resolve_camera_id(camera_id) or camera_id.strip().lower()
        when = recorded_at or int(time.time())
        if self.history_store is None:
            self._append_live_event(
                resolved,
                source=source,
                action=action,
                status=status,
                detail=detail,
                recorded_at=when,
            )
            return
        try:
            self.history_store.record_action_event(
                recorded_at=when,
                camera_id=resolved,
                source=source,
                action=action,
                status=status,
                detail=detail,
                payload_summary=payload_summary,
            )
        except Exception:
            LOG.warning("Failed to record action history for %s", resolved, exc_info=True)
        self._append_live_event(
            resolved,
            source=source,
            action=action,
            status=status,
            detail=detail,
            recorded_at=when,
        )

    def _append_live_event(
        self,
        camera_id: str,
        *,
        source: str,
        action: str,
        status: str,
        detail: str,
        recorded_at: int,
    ) -> dict[str, Any]:
        with self.state_lock:
            camera = self.cameras.get(camera_id)
            self.live_event_sequence += 1
            entry = {
                "sequence": self.live_event_sequence,
                "camera_id": camera_id,
                "camera_name": camera.name if camera is not None else camera_id,
                "timestamp": str(recorded_at),
                "at": self._format_timestamp(recorded_at),
                "source": source,
                "name": str(action or "").replace("_", " "),
                "status": str(status or "info"),
                "detail": str(detail or "").strip(),
            }
            self.live_events.insert(0, entry)
            self.live_events = self.live_events[:self.live_event_limit]
        return entry

    def _history_action_entry_for_ui(self, entry: dict[str, Any]) -> dict[str, Any]:
        camera_id = str(entry.get("camera_id") or "").strip().lower()
        with self.state_lock:
            camera = self.cameras.get(camera_id)
        return {
            "sequence": 0,
            "camera_id": camera_id,
            "camera_name": camera.name if camera is not None else camera_id,
            "timestamp": str(entry.get("recorded_at") or 0),
            "at": self._format_timestamp(entry.get("recorded_at")),
            "source": str(entry.get("source") or "native_api"),
            "name": str(entry.get("action") or "").replace("_", " "),
            "status": str(entry.get("status") or "info"),
            "detail": str(entry.get("detail") or ""),
        }

    def list_recent_events_for_ui(self, limit: int = 40) -> list[dict[str, Any]]:
        with self.state_lock:
            snapshot = [dict(item) for item in self.live_events[:max(1, int(limit))]]
        if snapshot:
            return snapshot
        if self.history_store is None:
            return []
        rows = self.history_store.recent_global_action_events(max(1, int(limit)))
        return [self._history_action_entry_for_ui(row) for row in rows]

    def live_events_since(self, last_sequence: int, limit: int = 20) -> tuple[int, list[dict[str, Any]]]:
        with self.state_lock:
            current_sequence = self.live_event_sequence
            events = [
                dict(item)
                for item in reversed(self.live_events)
                if int(item.get("sequence") or 0) > int(last_sequence)
            ]
        return current_sequence, events[:max(1, int(limit))]

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

    def _record_history_config_changes(
        self,
        camera_id: str,
        changes: list[dict[str, Any]],
        *,
        source: str,
        change_type: str,
        recorded_at: int | None = None,
    ) -> None:
        if not changes or self.history_store is None:
            return
        resolved = self._resolve_camera_id(camera_id) or camera_id.strip().lower()
        when = recorded_at or int(time.time())
        try:
            for change in changes:
                self.history_store.record_config_change(
                    recorded_at=when,
                    camera_id=resolved,
                    source=source,
                    change_type=change_type,
                    path=str(change.get("path") or "").strip() or "/",
                    previous_value=change.get("previous"),
                    new_value=change.get("new"),
                    detail=str(change.get("detail") or "").strip(),
                )
        except Exception:
            LOG.warning("Failed to record config changes for %s", resolved, exc_info=True)

    def _flatten_config_payload(self, payload: Any, prefix: str = "") -> list[dict[str, Any]]:
        if not isinstance(payload, dict):
            return [{"path": prefix or "/", "value": payload}]
        entries: list[dict[str, Any]] = []
        for key, value in sorted(payload.items()):
            path = f"{prefix}/{key}" if prefix else f"/{key}"
            if isinstance(value, dict):
                entries.extend(self._flatten_config_payload(value, path))
            else:
                entries.append({"path": path, "value": value})
        return entries

    def _config_changes_from_patch(self, payload: dict[str, Any]) -> list[dict[str, Any]]:
        return [
            {
                "path": entry["path"],
                "previous": None,
                "new": entry["value"],
                "detail": "Native API patch",
            }
            for entry in self._flatten_config_payload(payload)
        ]

    def _config_changes_from_mapping(
        self,
        previous: dict[str, Any],
        current: dict[str, Any],
        *,
        prefix: str,
        detail: str,
    ) -> list[dict[str, Any]]:
        changes: list[dict[str, Any]] = []
        keys = sorted(set(previous) | set(current))
        for key in keys:
            before = previous.get(key)
            after = current.get(key)
            if before == after:
                continue
            changes.append(
                {
                    "path": f"{prefix}/{key}",
                    "previous": before,
                    "new": after,
                    "detail": detail,
                }
            )
        return changes

    def _config_change_entry_for_ui(self, entry: dict[str, Any]) -> dict[str, str]:
        previous_text = str(entry.get("previous_json") or "null")
        new_text = str(entry.get("new_json") or "null")
        try:
            previous_value = json_module.loads(previous_text)
        except json_module.JSONDecodeError:
            previous_value = previous_text
        try:
            new_value = json_module.loads(new_text)
        except json_module.JSONDecodeError:
            new_value = new_text
        path = str(entry.get("path") or "/")
        detail = str(entry.get("detail") or "").strip()
        before_label = json_module.dumps(previous_value, sort_keys=True)
        after_label = json_module.dumps(new_value, sort_keys=True)
        summary = f"{path}: {before_label} -> {after_label}"
        if detail:
            summary = f"{summary} ({detail})"
        return {
            "at": self._format_timestamp(entry.get("recorded_at")),
            "timestamp": str(entry.get("recorded_at") or 0),
            "kind": "config",
            "name": str(entry.get("change_type") or "config change").replace("_", " "),
            "status": "info",
            "source": str(entry.get("source") or "hub"),
            "detail": summary,
        }

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
        kind_filter = kind_filter if kind_filter in {"all", "action", "state", "config"} else "all"
        sample_type_filter = sample_type_filter.strip() or "all"
        if self.history_store is not None:
            action_rows = self.history_store.recent_action_events(resolved, max(limit * 3, 100))
            state_rows = self.history_store.recent_state_samples(resolved, max(limit * 3, 100))
            config_rows = self.history_store.recent_config_changes(resolved, max(limit * 3, 100))
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

            if kind_filter in {"all", "config"}:
                for entry in config_rows:
                    timeline.append(self._config_change_entry_for_ui(entry))

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
        config_count = sum(1 for item in timeline if item.get("kind") == "config")
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
            "ip": self._normalized_camera_ip(camera.ip) or self._camera_public_host(camera) or "",
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
            "timeline_config_count": config_count,
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
                title="Network Reachability",
                rows=api_rows,
                extractor=lambda row: self._db_bool(row.get("network_online")),
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
            self._categorical_history_chart(
                title="Day/Night Target",
                rows=api_rows,
                extractor=lambda row: str(row.get("daynight_target_mode") or "").strip().lower() or "unknown",
            ),
            self._categorical_history_chart(
                title="IP Address",
                rows=api_rows,
                extractor=lambda row: str(row.get("ip") or "").strip() or "unknown",
                palette={"unknown": "#6c757d"},
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
        palette: dict[str, str] | None = None,
    ) -> dict[str, Any]:
        base_palette = {
            "day": "#f59f00",
            "night": "#0d6efd",
            "auto": "#20c997",
            "unknown": "#6c757d",
        }
        if palette:
            base_palette.update(palette)
        fallback_palette = [
            "#6610f2",
            "#d63384",
            "#fd7e14",
            "#198754",
            "#0dcaf0",
            "#dc3545",
            "#6f42c1",
            "#20c997",
        ]
        bars = []
        width = 320
        height = 44
        count = max(1, len(rows))
        gap = 1
        raw_bar_width = max(2, width // count)
        bar_width = max(2, raw_bar_width - gap)
        latest = "unknown"
        counts: dict[str, int] = {}
        category_fills: dict[str, str] = dict(base_palette)
        fallback_index = 0
        for index, row in enumerate(rows):
            value = extractor(row)
            latest = value if index == len(rows) - 1 else latest
            counts[value] = counts.get(value, 0) + 1
            if value not in category_fills:
                category_fills[value] = fallback_palette[fallback_index % len(fallback_palette)]
                fallback_index += 1
            timestamp_label = self._format_timestamp(row.get("recorded_at"))
            bars.append(
                {
                    "x": index * raw_bar_width,
                    "y": 6,
                    "width": bar_width,
                    "height": 32,
                    "fill": category_fills.get(value, category_fills.get("unknown", "#6c757d")),
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
                {"label": label, "fill": category_fills[label]}
                for label in sorted(counts)
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
            if entry.get("daynight_target_mode"):
                flags.append(f"target {entry['daynight_target_mode']}")
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

    def _default_camera_supported_controls_for_ui(self, camera: Camera) -> dict[str, Any]:
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
            "native_daynight_total_gain_night_threshold": "",
            "native_daynight_total_gain_day_threshold": "",
            "native_daynight_controls_color": False,
            "native_daynight_controls_ircut": False,
            "native_daynight_controls_ir850": False,
            "native_daynight_controls_ir940": False,
            "native_daynight_controls_white": False,
            "native_daynight_schedule_enabled": False,
            "native_daynight_schedule_start_at": "",
            "native_daynight_schedule_stop_at": "",
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
            "native_send2_motion_sensitivity": "",
            "native_send2_motion_cooldown": "",
            "native_send2_services": [],
            "config_patch_example": json.dumps({"image": {"brightness": 128}}, indent=2),
        }

        return defaults

    def _get_camera_supported_controls_for_ui_live(self, camera_id: str) -> dict[str, Any]:
        resolved = self._resolve_camera_id(camera_id) or camera_id.strip().lower()
        with self.state_lock:
            camera = self.cameras.get(resolved)
        if camera is None:
            raise RuntimeError(f"Unknown camera: {camera_id}")

        defaults = self._default_camera_supported_controls_for_ui(camera)

        capabilities: dict[str, Any] = {}
        config_payload: dict[str, Any] = {}
        state_payload: dict[str, Any] = {}
        live_stream_payloads: dict[str, dict[str, Any]] = {}
        native_controls_ok = False
        try:
            client = self._camera_api_client(camera)
            capabilities = client.get_capabilities()
            config_payload = client.get_config()
            state_payload = client.get_state()
            native_controls_ok = True
        except Exception as error:
            normalized_error = self._normalize_native_api_error(error)
            if self._is_native_api_unauthorized_error(normalized_error):
                recovered_camera = self._attempt_camera_token_refresh_for_ui(resolved, camera)
                if recovered_camera is not None:
                    try:
                        client = self._camera_api_client(recovered_camera)
                        capabilities = client.get_capabilities()
                        config_payload = client.get_config()
                        state_payload = client.get_state()
                        native_controls_ok = True
                    except Exception as retry_error:
                        defaults["native_controls_error"] = self._normalize_native_api_error(retry_error)
                else:
                    defaults["native_controls_error"] = normalized_error
            else:
                defaults["native_controls_error"] = normalized_error

        image = config_payload.get("image") or {}
        motion = config_payload.get("motion") or {}
        daynight = config_payload.get("daynight") or {}
        state_daynight = (state_payload.get("daynight") or {})
        stream_config_by_name: dict[str, dict[str, Any]] = {}
        for stream_config in (config_payload.get("streams") or []):
            if isinstance(stream_config, dict) and isinstance(stream_config.get("id"), int):
                stream_config_by_name[f"stream{stream_config['id']}"] = stream_config

        # Build stream-name → live state dict from state_payload's streams list
        for s in (state_payload.get("streams") or []):
            if isinstance(s, dict) and isinstance(s.get("id"), int):
                live_stream_payloads[f"stream{s['id']}"] = s
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
            for stream_name in sorted(stream_config_by_name):
                stream_config = stream_config_by_name.get(stream_name) or {}
                stream_suffix = stream_name[6:]
                if not stream_suffix.isdigit():
                    continue
                stream_index = int(stream_suffix)
                if stream_count is not None and stream_count >= 0 and stream_index >= stream_count:
                    continue
                live_stream_config = live_stream_payloads.get(stream_name) or {}
                osd_config = stream_config.get("osd") or {}
                privacy_config = (osd_config.get("privacy") or {}) if isinstance(osd_config, dict) else {}
                privacy_fill_color_value, privacy_fill_alpha = self._split_hex_color_alpha(privacy_config.get("fill_color"))
                privacy_stroke_color_value, privacy_stroke_alpha = self._split_hex_color_alpha(privacy_config.get("stroke_color"))
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
                        "width": self._format_stream_control_value(
                            stream_config,
                            "width",
                            zero_means_unset=True,
                            fallback_value=live_stream_config.get("width"),
                        ),
                        "height": self._format_stream_control_value(
                            stream_config,
                            "height",
                            zero_means_unset=True,
                            fallback_value=live_stream_config.get("height"),
                        ),
                        "fps": self._format_stream_control_value(
                            stream_config,
                            "fps",
                            zero_means_unset=True,
                            fallback_value=live_stream_config.get("fps"),
                        ),
                        "bitrate": "" if "bitrate" not in stream_config else str(stream_config.get("bitrate") or 0),
                        "format": str(stream_config.get("format") or "").strip(),
                        "mode": str(stream_config.get("mode") or "").strip(),
                        "osd_enabled_supported": isinstance(osd_config, dict) and "enabled" in osd_config,
                        "osd_enabled": bool(self._coerce_bool((osd_config or {}).get("enabled"))),
                        "osd_time_enabled_supported": isinstance((osd_config or {}).get("time"), dict) and "enabled" in (((osd_config or {}).get("time")) or {}),
                        "osd_time_enabled": bool(self._coerce_bool((((osd_config or {}).get("time") or {}).get("enabled")))),
                        "osd_usertext_enabled_supported": isinstance((osd_config or {}).get("usertext"), dict) and "enabled" in (((osd_config or {}).get("usertext")) or {}),
                        "osd_usertext_enabled": bool(self._coerce_bool((((osd_config or {}).get("usertext") or {}).get("enabled")))),
                        "osd_usertext_format_supported": isinstance((osd_config or {}).get("usertext"), dict) and "format" in (((osd_config or {}).get("usertext")) or {}),
                        "osd_usertext_format": str((((osd_config or {}).get("usertext") or {}).get("format") or "")).strip(),
                        "osd_privacy_enabled_supported": isinstance(privacy_config, dict) and "enabled" in privacy_config,
                        "osd_privacy_enabled": bool(self._coerce_bool((privacy_config or {}).get("enabled"))),
                        "osd_privacy_text_supported": isinstance(privacy_config, dict) and "text" in privacy_config,
                        "osd_privacy_text": str((privacy_config or {}).get("text") or "").strip(),
                        "osd_privacy_fill_color_supported": isinstance(privacy_config, dict) and "fill_color" in privacy_config,
                        "osd_privacy_fill_color": str((privacy_config or {}).get("fill_color") or "").strip(),
                        "osd_privacy_fill_color_value": privacy_fill_color_value,
                        "osd_privacy_fill_alpha": privacy_fill_alpha,
                        "osd_privacy_stroke_color_supported": isinstance(privacy_config, dict) and "stroke_color" in privacy_config,
                        "osd_privacy_stroke_color": str((privacy_config or {}).get("stroke_color") or "").strip(),
                        "osd_privacy_stroke_color_value": privacy_stroke_color_value,
                        "osd_privacy_stroke_alpha": privacy_stroke_alpha,
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
                    "total_gain_night_threshold": self._coerce_int(daynight.get("total_gain_night_threshold")),
                    "total_gain_day_threshold": self._coerce_int(daynight.get("total_gain_day_threshold")),
                    "controls": {
                        "color": self._coerce_bool((daynight.get("controls") or {}).get("color")),
                        "ircut": self._coerce_bool((daynight.get("controls") or {}).get("ircut")),
                        "ir850": self._coerce_bool((daynight.get("controls") or {}).get("ir850")),
                        "ir940": self._coerce_bool((daynight.get("controls") or {}).get("ir940")),
                        "white": self._coerce_bool((daynight.get("controls") or {}).get("white")),
                    },
                    "schedule": {
                        "enabled": self._coerce_bool((daynight.get("schedule") or {}).get("enabled")),
                        "start_at": str((daynight.get("schedule") or {}).get("start_at") or "").strip(),
                        "stop_at": str((daynight.get("schedule") or {}).get("stop_at") or "").strip(),
                    },
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
                if first_stream.get("osd_privacy_enabled_supported"):
                    patch_example[first_stream["name"]]["osd"]["privacy"] = {
                        "enabled": first_stream.get("osd_privacy_enabled", False),
                        "text": first_stream.get("osd_privacy_text") or "PRIVACY ENABLED",
                        "fill_color": first_stream.get("osd_privacy_fill_color") or "#000000FF",
                        "stroke_color": first_stream.get("osd_privacy_stroke_color") or "#FFFFFFFF",
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
                    "native_daynight_total_gain_night_threshold": "" if daynight.get("total_gain_night_threshold") in (None, "") else str(daynight.get("total_gain_night_threshold")),
                    "native_daynight_total_gain_day_threshold": "" if daynight.get("total_gain_day_threshold") in (None, "") else str(daynight.get("total_gain_day_threshold")),
                    "native_daynight_controls_color": bool(self._coerce_bool((daynight.get("controls") or {}).get("color"))),
                    "native_daynight_controls_ircut": bool(self._coerce_bool((daynight.get("controls") or {}).get("ircut"))),
                    "native_daynight_controls_ir850": bool(self._coerce_bool((daynight.get("controls") or {}).get("ir850"))),
                    "native_daynight_controls_ir940": bool(self._coerce_bool((daynight.get("controls") or {}).get("ir940"))),
                    "native_daynight_controls_white": bool(self._coerce_bool((daynight.get("controls") or {}).get("white"))),
                    "native_daynight_schedule_enabled": bool(self._coerce_bool((daynight.get("schedule") or {}).get("enabled"))),
                    "native_daynight_schedule_start_at": str((daynight.get("schedule") or {}).get("start_at") or "").strip(),
                    "native_daynight_schedule_stop_at": str((daynight.get("schedule") or {}).get("stop_at") or "").strip(),
                    "config_patch_example": json.dumps(patch_example, indent=2),
                }
            )

        defaults.update(self._camera_send2_controls_for_ui(camera))
        return defaults

    def get_camera_supported_controls_for_ui(self, camera_id: str) -> dict[str, Any]:
        return self.get_cached_camera_supported_controls_for_ui(camera_id)

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

    def _is_native_api_unauthorized_error(self, error: Exception | str) -> bool:
        normalized = self._normalize_native_api_error(error).lower()
        return (
            "unauthorized" in normalized
            or "http 401" in normalized
            or "http_401" in normalized
        )

    def _attempt_camera_token_refresh_for_ui(self, camera_id: str, camera: Camera) -> Camera | None:
        previous_token = str(camera.api_token or "").strip()
        try:
            updated = self._refresh_camera_api_token_from_camera(camera_id, camera)
        except Exception:
            return None

        if updated is not None:
            updated_token = str(updated.api_token or "").strip()
            if updated_token and updated_token != previous_token:
                return updated
        return None

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
        pending = None
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

        if pending is not None and pending.get("reply_event") is not None:
            pending["reply_text"] = text or ""
            pending["reply_payload"] = decoded if decoded is not None else payload.strip()
            pending["reply_event"].set()

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

    def api_probe_loop(self) -> None:
        while not self.stop_event.is_set():
            interval = self.api_probe_interval_seconds
            if interval <= 0:
                self.stop_event.wait(5)
                continue
            with self.state_lock:
                camera_ids = list(self.cameras.keys())
            for camera_id in camera_ids:
                if self.stop_event.is_set():
                    return
                self._schedule_api_refresh(camera_id)
            if self.stop_event.wait(interval):
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
        is_raptor = str(camera.api_streamer or "").strip().lower() == "raptor"
        snapshot_url = camera.snapshot_url.strip()
        parsed_snapshot = urllib.parse.urlsplit(snapshot_url) if snapshot_url else urllib.parse.SplitResult("", "", "", "", "")
        api_base_url = self._camera_api_base_url(camera)
        parsed_api = urllib.parse.urlsplit(api_base_url) if api_base_url else urllib.parse.SplitResult("", "", "", "", "")

        if is_raptor:
            host = self._camera_public_host(camera) or parsed_snapshot.hostname or parsed_api.hostname or ""
            if not host:
                return snapshot_url if normalized_stream == "ch0" else None
            scheme = parsed_api.scheme if parsed_api.scheme in {"http", "https"} else ""
            if not scheme:
                scheme = parsed_snapshot.scheme if parsed_snapshot.scheme in {"http", "https"} else "https"
            query = "stream=1" if normalized_stream == "ch1" else ""
            return urllib.parse.urlunsplit((scheme, f"{host}:8080", "/snap.jpg", query, ""))

        if snapshot_url:
            parsed = parsed_snapshot
            if parsed.netloc and not parsed.path.startswith("/api/"):
                if normalized_stream == "ch0":
                    return snapshot_url
                scheme = parsed.scheme or "http"
                return urllib.parse.urlunsplit((scheme, parsed.netloc, f"/x/{normalized_stream}.jpg", "", ""))

        host = self._camera_public_host(camera)
        if host:
            return f"http://{host}/x/{normalized_stream}.jpg"

        if not snapshot_url:
            return None
        parsed = parsed_snapshot
        if not parsed.netloc:
            return snapshot_url if normalized_stream == "ch0" else None
        scheme = parsed.scheme or "http"
        return urllib.parse.urlunsplit((scheme, parsed.netloc, f"/x/{normalized_stream}.jpg", "", ""))

    def _camera_mjpeg_url(self, camera: Camera, stream_name: str = "ch0") -> str:
        normalized_stream = str(stream_name or "ch0").strip().lower()
        normalized_stream = normalized_stream.split("?", 1)[0].split("&", 1)[0] or "ch0"
        is_raptor = str(camera.api_streamer or "").strip().lower() == "raptor"
        if is_raptor:
            api_base_url = self._camera_api_base_url(camera)
            parsed_api = urllib.parse.urlsplit(api_base_url) if api_base_url else urllib.parse.SplitResult("", "", "", "", "")
            snapshot_url = camera.snapshot_url.strip()
            parsed_snapshot = urllib.parse.urlsplit(snapshot_url) if snapshot_url else urllib.parse.SplitResult("", "", "", "", "")
            host = self._camera_public_host(camera) or parsed_snapshot.hostname or parsed_api.hostname or ""
            if not host:
                return ""
            scheme = parsed_api.scheme if parsed_api.scheme in {"http", "https"} else ""
            if not scheme:
                scheme = parsed_snapshot.scheme if parsed_snapshot.scheme in {"http", "https"} else "https"
            return urllib.parse.urlunsplit((scheme, f"{host}:8080", "/mjpeg", "", ""))

        host = self._camera_public_host(camera)
        if host:
            return f"http://{host}/x/{normalized_stream}.mjpg"

        snapshot_url = self._camera_snapshot_url(camera)
        if not snapshot_url:
            return ""

        parsed = urllib.parse.urlsplit(snapshot_url)
        if not parsed.netloc:
            return ""
        scheme = parsed.scheme or "http"
        return urllib.parse.urlunsplit((scheme, parsed.netloc, f"/x/{normalized_stream}.mjpg", "", ""))

    def _camera_rtsp_url(self, camera: Camera, stream_name: str = "ch0") -> str:
        normalized_stream = str(stream_name or "ch0").strip().lower()
        normalized_stream = normalized_stream.split("?", 1)[0].split("&", 1)[0] or "ch0"
        host = self._camera_public_host(camera)
        if host:
            return f"rtsp://{host}:554/{normalized_stream}"

        snapshot_url = self._camera_snapshot_url(camera)
        if not snapshot_url:
            return ""

        parsed = urllib.parse.urlsplit(snapshot_url)
        if not parsed.hostname:
            return ""
        return f"rtsp://{parsed.hostname}:554/{normalized_stream}"

    def _camera_webrtc_url(self, camera: Camera) -> str:
        if str(camera.api_streamer or "").strip().lower() != "raptor":
            return ""
        host = self._camera_public_host(camera)
        if not host:
            return ""
        api_base_url = self._camera_api_base_url(camera)
        scheme = "https"
        if api_base_url:
            parsed = urllib.parse.urlsplit(api_base_url)
            if parsed.scheme in {"http", "https"}:
                scheme = parsed.scheme
        return f"{scheme}://{host}:8554/webrtc"

    def _camera_send2_controls_for_ui(self, camera: Camera) -> dict[str, Any]:
        defaults = {
            "native_send2_available": False,
            "native_send2_error": "",
            "native_send2_motion_sensitivity": "",
            "native_send2_motion_cooldown": "",
            "native_send2_services": [],
        }

        try:
            client = self._camera_api_client(camera)
            config_payload = client.get_config()
            capabilities_payload = client.get_capabilities()
        except Exception as error:
            defaults["native_send2_error"] = str(error)
            return defaults

        motion_config = config_payload.get("motion") or {}

        # send2 settings live in /etc/send2.json on the camera, not in prudynt config.
        # Use capabilities to learn which services support video, then fetch actual
        # configured values from the per-service settings endpoints.
        send2_capabilities = capabilities_payload.get("send2") or {}

        services: list[dict[str, Any]] = []
        for service_name, service_label in SEND2_SERVICES:
            service_cap = send2_capabilities.get(service_name) or {}
            has_send_video = bool(service_cap.get("send_video")) if isinstance(service_cap, dict) else False

            try:
                photo_setting = client.get_setting(f"send2/services/{service_name}/send-photo")
                photo_enabled = self._coerce_bool(photo_setting.get("send_photo")) is not False
            except Exception:
                photo_enabled = True

            if has_send_video:
                try:
                    video_setting = client.get_setting(f"send2/services/{service_name}/send-video")
                    video_enabled = self._coerce_bool(video_setting.get("send_video")) is True
                except Exception:
                    video_enabled = False
            else:
                video_enabled = False

            motion_key = f"send2{service_name}"
            motion_enabled = bool(self._coerce_bool(motion_config.get(motion_key)))
            services.append(
                {
                    "name": service_name,
                    "label": service_label,
                    "motion_key": motion_key,
                    "photo_supported": True,
                    "video_supported": has_send_video,
                    "motion_enabled": motion_enabled,
                    "photo_enabled": photo_enabled,
                    "video_enabled": video_enabled,
                    "photo_test_supported": photo_enabled,
                    "video_test_supported": video_enabled,
                    "default_test_supported": not photo_enabled and not video_enabled,
                }
            )

        defaults.update(
            {
                "native_send2_available": True,
                "native_send2_motion_sensitivity": "" if motion_config.get("sensitivity") in (None, "") else str(motion_config.get("sensitivity")),
                "native_send2_motion_cooldown": "" if motion_config.get("cooldown_time") in (None, "") else str(motion_config.get("cooldown_time")),
                "native_send2_services": services,
            }
        )
        return defaults

    def _camera_onvif_endpoint(self, camera: Camera) -> str:
        endpoint = camera.onvif_endpoint.strip()
        if endpoint:
            return endpoint
        host = self._camera_public_host(camera)
        if host:
            return f"http://{host}/onvif/device_service"
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

    def queue_camera_onvif_refresh(self, camera_id: str) -> str:
        resolved = self._resolve_camera_id(camera_id) or camera_id.strip().lower()
        with self.state_lock:
            camera = self.cameras.get(resolved)
        if camera is None:
            raise RuntimeError(f"Unknown camera: {camera_id}")
        if not self._camera_onvif_endpoint(camera):
            raise RuntimeError(f"ONVIF endpoint is not configured for {camera.name}")
        return "scheduled" if self._schedule_onvif_refresh(resolved) else "already_running"

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
        try:
            with urllib.request.urlopen(request, timeout=self.snapshot_heartbeat_timeout_seconds) as response:
                content_type = response.headers.get("Content-Type", "image/jpeg")
                photo = response.read()
        except urllib.error.HTTPError as error:
            if error.code != 401 or camera.api_key:
                raise
            photo, content_type = self._fetch_snapshot_with_camera_login(camera, request.full_url)
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
        publish_result = self._publish_camera_command(
            camera_id,
            command,
            command_args,
            chat_id=chat_id,
            username=username,
            request_id=request_id,
            raw_text=" ".join(args[1:]),
        )
        if not publish_result["published"]:
            return "Failed to publish MQTT command"
        display_name = camera.name
        joined_args = " ".join(command_args)
        summary = command if not joined_args else f"{command} {joined_args}"
        return f"Queued for {display_name}: {summary}"

    def _publish_camera_command(
        self,
        camera_id: str,
        command: str,
        args: list[str] | None = None,
        *,
        chat_id: int = 0,
        username: str = "",
        request_id: str | None = None,
        raw_text: str | None = None,
        wait_for_reply_seconds: float = 0.0,
    ) -> dict[str, Any]:
        if not self._connect_mqtt() or self.mqtt_client is None:
            return {
                "published": False,
                "request_id": request_id or "",
                "reply_received": False,
                "reply_ok": None,
                "reply_text": "",
                "reply_payload": None,
            }

        command_args = [str(item) for item in (args or [])]
        request_id = request_id or uuid.uuid4().hex
        payload = {
            "request_id": request_id,
            "chat_id": chat_id,
            "username": username,
            "camera_id": camera_id,
            "command": command,
            "args": command_args,
            "raw_text": raw_text if raw_text is not None else (command if not command_args else f"{command} {' '.join(command_args)}"),
            "sent_at": int(time.time()),
        }
        pending: dict[str, Any] | None = None
        reply_event: threading.Event | None = None
        if chat_id > 0 or wait_for_reply_seconds > 0:
            pending = {
                "chat_id": chat_id,
                "camera_id": camera_id,
                "command": command,
                "created_at": time.time(),
            }
            if wait_for_reply_seconds > 0:
                reply_event = threading.Event()
                pending["reply_event"] = reply_event
                pending["reply_text"] = ""
                pending["reply_payload"] = None
            with self.reply_lock:
                self.pending_by_request[request_id] = pending
        if chat_id > 0:
            self.last_chat_by_camera[camera_id] = chat_id

        topic = self.command_topic_template.format(camera_id=camera_id)
        info = self.mqtt_client.publish(topic, json.dumps(payload), qos=1)
        info.wait_for_publish()
        if info.rc != mqtt.MQTT_ERR_SUCCESS:
            with self.reply_lock:
                self.pending_by_request.pop(request_id, None)
            return {
                "published": False,
                "request_id": request_id,
                "reply_received": False,
                "reply_ok": None,
                "reply_text": "",
                "reply_payload": None,
            }

        result = {
            "published": True,
            "request_id": request_id,
            "reply_received": False,
            "reply_ok": None,
            "reply_text": "",
            "reply_payload": None,
        }
        if reply_event is not None and pending is not None:
            if reply_event.wait(max(wait_for_reply_seconds, 0.1)):
                result["reply_received"] = True
                result["reply_text"] = str(pending.get("reply_text") or "")
                result["reply_payload"] = pending.get("reply_payload")
                reply_payload = pending.get("reply_payload")
                if isinstance(reply_payload, dict) and reply_payload.get("ok") is not None:
                    result["reply_ok"] = bool(reply_payload.get("ok"))
            else:
                with self.reply_lock:
                    self.pending_by_request.pop(request_id, None)
        return result

    def refresh_camera_mqtt_command_status(self, camera_id: str, *, wait_for_reply_seconds: float = 2.0) -> str:
        resolved = self._resolve_camera_id(camera_id) or camera_id.strip().lower()
        with self.state_lock:
            camera = self.cameras.get(resolved)
        if camera is None:
            raise RuntimeError(f"Unknown camera: {camera_id}")

        publish_result = self._publish_camera_command(
            resolved,
            "ping",
            wait_for_reply_seconds=max(wait_for_reply_seconds, 0.5),
        )
        now = int(time.time())
        status = "offline"
        error = ""
        last_ok_at = camera.mqtt_command_last_ok_at
        if publish_result.get("published") and publish_result.get("reply_received") and publish_result.get("reply_ok") is not False:
            status = "online"
            last_ok_at = now
        elif not publish_result.get("published") and not self.mqtt_connected:
            status = "unknown"
            error = "Hub MQTT is offline."
        else:
            error = str(publish_result.get("reply_text") or "").strip() or "Camera did not respond to hub MQTT commands; it likely published only a legacy registration and has no command subscription."

        updated = replace(
            camera,
            mqtt_command_status=status,
            mqtt_command_last_ok_at=last_ok_at,
            mqtt_command_last_error=error,
        )
        with self.state_lock:
            self.cameras[resolved] = updated
        self._persist_state()
        return status

    def _camera_accepts_hub_commands(self, camera_id: str, *, probe_if_needed: bool = True) -> bool:
        resolved = self._resolve_camera_id(camera_id) or camera_id.strip().lower()
        with self.state_lock:
            camera = self.cameras.get(resolved)
        if camera is None:
            raise RuntimeError(f"Unknown camera: {camera_id}")
        if camera.mqtt_command_status == "online":
            return True
        if not probe_if_needed:
            return False
        return self.refresh_camera_mqtt_command_status(resolved) == "online"

    def _camera_hub_command_error(self, camera_id: str) -> str:
        resolved = self._resolve_camera_id(camera_id) or camera_id.strip().lower()
        with self.state_lock:
            camera = self.cameras.get(resolved)
        if camera is None:
            return "Unknown camera."
        if camera.mqtt_command_last_error:
            return camera.mqtt_command_last_error
        if camera.mqtt_command_status == "unknown":
            return "This camera has not yet proven that it accepts hub MQTT commands."
        return "This camera published a registration but did not respond to hub MQTT commands."

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

        previous_entry = self.export_camera_override(resolved)

        config = self.export_config()
        cameras = list(config.get("cameras", []))
        index = None
        for idx, item in enumerate(cameras):
            if str(item.get("id") or "").strip().lower() == resolved:
                index = idx
                break

        def override_value(field: str, fallback: str = "") -> str:
            if field not in override:
                return str(previous_entry.get(field) or fallback)
            return str(override.get(field) or "")

        entry = {
            "id": resolved,
            "name": override_value("name", camera.name).strip() or camera.name,
            "ip": override_value("ip", camera.ip).strip(),
            "snapshot_url": override_value("snapshot_url", camera.snapshot_url).strip(),
            "api_key": override_value("api_key", camera.api_key).strip(),
            "api_base_url": override_value("api_base_url", camera.api_base_url).strip(),
            "api_token": override_value("api_token", camera.api_token).strip(),
            "onvif_endpoint": override_value("onvif_endpoint", camera.onvif_endpoint).strip(),
            "onvif_username": override_value("onvif_username", camera.onvif_username).strip(),
            "onvif_password": override_value("onvif_password", camera.onvif_password),
        }
        if index is None:
            cameras.append(entry)
        else:
            cameras[index] = entry
        config["cameras"] = cameras
        self.save_config(config)
        self.reload_config()
        self._record_history_config_changes(
            resolved,
            self._config_changes_from_mapping(
                previous_entry,
                entry,
                prefix="/hub/override",
                detail="Hub override save",
            ),
            source="hub",
            change_type="override_update",
        )
        self._record_history_action(
            resolved,
            "override_updated",
            "success",
            "Hub override saved",
            source="hub",
        )

    def connect_camera(self, enrollment: dict[str, str]) -> dict[str, Any]:
        camera_id = str(enrollment.get("camera_id") or enrollment.get("id") or "").strip().lower()
        resolved = self._resolve_camera_id(camera_id) or camera_id
        if not resolved:
            raise RuntimeError("Connect flow did not resolve a camera ID")
        with self.state_lock:
            camera = self.cameras.get(resolved)
        if camera is None:
            raise RuntimeError(f"Unknown camera: {camera_id}")
        if not self._camera_accepts_hub_commands(resolved, probe_if_needed=True):
            raise RuntimeError(self._camera_hub_command_error(resolved))

        previous_entry = self.export_camera_override(resolved) if resolved in self.static_camera_ids else {}
        enrollment_entry = self._normalized_enrollment_entry({
            **previous_entry,
            **enrollment,
            "camera_id": resolved,
            "id": resolved,
            "name": str(enrollment.get("name") or previous_entry.get("name") or camera.name),
            "ip": str(enrollment.get("ip") or previous_entry.get("ip") or camera.ip),
            "snapshot_url": str(enrollment.get("snapshot_url") or previous_entry.get("snapshot_url") or camera.snapshot_url),
            "api_key": str(enrollment.get("api_key") or previous_entry.get("api_key") or camera.api_key),
            "api_base_url": str(enrollment.get("api_base_url") or previous_entry.get("api_base_url") or camera.api_base_url),
            "api_token": str(previous_entry.get("api_token") or ""),
            "onvif_endpoint": str(enrollment.get("onvif_endpoint") or previous_entry.get("onvif_endpoint") or camera.onvif_endpoint),
            "onvif_username": str(enrollment.get("onvif_username") or previous_entry.get("onvif_username") or camera.onvif_username),
            "onvif_password": str(enrollment.get("onvif_password") or previous_entry.get("onvif_password") or camera.onvif_password),
        })
        enroll_result = self.enroll_camera(enrollment_entry)
        if not camera_id:
            raise RuntimeError("Connect flow did not resolve a camera ID")
        self._record_history_action(
            resolved,
            "connect",
            "success",
            "Connected to hub",
            source="hub",
            payload_summary=json.dumps({
                "api_base_url": enrollment_entry.get("api_base_url") or "",
            }, sort_keys=True),
        )
        onvif_username = str(enrollment.get("onvif_username") or "").strip()
        onvif_password = str(enrollment.get("onvif_password") or "")
        if onvif_username and onvif_username != self.default_onvif_username:
            self._save_default_onvif_credentials(onvif_username, onvif_password or self.default_onvif_password)
        elif onvif_password and onvif_password != self.default_onvif_password:
            self._save_default_onvif_credentials(self.default_onvif_username, onvif_password)
        return {
            **enroll_result,
            "camera_id": resolved,
            "status": "success",
            "status_detail": f"Connected {resolved} to the hub.",
            "api_base_url": enrollment_entry.get("api_base_url") or "",
            "api_token": str(enrollment_entry.get("api_token") or ""),
        }

    def _save_default_onvif_credentials(self, username: str, password: str) -> None:
        config = json.loads(json.dumps(self.config))
        if "defaults" not in config or not isinstance(config.get("defaults"), dict):
            config["defaults"] = {}
        config["defaults"]["onvif_username"] = username
        config["defaults"]["onvif_password"] = password
        self.save_config(config)
        self.reload_config()

    def _normalized_enrollment_entry(self, enrollment: dict[str, str]) -> dict[str, str]:
        camera_id = str(enrollment.get("camera_id") or enrollment.get("id") or "").strip().lower()
        ip = str(enrollment.get("ip") or "").strip()
        snapshot_url = str(enrollment.get("snapshot_url") or "").strip()
        api_key = str(enrollment.get("api_key") or "").strip()
        api_base_url = str(enrollment.get("api_base_url") or "").strip()
        api_token = str(enrollment.get("api_token") or "").strip()
        onvif_endpoint = str(enrollment.get("onvif_endpoint") or "").strip()
        onvif_username = str(enrollment.get("onvif_username") or "").strip()
        onvif_password = str(enrollment.get("onvif_password") or "")

        resolved_name = str(enrollment.get("name") or "").strip()
        if ip:
            resolved_camera_id, discovered_name = self._resolve_enrollment_camera_identity(ip)
            if not camera_id:
                camera_id = resolved_camera_id
            if not resolved_name:
                resolved_name = discovered_name

        name = resolved_name or camera_id

        if not api_base_url and ip:
            api_base_url = f"https://{ip}:1998/api/v1"
        if not snapshot_url and ip:
            snapshot_url = f"http://{ip}/x/ch0.jpg"

        return {
            "id": camera_id,
            "name": name,
            "ip": ip,
            "snapshot_url": snapshot_url,
            "api_key": api_key,
            "api_base_url": api_base_url,
            "api_token": api_token,
            "onvif_endpoint": onvif_endpoint,
            "onvif_username": onvif_username,
            "onvif_password": onvif_password,
        }

    def _resolve_enrollment_camera_identity(self, ip: str) -> tuple[str, str]:
        normalized_ip = str(ip or "").strip()
        if not normalized_ip:
            return "", ""
        with self.state_lock:
            matches = [camera for camera in self.cameras.values() if str(camera.ip or "").strip() == normalized_ip]
        if not matches:
            return "", ""
        if len(matches) > 1:
            raise RuntimeError(f"Multiple cameras currently use IP {normalized_ip}; wait for stale entries to clear or remove the duplicate first")
        camera = matches[0]
        discovered_name = str(camera.name or camera.hostname or camera.camera_id).strip() or camera.camera_id
        return camera.camera_id, discovered_name

    def _camera_conflicts_for_enrollment(self, camera_id: str, ip: str) -> dict[str, str]:
        conflicts: dict[str, str] = {}
        normalized_id = str(camera_id or "").strip().lower()
        normalized_ip = str(ip or "").strip()
        with self.state_lock:
            snapshot = list(self.cameras.values())
        for camera in snapshot:
            if normalized_id and camera.camera_id == normalized_id:
                conflicts["camera_id"] = camera.camera_id
            if normalized_ip and camera.ip == normalized_ip and camera.camera_id != normalized_id:
                conflicts["ip"] = camera.camera_id
        return conflicts

    def probe_camera_enrollment(self, enrollment: dict[str, str]) -> dict[str, Any]:
        entry = self._normalized_enrollment_entry(enrollment)
        camera_id = entry["id"]
        ip = entry["ip"]
        if not entry["api_base_url"] and not ip and not entry["snapshot_url"] and not entry["onvif_endpoint"]:
            raise RuntimeError("Provide at least an IP address, API base URL, snapshot URL, or ONVIF endpoint")

        conflicts = self._camera_conflicts_for_enrollment(camera_id, ip)
        probe_camera = Camera(
            camera_id=camera_id or (ip or "camera"),
            name=entry["name"] or camera_id or ip or "camera",
            ip=ip,
            snapshot_url=entry["snapshot_url"],
            api_key=entry["api_key"],
            api_base_url=entry["api_base_url"],
            api_token=entry["api_token"],
            onvif_endpoint=entry["onvif_endpoint"],
            onvif_username=entry["onvif_username"],
            onvif_password=entry["onvif_password"],
            status="static",
        )

        api_probe: dict[str, Any] = {
            "configured": bool(entry["api_base_url"]),
            "ok": False,
            "base_url": entry["api_base_url"],
            "error": "",
            "device_name": "",
            "device_model": "",
            "streamer": "",
            "version": "",
        }
        if entry["api_base_url"]:
            try:
                api_info = self._fetch_camera_api_details(probe_camera)
                api_probe.update(
                    {
                        "ok": True,
                        "device_name": str(api_info.get("device_name") or ""),
                        "device_model": str(api_info.get("device_model") or ""),
                        "streamer": str(api_info.get("streamer") or ""),
                        "version": str(api_info.get("version") or ""),
                    }
                )
            except Exception as error:
                api_probe["error"] = self._normalize_native_api_error(error)

        onvif_endpoint = self._camera_onvif_endpoint(probe_camera)
        onvif_probe: dict[str, Any] = {
            "configured": bool(onvif_endpoint),
            "ok": False,
            "endpoint": onvif_endpoint,
            "error": "",
            "manufacturer": "",
            "model": "",
            "firmware_version": "",
        }
        if onvif_endpoint:
            try:
                onvif_info = self._fetch_onvif_device_information(probe_camera)
                onvif_probe.update(
                    {
                        "ok": True,
                        "manufacturer": str(onvif_info.get("manufacturer") or ""),
                        "model": str(onvif_info.get("model") or ""),
                        "firmware_version": str(onvif_info.get("firmware_version") or ""),
                    }
                )
            except Exception as error:
                onvif_probe["error"] = str(error)

        snapshot_url = self._camera_snapshot_url(probe_camera) or ""
        result = {
            "camera_id": camera_id,
            "name": entry["name"] or camera_id or ip,
            "ip": ip,
            "conflicts": conflicts,
            "api": api_probe,
            "onvif": onvif_probe,
            "snapshot_url": snapshot_url,
            "can_save": "ip" not in conflicts,
        }
        self._record_history_action(
            camera_id,
            "enrollment_probe",
            "success" if result["can_save"] else "warning",
            f"api={'ok' if api_probe['ok'] else 'fail'} onvif={'ok' if onvif_probe['ok'] else 'fail'}",
            source="hub",
        )
        return result

    def generate_pairing_bundle(self, enrollment: dict[str, str]) -> dict[str, Any]:
        entry = self._normalized_enrollment_entry(enrollment)
        camera_id = entry["id"]
        if not entry["api_base_url"] and not entry["ip"]:
            raise RuntimeError("Provide at least an IP address or API base URL to prepare pairing")
        if not camera_id:
            raise RuntimeError(f"Camera at IP {entry['ip']} is not currently registered with the hub, so its MQTT camera ID cannot be resolved automatically yet")

        token = entry["api_token"] or secrets.token_urlsafe(24)
        port = 1998
        listen_addr = "0.0.0.0"
        bootstrap_payload = {
            "agent": {
                "enabled": True,
                "tls": True,
                "listen": listen_addr,
                "port": port,
                "token": token,
            }
        }
        # Bootstrap always enables TLS, so always use https:// for the saved URL.
        # Ignore any pre-existing http:// URL — it predates bootstrap.
        api_base_url = f"https://{entry['ip']}:{port}/api/v1" if entry["ip"] else entry["api_base_url"]
        bootstrap_json = json.dumps(bootstrap_payload, indent=2, sort_keys=True)
        compact_bootstrap_json = json.dumps(bootstrap_payload, sort_keys=True)
        token_json = json.dumps(token)
        listen_json = json.dumps(listen_addr)
        shell_bootstrap_json = json.dumps(compact_bootstrap_json)
        commands = [
            "jct /etc/thingino.json set agent.enabled true",
            "jct /etc/thingino.json set agent.tls true",
            f"jct /etc/thingino.json set agent.listen {listen_json}",
            f"jct /etc/thingino.json set agent.port {port}",
            f"jct /etc/thingino.json set agent.token {token_json}",
            "/etc/init.d/S95thingino-agent restart",
        ]
        bootstrap_install_commands = [
            f"printf '%s\\n' {shell_bootstrap_json} > /etc/thingino-agent-bootstrap.json",
            "/etc/init.d/S95thingino-agent restart",
        ]
        save_entry = dict(entry)
        save_entry["api_base_url"] = api_base_url
        save_entry["api_token"] = token

        self._record_history_action(
            camera_id,
            "pairing_bundle",
            "success",
            f"Prepared pairing bundle for {api_base_url or camera_id}",
            source="hub",
            payload_summary=json.dumps({"api_base_url": api_base_url, "listen": listen_addr, "port": port}, sort_keys=True),
        )

        return {
            "camera_id": camera_id,
            "name": entry["name"],
            "ip": entry["ip"],
            "api_base_url": api_base_url,
            "api_token": token,
            "bootstrap_payload": bootstrap_payload,
            "bootstrap_json": bootstrap_json,
            "commands": commands,
            "bootstrap_install_commands": bootstrap_install_commands,
            "save_entry": save_entry,
        }

    def install_pairing_bundle_via_mqtt(self, enrollment: dict[str, str]) -> dict[str, Any]:
        bundle = self.generate_pairing_bundle(enrollment)
        camera_id = str(bundle.get("camera_id") or "").strip().lower()
        resolved = self._resolve_camera_id(camera_id) or camera_id
        if not resolved or resolved not in self.cameras:
            raise RuntimeError("MQTT pairing install requires the camera to be currently registered with the hub")

        with self.state_lock:
            camera = self.cameras.get(resolved)
        if camera is None:
            raise RuntimeError("MQTT pairing install requires the camera to be currently registered with the hub")
        if not self._camera_accepts_hub_commands(resolved, probe_if_needed=True):
            raise RuntimeError(self._camera_hub_command_error(resolved))

        save_entry = bundle.get("save_entry") if isinstance(bundle.get("save_entry"), dict) else {}
        pairing_camera = replace(
            camera,
            onvif_username=str(save_entry.get("onvif_username") or camera.onvif_username),
            onvif_password=str(save_entry.get("onvif_password") or camera.onvif_password),
            ip=str(save_entry.get("ip") or camera.ip),
        )

        mqtt_cfg = self.config.get("mqtt") or {}
        mqtt_host = str(mqtt_cfg.get("host") or "").strip()
        mqtt_port = str(int(mqtt_cfg.get("port") or 1883))
        mqtt_username = str(mqtt_cfg.get("username") or "").strip()
        mqtt_password = str(mqtt_cfg.get("password") or "")

        publish_result = self._publish_camera_command(
            resolved,
            "install-agent-bootstrap",
            [str(bundle.get("api_token") or ""), mqtt_host, mqtt_port, mqtt_username, mqtt_password],
            wait_for_reply_seconds=max(float(self.command_reply_timeout_seconds), 20.0),
        )
        if not publish_result["published"]:
            raise RuntimeError("Failed to publish MQTT pairing install command")

        reply_text = str(publish_result.get("reply_text") or "").strip()
        lowered_reply = reply_text.lower()
        reply_failed = bool(
            publish_result.get("reply_ok") is False
            or (
                publish_result.get("reply_received")
                and publish_result.get("reply_ok") is None
                and (
                    lowered_reply.startswith("failed")
                    or lowered_reply.startswith("unsupported")
                    or "mismatch" in lowered_reply
                    or lowered_reply.startswith("unable")
                )
            )
        )
        if reply_failed:
            self._record_history_action(
                resolved,
                "pairing_install",
                "error",
                reply_text or "Camera rejected MQTT pairing install",
                source="hub",
                payload_summary=json.dumps({"api_base_url": bundle.get("api_base_url")}, sort_keys=True),
            )
            raise RuntimeError(reply_text or "Camera rejected MQTT pairing install")

        if publish_result["reply_received"]:
            status = "success"
            detail = reply_text or "Agent bootstrap installed via MQTT"
            save_entry = bundle.get("save_entry")
            if isinstance(save_entry, dict):
                self.enroll_camera({str(key): str(value or "") for key, value in save_entry.items()})
        else:
            confirmed_via_api = self._confirm_pairing_install_via_api(resolved, bundle)
            if confirmed_via_api:
                status = "success"
                detail = "Camera did not confirm over MQTT, but the native API came back with the newly installed token"
            else:
                status = "warning"
                detail = "Pairing install published over MQTT, but the camera did not confirm before the timeout"

        self._record_history_action(
            resolved,
            "pairing_install",
            status,
            detail,
            source="hub",
            payload_summary=json.dumps({
                "api_base_url": bundle.get("api_base_url"),
            }, sort_keys=True),
        )

        if status == "success":
            self._refresh_camera_state_after_pairing(resolved)

        return {
            **bundle,
            "status": status,
            "status_detail": detail,
            "mqtt": {
                "camera_id": resolved,
                "published": True,
                "reply_received": bool(publish_result.get("reply_received")),
                "reply_ok": publish_result.get("reply_ok"),
                "reply_text": reply_text,
                "request_id": publish_result.get("request_id") or "",
            },
        }

    def _confirm_pairing_install_via_api(self, camera_id: str, bundle: dict[str, Any]) -> bool:
        api_base_url = str(bundle.get("api_base_url") or "").strip()
        api_token = str(bundle.get("api_token") or "").strip()
        if not api_base_url or not api_token:
            return False

        with self.state_lock:
            current = self.cameras.get(camera_id)
        if current is None:
            return False

        probe_camera = replace(current, api_base_url=api_base_url, api_token=api_token)
        deadline = time.monotonic() + max(float(self.command_reply_timeout_seconds), 12.0)
        last_error = ""
        while time.monotonic() < deadline:
            try:
                self._camera_api_client(probe_camera).get_device()
            except Exception as error:
                last_error = self._normalize_native_api_error(error)
                time.sleep(1)
                continue

            save_entry = bundle.get("save_entry")
            if isinstance(save_entry, dict):
                self.enroll_camera({str(key): str(value or "") for key, value in save_entry.items()})
            return True

        if last_error:
            LOG.info("Pairing API confirmation did not succeed for %s: %s", camera_id, last_error)
        return False

    def _refresh_camera_state_after_pairing(self, camera_id: str) -> None:
        deadline = time.monotonic() + 20.0
        while time.monotonic() < deadline:
            try:
                if self.refresh_camera_api_details(camera_id):
                    try:
                        self.refresh_camera_supported_controls_for_ui(camera_id)
                    except Exception:
                        LOG.debug("Supported controls refresh after pairing failed for %s", camera_id, exc_info=True)
                    return
            except Exception:
                LOG.debug("API refresh after pairing failed for %s", camera_id, exc_info=True)
            time.sleep(1)

    def enroll_camera(self, enrollment: dict[str, str]) -> dict[str, Any]:
        raw_camera_id = str(enrollment.get("camera_id") or enrollment.get("id") or "").strip().lower()
        entry = self._normalized_enrollment_entry(enrollment)
        raw_camera_id = raw_camera_id or entry["id"]
        if not raw_camera_id:
            raise RuntimeError(f"Camera at IP {entry['ip']} is not currently registered with the hub, so its MQTT camera ID cannot be resolved automatically yet")
        conflicts = self._camera_conflicts_for_enrollment(raw_camera_id, entry["ip"])
        if "ip" in conflicts:
            raise RuntimeError(f"IP {entry['ip']} is already assigned to {conflicts['ip']}")

        config = self.export_config()
        cameras = list(config.get("cameras", []))
        updated_existing = False
        previous_entry: dict[str, Any] = {"id": raw_camera_id}
        for index, item in enumerate(cameras):
            if str(item.get("id") or "").strip().lower() != raw_camera_id:
                continue
            previous_entry = {
                "id": str(item.get("id") or raw_camera_id).strip().lower(),
                "name": str(item.get("name") or raw_camera_id).strip(),
                "ip": str(item.get("ip") or "").strip(),
                "snapshot_url": str(item.get("snapshot_url") or "").strip(),
                "api_key": str(item.get("api_key") or "").strip(),
                "api_base_url": str(item.get("api_base_url") or "").strip(),
                "api_token": str(item.get("api_token") or "").strip(),
                "onvif_endpoint": str(item.get("onvif_endpoint") or "").strip(),
                "onvif_username": str(item.get("onvif_username") or "").strip(),
                "onvif_password": str(item.get("onvif_password") or ""),
            }
            cameras[index] = entry
            updated_existing = True
            break
        if not updated_existing:
            cameras.append(entry)
        config["cameras"] = cameras
        self.save_config(config)
        self.reload_config()

        rescan_total, rescan_published = self.rescan_cameras(raw_camera_id)
        api_refresh = "not_configured"
        onvif_refresh = "not_configured"
        controls_refresh = "not_configured"
        try:
            api_refresh = self.queue_camera_api_refresh(raw_camera_id)
            controls_refresh = "scheduled"
        except Exception:
            api_refresh = "not_configured"
            controls_refresh = "not_configured"
        try:
            onvif_refresh = self.queue_camera_onvif_refresh(raw_camera_id)
        except Exception:
            onvif_refresh = "not_configured"

        detail = entry["name"] if not entry["ip"] else f"{entry['name']} @ {entry['ip']}"
        self._record_history_config_changes(
            raw_camera_id,
            self._config_changes_from_mapping(
                previous_entry,
                entry,
                prefix="/hub/enrollment",
                detail="Camera enrollment",
            ),
            source="hub",
            change_type="enrollment_update" if updated_existing else "enrollment_create",
        )
        self._record_history_action(
            raw_camera_id,
            "enrolled",
            "success",
            detail,
            source="hub",
        )

        return {
            "camera_id": raw_camera_id,
            "updated_existing": updated_existing,
            "rescan_requested": rescan_total > 0 and rescan_published > 0,
            "api_refresh": api_refresh,
            "onvif_refresh": onvif_refresh,
            "controls_refresh": controls_refresh,
        }

    def perform_bulk_action(self, camera_ids: list[str], action: str) -> dict[str, Any]:
        targets: list[str] = []
        seen: set[str] = set()
        for camera_id in camera_ids:
            resolved = self._resolve_camera_id(camera_id) or str(camera_id or "").strip().lower()
            if not resolved or resolved in seen:
                continue
            with self.state_lock:
                exists = resolved in self.cameras
            if not exists:
                continue
            seen.add(resolved)
            targets.append(resolved)

        if not targets:
            raise RuntimeError("Select at least one known camera")

        normalized_action = str(action or "").strip().lower()
        results: list[dict[str, str]] = []
        for camera_id in targets:
            try:
                if normalized_action == "refresh-api":
                    detail = self.queue_camera_api_refresh(camera_id)
                    self._record_history_action(camera_id, "bulk_refresh_api", "success", detail, source="hub")
                elif normalized_action == "refresh-onvif":
                    detail = self.queue_camera_onvif_refresh(camera_id)
                    self._record_history_action(camera_id, "bulk_refresh_onvif", "success", detail, source="hub")
                elif normalized_action == "refresh-snapshot":
                    detail = self.queue_snapshot_refresh(camera_id)
                    self._record_history_action(camera_id, "bulk_refresh_snapshot", "success", detail, source="hub")
                elif normalized_action == "rescan":
                    total, published = self.rescan_cameras(camera_id)
                    detail = "scheduled" if published == total else "publish_failed"
                    self._record_history_action(camera_id, "bulk_rescan", "success" if published == total else "error", detail, source="hub")
                elif normalized_action == "restart-streaming":
                    self.control_camera_service(camera_id, "streaming", "restart", refresh_after=False)
                    detail = "requested"
                elif normalized_action == "start-streaming":
                    self.control_camera_service(camera_id, "streaming", "start", refresh_after=False)
                    detail = "requested"
                elif normalized_action == "stop-streaming":
                    self.control_camera_service(camera_id, "streaming", "stop", refresh_after=False)
                    detail = "requested"
                else:
                    raise RuntimeError(f"Unsupported bulk action: {action}")
                results.append({"camera_id": camera_id, "status": "success", "detail": detail})
            except Exception as error:
                self._record_history_action(camera_id, f"bulk_{normalized_action.replace('-', '_')}", "error", str(error), source="hub")
                results.append({"camera_id": camera_id, "status": "error", "detail": str(error)})

        success_count = sum(1 for item in results if item["status"] == "success")
        error_count = len(results) - success_count
        return {
            "action": normalized_action,
            "total": len(results),
            "success_count": success_count,
            "error_count": error_count,
            "results": results,
        }

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
                    "is_paired": bool(self._camera_api_token(camera)),
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
        if camera.mqtt_command_status == "unknown":
            try:
                self.refresh_camera_mqtt_command_status(resolved)
            except Exception:
                LOG.debug("MQTT command capability probe failed for %s", resolved, exc_info=True)
            with self.state_lock:
                camera = self.cameras.get(resolved)
            if camera is None:
                raise RuntimeError(f"Unknown camera: {camera_id}")
        override = self.export_camera_override(resolved)
        hub_connected = resolved in self.static_camera_ids
        conflict = self._camera_ip_conflict(camera)
        live_links_available = conflict is None
        api_error = self._normalize_native_api_error(camera.api_last_error)
        api_status = camera.api_status
        if camera.api_status == "offline" and api_error == "Native API is not available on this camera build.":
            api_status = "unsupported"
        present_on_mqtt_broker = self._camera_registration_status_for_ui(camera) == "online"
        is_paired = bool(self._camera_api_token(camera))
        registered_on_hub = hub_connected or is_paired
        has_agent = self._camera_has_agent_for_ui(camera)
        setup_status = "pair"
        if is_paired:
            setup_status = "paired"
        elif not present_on_mqtt_broker:
            setup_status = "unavailable"
        elif camera.mqtt_command_status == "unknown":
            setup_status = "verifying"
        elif not has_agent:
            setup_status = "unavailable"
        elif not registered_on_hub:
            setup_status = "connect"
        preview_state = self._camera_preview_state(camera)
        camera_image_id = self._camera_image_id_for_ui(camera)
        return {
            "camera_id": camera.camera_id,
            "name": camera.name,
            "hostname": camera.hostname or "n/a",
            "ip": self._normalized_camera_ip(camera.ip) or self._camera_public_host(camera) or "",
            "camera_image_id": camera_image_id,
            "ota_upgrade_command": self._camera_ota_upgrade_command_for_ui(camera),
            "snapshot_url": (self._camera_snapshot_url(camera) or "") if live_links_available else "",
            "snapshot_ch1_url": (self._camera_snapshot_url(camera, "ch1") or "") if live_links_available else "",
            "mjpeg_ch0_url": self._camera_mjpeg_url(camera, "ch0") if live_links_available else "",
            "mjpeg_ch1_url": self._camera_mjpeg_url(camera, "ch1") if live_links_available else "",
            "rtsp_ch0_url": self._camera_rtsp_url(camera, "ch0") if live_links_available else "",
            "rtsp_ch1_url": self._camera_rtsp_url(camera, "ch1") if live_links_available else "",
            "webrtc_url": self._camera_webrtc_url(camera) if live_links_available else "",
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
            "mqtt_command_status": camera.mqtt_command_status,
            "mqtt_command_capable": camera.mqtt_command_status == "online",
            "mqtt_command_last_ok_at": self._format_timestamp(camera.mqtt_command_last_ok_at),
            "mqtt_command_last_error": camera.mqtt_command_last_error,
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
            "hub_connected": registered_on_hub,
            "present_on_mqtt_broker": present_on_mqtt_broker,
            "has_agent": has_agent,
            "registered_on_hub": registered_on_hub,
            "is_paired": is_paired,
            "setup_status": setup_status,
            "default_onvif_username": self.default_onvif_username,
            "default_onvif_password": self.default_onvif_password,
        }

    def _camera_image_id_for_ui(self, camera: Camera) -> str:
        for candidate in (camera.api_device_model, camera.onvif_model):
            value = str(candidate or "").strip()
            if value:
                return value
        return ""

    def _camera_ota_upgrade_command_for_ui(self, camera: Camera) -> str:
        camera_image_id = self._camera_image_id_for_ui(camera)
        camera_ip = str(camera.ip or "").strip()
        if not camera_image_id or not camera_ip:
            return ""
        return f"CAMERA={camera_image_id} IP={camera_ip} make cleanbuild upgrade_ota"

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
        if not self._camera_has_agent_for_ui(camera):
            return camera.status
        if camera.last_registration_at is None:
            return camera.status
        if self.registration_stale_after_seconds <= 0:
            return camera.status
        if time.time() - float(camera.last_registration_at) > self.registration_stale_after_seconds:
            return "offline"
        return camera.status

    def _camera_has_agent_for_ui(self, camera: Camera) -> bool:
        return bool(self._camera_api_token(camera)) or camera.mqtt_command_status == "online"

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

    def get_camera_webrtc_url_for_ui(self, camera_id: str) -> str:
        resolved = self._resolve_camera_id(camera_id) or camera_id.strip().lower()
        with self.state_lock:
            camera = self.cameras.get(resolved)
        if camera is None:
            raise RuntimeError(f"Unknown camera: {camera_id}")
        conflict = self._camera_ip_conflict(camera)
        if conflict is not None:
            raise RuntimeError(self._camera_ip_conflict_error(camera, conflict))
        return self._camera_webrtc_url(camera)

    def get_camera_login_credentials_for_ui(self, camera_id: str) -> tuple[str, str]:
        resolved = self._resolve_camera_id(camera_id) or camera_id.strip().lower()
        with self.state_lock:
            camera = self.cameras.get(resolved)
        if camera is None:
            raise RuntimeError(f"Unknown camera: {camera_id}")
        conflict = self._camera_ip_conflict(camera)
        if conflict is not None:
            raise RuntimeError(self._camera_ip_conflict_error(camera, conflict))
        return self._camera_login_credentials(camera)

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

    def _schedule_snapshot_refresh(self, camera_id: str) -> bool:
        resolved = self._resolve_camera_id(camera_id) or camera_id.strip().lower()
        with self.state_lock:
            camera = self.cameras.get(resolved)
            if camera is None:
                return False
            if not self._camera_snapshot_url(camera):
                return False
            if resolved in self.snapshot_refreshing:
                return False
            self.snapshot_refreshing.add(resolved)
        worker = threading.Thread(
            target=self._refresh_snapshot_worker,
            args=(resolved,),
            name=f"telegrambothub-snapshot-{resolved[:8]}",
            daemon=True,
        )
        worker.start()
        return True

    def _refresh_snapshot_worker(self, camera_id: str) -> None:
        try:
            self.refresh_snapshot_cache(camera_id)
        finally:
            with self.state_lock:
                self.snapshot_refreshing.discard(camera_id)

    def queue_snapshot_refresh(self, camera_id: str) -> str:
        resolved = self._resolve_camera_id(camera_id) or camera_id.strip().lower()
        with self.state_lock:
            camera = self.cameras.get(resolved)
        if camera is None:
            raise RuntimeError(f"Unknown camera: {camera_id}")
        if not self._camera_snapshot_url(camera):
            raise RuntimeError(f"Snapshot URL is not configured for {camera.name}")
        return "scheduled" if self._schedule_snapshot_refresh(resolved) else "already_running"

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
        result = self._publish_camera_command(camera_id, command)
        if result["published"]:
            return True
        LOG.warning("Failed to publish %s request for %s", command, camera_id)
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

    def _format_stream_control_value(
        self,
        stream_config: dict[str, Any],
        field_name: str,
        *,
        zero_means_unset: bool = False,
        fallback_value: Any = None,
    ) -> str:
        if field_name not in stream_config:
            return ""
        value = self._coerce_int(stream_config.get(field_name))
        if value is None:
            raw_value = str(stream_config.get(field_name) or "").strip()
            return raw_value
        if zero_means_unset and value <= 0:
            fallback = self._coerce_int(fallback_value)
            if fallback is not None and fallback > 0:
                return str(fallback)
            return ""
        return str(value)

    def _split_hex_color_alpha(self, value: Any) -> tuple[str, str]:
        normalized = str(value or "").strip().upper()
        if re.fullmatch(r"#[0-9A-F]{8}", normalized):
            return normalized[:7], str(int(normalized[7:], 16))
        if re.fullmatch(r"#[0-9A-F]{6}", normalized):
            return normalized, "255"
        return "#000000", "255"

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
    return _load_config_dict(config)


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
    background_threads: list[threading.Thread] = []
    if hub.snapshot_heartbeat_interval_seconds > 0:
        background_threads.append(
            threading.Thread(target=hub.snapshot_probe_loop, name="telegrambothub-probe", daemon=True)
        )
    if hub.api_probe_interval_seconds > 0:
        background_threads.append(
            threading.Thread(target=hub.api_probe_loop, name="telegrambothub-api-probe", daemon=True)
        )

    def handle_signal(_signum: int, _frame: Any) -> None:
        LOG.info("Stopping hub")
        hub.stop()
        web_server.stop()

    signal.signal(signal.SIGINT, handle_signal)
    signal.signal(signal.SIGTERM, handle_signal)

    LOG.info("Starting telegrambothub")
    hub_thread.start()
    for worker in background_threads:
        worker.start()
    web_server.start()
    while (hub_thread.is_alive() or any(worker.is_alive() for worker in background_threads)) and not hub.stop_event.wait(0.5):
        pass
    hub.stop()
    web_server.stop()
    hub_thread.join(timeout=5)
    probe_thread.join(timeout=5)
    LOG.info("Stopped telegrambothub")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
