import copy
import base64
import http.cookiejar
import hmac
import json
import logging
import os
import re
import ssl
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
from urllib.parse import urlsplit
from typing import TYPE_CHECKING, Any

import yaml

from .action_result_adapter import delete_outcome, pairing_outcome
from .config_model import load_config_dict
from flask import Flask, Response, flash, jsonify, redirect, render_template, request, send_file, session, url_for
from werkzeug.serving import make_server

if TYPE_CHECKING:
    from .main import Hub


LOG = logging.getLogger("telegrambothub.web")
_BULK_ACTION_RESULT_SESSION_KEY = "dashboard_bulk_action_result"


class WebServer:
    def __init__(self, app: Flask, host: str, port: int) -> None:
        self.app = app
        self.host = host
        self.port = port
        self._server = make_server(host, port, app, threaded=True)
        self._thread = threading.Thread(target=self._server.serve_forever, name="telegrambothub-web", daemon=True)

    def start(self) -> None:
        LOG.info("Starting web UI on http://%s:%s", self.host, self.port)
        self._thread.start()

    def stop(self) -> None:
        self._server.shutdown()
        self._thread.join(timeout=5)


def create_web_app(hub: "Hub", ui_username: str = "", ui_password: str = "", api_v2_client: Any | None = None) -> Flask:
    app = Flask(__name__)
    app.config["SECRET_KEY"] = "telegrambothub-ui"
    app.config["SESSION_PERMANENT"] = False
    app.config["TEMPLATES_AUTO_RELOAD"] = True
    app.config["SEND_FILE_MAX_AGE_DEFAULT"] = 0
    app.jinja_env.auto_reload = True

    auth_enabled = bool(ui_username and ui_password)
    api_v2_enabled = str(os.environ.get("HUB_API_V2_ENABLED", "0")).strip().lower() in {"1", "true", "yes", "on"}
    api_v2_host = os.environ.get("HUB_API_V2_HOST", "127.0.0.1")
    api_v2_port = int(os.environ.get("HUB_API_V2_PORT", "8090"))
    api_v2_base_url = f"http://{api_v2_host}:{api_v2_port}"

    def current_ui_competency_level() -> str:
        try:
            config = hub.export_config()
        except Exception:
            return "basic"
        ui_config = config.get("ui") if isinstance(config, dict) else {}
        return _normalize_competency_level((ui_config or {}).get("competency_level"))

    def current_user_has_advanced_access() -> bool:
        return current_ui_competency_level() in {"advanced", "expert"}

    def current_user_has_expert_access() -> bool:
        return current_ui_competency_level() == "expert"

    @app.context_processor
    def inject_auth_state() -> dict[str, Any]:
        return {
            "auth_enabled": auth_enabled,
            "is_authenticated": bool(session.get("ui_authenticated")),
            "ui_competency_level": current_ui_competency_level(),
            "ui_has_advanced_access": current_user_has_advanced_access(),
            "ui_has_expert_access": current_user_has_expert_access(),
        }

    def credentials_are_valid(username: str, password: str) -> bool:
        username_ok = hmac.compare_digest(username, ui_username)
        password_ok = hmac.compare_digest(password, ui_password)
        return username_ok and password_ok

    def wants_json_response() -> bool:
        requested_with = str(request.headers.get("X-Requested-With") or "").strip().lower()
        accept = str(request.headers.get("Accept") or "").strip().lower()
        return requested_with == "fetch" or "application/json" in accept

    def api_v2_post(path: str, payload: dict[str, Any] | None = None) -> dict[str, Any]:
        if api_v2_client is not None:
            response = api_v2_client.post(path, json=payload)
            body = response.json() if response.content else {}
            if response.status_code >= 400:
                if isinstance(body, dict):
                    detail = str(body.get("detail") or body.get("message") or f"HTTP {response.status_code}")
                else:
                    detail = f"HTTP {response.status_code}"
                raise RuntimeError(detail)
            return body if isinstance(body, dict) else {}

        upstream_url = f"{api_v2_base_url}{path}"
        request_data = json.dumps(payload).encode("utf-8") if payload is not None else None
        request_to_api = urllib.request.Request(upstream_url, data=request_data, method="POST")
        request_to_api.add_header("Accept", "application/json")
        if request_data is not None:
            request_to_api.add_header("Content-Type", "application/json")
        try:
            with urllib.request.urlopen(request_to_api, timeout=10) as response:
                raw = response.read().decode("utf-8")
        except urllib.error.HTTPError as error:
            detail = str(error)
            try:
                error_body = error.read().decode("utf-8")
                parsed = json.loads(error_body)
                if isinstance(parsed, dict):
                    detail = str(parsed.get("detail") or parsed.get("message") or detail)
            except Exception:
                pass
            raise RuntimeError(detail) from error
        except Exception as error:
            raise RuntimeError(str(error)) from error

        parsed = json.loads(raw or "{}")
        if not isinstance(parsed, dict):
            raise RuntimeError("API v2 returned an invalid response payload")
        return parsed

    def _pop_bulk_action_result() -> dict[str, Any] | None:
        value = session.pop(_BULK_ACTION_RESULT_SESSION_KEY, None)
        return value if isinstance(value, dict) else None

    def enrollment_request_payload() -> dict[str, str]:
        return {
            "ip": str(request.form.get("ip") or "").strip(),
            "api_token": str(request.form.get("api_token") or "").strip(),
            "onvif_username": str(request.form.get("onvif_username") or "").strip(),
            "onvif_password": str(request.form.get("onvif_password") or ""),
        }

    def camera_detail_payload(camera_id: str) -> dict[str, Any]:
        camera = hub.get_camera_for_ui(camera_id)
        controls = hub.get_camera_supported_controls_for_ui(camera_id)
        camera.update(controls)
        if controls.get("native_controls_available"):
            camera["api_status"] = "online"
            if camera.get("api_last_error") and camera.get("api_last_error") != "Native API is not available on this camera build.":
                camera["api_last_error"] = ""
        return camera

    def camera_web_request(
        camera_id: str,
        relative_path: str,
        *,
        method: str = "GET",
        data: bytes | None = None,
        accept: str = "*/*",
    ) -> Any:
        camera = hub.get_camera_for_ui(camera_id)
        web_ui_url = str(camera.get("web_ui_url") or "").strip()
        if not web_ui_url:
            raise RuntimeError("Camera Web UI URL is not available for this camera")

        upstream_url = urllib.request.urljoin(web_ui_url, relative_path.lstrip("/"))
        request_to_camera = urllib.request.Request(upstream_url, data=data, method=method)
        request_to_camera.add_header("Accept", accept)

        if data is not None:
            request_to_camera.add_header("Content-Type", "application/json")

        api_key = str(camera.get("api_key") or "").strip()
        if api_key:
            request_to_camera.add_header("X-API-Key", api_key)

        open_kwargs: dict[str, Any] = {"timeout": 30}
        if upstream_url.startswith("https://"):
            open_kwargs["context"] = ssl._create_unverified_context()

        return urllib.request.urlopen(request_to_camera, **open_kwargs)

    def camera_agent_request(
        camera_id: str,
        relative_path: str,
        *,
        method: str = "GET",
        data: bytes | None = None,
        accept: str = "application/json",
    ) -> Any:
        resolved = camera_id.strip().lower()
        if hasattr(hub, "_resolve_camera_id"):
            try:
                resolved = hub._resolve_camera_id(camera_id) or resolved
            except Exception:
                resolved = camera_id.strip().lower()

        camera = None
        if hasattr(hub, "state_lock") and hasattr(hub, "cameras"):
            try:
                with hub.state_lock:
                    camera = hub.cameras.get(resolved)
            except Exception:
                camera = None

        if camera is not None and hasattr(hub, "_camera_api_base_url"):
            api_base_url = str(hub._camera_api_base_url(camera) or "").strip().rstrip("/")
            api_token = str(hub._camera_api_token(camera) or "").strip() if hasattr(hub, "_camera_api_token") else ""
        else:
            camera_dict = hub.get_camera_for_ui(camera_id)
            api_base_url = str(camera_dict.get("api_base_url") or "").strip().rstrip("/")
            api_token = str(camera_dict.get("api_token") or "").strip()

        if not api_base_url:
            raise RuntimeError("Camera native API URL is not available for this camera")

        normalized_path = "/" + str(relative_path or "").lstrip("/")
        upstream_url = f"{api_base_url}{normalized_path}"
        request_to_camera = urllib.request.Request(upstream_url, data=data, method=method)
        request_to_camera.add_header("Accept", accept)

        if data is not None:
            request_to_camera.add_header("Content-Type", "application/json")

        if api_token:
            request_to_camera.add_header("Authorization", f"Bearer {api_token}")

        open_kwargs: dict[str, Any] = {"timeout": 30}
        if upstream_url.startswith("https://"):
            open_kwargs["context"] = ssl._create_unverified_context()

        return urllib.request.urlopen(request_to_camera, **open_kwargs)

    def camera_agent_bridge_request(
        camera_id: str,
        agent_path: str,
        *,
        method: str = "GET",
        data: bytes | None = None,
        accept: str = "application/json",
    ) -> Any:
        normalized_path = "/" + str(agent_path or "").lstrip("/")
        encoded_path = urllib.parse.quote(normalized_path, safe="/")
        return camera_web_request(
            camera_id,
            f"/x/agent.cgi?agent_path={encoded_path}",
            method=method,
            data=data,
            accept=accept,
        )

    def camera_webrtc_request(
        camera_id: str,
        path: str,
        *,
        method: str = "GET",
        data: bytes | None = None,
        query: str = "",
        accept: str = "*/*",
        content_type: str | None = None,
    ) -> Any:
        webrtc_url = str(hub.get_camera_webrtc_url_for_ui(camera_id) or "").strip()
        if not webrtc_url:
            raise RuntimeError("Camera WebRTC URL is not available for this camera")
        parsed = urllib.parse.urlsplit(webrtc_url)
        if not parsed.scheme or not parsed.netloc:
            raise RuntimeError("Camera WebRTC URL is invalid for this camera")

        normalized_path = "/" + str(path or "").lstrip("/")
        upstream_url = urllib.parse.urlunsplit((parsed.scheme, parsed.netloc, normalized_path, str(query or ""), ""))
        request_to_camera = urllib.request.Request(upstream_url, data=data, method=method)
        request_to_camera.add_header("Accept", accept)

        if data is not None and content_type:
            request_to_camera.add_header("Content-Type", content_type)

        username, password = hub.get_camera_login_credentials_for_ui(camera_id)
        if username and password != "":
            token = base64.b64encode(f"{username}:{password}".encode("utf-8")).decode("ascii")
            request_to_camera.add_header("Authorization", f"Basic {token}")

        open_kwargs: dict[str, Any] = {"timeout": 30}
        if upstream_url.startswith("https://"):
            open_kwargs["context"] = ssl._create_unverified_context()

        return urllib.request.urlopen(request_to_camera, **open_kwargs)

    def open_camera_media_request(
        camera_id: str,
        media_url: str,
        *,
        timeout: int = 30,
        accept: str = "*/*",
    ) -> Any:
        camera = hub.get_camera_for_ui(camera_id)
        is_raptor = str(camera.get("api_streamer") or "").strip().lower() == "raptor"
        username, password = hub.get_camera_login_credentials_for_ui(camera_id)
        basic_token = ""
        if username and password != "":
            basic_token = base64.b64encode(f"{username}:{password}".encode("utf-8")).decode("ascii")

        request_to_camera = urllib.request.Request(media_url, method="GET")
        request_to_camera.add_header("Accept", accept)
        api_key = str(camera.get("api_key") or "").strip()
        if api_key:
            request_to_camera.add_header("X-API-Key", api_key)
        if is_raptor and basic_token:
            request_to_camera.add_header("Authorization", f"Basic {basic_token}")

        open_kwargs: dict[str, Any] = {"timeout": timeout}
        if media_url.startswith("https://"):
            open_kwargs["context"] = ssl._create_unverified_context()

        try:
            return urllib.request.urlopen(request_to_camera, **open_kwargs)
        except urllib.error.HTTPError as error:
            if error.code != 401:
                raise
            if is_raptor:
                if not basic_token:
                    raise RuntimeError(
                        "Raptor media endpoint requires authentication and camera credentials are not configured"
                    )
                raise
            if api_key:
                raise

        if not basic_token:
            raise RuntimeError("Camera media endpoint requires authentication and camera credentials are not configured")

        parsed_media = urllib.parse.urlsplit(media_url)
        media_origin = urllib.parse.urlunsplit((parsed_media.scheme, parsed_media.netloc, "/", "", ""))
        login_url = urllib.request.urljoin(media_origin, "/x/login.cgi")
        login_payload = json.dumps(
            {
                "username": username,
                "password": password,
            },
            separators=(",", ":"),
        ).encode("utf-8")

        handlers: list[Any] = [urllib.request.HTTPCookieProcessor(http.cookiejar.CookieJar())]
        if login_url.startswith("https://") or media_url.startswith("https://"):
            handlers.append(urllib.request.HTTPSHandler(context=ssl._create_unverified_context()))
        opener = urllib.request.build_opener(*handlers)

        login_request = urllib.request.Request(login_url, data=login_payload, method="POST")
        login_request.add_header("Content-Type", "application/json")
        login_request.add_header("Accept", "application/json")
        with opener.open(login_request, timeout=timeout):
            pass

        media_request = urllib.request.Request(media_url, method="GET")
        media_request.add_header("Accept", accept)
        return opener.open(media_request, timeout=timeout)

    def rewrite_webrtc_html(camera_id: str, html: str) -> str:
        prefix = f"/preview-webrtc/{camera_id}"
        rewritten = html
        rewritten = rewritten.replace("fetch('/whip", f"fetch('{prefix}/whip")
        rewritten = rewritten.replace('fetch("/whip', f'fetch("{prefix}/whip')
        return rewritten

    def rewrite_webrtc_location(camera_id: str, location: str) -> str:
        value = str(location or "").strip()
        if not value:
            return ""
        if value.startswith("/"):
            return f"/preview-webrtc/{camera_id}{value}"
        parsed = urllib.parse.urlsplit(value)
        if parsed.path.startswith("/"):
            query_suffix = f"?{parsed.query}" if parsed.query else ""
            return f"/preview-webrtc/{camera_id}{parsed.path}{query_suffix}"
        return value

    def save_camera_overrides(camera_id: str) -> None:
        override_fields = (
            "name",
            "ip",
            "snapshot_url",
            "api_key",
            "api_base_url",
            "api_token",
            "onvif_endpoint",
            "onvif_username",
            "onvif_password",
        )
        hub.update_camera_override(
            camera_id,
            {field: request.form.get(field, "") for field in override_fields if field in request.form},
        )

    def camera_page_redirect(camera_id: str, default_endpoint: str = "camera_detail") -> str:
        page = str(request.form.get("redirect_page") or request.args.get("redirect_page") or "").strip().lower()
        if page == "info":
            return url_for("camera_info", camera_id=camera_id)
        if page == "settings":
            return url_for("camera_settings", camera_id=camera_id)
        if page == "sensor-data":
            return url_for("camera_sensor_data", camera_id=camera_id)
        if page == "send2":
            return url_for("camera_send2", camera_id=camera_id)
        if page == "overrides":
            return url_for("camera_overrides", camera_id=camera_id)
        if page == "native-actions":
            return url_for("camera_native_actions", camera_id=camera_id)
        if page == "history":
            return url_for("camera_history", camera_id=camera_id)
        if page == "expert":
            return url_for("camera_expert_config", camera_id=camera_id)
        return url_for(default_endpoint, camera_id=camera_id)

    def expert_access_required(camera_id: str) -> Response:
        message = "Expert access is required for the Native API Config Patch page."
        redirect_url = url_for("camera_settings", camera_id=camera_id)
        if wants_json_response():
            response = jsonify({
                "ok": False,
                "message": message,
                "category": "error",
                "redirect_url": redirect_url,
            })
            response.status_code = 403
            return response
        flash(message, "error")
        return redirect(redirect_url)

    def advanced_access_required(camera_id: str) -> Response:
        message = "Advanced access is required for this camera maintenance page."
        redirect_url = url_for("camera_settings", camera_id=camera_id)
        if wants_json_response():
            response = jsonify({
                "ok": False,
                "message": message,
                "category": "error",
                "redirect_url": redirect_url,
            })
            response.status_code = 403
            return response
        flash(message, "error")
        return redirect(redirect_url)

    def supported_controls_delta_payload(form: Any) -> dict[str, Any]:
        payload: dict[str, Any] = {}

        for field in ("brightness", "contrast", "saturation", "sharpness"):
            form_key = f"image_{field}"
            raw_value = form.get(form_key)
            if raw_value is not None and str(raw_value).strip() != "":
                payload[f"native_image_{field}"] = str(raw_value).strip()

        anti_flicker_value = str(form.get("image_anti_flicker") or "").strip().lower()
        if anti_flicker_value:
            aliases = {
                "0": "off",
                "1": "50hz",
                "2": "60hz",
            }
            payload["native_image_anti_flicker"] = aliases.get(anti_flicker_value, anti_flicker_value)

        if str(form.get("image_hflip_present") or "").strip() == "1":
            payload["native_image_hflip"] = form.get("image_hflip") == "on"

        if str(form.get("image_vflip_present") or "").strip() == "1":
            payload["native_image_vflip"] = form.get("image_vflip") == "on"

        if str(form.get("motion_enabled_present") or "").strip() == "1":
            payload["native_motion_enabled"] = form.get("motion_enabled") == "on"

        if str(form.get("daynight_enabled_present") or "").strip() == "1":
            enabled = form.get("daynight_enabled") == "on"
            force_mode = str(form.get("daynight_force_mode") or "").strip()
            payload["native_daynight_enabled"] = enabled
            payload["native_daynight_force_mode"] = force_mode
            payload["native_daynight_requested_mode"] = force_mode or "auto"
            for field in ("total_gain_night_threshold", "total_gain_day_threshold"):
                raw_value = form.get(f"daynight_{field}")
                if raw_value is not None and str(raw_value).strip() != "":
                    payload[f"native_daynight_{field}"] = str(raw_value).strip()
            for field in ("color", "ircut", "ir850", "ir940", "white"):
                present = str(form.get(f"daynight_controls_{field}_present") or "").strip() == "1"
                if present:
                    payload[f"native_daynight_controls_{field}"] = form.get(f"daynight_controls_{field}") == "on"
            if str(form.get("daynight_schedule_enabled_present") or "").strip() == "1":
                payload["native_daynight_schedule_enabled"] = form.get("daynight_schedule_enabled") == "on"
            for field in ("start_at", "stop_at"):
                raw_value = form.get(f"daynight_schedule_{field}")
                if raw_value is not None:
                    payload[f"native_daynight_schedule_{field}"] = str(raw_value or "").strip()

        stream_field_pattern = re.compile(r"^(stream\d+)_(enabled|audio_enabled|width|height|fps|bitrate|format|mode|osd_enabled|osd_time_enabled|osd_usertext_enabled|osd_usertext_format)$")
        stream_present_pattern = re.compile(r"^(stream\d+)_(enabled|audio_enabled|osd_enabled|osd_time_enabled|osd_usertext_enabled)_present$")
        stream_updates: dict[str, dict[str, Any]] = {}
        for key in form.keys():
            form_key = str(key)
            match = stream_present_pattern.match(form_key)
            if match is not None:
                stream_name, field_name = match.groups()
                stream_update = stream_updates.setdefault(stream_name, {"name": stream_name})
                stream_update[field_name] = form.get(f"{stream_name}_{field_name}") == "on"
                continue

            match = stream_field_pattern.match(form_key)
            if match is None:
                continue

            stream_name, field_name = match.groups()
            if field_name in {"enabled", "audio_enabled", "osd_enabled", "osd_time_enabled", "osd_usertext_enabled"}:
                continue

            stream_update = stream_updates.setdefault(stream_name, {"name": stream_name})
            raw_value = form.get(form_key)
            if field_name in {"width", "height", "fps", "bitrate"}:
                if raw_value is None or str(raw_value).strip() == "":
                    continue
                stream_update[field_name] = str(raw_value).strip()
            elif field_name == "osd_usertext_format":
                stream_update[field_name] = str(raw_value or "")
            else:
                stream_update[field_name] = str(raw_value or "")

        if stream_updates:
            payload["native_stream_controls"] = list(stream_updates.values())

        privacy_enabled_present_pattern = re.compile(r"^(stream\d+)_osd_privacy_enabled_present$")
        privacy_text_pattern = re.compile(r"^(stream\d+)_osd_privacy_text$")
        privacy_color_pattern = re.compile(r"^(stream\d+)_osd_privacy_(fill|stroke)_color$")
        for key in form.keys():
            form_key = str(key)
            match = privacy_enabled_present_pattern.match(form_key)
            if match is not None:
                stream_name = match.group(1)
                stream_update = stream_updates.setdefault(stream_name, {"name": stream_name})
                stream_update["osd_privacy_enabled"] = form.get(f"{stream_name}_osd_privacy_enabled") == "on"
                continue

            match = privacy_text_pattern.match(form_key)
            if match is not None:
                stream_name = match.group(1)
                stream_update = stream_updates.setdefault(stream_name, {"name": stream_name})
                stream_update["osd_privacy_text"] = str(form.get(form_key) or "")
                continue

            match = privacy_color_pattern.match(form_key)
            if match is None:
                continue
            stream_name, color_kind = match.groups()
            stream_update = stream_updates.setdefault(stream_name, {"name": stream_name})
            color_value = _normalize_hex_color(form.get(form_key), f"{stream_name}.osd.privacy.{color_kind}_color")
            alpha_value = _color_alpha_value(
                form.get(f"{stream_name}_osd_privacy_{color_kind}_alpha"),
                f"{stream_name}.osd.privacy.{color_kind}_alpha",
            )
            if color_value is not None:
                stream_update[f"osd_privacy_{color_kind}_color_value"] = color_value
                stream_update[f"osd_privacy_{color_kind}_alpha"] = str(alpha_value)

        if stream_updates:
            payload["native_stream_controls"] = list(stream_updates.values())

        if form.get("send2_motion_sensitivity") is not None:
            payload["native_send2_motion_sensitivity"] = str(form.get("send2_motion_sensitivity") or "").strip()

        if form.get("send2_motion_cooldown") is not None:
            payload["native_send2_motion_cooldown"] = str(form.get("send2_motion_cooldown") or "").strip()

        send2_service_updates: dict[str, dict[str, Any]] = {}
        for key in form.keys():
            form_key = str(key)
            if form_key.startswith("motion_send2") and form_key.endswith("_present"):
                service_name = form_key[len("motion_send2"):-len("_present")]
                service_update = send2_service_updates.setdefault(service_name, {"name": service_name})
                service_update["motion_enabled"] = form.get(f"motion_send2{service_name}") == "on"
            elif form_key.startswith("send2") and form_key.endswith("_photo_present"):
                service_name = form_key[len("send2"):-len("_photo_present")]
                service_update = send2_service_updates.setdefault(service_name, {"name": service_name})
                service_update["photo_enabled"] = form.get(f"send2{service_name}_photo") == "on"
            elif form_key.startswith("send2") and form_key.endswith("_video_present"):
                service_name = form_key[len("send2"):-len("_video_present")]
                service_update = send2_service_updates.setdefault(service_name, {"name": service_name})
                service_update["video_enabled"] = form.get(f"send2{service_name}_video") == "on"

        if send2_service_updates:
            payload["native_send2_services"] = list(send2_service_updates.values())

        return payload

    def supported_controls_delta_from_payload(config_payload: dict[str, Any]) -> dict[str, Any]:
        payload: dict[str, Any] = {}

        image = config_payload.get("image") or {}
        if isinstance(image, dict):
            for field in ("brightness", "contrast", "saturation", "sharpness"):
                if field in image:
                    payload[f"native_image_{field}"] = str(image.get(field) or "")
            if "anti_flicker" in image:
                anti_flicker = str(image.get("anti_flicker") or "").strip().lower()
                aliases = {
                    "0": "off",
                    "1": "50hz",
                    "2": "60hz",
                }
                payload["native_image_anti_flicker"] = aliases.get(anti_flicker, anti_flicker)
            if "hflip" in image:
                payload["native_image_hflip"] = bool(image.get("hflip"))
            if "vflip" in image:
                payload["native_image_vflip"] = bool(image.get("vflip"))

        motion = config_payload.get("motion") or {}
        if isinstance(motion, dict) and "enabled" in motion:
            payload["native_motion_enabled"] = bool(motion.get("enabled"))

        daynight = config_payload.get("daynight") or {}
        if isinstance(daynight, dict):
            if "enabled" in daynight:
                payload["native_daynight_enabled"] = bool(daynight.get("enabled"))
            if "force_mode" in daynight:
                force_mode = str(daynight.get("force_mode") or "").strip()
                payload["native_daynight_force_mode"] = force_mode
                payload["native_daynight_requested_mode"] = force_mode or "auto"
            for field in ("total_gain_night_threshold", "total_gain_day_threshold"):
                if field in daynight and daynight.get(field) not in (None, ""):
                    payload[f"native_daynight_{field}"] = str(daynight.get(field))
            controls = daynight.get("controls") or {}
            if isinstance(controls, dict):
                for field in ("color", "ircut", "ir850", "ir940", "white"):
                    if field in controls:
                        payload[f"native_daynight_controls_{field}"] = bool(controls.get(field))
            schedule = daynight.get("schedule") or {}
            if isinstance(schedule, dict):
                if "enabled" in schedule:
                    payload["native_daynight_schedule_enabled"] = bool(schedule.get("enabled"))
                if "start_at" in schedule:
                    payload["native_daynight_schedule_start_at"] = str(schedule.get("start_at") or "")
                if "stop_at" in schedule:
                    payload["native_daynight_schedule_stop_at"] = str(schedule.get("stop_at") or "")

        privacy = config_payload.get("privacy") or {}
        if isinstance(privacy, dict) and "enabled" in privacy:
            payload["native_privacy_enabled"] = bool(privacy.get("enabled"))

        stream_updates: dict[str, dict[str, Any]] = {}
        for stream_name, stream_payload in config_payload.items():
            if not str(stream_name).startswith("stream") or not isinstance(stream_payload, dict):
                continue
            stream_update = stream_updates.setdefault(str(stream_name), {"name": str(stream_name)})
            osd_payload = stream_payload.get("osd") or {}
            if not isinstance(osd_payload, dict):
                continue
            privacy_payload = osd_payload.get("privacy") or {}
            if not isinstance(privacy_payload, dict):
                continue
            if "enabled" in privacy_payload:
                stream_update["osd_privacy_enabled"] = bool(privacy_payload.get("enabled"))
            if "text" in privacy_payload:
                stream_update["osd_privacy_text"] = str(privacy_payload.get("text") or "")
            if "fill_color" in privacy_payload:
                fill_color_value, fill_alpha = _split_hex_color_alpha(privacy_payload.get("fill_color"))
                stream_update["osd_privacy_fill_color_value"] = fill_color_value
                stream_update["osd_privacy_fill_alpha"] = fill_alpha
            if "stroke_color" in privacy_payload:
                stroke_color_value, stroke_alpha = _split_hex_color_alpha(privacy_payload.get("stroke_color"))
                stream_update["osd_privacy_stroke_color_value"] = stroke_color_value
                stream_update["osd_privacy_stroke_alpha"] = stroke_alpha

        if stream_updates:
            payload["native_stream_controls"] = list(stream_updates.values())

        return payload

    def merge_camera_payloads(*payloads: dict[str, Any]) -> dict[str, Any]:
        merged: dict[str, Any] = {}
        for payload in payloads:
            if payload:
                merged.update(payload)
        return merged

    def camera_fields_payload(camera_id: str, *field_names: str) -> dict[str, Any]:
        camera = hub.get_camera_for_ui(camera_id)
        return {
            field_name: camera[field_name]
            for field_name in field_names
            if field_name in camera
        }

    def camera_detail_hydration_payload(camera_id: str) -> dict[str, Any]:
        camera_payload = camera_fields_payload(
            camera_id,
            "status",
            "preview_version",
            "api_status",
            "api_device_name",
            "api_device_model",
            "api_streamer",
            "api_version",
            "api_last_ok_at",
            "api_last_error",
            "onvif_manufacturer",
            "onvif_model",
            "onvif_firmware_version",
            "onvif_serial_number",
            "onvif_hardware_id",
            "onvif_last_ok_at",
            "onvif_last_error",
            "last_registration_at",
            "last_probe_at",
            "last_snapshot_ok_at",
            "last_probe_error",
            "native_action_history",
        )
        return merge_camera_payloads(
            camera_payload,
            hub.get_camera_supported_controls_for_ui(camera_id),
        )

    def action_history_delta_payload(camera_id: str) -> dict[str, Any]:
        return camera_fields_payload(camera_id, "native_action_history")

    def latest_action_history_delta_payload(camera_id: str) -> dict[str, Any]:
        history = camera_fields_payload(camera_id, "native_action_history").get("native_action_history") or []
        if not history:
            return {}
        return {"native_action_history_latest": history[0]}

    def daynight_delta_payload(mode: str) -> dict[str, Any]:
        normalized_mode = str(mode or "").strip().lower() or "auto"
        return {
            "native_daynight_requested_mode": normalized_mode,
        }

    def privacy_delta_payload(enabled: bool) -> dict[str, Any]:
        return {
            "native_privacy_enabled": enabled,
        }

    def merge_flip_state(camera_id: str, native_payload: dict[str, Any]) -> dict[str, Any]:
        image_payload = native_payload.get("image")
        if not isinstance(image_payload, dict):
            return native_payload

        has_hflip = "hflip" in image_payload
        has_vflip = "vflip" in image_payload
        if has_hflip == has_vflip:
            return native_payload

        current = hub.get_camera_supported_controls_for_ui(camera_id)
        merged_image_payload = dict(image_payload)
        if not has_hflip:
            merged_image_payload["hflip"] = bool(current.get("native_image_hflip"))
        if not has_vflip:
            merged_image_payload["vflip"] = bool(current.get("native_image_vflip"))

        merged_payload = dict(native_payload)
        merged_payload["image"] = merged_image_payload
        return merged_payload

    def action_response(
        message: str,
        category: str,
        redirect_url: str,
        *,
        camera_id: str | None = None,
        camera_payload: dict[str, Any] | None = None,
        status_code: int = 200,
        reload: bool = False,
    ) -> Response:
        if wants_json_response():
            payload: dict[str, Any] = {
                "ok": category == "success",
                "message": message,
                "category": category,
                "redirect_url": redirect_url,
            }
            if reload:
                payload["reload"] = True
            if camera_payload is not None:
                payload["camera"] = camera_payload
            elif camera_id is not None:
                try:
                    payload["camera"] = camera_detail_payload(camera_id)
                except Exception as error:
                    payload["camera_error"] = str(error)
            response = jsonify(payload)
            response.status_code = status_code
            return response

        flash(message, category)
        return redirect(redirect_url)

    @app.before_request
    def require_basic_auth() -> Response | None:
        if not auth_enabled:
            return None
        if request.endpoint in {"login", "logout", "static"}:
            return None
        if session.get("ui_authenticated"):
            return None

        auth = request.authorization
        if auth is not None and credentials_are_valid(auth.username or "", auth.password or ""):
            session["ui_authenticated"] = True
            return None

        login_url = url_for("login", next=_sanitize_next_url(request.full_path or request.path))
        return redirect(login_url)

    @app.after_request
    def disable_cache(response: Response) -> Response:
        content_type = (response.mimetype or "").lower()
        if content_type in {"text/html", "text/css", "application/javascript", "text/javascript"}:
            response.headers["Cache-Control"] = "no-store, no-cache, must-revalidate, max-age=0"
            response.headers["Pragma"] = "no-cache"
            response.headers["Expires"] = "0"
        return response

    @app.get("/")
    def dashboard() -> str:
        return render_template(
            "dashboard.html",
            cameras=hub.list_cameras_for_ui(),
            bulk_action_result=_pop_bulk_action_result(),
        )

    @app.get("/status")
    def status_page() -> str:
        return render_template(
            "status.html",
            status=hub.snapshot_status(),
        )

    @app.get("/events")
    def events_page() -> str:
        return render_template(
            "events.html",
            recent_events=hub.list_recent_events_for_ui(),
        )

    @app.get("/enroll")
    def enroll_page() -> str:
        return render_template("enroll.html")

    @app.get("/api/export/tinycam")
    def api_export_tinycam() -> Response:
        """Export cameras as TinyCam Monitor XML"""
        cameras = hub.list_cameras_for_ui()
        xml_content = _generate_tinycam_xml(cameras)
        return Response(
            xml_content,
            mimetype="application/xml",
            headers={"Content-Disposition": 'attachment; filename="cameras.xml"'}
        )

    @app.get("/api/events")
    @app.get("/events/feed")
    def api_events() -> Response:
        limit = _optional_int_value(request.args.get("limit"), "events.limit")
        return jsonify(
            {
                "ok": True,
                "events": hub.list_recent_events_for_ui(limit=limit if limit is not None else 40),
            }
        )

    @app.get("/events/stream")
    def event_stream() -> Response:
        last_sequence = _optional_int_value(request.args.get("since"), "events.since") or 0

        def stream() -> Any:
            current_sequence = last_sequence
            yield ": connected\n\n"
            while True:
                next_sequence, events = hub.live_events_since(current_sequence)
                if events:
                    for entry in events:
                        payload = json.dumps(entry, sort_keys=True)
                        yield f"event: camera-event\ndata: {payload}\n\n"
                    current_sequence = max(int(events[-1].get("sequence") or 0), next_sequence)
                    continue
                current_sequence = max(current_sequence, next_sequence)
                yield f": ping {int(time.time())}\n\n"
                time.sleep(2)

        response = Response(stream(), mimetype="text/event-stream")
        response.headers["Cache-Control"] = "no-store"
        response.headers["X-Accel-Buffering"] = "no"
        return response

    @app.route("/login", methods=["GET", "POST"])
    def login() -> str | Response:
        if not auth_enabled:
            return redirect(url_for("dashboard"))
        if session.get("ui_authenticated"):
            return redirect(_post_login_redirect_target(request.args.get("next")))

        next_target = _sanitize_next_url(request.values.get("next"))
        if request.method == "POST":
            username = str(request.form.get("username") or "")
            password = str(request.form.get("password") or "")
            if credentials_are_valid(username, password):
                session["ui_authenticated"] = True
                flash("Signed in.", "success")
                return redirect(_post_login_redirect_target(next_target))
            flash("Invalid username or password.", "error")

        return render_template("login.html", next_target=next_target)

    @app.post("/logout")
    def logout() -> Response:
        session.pop("ui_authenticated", None)
        flash("Signed out.", "success")
        return redirect(url_for("login"))

    @app.route("/camera/<camera_id>", methods=["GET", "POST"])
    def camera_detail(camera_id: str) -> str | Response:
        if request.method == "POST":
            try:
                save_camera_overrides(camera_id)
                flash(f"Saved camera overrides for {camera_id}.", "success")
                return redirect(url_for("camera_detail", camera_id=camera_id))
            except Exception as error:
                flash(f"Failed to save camera overrides: {error}", "error")

        return render_template("camera_detail.html", camera=camera_detail_payload(camera_id))

    @app.get("/camera/<camera_id>/info")
    def camera_info(camera_id: str) -> str:
        return render_template("camera_info.html", camera=camera_detail_payload(camera_id))

    @app.route("/camera/<camera_id>/overrides", methods=["GET", "POST"])
    def camera_overrides(camera_id: str) -> str | Response:
        if not current_user_has_advanced_access():
            return advanced_access_required(camera_id)
        if request.method == "POST":
            try:
                save_camera_overrides(camera_id)
                flash(f"Saved camera overrides for {camera_id}.", "success")
                return redirect(url_for("camera_overrides", camera_id=camera_id))
            except Exception as error:
                flash(f"Failed to save camera overrides: {error}", "error")

        return render_template("camera_overrides.html", camera=camera_detail_payload(camera_id))

    @app.get("/camera/<camera_id>/native-actions")
    def camera_native_actions(camera_id: str) -> str:
        if not current_user_has_advanced_access():
            return advanced_access_required(camera_id)
        return render_template("camera_native_actions.html", camera=camera_detail_payload(camera_id))

    @app.get("/camera/<camera_id>/settings")
    def camera_settings(camera_id: str) -> str:
        return render_template("camera_settings.html", camera=camera_detail_payload(camera_id))

    @app.get("/camera/<camera_id>/sensor-data")
    def camera_sensor_data(camera_id: str) -> str:
        return render_template("camera_sensor_data.html", camera=camera_detail_payload(camera_id))

    @app.get("/camera/<camera_id>/send2")
    def camera_send2(camera_id: str) -> str:
        return render_template("camera_send2.html", camera=camera_detail_payload(camera_id))

    @app.get("/camera/<camera_id>/expert-config")
    def camera_expert_config(camera_id: str) -> str | Response:
        if not current_user_has_expert_access():
            return expert_access_required(camera_id)
        return render_template("camera_expert_config.html", camera=camera_detail_payload(camera_id))

    @app.post("/camera/<camera_id>/hydrate")
    def hydrate_camera_detail(camera_id: str) -> Response:
        refreshes = {"api": "skipped", "onvif": "skipped"}
        try:
            refreshes = hub.queue_camera_detail_hydration_refresh(camera_id)
        except Exception as error:
            refreshes = {"api": f"error: {error}", "onvif": f"error: {error}"}

        payload = {
            "ok": True,
            "camera": camera_detail_payload(camera_id),
            "refreshes": refreshes,
        }
        return jsonify(payload)

    @app.get("/camera/<camera_id>/payload")
    def camera_detail_state(camera_id: str) -> Response:
        return jsonify({
            "ok": True,
            "camera": camera_detail_payload(camera_id),
        })

    @app.get("/camera/<camera_id>/hydrate-payload")
    def camera_detail_hydration_state(camera_id: str) -> Response:
        return jsonify({
            "ok": True,
            "camera": camera_detail_hydration_payload(camera_id),
        })

    @app.get("/camera/<camera_id>/history")
    def camera_history(camera_id: str) -> str:
        limit = _optional_int_value(request.args.get("limit"), "history.limit")
        camera = merge_camera_payloads(
            hub.get_camera_history_for_ui(
                camera_id,
                limit=min(limit if limit is not None else 100, 500),
                kind_filter=str(request.args.get("kind") or "all").strip().lower(),
                sample_type_filter=str(request.args.get("sample_type") or "all").strip(),
            ),
            camera_fields_payload(
                camera_id,
                "setup_status",
                "hub_connected",
                "present_on_mqtt_broker",
                "has_agent",
                "registered_on_hub",
                "is_paired",
                "api_status",
                "mqtt_command_status",
                "mqtt_command_capable",
                "mqtt_command_last_error",
                "web_ui_url",
                "default_onvif_username",
                "default_onvif_password",
                "override_onvif_username",
                "override_onvif_password",
            ),
        )
        return render_template("camera_history.html", camera=camera)

    @app.route("/config", methods=["GET", "POST"])
    def config_editor() -> str | Response:
        if request.method == "POST":
            try:
                existing = hub.export_config()
                new_config = _config_from_form(request.form, existing=existing)
                hub.save_config(new_config)
                if request.form.get("action") == "save-reload":
                    hub.reload_config()
                    flash("Configuration saved and reloaded.", "success")
                else:
                    # Keep MQTT/Telegram sockets up, but sync the document so the
                    # form does not snap back to pre-save in-memory values.
                    hub.set_config_document(new_config)
                    flash("Configuration saved.", "success")
                return redirect(url_for("config_editor"))
            except Exception as error:
                flash(f"Failed to save configuration: {error}", "error")
                config = _config_from_form(request.form, allow_partial=True)
                return render_template("config.html", config=config, cameras_yaml=_dump_cameras(config))

        config = hub.export_config()
        return render_template("config.html", config=config, cameras_yaml=_dump_cameras(config))

    @app.post("/reload")
    def reload_config() -> Response:
        try:
            hub.reload_config()
            flash("Configuration reloaded.", "success")
        except Exception as error:
            flash(f"Reload failed: {error}", "error")
        return redirect(url_for("dashboard"))

    @app.post("/rescan")
    def rescan_all() -> Response:
        try:
            total, published = hub.rescan_cameras()
            if total == 0:
                flash("No known cameras to rescan.", "error")
            elif published == total:
                flash(f"Requested metadata refresh from {published} camera(s).", "success")
            else:
                flash(f"Requested metadata refresh from {published} of {total} camera(s).", "error")
        except Exception as error:
            flash(f"Rescan failed: {error}", "error")
        return redirect(url_for("dashboard"))

    @app.post("/bulk-action")
    def bulk_action() -> Response:
        selected_ids = request.form.getlist("camera_ids")
        action = str(request.form.get("bulk_action") or "").strip()
        if api_v2_enabled:
            try:
                payload = api_v2_post(
                    "/api/v2/bulk-action",
                    {
                        "camera_ids": selected_ids,
                        "action": action,
                    },
                )
                result = payload.get("result") if isinstance(payload.get("result"), dict) else None
                message = str(payload.get("message") or "")
                ok = bool(payload.get("ok"))
                if wants_json_response():
                    return jsonify(
                        {
                            "ok": ok,
                            "message": message,
                            "result": result or {},
                        }
                    )
                if result is not None:
                    session[_BULK_ACTION_RESULT_SESSION_KEY] = result
                flash(message, "success" if ok else "error")
                return redirect(url_for("dashboard"))
            except Exception as error:
                LOG.warning("API v2 bulk action failed; falling back to Flask handler: %s", error)
        try:
            result = hub.perform_bulk_action(selected_ids, action)
            message = f"{result['action'].replace('-', ' ').title()} finished for {result['success_count']} of {result['total']} camera(s)."
            if wants_json_response():
                return jsonify({"ok": result["error_count"] == 0, "message": message, "result": result})
            session[_BULK_ACTION_RESULT_SESSION_KEY] = result
            flash(message, "success" if result["error_count"] == 0 else "error")
        except Exception as error:
            if wants_json_response():
                response = jsonify({"ok": False, "message": str(error)})
                response.status_code = 400
                return response
            session.pop(_BULK_ACTION_RESULT_SESSION_KEY, None)
            flash(f"Bulk action failed: {error}", "error")
        return redirect(url_for("dashboard"))

    @app.post("/enroll")
    def enroll_camera() -> Response:
        enrollment = enrollment_request_payload()
        if api_v2_enabled:
            try:
                payload = api_v2_post("/api/v2/enroll", enrollment)
                details = payload.get("details") if isinstance(payload.get("details"), dict) else {}
                camera_id = str(payload.get("camera_id") or details.get("camera_id") or "").strip()
                message = str(payload.get("message") or f"Connected {camera_id} to the hub.")
                if wants_json_response():
                    return jsonify({"ok": True, "message": message, "result": details})
                flash(message, "success")
                if camera_id:
                    return redirect(url_for("camera_detail", camera_id=camera_id))
                return redirect(url_for("dashboard"))
            except Exception as error:
                LOG.warning("API v2 enroll failed; falling back to Flask handler: %s", error)
        try:
            result = hub.connect_camera(enrollment)
            message = f"Connected {result['camera_id']} to the hub."
            if wants_json_response():
                return jsonify({"ok": True, "message": message, "result": result})
            flash(message, "success")
            return redirect(url_for("camera_detail", camera_id=result["camera_id"]))
        except Exception as error:
            if wants_json_response():
                response = jsonify({"ok": False, "message": str(error)})
                response.status_code = 400
                return response
            flash(f"Enrollment failed: {error}", "error")
            return redirect(url_for("dashboard"))

    @app.post("/connect/<camera_id>")
    def connect_camera(camera_id: str) -> Response:
        redirect_url = camera_page_redirect(camera_id)
        if api_v2_enabled:
            try:
                payload = api_v2_post(
                    f"/api/v2/cameras/{camera_id}/connect",
                    {
                        "api_token": str(request.form.get("api_token") or "").strip(),
                        "onvif_username": str(request.form.get("onvif_username") or "").strip(),
                        "onvif_password": str(request.form.get("onvif_password") or ""),
                    },
                )
                result = str(payload.get("result") or "success").strip().lower()
                category = "warning" if result == "warning" else "success"
                message = str(payload.get("message") or f"Connected {camera_id} to the hub.")
                return action_response(
                    message,
                    category,
                    redirect_url,
                    camera_id=camera_id,
                )
            except Exception as error:
                LOG.warning("API v2 connect failed for %s; falling back to Flask handler: %s", camera_id, error)
        try:
            camera = hub.get_camera_for_ui(camera_id)
            enrollment = {
                "camera_id": camera_id,
                "ip": str(camera.get("ip") or "").strip(),
                "api_token": str(request.form.get("api_token") or "").strip(),
                "onvif_username": str(request.form.get("onvif_username") or "").strip(),
                "onvif_password": str(request.form.get("onvif_password") or ""),
            }
            result = hub.connect_camera(enrollment)
            category = "warning" if str(result.get("status") or "").strip().lower() == "warning" else "success"
            detail = str(result.get("status_detail") or "").strip()
            message = detail or f"Connected {camera_id} to the hub."
            return action_response(
                message,
                category,
                redirect_url,
                camera_id=camera_id,
            )
        except Exception as error:
            return action_response(
                f"Connect failed for {camera_id}: {error}",
                "error",
                redirect_url,
                camera_id=camera_id,
                status_code=500,
            )

    @app.post("/enroll/probe")
    def probe_enrollment() -> Response:
        enrollment = enrollment_request_payload()
        try:
            result = hub.probe_camera_enrollment(enrollment)
            return jsonify({
                "ok": True,
                "message": "Enrollment probe finished.",
                "result": result,
            })
        except Exception as error:
            response = jsonify({
                "ok": False,
                "message": str(error),
            })
            response.status_code = 400
            return response

    @app.post("/enroll/pairing-bundle")
    def pairing_bundle() -> Response:
        enrollment = enrollment_request_payload()
        try:
            result = hub.generate_pairing_bundle(enrollment)
            return jsonify({
                "ok": True,
                "message": "Pairing bundle prepared.",
                "result": result,
            })
        except Exception as error:
            response = jsonify({
                "ok": False,
                "message": str(error),
            })
            response.status_code = 400
            return response

    @app.post("/enroll/pairing-install")
    def pairing_install() -> Response:
        enrollment = enrollment_request_payload()
        try:
            result = hub.install_pairing_bundle_via_mqtt(enrollment)
            message = "Pairing installed over MQTT."
            if result.get("status") == "warning":
                message = "Pairing install was published over MQTT, but camera confirmation timed out."
            return jsonify({
                "ok": True,
                "message": message,
                "result": result,
            })
        except Exception as error:
            response = jsonify({
                "ok": False,
                "message": str(error),
            })
            response.status_code = 400
            return response

    @app.post("/pair/<camera_id>")
    def pair_camera(camera_id: str) -> Response:
        redirect_url = camera_page_redirect(camera_id)
        if api_v2_enabled:
            try:
                payload = api_v2_post(f"/api/v2/cameras/{camera_id}/pair")
                result = str(payload.get("result") or "success").strip().lower()
                category = "warning" if result == "warning" else "success"
                message = str(payload.get("message") or f"Pairing installed for {camera_id}.")
                return action_response(
                    message,
                    category,
                    redirect_url,
                    camera_id=camera_id,
                    reload=(result == "success"),
                )
            except Exception as error:
                LOG.warning("API v2 pair failed for %s; falling back to Flask handler: %s", camera_id, error)
        try:
            camera = hub.get_camera_for_ui(camera_id)
            enrollment = {
                "camera_id": camera_id,
                "ip": str(camera.get("ip") or "").strip(),
            }
            result = hub.install_pairing_bundle_via_mqtt(enrollment)
            outcome = pairing_outcome(camera_id, result)
            return action_response(
                str(outcome["message"]),
                str(outcome["category"]),
                redirect_url,
                camera_id=camera_id,
                reload=bool(outcome["reload"]),
            )
        except Exception as error:
            return action_response(
                f"Pairing failed for {camera_id}: {error}",
                "error",
                redirect_url,
                camera_id=camera_id,
                status_code=500,
            )

    @app.post("/rescan/<camera_id>")
    def rescan_one(camera_id: str) -> Response:
        if api_v2_enabled:
            try:
                payload = api_v2_post(f"/api/v2/cameras/{camera_id}/rescan")
                ok = bool(payload.get("ok"))
                fallback_message = "Metadata refresh requested." if ok else "Metadata refresh request failed."
                message = str(payload.get("message") or fallback_message)
                return action_response(
                    message,
                    "success" if ok else "error",
                    url_for("camera_detail", camera_id=camera_id),
                    status_code=200 if ok else 400,
                )
            except Exception as error:
                LOG.warning("API v2 rescan failed for %s; falling back to Flask handler: %s", camera_id, error)
        try:
            total, published = hub.rescan_cameras(camera_id)
            if published == total:
                return action_response(
                    "Metadata refresh requested.",
                    "success",
                    url_for("camera_detail", camera_id=camera_id),
                )
            else:
                return action_response(
                    "Metadata refresh request failed.",
                    "error",
                    url_for("camera_detail", camera_id=camera_id),
                    status_code=400,
                )
        except Exception as error:
            return action_response(
                f"Metadata refresh failed: {error}",
                "error",
                url_for("camera_detail", camera_id=camera_id),
                status_code=500,
            )

    @app.post("/refresh-snapshot/<camera_id>")
    def refresh_snapshot(camera_id: str) -> Response:
        if api_v2_enabled:
            try:
                payload = api_v2_post(f"/api/v2/cameras/{camera_id}/refresh/snapshot")
                return action_response(
                    str(payload.get("message") or "Snapshot refresh queued."),
                    "success",
                    url_for("camera_detail", camera_id=camera_id),
                )
            except Exception as error:
                LOG.warning("API v2 snapshot refresh failed for %s; falling back to Flask handler: %s", camera_id, error)
        try:
            result = hub.queue_snapshot_refresh(camera_id)
            if result == "scheduled":
                return action_response(
                    "Snapshot refresh queued.",
                    "success",
                    url_for("camera_detail", camera_id=camera_id),
                )
            else:
                return action_response(
                    "Snapshot refresh is already running.",
                    "success",
                    url_for("camera_detail", camera_id=camera_id),
                )
        except Exception as error:
            return action_response(
                f"Snapshot refresh failed: {error}",
                "error",
                url_for("camera_detail", camera_id=camera_id),
                status_code=500,
            )

    @app.post("/refresh-onvif/<camera_id>")
    def refresh_onvif(camera_id: str) -> Response:
        if api_v2_enabled:
            try:
                payload = api_v2_post(f"/api/v2/cameras/{camera_id}/refresh/onvif")
                return action_response(
                    str(payload.get("message") or "ONVIF refresh queued."),
                    "success",
                    url_for("camera_detail", camera_id=camera_id),
                )
            except Exception as error:
                LOG.warning("API v2 ONVIF refresh failed for %s; falling back to Flask handler: %s", camera_id, error)
        try:
            result = hub.queue_camera_onvif_refresh(camera_id)
            if result == "scheduled":
                return action_response(
                    "ONVIF refresh queued.",
                    "success",
                    url_for("camera_detail", camera_id=camera_id),
                )
            else:
                return action_response(
                    "ONVIF refresh is already running.",
                    "success",
                    url_for("camera_detail", camera_id=camera_id),
                )
        except Exception as error:
            return action_response(
                f"ONVIF refresh failed: {error}",
                "error",
                url_for("camera_detail", camera_id=camera_id),
                status_code=500,
            )

    @app.post("/refresh-api/<camera_id>")
    def refresh_api(camera_id: str) -> Response:
        if api_v2_enabled:
            try:
                payload = api_v2_post(f"/api/v2/cameras/{camera_id}/refresh/api")
                return action_response(
                    str(payload.get("message") or "Native API refresh queued."),
                    "success",
                    url_for("camera_detail", camera_id=camera_id),
                )
            except Exception as error:
                LOG.warning("API v2 API refresh failed for %s; falling back to Flask handler: %s", camera_id, error)
        try:
            result = hub.queue_camera_api_refresh(camera_id)
            if result == "scheduled":
                return action_response(
                    "Native API refresh queued.",
                    "success",
                    url_for("camera_detail", camera_id=camera_id),
                )
            else:
                return action_response(
                    "Native API refresh is already running.",
                    "success",
                    url_for("camera_detail", camera_id=camera_id),
                )
        except Exception as error:
            return action_response(
                f"Native API refresh failed: {error}",
                "error",
                url_for("camera_detail", camera_id=camera_id),
                status_code=500,
            )

    @app.post("/service-action/<camera_id>/<service_name>/<operation>")
    def service_action(camera_id: str, service_name: str, operation: str) -> Response:
        try:
            result = hub.control_camera_service(camera_id, service_name, operation, refresh_after=False)
            return action_response(
                f"{service_name.replace('_', ' ').title()} {operation} requested.",
                "success",
                url_for("camera_detail", camera_id=camera_id),
                camera_payload=latest_action_history_delta_payload(camera_id),
            )
        except Exception as error:
            return action_response(
                f"{service_name.replace('_', ' ').title()} {operation} failed: {error}",
                "error",
                url_for("camera_detail", camera_id=camera_id),
                camera_payload={},
                status_code=500,
            )

    @app.post("/restart-streaming/<camera_id>")
    def restart_streaming(camera_id: str) -> Response:
        return service_action(camera_id, "streaming", "restart")

    @app.post("/start-streaming/<camera_id>")
    def start_streaming(camera_id: str) -> Response:
        return service_action(camera_id, "streaming", "start")

    @app.post("/stop-streaming/<camera_id>")
    def stop_streaming(camera_id: str) -> Response:
        return service_action(camera_id, "streaming", "stop")

    @app.post("/restart-streamer/<camera_id>")
    def restart_streamer(camera_id: str) -> Response:
        try:
            result = hub.restart_camera_streaming_service(camera_id)
            return action_response(
                "Streaming restart requested.",
                "success",
                url_for("camera_detail", camera_id=camera_id),
                camera_id=camera_id,
            )
        except Exception as error:
            return action_response(
                f"Streaming restart failed: {error}",
                "error",
                url_for("camera_detail", camera_id=camera_id),
                camera_id=camera_id,
                status_code=500,
            )

    @app.post("/patch-config/<camera_id>")
    def patch_config(camera_id: str) -> Response:
        if not current_user_has_expert_access():
            return expert_access_required(camera_id)
        raw_payload = str(request.form.get("config_patch") or "").strip()
        redirect_url = camera_page_redirect(camera_id)
        if not raw_payload:
            return action_response(
                "Native config patch payload is empty.",
                "error",
                redirect_url,
                camera_id=camera_id,
                status_code=400,
            )

        try:
            payload = json.loads(raw_payload)
        except json.JSONDecodeError as error:
            return action_response(
                f"Invalid native config patch JSON: {error}",
                "error",
                redirect_url,
                camera_id=camera_id,
                status_code=400,
            )

        if not isinstance(payload, dict):
            return action_response(
                "Native config patch must be a JSON object.",
                "error",
                redirect_url,
                camera_id=camera_id,
                status_code=400,
            )

        try:
            result = hub.patch_camera_config(camera_id, payload, refresh_after=False)
            return action_response(
                "Config patch applied.",
                "success",
                redirect_url,
                camera_payload=merge_camera_payloads(
                    supported_controls_delta_from_payload(payload),
                    latest_action_history_delta_payload(camera_id),
                ),
            )
        except Exception as error:
            return action_response(
                f"Config patch failed: {error}",
                "error",
                redirect_url,
                camera_payload={},
                status_code=500,
            )

    @app.post("/apply-supported-config/<camera_id>")
    def apply_supported_config(camera_id: str) -> Response:
        redirect_url = camera_page_redirect(camera_id)
        try:
            native_payload = merge_flip_state(camera_id, _supported_config_patch_from_form(request.form))
            send2_payload = _send2_motion_patch_from_form(request.form)
            results: list[str] = []
            if native_payload:
                result = hub.patch_camera_config(camera_id, native_payload, refresh_after=False)
                results.append(f"native config: {result.get('status', 'ok')}")
            if send2_payload:
                result = hub.update_camera_send2_config(camera_id, send2_payload)
                results.append(f"send2: {result.get('result', result.get('status', 'ok'))}")
            if not results:
                raise ValueError("No supported settings were provided")
            return action_response(
                f"Settings applied: {'; '.join(results)}",
                "success",
                redirect_url,
                camera_payload=merge_camera_payloads(
                    supported_controls_delta_payload(request.form),
                    latest_action_history_delta_payload(camera_id),
                ),
            )
        except Exception as error:
            return action_response(
                f"Settings update failed: {error}",
                "error",
                redirect_url,
                camera_payload={},
                status_code=500,
            )

    @app.post("/send2-test/<camera_id>/<service_name>")
    def send2_test(camera_id: str, service_name: str) -> Response:
        redirect_url = camera_page_redirect(camera_id, default_endpoint="camera_send2")
        verbose_value = str(request.form.get("verbose") or request.args.get("verbose") or "1").strip().lower()
        verbose = verbose_value not in {"0", "false", "no", "off"}
        send_type = str(request.form.get("type") or request.args.get("type") or "").strip().lower()
        try:
            result = hub.test_camera_send2_service(camera_id, service_name, verbose=verbose, send_type=send_type)
            label = service_name
            if send_type:
                label = f"{service_name} {send_type}"
            if wants_json_response():
                payload: dict[str, Any] = {
                    "ok": True,
                    "message": f"Send2 test finished for {label}.",
                    "category": "success",
                    "redirect_url": redirect_url,
                    "send2_test": result,
                }
                return jsonify(payload)
            flash(f"Send2 test finished for {label}.", "success")
            return redirect(redirect_url)
        except Exception as error:
            label = service_name
            if send_type:
                label = f"{service_name} {send_type}"
            if wants_json_response():
                payload = {
                    "ok": False,
                    "message": f"Send2 test failed for {label}: {error}",
                    "category": "error",
                    "redirect_url": redirect_url,
                }
                response = jsonify(payload)
                response.status_code = 500
                return response
            flash(f"Send2 test failed for {label}: {error}", "error")
            return redirect(redirect_url)

    @app.post("/privacy/<camera_id>")
    def set_privacy(camera_id: str) -> Response:
        enabled_value = str(request.form.get("privacy_enabled") or "").strip().lower()
        enabled = enabled_value in {"1", "true", "yes", "on", "enabled"}
        channel = str(request.form.get("privacy_channel") or "all").strip() or "all"
        if api_v2_enabled:
            try:
                payload = api_v2_post(
                    f"/api/v2/cameras/{camera_id}/privacy",
                    {"enabled": enabled, "channel": channel},
                )
                return action_response(
                    str(payload.get("message") or ("Privacy enabled." if enabled else "Privacy disabled.")),
                    "success",
                    url_for("camera_detail", camera_id=camera_id),
                    camera_payload=privacy_delta_payload(enabled),
                )
            except Exception as error:
                LOG.warning("API v2 privacy update failed for %s; falling back to Flask handler: %s", camera_id, error)
        try:
            result = hub.set_camera_privacy(camera_id, enabled=enabled, channel=channel, refresh_after=False)
            state = "enabled" if enabled else "disabled"
            return action_response(
                f"Privacy {state}.",
                "success",
                url_for("camera_detail", camera_id=camera_id),
                camera_payload=privacy_delta_payload(enabled),
            )
        except Exception as error:
            return action_response(
                f"Privacy update failed: {error}",
                "error",
                url_for("camera_detail", camera_id=camera_id),
                camera_payload={},
                status_code=500,
            )

    @app.post("/daynight/<camera_id>")
    def set_daynight(camera_id: str) -> Response:
        mode = str(request.form.get("daynight_mode") or "").strip().lower() or "auto"
        if api_v2_enabled:
            try:
                payload = api_v2_post(
                    f"/api/v2/cameras/{camera_id}/daynight",
                    {"mode": mode},
                )
                return action_response(
                    str(payload.get("message") or f"Day/night set to {mode}."),
                    "success",
                    url_for("camera_detail", camera_id=camera_id),
                    camera_payload=daynight_delta_payload(mode),
                )
            except Exception as error:
                LOG.warning("API v2 day/night update failed for %s; falling back to Flask handler: %s", camera_id, error)
        try:
            result = hub.set_camera_daynight_mode(camera_id, mode=mode, refresh_after=False)
            return action_response(
                f"Day/night set to {mode}.",
                "success",
                url_for("camera_detail", camera_id=camera_id),
                camera_payload=daynight_delta_payload(mode),
            )
        except Exception as error:
            return action_response(
                f"Day/night update failed: {error}",
                "error",
                url_for("camera_detail", camera_id=camera_id),
                camera_payload={},
                status_code=500,
            )

    @app.post("/record/<camera_id>")
    def record_clip(camera_id: str) -> Response:
        try:
            duration_seconds = _optional_int_value(request.form.get("record_duration_seconds"), "record.duration_seconds")
            stream_id = _optional_int_value(request.form.get("record_stream_id"), "record.stream_id")
            if api_v2_enabled:
                try:
                    payload = api_v2_post(
                        f"/api/v2/cameras/{camera_id}/record",
                        {
                            "duration_seconds": duration_seconds if duration_seconds is not None else 10,
                            "stream_id": stream_id if stream_id is not None else 0,
                            "path": str(request.form.get("record_path") or "").strip(),
                        },
                    )
                    return action_response(
                        str(payload.get("message") or "Clip recording requested"),
                        "success",
                        url_for("camera_detail", camera_id=camera_id),
                        camera_payload=action_history_delta_payload(camera_id),
                    )
                except Exception as error:
                    LOG.warning("API v2 record failed for %s; falling back to Flask handler: %s", camera_id, error)
            result = hub.record_camera_clip(
                camera_id,
                duration_seconds=duration_seconds if duration_seconds is not None else 10,
                stream_id=stream_id if stream_id is not None else 0,
                path=str(request.form.get("record_path") or "").strip(),
            )
            clip_path = ((result.get("result") or {}).get("path") or "") if isinstance(result, dict) else ""
            detail = f" -> {clip_path}" if clip_path else ""
            return action_response(
                f"Clip recording requested{detail}",
                "success",
                url_for("camera_detail", camera_id=camera_id),
                camera_payload=action_history_delta_payload(camera_id),
            )
        except Exception as error:
            return action_response(
                f"Clip recording failed: {error}",
                "error",
                url_for("camera_detail", camera_id=camera_id),
                camera_payload={},
                status_code=500,
            )

    @app.post("/delete/<camera_id>")
    def delete_camera(camera_id: str) -> Response:
        try:
            result = hub.unregister_camera(camera_id)
            outcome = delete_outcome(camera_id, result)
            return action_response(
                str(outcome["message"]),
                str(outcome["category"]),
                url_for("dashboard"),
                status_code=int(outcome["http_status"]),
            )
        except Exception as error:
            return action_response(
                f"Delete failed for {camera_id}: {error}",
                "error",
                url_for("dashboard"),
                status_code=500,
            )

    @app.get("/snapshot/<camera_id>")
    def preview_snapshot(camera_id: str) -> Response:
        stream_name = str(request.args.get("stream") or "ch0").strip().lower() or "ch0"

        if stream_name != "ch0":
            try:
                snapshot_url = hub.get_camera_snapshot_url_for_ui(camera_id, stream_name)
            except Exception as error:
                LOG.warning("Snapshot preview lookup failed for %s", camera_id, exc_info=True)
                return Response(f"Snapshot preview unavailable: {error}\n", status=404, mimetype="text/plain")

            if snapshot_url:
                try:
                    with open_camera_media_request(camera_id, snapshot_url, timeout=15, accept="image/jpeg, */*") as upstream:
                        body = upstream.read()
                        content_type = upstream.headers.get("Content-Type", "image/jpeg")
                    if not body:
                        raise ValueError("empty snapshot response")
                    if "image/" not in str(content_type).lower():
                        raise ValueError(f"unsupported snapshot content type: {content_type}")
                except Exception as error:
                    LOG.debug("Snapshot preview stream fallback for %s/%s: %s", camera_id, stream_name, error, exc_info=True)
                    stream_name = "ch0"
                else:
                    response = Response(body, mimetype=content_type)
                    response.headers["Content-Type"] = content_type
                    response.headers["Cache-Control"] = "no-store, no-cache, must-revalidate, max-age=0"
                    response.headers["Pragma"] = "no-cache"
                    response.headers["Expires"] = "0"
                    return response

            if stream_name != "ch0":
                stream_name = "ch0"

        try:
            cached_snapshot = hub.get_cached_snapshot_for_ui(camera_id)
        except Exception:
            LOG.warning("Snapshot preview lookup failed for %s; using fallback image", camera_id, exc_info=True)
            cached_snapshot = None
        if cached_snapshot is not None:
            try:
                cached_snapshot_path = str(cached_snapshot)
                if not os.path.isfile(cached_snapshot_path) or os.path.getsize(cached_snapshot_path) <= 0:
                    LOG.debug("Ignoring invalid cached snapshot for %s: %s", camera_id, cached_snapshot_path)
                    cached_snapshot = None
            except Exception:
                LOG.debug("Failed to validate cached snapshot for %s", camera_id, exc_info=True)
                cached_snapshot = None

        if cached_snapshot is None:
            try:
                snapshot_url = hub.get_camera_snapshot_url_for_ui(camera_id, "ch0")
            except Exception:
                snapshot_url = ""
            if snapshot_url:
                try:
                    with open_camera_media_request(camera_id, snapshot_url, timeout=15, accept="image/jpeg, */*") as upstream:
                        body = upstream.read()
                        content_type = upstream.headers.get("Content-Type", "image/jpeg")
                    if not body:
                        raise ValueError("empty snapshot response")
                    if "image/" not in str(content_type).lower():
                        raise ValueError(f"unsupported snapshot content type: {content_type}")
                    response = Response(body, mimetype=content_type)
                    response.headers["Content-Type"] = content_type
                    response.headers["Cache-Control"] = "no-store, no-cache, must-revalidate, max-age=0"
                    response.headers["Pragma"] = "no-cache"
                    response.headers["Expires"] = "0"
                    return response
                except Exception:
                    LOG.debug("Live snapshot fallback failed for %s", camera_id, exc_info=True)

        if cached_snapshot is None:
            response = app.send_static_file("a/nostream.svg")
        else:
            response = send_file(cached_snapshot)

        response.headers["Cache-Control"] = "no-store, no-cache, must-revalidate, max-age=0"
        response.headers["Pragma"] = "no-cache"
        response.headers["Expires"] = "0"
        return response

    @app.get("/preview-live/<camera_id>")
    def preview_live(camera_id: str) -> Response:
        try:
            stream_name = str(request.args.get("stream") or "ch0").strip().lower() or "ch0"
            stream_url = hub.get_camera_mjpeg_url_for_ui(camera_id, stream_name)
        except Exception as error:
            LOG.warning("Live preview lookup failed for %s", camera_id, exc_info=True)
            return Response(f"Live preview unavailable: {error}\n", status=404, mimetype="text/plain")

        if not stream_url:
            return Response("Live preview unavailable\n", status=404, mimetype="text/plain")

        try:
            upstream = open_camera_media_request(camera_id, stream_url, timeout=30, accept="multipart/x-mixed-replace, */*")
        except urllib.error.HTTPError as error:
            detail = error.read().decode("utf-8", errors="replace").strip() or error.reason or f"HTTP {error.code}"
            return Response(f"Live preview failed: {detail}\n", status=error.code, mimetype="text/plain")
        except urllib.error.URLError as error:
            return Response(f"Live preview failed: {error.reason}\n", status=502, mimetype="text/plain")
        except Exception as error:
            return Response(f"Live preview failed: {error}\n", status=500, mimetype="text/plain")

        def stream() -> Any:
            try:
                while True:
                    chunk = upstream.read(8192)
                    if not chunk:
                        break
                    yield chunk
            finally:
                upstream.close()

        content_type = upstream.headers.get("Content-Type", "multipart/x-mixed-replace; boundary=frame")
        response = Response(stream(), mimetype=content_type)
        response.headers["Content-Type"] = content_type
        response.headers["Cache-Control"] = "no-store, no-cache, must-revalidate, max-age=0"
        response.headers["Pragma"] = "no-cache"
        response.headers["Expires"] = "0"
        return response

    @app.get("/preview-webrtc/<camera_id>")
    def preview_webrtc(camera_id: str) -> Response:
        try:
            with camera_webrtc_request(
                camera_id,
                "/webrtc",
                method="GET",
                accept="text/html, */*",
            ) as upstream:
                body = upstream.read()
                content_type = upstream.headers.get("Content-Type", "text/html; charset=utf-8")
        except urllib.error.HTTPError as error:
            detail = error.read().decode("utf-8", errors="replace").strip() or error.reason or f"HTTP {error.code}"
            return Response(f"WebRTC preview failed: {detail}\n", status=error.code, mimetype="text/plain")
        except urllib.error.URLError as error:
            return Response(f"WebRTC preview failed: {error.reason}\n", status=502, mimetype="text/plain")
        except Exception as error:
            return Response(f"WebRTC preview failed: {error}\n", status=500, mimetype="text/plain")

        if "text/html" in content_type.lower():
            body_text = rewrite_webrtc_html(camera_id, body.decode("utf-8", errors="replace"))
            response = Response(body_text, mimetype="text/html")
            response.headers["Content-Type"] = content_type
        else:
            response = Response(body, mimetype=content_type)
            response.headers["Content-Type"] = content_type
        response.headers["Cache-Control"] = "no-store, no-cache, must-revalidate, max-age=0"
        response.headers["Pragma"] = "no-cache"
        response.headers["Expires"] = "0"
        return response

    @app.post("/preview-webrtc/<camera_id>/whip")
    def preview_webrtc_whip(camera_id: str) -> Response:
        raw_query = request.query_string.decode("utf-8", errors="ignore")
        raw_body = request.get_data(cache=False)
        content_type = str(request.headers.get("Content-Type") or "application/sdp")
        try:
            with camera_webrtc_request(
                camera_id,
                "/whip",
                method="POST",
                data=raw_body,
                query=raw_query,
                accept="application/sdp, text/plain, */*",
                content_type=content_type,
            ) as upstream:
                body = upstream.read()
                upstream_content_type = upstream.headers.get("Content-Type", "application/sdp")
                upstream_location = upstream.headers.get("Location", "")
                upstream_status = int(getattr(upstream, "status", 200))
        except urllib.error.HTTPError as error:
            detail = error.read().decode("utf-8", errors="replace").strip() or error.reason or f"HTTP {error.code}"
            return Response(f"WebRTC signaling failed: {detail}\n", status=error.code, mimetype="text/plain")
        except urllib.error.URLError as error:
            return Response(f"WebRTC signaling failed: {error.reason}\n", status=502, mimetype="text/plain")
        except Exception as error:
            return Response(f"WebRTC signaling failed: {error}\n", status=500, mimetype="text/plain")

        response = Response(body, status=upstream_status, mimetype=upstream_content_type)
        response.headers["Content-Type"] = upstream_content_type
        rewritten_location = rewrite_webrtc_location(camera_id, upstream_location)
        if rewritten_location:
            response.headers["Location"] = rewritten_location
        response.headers["Cache-Control"] = "no-store, no-cache, must-revalidate, max-age=0"
        response.headers["Pragma"] = "no-cache"
        response.headers["Expires"] = "0"
        return response

    @app.delete("/preview-webrtc/<camera_id>/whip")
    @app.delete("/preview-webrtc/<camera_id>/whip/<path:resource_path>")
    def preview_webrtc_whip_delete(camera_id: str, resource_path: str = "") -> Response:
        relative_path = "/whip"
        if resource_path:
            relative_path = f"/whip/{resource_path.lstrip('/')}"
        raw_query = request.query_string.decode("utf-8", errors="ignore")
        try:
            with camera_webrtc_request(
                camera_id,
                relative_path,
                method="DELETE",
                query=raw_query,
                accept="text/plain, */*",
            ) as upstream:
                body = upstream.read()
                upstream_content_type = upstream.headers.get("Content-Type", "text/plain")
                upstream_status = int(getattr(upstream, "status", 200))
        except urllib.error.HTTPError as error:
            detail = error.read().decode("utf-8", errors="replace").strip() or error.reason or f"HTTP {error.code}"
            return Response(f"WebRTC signaling failed: {detail}\n", status=error.code, mimetype="text/plain")
        except urllib.error.URLError as error:
            return Response(f"WebRTC signaling failed: {error.reason}\n", status=502, mimetype="text/plain")
        except Exception as error:
            return Response(f"WebRTC signaling failed: {error}\n", status=500, mimetype="text/plain")

        response = Response(body, status=upstream_status, mimetype=upstream_content_type)
        response.headers["Content-Type"] = upstream_content_type
        response.headers["Cache-Control"] = "no-store, no-cache, must-revalidate, max-age=0"
        response.headers["Pragma"] = "no-cache"
        response.headers["Expires"] = "0"
        return response

    @app.post("/camera/<camera_id>/sensor-data/history")
    def camera_sensor_data_history(camera_id: str) -> Response:
        try:
            with camera_agent_request(
                camera_id,
                "/runtime/sensor-data",
                method="GET",
                accept="application/json",
            ) as upstream:
                body = upstream.read()
        except Exception:
            try:
                with camera_agent_bridge_request(
                    camera_id,
                    "/api/v1/runtime/sensor-data",
                    method="GET",
                    accept="application/json",
                ) as upstream:
                    body = upstream.read()
            except urllib.error.HTTPError as error:
                detail = error.read().decode("utf-8", errors="replace").strip() or error.reason or f"HTTP {error.code}"
                return Response(f"Sensor data history failed: {detail}\n", status=error.code, mimetype="text/plain")
            except urllib.error.URLError as error:
                return Response(f"Sensor data history failed: {error.reason}\n", status=502, mimetype="text/plain")
            except Exception as error:
                return Response(f"Sensor data history failed: {error}\n", status=500, mimetype="text/plain")
        except urllib.error.HTTPError as error:
            detail = error.read().decode("utf-8", errors="replace").strip() or error.reason or f"HTTP {error.code}"
            return Response(f"Sensor data history failed: {detail}\n", status=error.code, mimetype="text/plain")
        except urllib.error.URLError as error:
            return Response(f"Sensor data history failed: {error.reason}\n", status=502, mimetype="text/plain")
        except Exception as error:
            return Response(f"Sensor data history failed: {error}\n", status=500, mimetype="text/plain")

        try:
            payload = json.loads(body.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError) as error:
            return Response(f"Sensor data history failed: invalid JSON: {error}\n", status=502, mimetype="text/plain")

        history = []
        if isinstance(payload, dict):
            history_value = payload.get("history")
            if isinstance(history_value, list):
                history = history_value
        body_out = json.dumps({"daynight": {"history": history}}).encode("utf-8")

        response = Response(body_out, mimetype="application/json")
        response.headers["Content-Type"] = "application/json"
        response.headers["Cache-Control"] = "no-store, no-cache, must-revalidate, max-age=0"
        response.headers["Pragma"] = "no-cache"
        response.headers["Expires"] = "0"
        return response

    @app.get("/camera/<camera_id>/sensor-data/stream")
    def camera_sensor_data_stream(camera_id: str) -> Response:
        try:
            upstream = camera_agent_request(
                camera_id,
                "/events/sensor-data",
                method="GET",
                accept="text/event-stream",
            )
        except Exception:
            try:
                upstream = camera_agent_bridge_request(
                    camera_id,
                    "/api/v1/events/sensor-data",
                    method="GET",
                    accept="text/event-stream",
                )
            except urllib.error.HTTPError as error:
                detail = error.read().decode("utf-8", errors="replace").strip() or error.reason or f"HTTP {error.code}"
                return Response(f"Sensor data stream failed: {detail}\n", status=error.code, mimetype="text/plain")
            except urllib.error.URLError as error:
                return Response(f"Sensor data stream failed: {error.reason}\n", status=502, mimetype="text/plain")
            except Exception as error:
                return Response(f"Sensor data stream failed: {error}\n", status=500, mimetype="text/plain")
        except urllib.error.HTTPError as error:
            detail = error.read().decode("utf-8", errors="replace").strip() or error.reason or f"HTTP {error.code}"
            return Response(f"Sensor data stream failed: {detail}\n", status=error.code, mimetype="text/plain")
        except urllib.error.URLError as error:
            return Response(f"Sensor data stream failed: {error.reason}\n", status=502, mimetype="text/plain")
        except Exception as error:
            return Response(f"Sensor data stream failed: {error}\n", status=500, mimetype="text/plain")

        def stream() -> Any:
            try:
                while True:
                    line = upstream.readline()
                    if not line:
                        break
                    yield line
            finally:
                upstream.close()

        content_type = upstream.headers.get("Content-Type", "text/event-stream")
        response = Response(stream(), mimetype=content_type)
        response.headers["Content-Type"] = content_type
        response.headers["Cache-Control"] = "no-store, no-cache, must-revalidate, max-age=0"
        response.headers["Pragma"] = "no-cache"
        response.headers["Expires"] = "0"
        response.headers["X-Accel-Buffering"] = "no"
        return response

    @app.get("/camera/<camera_id>/screenshot/download")
    def camera_screenshot_download(camera_id: str) -> Response:
        camera = hub.get_camera_for_ui(camera_id)
        default_stream = "ch1" if str(camera.get("api_streamer") or "").strip().lower() == "raptor" else "ch0"
        stream_name = str(request.args.get("stream") or default_stream).strip().lower()
        stream_id = 1 if stream_name == "ch1" else 0
        request_body = json.dumps(
            {
                "stream_id": stream_id,
                "mode": "inline",
            },
            separators=(",", ":"),
        ).encode("utf-8")

        try:
            with camera_agent_request(
                camera_id,
                "/actions/snapshot",
                method="POST",
                data=request_body,
                accept="image/jpeg, application/json",
            ) as upstream:
                body = upstream.read()
                content_type = upstream.headers.get("Content-Type", "image/jpeg")
        except Exception:
            try:
                with camera_agent_bridge_request(
                    camera_id,
                    "/api/v1/actions/snapshot",
                    method="POST",
                    data=request_body,
                    accept="image/jpeg, application/json",
                ) as upstream:
                    body = upstream.read()
                    content_type = upstream.headers.get("Content-Type", "image/jpeg")
            except urllib.error.HTTPError as error:
                detail = error.read().decode("utf-8", errors="replace").strip() or error.reason or f"HTTP {error.code}"
                return Response(f"Screenshot download failed: {detail}\n", status=error.code, mimetype="text/plain")
            except urllib.error.URLError as error:
                return Response(f"Screenshot download failed: {error.reason}\n", status=502, mimetype="text/plain")
            except Exception as error:
                return Response(f"Screenshot download failed: {error}\n", status=500, mimetype="text/plain")
        except urllib.error.HTTPError as error:
            detail = error.read().decode("utf-8", errors="replace").strip() or error.reason or f"HTTP {error.code}"
            return Response(f"Screenshot download failed: {detail}\n", status=error.code, mimetype="text/plain")
        except urllib.error.URLError as error:
            return Response(f"Screenshot download failed: {error.reason}\n", status=502, mimetype="text/plain")
        except Exception as error:
            return Response(f"Screenshot download failed: {error}\n", status=500, mimetype="text/plain")

        response = Response(body, mimetype=content_type)
        response.headers["Content-Type"] = content_type
        response.headers["Content-Disposition"] = (
            f'attachment; filename="{camera_id}-{stream_name}-{int(time.time())}.jpg"'
        )
        response.headers["Cache-Control"] = "no-store, no-cache, must-revalidate, max-age=0"
        response.headers["Pragma"] = "no-cache"
        response.headers["Expires"] = "0"
        return response

    return app


def _auth_required_response() -> Response:
    return Response(
        "Authentication required\n",
        status=401,
        headers={"WWW-Authenticate": 'Basic realm="telegrambothub"'},
        mimetype="text/plain",
    )


def _sanitize_next_url(target: Any) -> str:
    candidate = str(target or "").strip()
    if not candidate:
        return ""
    parsed = urlsplit(candidate)
    if parsed.scheme or parsed.netloc:
        return ""
    if not candidate.startswith("/"):
        return ""
    return candidate


def _post_login_redirect_target(target: Any) -> str:
    sanitized = _sanitize_next_url(target)
    if sanitized and sanitized != "/login":
        return sanitized
    return url_for("dashboard")


def _normalize_competency_level(value: Any) -> str:
    normalized = str(value or "").strip().lower()
    if normalized in {"basic", "advanced", "expert"}:
        return normalized
    return "basic"


def _native_anti_flicker_value(value: Any) -> str:
    mode = str(value or "").strip().lower()
    aliases = {
        "off": "0",
        "0": "0",
        "50hz": "1",
        "50": "1",
        "1": "1",
        "60hz": "2",
        "60": "2",
        "2": "2",
    }
    return aliases.get(mode, mode)


def _supported_config_patch_from_form(form: Any) -> dict[str, Any]:
    thread_rtsp = 1
    thread_video = 2
    thread_osd = 8

    image: dict[str, Any] = {}
    for field in ("brightness", "contrast", "saturation", "sharpness"):
        value = _optional_int_value(form.get(f"image_{field}"), f"image.{field}")
        if value is not None:
            image[field] = value

    if str(form.get("image_hflip_present") or "").strip() == "1":
        image["hflip"] = form.get("image_hflip") == "on"

    if str(form.get("image_vflip_present") or "").strip() == "1":
        image["vflip"] = form.get("image_vflip") == "on"

    anti_flicker = _native_anti_flicker_value(form.get("image_anti_flicker"))
    if anti_flicker:
        image["anti_flicker"] = anti_flicker

    payload: dict[str, Any] = {}
    if image:
        payload["image"] = image

    if str(form.get("motion_enabled_present") or "").strip() == "1":
        payload["motion"] = {
            "enabled": form.get("motion_enabled") == "on",
        }

    if str(form.get("daynight_enabled_present") or "").strip() == "1":
        daynight: dict[str, Any] = {
            "enabled": form.get("daynight_enabled") == "on",
        }
        force_mode = str(form.get("daynight_force_mode") or "").strip()
        if force_mode:
            daynight["force_mode"] = force_mode
        for field in ("total_gain_night_threshold", "total_gain_day_threshold"):
            value = _optional_int_value(form.get(f"daynight_{field}"), f"daynight.{field}")
            if value is not None:
                daynight[field] = value
        controls: dict[str, Any] = {}
        for field in ("color", "ircut", "ir850", "ir940", "white"):
            if str(form.get(f"daynight_controls_{field}_present") or "").strip() == "1":
                controls[field] = form.get(f"daynight_controls_{field}") == "on"
        if controls:
            daynight["controls"] = controls
        schedule: dict[str, Any] = {}
        if str(form.get("daynight_schedule_enabled_present") or "").strip() == "1":
            schedule["enabled"] = form.get("daynight_schedule_enabled") == "on"
        for field in ("start_at", "stop_at"):
            raw_value = str(form.get(f"daynight_schedule_{field}") or "").strip()
            if raw_value:
                schedule[field] = raw_value
        if schedule:
            daynight["schedule"] = schedule
        payload["daynight"] = daynight

    stream_field_pattern = re.compile(r"^(stream\d+)_(enabled|audio_enabled|width|height|fps|bitrate|format|mode|osd_enabled|osd_time_enabled|osd_usertext_enabled|osd_usertext_format)$")
    stream_present_pattern = re.compile(r"^(stream\d+)_(enabled|audio_enabled|osd_enabled|osd_time_enabled|osd_usertext_enabled)_present$")
    privacy_enabled_present_pattern = re.compile(r"^(stream\d+)_osd_privacy_enabled_present$")
    privacy_text_pattern = re.compile(r"^(stream\d+)_osd_privacy_text$")
    privacy_color_pattern = re.compile(r"^(stream\d+)_osd_privacy_(fill|stroke)_color$")
    stream_payloads: dict[str, dict[str, Any]] = {}
    restart_thread_mask = 0
    for key in form.keys():
        match = stream_present_pattern.match(str(key))
        if match is None:
            continue
        stream_name, field_name = match.groups()
        stream_payload = stream_payloads.setdefault(stream_name, {})
        field_value = form.get(f"{stream_name}_{field_name}") == "on"
        if field_name == "osd_enabled":
            osd_payload = stream_payload.setdefault("osd", {})
            osd_payload["enabled"] = field_value
            restart_thread_mask |= thread_video | thread_osd
        elif field_name == "osd_time_enabled":
            osd_payload = stream_payload.setdefault("osd", {})
            time_payload = osd_payload.setdefault("time", {})
            time_payload["enabled"] = field_value
            restart_thread_mask |= thread_video | thread_osd
        elif field_name == "osd_usertext_enabled":
            osd_payload = stream_payload.setdefault("osd", {})
            usertext_payload = osd_payload.setdefault("usertext", {})
            usertext_payload["enabled"] = field_value
            restart_thread_mask |= thread_video | thread_osd
        else:
            stream_payload[field_name] = field_value
            restart_thread_mask |= thread_rtsp | thread_video

    for key in form.keys():
        match = privacy_enabled_present_pattern.match(str(key))
        if match is None:
            continue
        stream_name = match.group(1)
        stream_payload = stream_payloads.setdefault(stream_name, {})
        osd_payload = stream_payload.setdefault("osd", {})
        privacy_payload = osd_payload.setdefault("privacy", {})
        privacy_payload["enabled"] = form.get(f"{stream_name}_osd_privacy_enabled") == "on"
        restart_thread_mask |= thread_video | thread_osd

    for key in form.keys():
        match = stream_field_pattern.match(str(key))
        if match is None:
            continue
        stream_name, field_name = match.groups()
        stream_payload = stream_payloads.setdefault(stream_name, {})
        raw_value = form.get(key)
        if field_name in {"enabled", "audio_enabled", "osd_enabled", "osd_time_enabled", "osd_usertext_enabled"}:
            continue
        if field_name in {"width", "height", "fps", "bitrate"}:
            value = _optional_int_value(raw_value, f"{stream_name}.{field_name}")
            if value is not None:
                stream_payload[field_name] = value
                restart_thread_mask |= thread_rtsp | thread_video
            continue
        if field_name == "osd_usertext_format":
            value = str(raw_value or "").strip()
            if value:
                osd_payload = stream_payload.setdefault("osd", {})
                usertext_payload = osd_payload.setdefault("usertext", {})
                usertext_payload["format"] = value
                restart_thread_mask |= thread_video | thread_osd
            continue
        value = str(raw_value or "").strip()
        if value:
            stream_payload[field_name] = value
            restart_thread_mask |= thread_rtsp | thread_video

    for key in form.keys():
        form_key = str(key)
        match = privacy_text_pattern.match(form_key)
        if match is not None:
            stream_name = match.group(1)
            stream_payload = stream_payloads.setdefault(stream_name, {})
            osd_payload = stream_payload.setdefault("osd", {})
            privacy_payload = osd_payload.setdefault("privacy", {})
            privacy_payload["text"] = str(form.get(form_key) or "")
            restart_thread_mask |= thread_video | thread_osd
            continue

        match = privacy_color_pattern.match(form_key)
        if match is None:
            continue
        stream_name, color_kind = match.groups()
        color_value = _normalize_hex_color(form.get(form_key), f"{stream_name}.osd.privacy.{color_kind}_color")
        if color_value is None:
            continue
        alpha_value = _color_alpha_value(
            form.get(f"{stream_name}_osd_privacy_{color_kind}_alpha"),
            f"{stream_name}.osd.privacy.{color_kind}_alpha",
        )
        stream_payload = stream_payloads.setdefault(stream_name, {})
        osd_payload = stream_payload.setdefault("osd", {})
        privacy_payload = osd_payload.setdefault("privacy", {})
        privacy_payload[f"{color_kind}_color"] = _combine_hex_color_and_alpha(color_value, alpha_value)
        restart_thread_mask |= thread_video | thread_osd

    payload.update({stream_name: stream_payload for stream_name, stream_payload in stream_payloads.items() if stream_payload})
    if restart_thread_mask:
        payload["action"] = {"restart_thread": restart_thread_mask}

    return payload


def _send2_motion_patch_from_form(form: Any) -> dict[str, Any]:
    payload: dict[str, Any] = {}
    motion: dict[str, Any] = {}

    sensitivity = _optional_int_value(form.get("send2_motion_sensitivity"), "send2.motion.sensitivity")
    if sensitivity is not None:
        motion["sensitivity"] = sensitivity

    cooldown_time = _optional_int_value(form.get("send2_motion_cooldown"), "send2.motion.cooldown_time")
    if cooldown_time is not None:
        motion["cooldown_time"] = cooldown_time

    for service_name in ("email", "ftp", "telegram", "mqtt", "webhook", "storage", "ntfy", "gphotos"):
        present_key = f"motion_send2{service_name}_present"
        if str(form.get(present_key) or "").strip() != "1":
            continue
        motion[f"send2{service_name}"] = form.get(f"motion_send2{service_name}") == "on"

    if motion:
        payload["motion"] = motion

    for service_name in ("email", "ftp", "telegram", "mqtt", "webhook", "storage", "ntfy", "gphotos"):
        service_payload: dict[str, Any] = {}

        photo_present_key = f"send2{service_name}_photo_present"
        if str(form.get(photo_present_key) or "").strip() == "1":
            service_payload["send_photo"] = form.get(f"send2{service_name}_photo") == "on"

        video_present_key = f"send2{service_name}_video_present"
        if str(form.get(video_present_key) or "").strip() == "1":
            service_payload["send_video"] = form.get(f"send2{service_name}_video") == "on"

        if service_payload:
            payload[service_name] = service_payload

    if not payload:
        return {}
    return payload


def _optional_int_value(raw_value: Any, field_name: str) -> int | None:
    value = str(raw_value or "").strip()
    if not value:
        return None
    try:
        return int(value)
    except ValueError as error:
        raise ValueError(f"{field_name} must be an integer") from error


def _normalize_hex_color(raw_value: Any, field_name: str) -> str | None:
    value = str(raw_value or "").strip().upper()
    if not value:
        return None
    if not re.fullmatch(r"#[0-9A-F]{6}", value):
        raise ValueError(f"{field_name} must be a #RRGGBB color")
    return value


def _color_alpha_value(raw_value: Any, field_name: str) -> int:
    value = str(raw_value or "").strip()
    if not value:
        return 255
    try:
        parsed = int(value)
    except ValueError as error:
        raise ValueError(f"{field_name} must be an integer") from error
    if parsed < 0 or parsed > 255:
        raise ValueError(f"{field_name} must be between 0 and 255")
    return parsed


def _combine_hex_color_and_alpha(color_value: str, alpha_value: int) -> str:
    return f"{color_value}{alpha_value:02X}"


def _split_hex_color_alpha(raw_value: Any) -> tuple[str, str]:
    value = str(raw_value or "").strip().upper()
    if re.fullmatch(r"#[0-9A-F]{8}", value):
        return value[:7], str(int(value[7:], 16))
    if re.fullmatch(r"#[0-9A-F]{6}", value):
        return value, "255"
    return "#000000", "255"


def _config_from_form(
    form: Any,
    allow_partial: bool = False,
    existing: dict[str, Any] | None = None,
) -> dict[str, Any]:
    config = {
        "telegram": {
            "token": form.get("telegram_token", "").strip(),
            "api_url": form.get("telegram_api_url", "https://api.telegram.org").strip(),
            "polling_timeout": _int_value(form.get("telegram_polling_timeout"), 30),
            "allowed_chat_ids": _csv_lines(form.get("telegram_allowed_chat_ids", ""), cast=int),
            "allowed_usernames": _csv_lines(form.get("telegram_allowed_usernames", ""), cast=str),
        },
        "mqtt": {
            "host": form.get("mqtt_host", "").strip(),
            "port": _int_value(form.get("mqtt_port"), 1883),
            "username": form.get("mqtt_username", "").strip(),
            "password": form.get("mqtt_password", ""),
            "keepalive": _int_value(form.get("mqtt_keepalive"), 60),
            "use_tls": form.get("mqtt_use_tls") == "on",
        },
        "routing": {
            "command_topic": form.get("routing_command_topic", "thingino/cam/{camera_id}/cmd").strip(),
            "reply_topic": form.get("routing_reply_topic", "thingino/cam/+/reply").strip(),
            "registration_topic": form.get("routing_registration_topic", "thingino/cam/+/hello").strip(),
            "event_topic": form.get("routing_event_topic", "thingino/cam/+/event").strip(),
            "state_topic": form.get("routing_state_topic", "thingino/cam/+/state").strip(),
        },
        "ui": {
            "username": form.get("ui_username", "").strip(),
            "password": form.get("ui_password", ""),
            "competency_level": _normalize_competency_level(form.get("ui_competency_level")),
            "registration_stale_after_seconds": _int_value(form.get("ui_registration_stale_after_seconds"), 0),
            "snapshot_heartbeat_interval_seconds": _int_value(form.get("ui_snapshot_heartbeat_interval_seconds"), 0),
            "snapshot_heartbeat_timeout_seconds": _int_value(form.get("ui_snapshot_heartbeat_timeout_seconds"), 5),
            "snapshot_cache_stale_after_seconds": _int_value(form.get("ui_snapshot_cache_stale_after_seconds"), 3600),
            "api_probe_interval_seconds": _int_value(form.get("ui_api_probe_interval_seconds"), 0),
        },
        "defaults": {
            "onvif_username": str(form.get("defaults_onvif_username") or "thingino").strip(),
            "onvif_password": str(form.get("defaults_onvif_password") or "thingino"),
        },
        "pairing": {
            "auto_install_on_registration": (
                form.get("pairing_auto_install_on_registration") == "on"
                if "pairing_auto_install_on_registration_present" in form
                else bool((existing or {}).get("pairing", {}).get("auto_install_on_registration", True))
            ),
            "auto_install_retry_seconds": _int_value(
                form.get("pairing_auto_install_retry_seconds"),
                int((existing or {}).get("pairing", {}).get("auto_install_retry_seconds", 300) or 300),
            ),
        },
        "history": {
            "enabled": form.get("history_enabled") == "on",
            "path": form.get("history_path", "").strip(),
            "recent_actions_limit": _int_value(form.get("history_recent_actions_limit"), 20),
            "max_action_events_per_camera": _int_value(form.get("history_max_action_events_per_camera"), 1000),
            "max_state_samples_per_camera": _int_value(form.get("history_max_state_samples_per_camera"), 5000),
        },
        "cameras": _load_cameras_yaml(form.get("cameras_yaml", "")),
    }

    # Preserve unknown top-level keys the form does not edit (future-proofing).
    if existing:
        for key, value in existing.items():
            if key not in config:
                config[key] = copy.deepcopy(value)

    if allow_partial:
        return config

    return load_config_dict(copy.deepcopy(config))


def _load_cameras_yaml(text: str) -> list[dict[str, Any]]:
    if not text.strip():
        return []
    decoded = yaml.safe_load(text)
    if decoded is None:
        return []
    if not isinstance(decoded, list):
        raise ValueError("Cameras must be a YAML list")
    cameras: list[dict[str, Any]] = []
    for index, entry in enumerate(decoded, start=1):
        if not isinstance(entry, dict):
            raise ValueError(f"Camera entry {index} must be a mapping")
        camera_id = str(entry.get("id") or "").strip().lower()
        if not camera_id:
            raise ValueError(f"Camera entry {index} is missing id")
        cameras.append(
            {
                "id": camera_id,
                "name": str(entry.get("name") or camera_id).strip(),
                "ip": str(entry.get("ip") or "").strip(),
                "snapshot_url": str(entry.get("snapshot_url") or "").strip(),
                "api_key": str(entry.get("api_key") or "").strip(),
                "api_base_url": str(entry.get("api_base_url") or "").strip(),
                "api_token": str(entry.get("api_token") or "").strip(),
                "onvif_endpoint": str(entry.get("onvif_endpoint") or "").strip(),
                "onvif_username": str(entry.get("onvif_username") or "").strip(),
                "onvif_password": str(entry.get("onvif_password") or ""),
            }
        )
    return cameras


def _dump_cameras(config: dict[str, Any]) -> str:
    return yaml.safe_dump(config.get("cameras", []), sort_keys=False).strip()


def _csv_lines(raw: str, cast: type[int] | type[str]) -> list[int] | list[str]:
    items = []
    for chunk in raw.replace("\n", ",").split(","):
        value = chunk.strip()
        if not value:
            continue
        items.append(cast(value))
    return items


def _int_value(raw: Any, default: int) -> int:
    text = str(raw or "").strip()
    if not text:
        return default
    return int(text)


def _generate_tinycam_xml(cameras: list[dict[str, Any]]) -> str:
    """Generate TinyCam Monitor cameras.xml format from hub camera list.

    This creates an Android SharedPreferences XML file compatible with
    TinyCam Monitor app for importing camera configurations.
    """
    import base64

    lines = [
        "<?xml version='1.0' encoding='utf-8' standalone='yes' ?>",
        "<map>",
    ]

    camera_index = 1
    for camera in cameras:
        cam_key = f"cam{camera_index}"

        # Basic camera info
        name = camera.get("name", f"Camera {camera_index}").strip()
        if name:
            lines.append(f'    <string name="preference_{cam_key}_name">{_xml_escape(name)}</string>')

        # Snapshot URL (for preview)
        snapshot_url = camera.get("snapshot_url", "").strip()
        if snapshot_url:
            lines.append(f'    <string name="preference_{cam_key}_url">{_xml_escape(snapshot_url)}</string>')

        # IP address
        ip = camera.get("ip", "").strip()
        if ip:
            lines.append(f'    <string name="preference_{cam_key}_hostname">{_xml_escape(ip)}</string>')

        # RTSP stream URL (if available from API)
        if ip:
            # Standard RTSP stream
            rtsp_url = f"rtsp://{ip}:554/ch0"
            lines.append(f'    <string name="preference_{cam_key}_stream">{_xml_escape(rtsp_url)}</string>')

        # ONVIF endpoint
        onvif_endpoint = camera.get("onvif_endpoint", "").strip()
        if onvif_endpoint:
            lines.append(f'    <string name="preference_{cam_key}_onvif">{_xml_escape(onvif_endpoint)}</string>')

        # Camera ID (for reference)
        camera_id = camera.get("camera_id", "").strip()
        if camera_id:
            lines.append(f'    <string name="preference_{cam_key}_id">{_xml_escape(camera_id)}</string>')

        # ONVIF username (base64 encoded like TinyCam does)
        onvif_username = camera.get("onvif_username", "").strip()
        if onvif_username:
            encoded = base64.b64encode(onvif_username.encode()).decode()
            lines.append(f'    <string name="{cam_key}_username">{_xml_escape(encoded)}</string>')

        # ONVIF password (base64 encoded like TinyCam does)
        onvif_password = camera.get("onvif_password", "").strip()
        if onvif_password:
            encoded = base64.b64encode(onvif_password.encode()).decode()
            lines.append(f'    <string name="{cam_key}_password">{_xml_escape(encoded)}</string>')

        # Device model/vendor if available
        vendor = camera.get("onvif_manufacturer", "").strip()
        if vendor:
            lines.append(f'    <string name="preference_{cam_key}_vendor">{_xml_escape(vendor)}</string>')

        model = camera.get("onvif_model", "").strip()
        if model:
            lines.append(f'    <string name="preference_{cam_key}_model">{_xml_escape(model)}</string>')

        camera_index += 1

    lines.append("</map>")
    return "\n".join(lines)


def _xml_escape(text: str) -> str:
    """Escape special XML characters"""
    return (text
        .replace("&", "&amp;")
        .replace("<", "&lt;")
        .replace(">", "&gt;")
        .replace('"', "&quot;")
        .replace("'", "&apos;")
    )
