import copy
import hmac
import json
import logging
import re
import threading
import time
import urllib.error
import urllib.request
from urllib.parse import urlsplit
from typing import TYPE_CHECKING, Any

import yaml
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


def create_web_app(hub: "Hub", ui_username: str = "", ui_password: str = "") -> Flask:
    app = Flask(__name__)
    app.config["SECRET_KEY"] = "telegrambothub-ui"
    app.config["SESSION_PERMANENT"] = False
    app.config["TEMPLATES_AUTO_RELOAD"] = True
    app.config["SEND_FILE_MAX_AGE_DEFAULT"] = 0
    app.jinja_env.auto_reload = True

    auth_enabled = bool(ui_username and ui_password)

    @app.context_processor
    def inject_auth_state() -> dict[str, Any]:
        return {
            "auth_enabled": auth_enabled,
            "is_authenticated": bool(session.get("ui_authenticated")),
        }

    def credentials_are_valid(username: str, password: str) -> bool:
        username_ok = hmac.compare_digest(username, ui_username)
        password_ok = hmac.compare_digest(password, ui_password)
        return username_ok and password_ok

    def wants_json_response() -> bool:
        requested_with = str(request.headers.get("X-Requested-With") or "").strip().lower()
        accept = str(request.headers.get("Accept") or "").strip().lower()
        return requested_with == "fetch" or "application/json" in accept

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
        camera.update(hub.get_camera_supported_controls_for_ui(camera_id))
        return camera

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

        privacy = config_payload.get("privacy") or {}
        if isinstance(privacy, dict) and "enabled" in privacy:
            payload["native_privacy_enabled"] = bool(privacy.get("enabled"))

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
                flash(f"Saved camera overrides for {camera_id}.", "success")
                return redirect(url_for("camera_detail", camera_id=camera_id))
            except Exception as error:
                flash(f"Failed to save camera overrides: {error}", "error")

        camera = hub.get_camera_for_ui(camera_id)
        camera.update(hub.get_camera_supported_controls_for_ui(camera_id))
        return render_template("camera_detail.html", camera=camera)

    @app.post("/camera/<camera_id>/hydrate")
    def hydrate_camera_detail(camera_id: str) -> Response:
        try:
            api_refresh = hub.queue_camera_api_refresh(camera_id)
        except Exception as error:
            api_refresh = f"error: {error}"

        try:
            onvif_refresh = hub.queue_camera_onvif_refresh(camera_id)
        except Exception as error:
            onvif_refresh = f"error: {error}"

        payload = {
            "ok": True,
            "camera": camera_detail_payload(camera_id),
            "refreshes": {
                "api": api_refresh,
                "onvif": onvif_refresh,
            },
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
        camera = hub.get_camera_history_for_ui(
            camera_id,
            limit=min(limit if limit is not None else 100, 500),
            kind_filter=str(request.args.get("kind") or "all").strip().lower(),
            sample_type_filter=str(request.args.get("sample_type") or "all").strip(),
        )
        return render_template("camera_history.html", camera=camera)

    @app.route("/config", methods=["GET", "POST"])
    def config_editor() -> str | Response:
        if request.method == "POST":
            try:
                new_config = _config_from_form(request.form)
                hub.save_config(new_config)
                if request.form.get("action") == "save-reload":
                    hub.reload_config()
                    flash("Configuration saved and reloaded.", "success")
                else:
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
        try:
            camera = hub.get_camera_for_ui(camera_id)
            enrollment = {
                "camera_id": camera_id,
                "ip": str(camera.get("ip") or "").strip(),
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
                url_for("camera_detail", camera_id=camera_id),
                camera_id=camera_id,
            )
        except Exception as error:
            return action_response(
                f"Connect failed for {camera_id}: {error}",
                "error",
                url_for("camera_detail", camera_id=camera_id),
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
        try:
            camera = hub.get_camera_for_ui(camera_id)
            enrollment = {
                "camera_id": camera_id,
                "ip": str(camera.get("ip") or "").strip(),
            }
            result = hub.install_pairing_bundle_via_mqtt(enrollment)
            status = str(result.get("status") or "success").strip().lower()
            if status == "warning":
                return action_response(
                    f"Pairing install was published for {camera_id}, but the camera did not confirm before the timeout.",
                    "warning",
                    url_for("camera_detail", camera_id=camera_id),
                    camera_id=camera_id,
                )
            return action_response(
                f"Pairing installed for {camera_id}; native API should come back after the agent restarts.",
                "success",
                url_for("camera_detail", camera_id=camera_id),
                camera_id=camera_id,
                reload=True,
            )
        except Exception as error:
            return action_response(
                f"Pairing failed for {camera_id}: {error}",
                "error",
                url_for("camera_detail", camera_id=camera_id),
                camera_id=camera_id,
                status_code=500,
            )

    @app.post("/rescan/<camera_id>")
    def rescan_one(camera_id: str) -> Response:
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
        raw_payload = str(request.form.get("config_patch") or "").strip()
        if not raw_payload:
            return action_response(
                "Native config patch payload is empty.",
                "error",
                url_for("camera_detail", camera_id=camera_id),
                camera_id=camera_id,
                status_code=400,
            )

        try:
            payload = json.loads(raw_payload)
        except json.JSONDecodeError as error:
            return action_response(
                f"Invalid native config patch JSON: {error}",
                "error",
                url_for("camera_detail", camera_id=camera_id),
                camera_id=camera_id,
                status_code=400,
            )

        if not isinstance(payload, dict):
            return action_response(
                "Native config patch must be a JSON object.",
                "error",
                url_for("camera_detail", camera_id=camera_id),
                camera_id=camera_id,
                status_code=400,
            )

        try:
            result = hub.patch_camera_config(camera_id, payload, refresh_after=False)
            return action_response(
                "Config patch applied.",
                "success",
                url_for("camera_detail", camera_id=camera_id),
                camera_payload=merge_camera_payloads(
                    supported_controls_delta_from_payload(payload),
                    latest_action_history_delta_payload(camera_id),
                ),
            )
        except Exception as error:
            return action_response(
                f"Config patch failed: {error}",
                "error",
                url_for("camera_detail", camera_id=camera_id),
                camera_payload={},
                status_code=500,
            )

    @app.post("/apply-supported-config/<camera_id>")
    def apply_supported_config(camera_id: str) -> Response:
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
                url_for("camera_detail", camera_id=camera_id),
                camera_payload=merge_camera_payloads(
                    supported_controls_delta_payload(request.form),
                    latest_action_history_delta_payload(camera_id),
                ),
            )
        except Exception as error:
            return action_response(
                f"Settings update failed: {error}",
                "error",
                url_for("camera_detail", camera_id=camera_id),
                camera_payload={},
                status_code=500,
            )

    @app.post("/send2-test/<camera_id>/<service_name>")
    def send2_test(camera_id: str, service_name: str) -> Response:
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
                    "redirect_url": url_for("camera_detail", camera_id=camera_id),
                    "send2_test": result,
                }
                return jsonify(payload)
            flash(f"Send2 test finished for {label}.", "success")
            return redirect(url_for("camera_detail", camera_id=camera_id))
        except Exception as error:
            label = service_name
            if send_type:
                label = f"{service_name} {send_type}"
            if wants_json_response():
                payload = {
                    "ok": False,
                    "message": f"Send2 test failed for {label}: {error}",
                    "category": "error",
                    "redirect_url": url_for("camera_detail", camera_id=camera_id),
                }
                response = jsonify(payload)
                response.status_code = 500
                return response
            flash(f"Send2 test failed for {label}: {error}", "error")
            return redirect(url_for("camera_detail", camera_id=camera_id))

    @app.post("/privacy/<camera_id>")
    def set_privacy(camera_id: str) -> Response:
        enabled_value = str(request.form.get("privacy_enabled") or "").strip().lower()
        enabled = enabled_value in {"1", "true", "yes", "on", "enabled"}
        channel = str(request.form.get("privacy_channel") or "all").strip() or "all"
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
            if result["retained_cleared"]:
                if result.get("command_published"):
                    return action_response(
                        f"Removed {camera_id} from the roster, asked the camera to revoke registration, and cleared its retained registration.",
                        "success",
                        url_for("dashboard"),
                    )
                else:
                    return action_response(
                        f"Removed {camera_id} from the roster and cleared its retained registration.",
                        "success",
                        url_for("dashboard"),
                    )
            elif result["config_removed"]:
                detail = result["retained_error"] or "camera may reappear if it republishes registration"
                return action_response(
                    f"Removed {camera_id} from saved config and current roster; retained unregister did not complete: {detail}",
                    "error",
                    url_for("dashboard"),
                    status_code=500,
                )
            else:
                detail = result["retained_error"] or "camera may reappear if it republishes registration"
                return action_response(
                    f"Removed {camera_id} from the current roster only; retained unregister did not complete: {detail}",
                    "error",
                    url_for("dashboard"),
                    status_code=500,
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
                camera = hub.get_camera_for_ui(camera_id)
            except Exception as error:
                LOG.warning("Snapshot preview lookup failed for %s", camera_id, exc_info=True)
                return Response(f"Snapshot preview unavailable: {error}\n", status=404, mimetype="text/plain")

            if not snapshot_url:
                return Response("Snapshot preview unavailable\n", status=404, mimetype="text/plain")

            request_to_camera = urllib.request.Request(snapshot_url, method="GET")
            api_key = str(camera.get("api_key") or "").strip()
            if api_key:
                request_to_camera.add_header("X-API-Key", api_key)

            try:
                with urllib.request.urlopen(request_to_camera, timeout=15) as upstream:
                    body = upstream.read()
                    content_type = upstream.headers.get("Content-Type", "image/jpeg")
            except urllib.error.HTTPError as error:
                detail = error.read().decode("utf-8", errors="replace").strip() or error.reason or f"HTTP {error.code}"
                return Response(f"Snapshot preview failed: {detail}\n", status=error.code, mimetype="text/plain")
            except urllib.error.URLError as error:
                return Response(f"Snapshot preview failed: {error.reason}\n", status=502, mimetype="text/plain")

            response = Response(body, mimetype=content_type)
            response.headers["Content-Type"] = content_type
            response.headers["Cache-Control"] = "no-store, no-cache, must-revalidate, max-age=0"
            response.headers["Pragma"] = "no-cache"
            response.headers["Expires"] = "0"
            return response

        try:
            cached_snapshot = hub.get_cached_snapshot_for_ui(camera_id)
        except Exception:
            LOG.warning("Snapshot preview lookup failed for %s; using fallback image", camera_id, exc_info=True)
            cached_snapshot = None

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
            camera = hub.get_camera_for_ui(camera_id)
        except Exception as error:
            LOG.warning("Live preview lookup failed for %s", camera_id, exc_info=True)
            return Response(f"Live preview unavailable: {error}\n", status=404, mimetype="text/plain")

        if not stream_url:
            return Response("Live preview unavailable\n", status=404, mimetype="text/plain")

        request_to_camera = urllib.request.Request(stream_url, method="GET")
        api_key = str(camera.get("api_key") or "").strip()
        if api_key:
            request_to_camera.add_header("X-API-Key", api_key)

        try:
            upstream = urllib.request.urlopen(request_to_camera, timeout=30)
        except urllib.error.HTTPError as error:
            detail = error.read().decode("utf-8", errors="replace").strip() or error.reason or f"HTTP {error.code}"
            return Response(f"Live preview failed: {detail}\n", status=error.code, mimetype="text/plain")
        except urllib.error.URLError as error:
            return Response(f"Live preview failed: {error.reason}\n", status=502, mimetype="text/plain")

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
        payload["daynight"] = daynight

    stream_field_pattern = re.compile(r"^(stream\d+)_(enabled|audio_enabled|width|height|fps|bitrate|format|mode|osd_enabled|osd_time_enabled|osd_usertext_enabled|osd_usertext_format)$")
    stream_present_pattern = re.compile(r"^(stream\d+)_(enabled|audio_enabled|osd_enabled|osd_time_enabled|osd_usertext_enabled)_present$")
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


def _config_from_form(form: Any, allow_partial: bool = False) -> dict[str, Any]:
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
        },
        "ui": {
            "username": form.get("ui_username", "").strip(),
            "password": form.get("ui_password", ""),
            "registration_stale_after_seconds": _int_value(form.get("ui_registration_stale_after_seconds"), 0),
            "snapshot_heartbeat_interval_seconds": _int_value(form.get("ui_snapshot_heartbeat_interval_seconds"), 60),
            "snapshot_heartbeat_timeout_seconds": _int_value(form.get("ui_snapshot_heartbeat_timeout_seconds"), 5),
            "snapshot_cache_stale_after_seconds": _int_value(form.get("ui_snapshot_cache_stale_after_seconds"), 3600),
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

    if allow_partial:
        return config

    from .main import load_config_dict

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