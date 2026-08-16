import json
import mimetypes
import ssl
import socket
import http.client
import urllib.error
import urllib.parse
import urllib.request
from typing import Any


class CameraApiError(RuntimeError):
    pass


class CameraApiClient:
    _MAX_READ_ATTEMPTS = 3

    def __init__(self, base_url: str, token: str = "", timeout: int = 5) -> None:
        self.base_url = base_url.rstrip("/")
        self.token = token
        self.timeout = timeout

    def _control_timeout(self) -> int:
        return max(self.timeout, 15)

    def get_device(self) -> dict[str, Any]:
        return self._json_request("GET", "/device")

    def get_capabilities(self) -> dict[str, Any]:
        return self._json_request("GET", "/capabilities")

    def get_capability_group(self, group: str) -> dict[str, Any]:
        normalized = str(group or "").strip().strip("/")
        if not normalized:
            raise CameraApiError("Capability group is required")
        return self._json_request("GET", f"/capabilities/{normalized}")

    def get_state(self) -> dict[str, Any]:
        return self._json_request("GET", "/state")

    def get_runtime(self, resource: str) -> dict[str, Any]:
        normalized = str(resource or "").strip().strip("/")
        if not normalized:
            raise CameraApiError("Runtime resource is required")
        return self._json_request("GET", f"/runtime/{normalized}")

    def get_config(self, timeout: int | None = None) -> dict[str, Any]:
        # Full config reads are often slower than narrow /settings leaves.
        return self._json_request("GET", "/config", timeout=self.timeout if timeout is None else timeout)

    def patch_config(self, payload: dict[str, Any]) -> dict[str, Any]:
        # Some agent builds return an empty body on successful omnibus PATCH.
        return self._json_request(
            "PATCH",
            "/config",
            payload=payload,
            timeout=self._control_timeout(),
            allow_empty=True,
        )

    def control_service(self, service: str, operation: str) -> dict[str, Any]:
        normalized_service = urllib.parse.quote(str(service or "").strip().lower(), safe="")
        normalized_operation = urllib.parse.quote(str(operation or "").strip().lower(), safe="")
        return self._json_request(
            "POST",
            f"/actions/services/{normalized_service}/{normalized_operation}",
            timeout=self._control_timeout(),
        )

    def restart_streaming_service(self) -> dict[str, Any]:
        return self.control_service("streaming", "restart")

    def start_streaming_service(self) -> dict[str, Any]:
        return self.control_service("streaming", "start")

    def stop_streaming_service(self) -> dict[str, Any]:
        return self.control_service("streaming", "stop")

    def restart_streamer(self) -> dict[str, Any]:
        return self.restart_streaming_service()

    def set_privacy(self, enabled: bool, channel: str = "all") -> dict[str, Any]:
        return self._json_request(
            "POST",
            "/actions/privacy",
            payload={"enabled": enabled, "channel": channel},
            timeout=self._control_timeout(),
        )

    def set_daynight_mode(self, mode: str) -> dict[str, Any]:
        return self._json_request(
            "POST",
            "/actions/daynight",
            payload={"mode": str(mode).strip().lower()},
            timeout=self._control_timeout(),
        )

    def record_clip(self, duration_seconds: int = 10, stream_id: int = 0, path: str = "") -> dict[str, Any]:
        payload: dict[str, Any] = {"duration_seconds": duration_seconds, "stream_id": stream_id}
        if path:
            payload["path"] = path
        return self._json_request("POST", "/actions/record", payload=payload, timeout=self._control_timeout())

    def fetch_snapshot(self, stream_id: int = 0) -> tuple[bytes, str]:
        payload = {"stream_id": stream_id, "mode": "inline"}
        body, headers = self._request(
            "POST",
            "/actions/snapshot",
            payload=payload,
            accept="image/jpeg, application/json",
        )
        content_type = headers.get("Content-Type", "image/jpeg")
        extension = mimetypes.guess_extension(content_type.split(";", 1)[0].strip()) or ".jpg"
        return body, f"snapshot{extension}"

    def get_setting(self, path: str) -> dict[str, Any]:
        normalized = path.strip("/")
        return self._json_request("GET", f"/settings/{normalized}")

    def patch_setting(self, path: str, payload: dict[str, Any]) -> dict[str, Any]:
        normalized = path.strip("/")
        body, _headers = self._request(
            "PATCH",
            f"/settings/{normalized}",
            payload=payload,
            accept="application/json",
            timeout=self._control_timeout(),
        )
        if not body.strip():
            return {"status": "accepted"}
        try:
            decoded = json.loads(body.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError) as error:
            raise CameraApiError(f"Invalid JSON response for /settings/{normalized}: {error}") from error
        if not isinstance(decoded, dict):
            raise CameraApiError(f"Unexpected JSON response for /settings/{normalized}")
        return decoded

    def post_action(self, path: str, payload: dict[str, Any] | None = None, timeout: int | None = None) -> dict[str, Any]:
        normalized = path.strip("/")
        return self._json_request("POST", f"/actions/{normalized}", payload=payload, timeout=timeout or self._control_timeout())

    def probe(self) -> dict[str, Any]:
        # Legacy omnibus probe. Prefer probe_light() for routine hub refreshes.
        t = self._control_timeout()
        return {
            "device": self._json_request("GET", "/device", timeout=t),
            "capabilities": self._json_request("GET", "/capabilities", timeout=t),
            "state": self._json_request("GET", "/state", timeout=t),
        }

    def probe_light(self) -> dict[str, Any]:
        """Status probe using narrow routes (no /config, /state, or full /capabilities)."""
        t = self._control_timeout()
        return {
            "device": self._json_request("GET", "/device", timeout=t),
            "system": self.get_runtime("system"),
            "network": self.get_runtime("network"),
            "motion": self.get_runtime("motion"),
            "daynight": self.get_runtime("daynight"),
            "privacy": self.get_runtime("privacy"),
        }

    def diagnose_path(self, path: str, *, timeout: int | None = None, allow_empty: bool = False) -> dict[str, Any]:
        """Probe one API path and classify the outcome without raising."""
        normalized = "/" + str(path or "").strip().lstrip("/")
        try:
            body, headers = self._request("GET", normalized, accept="application/json", timeout=timeout)
        except CameraApiError as error:
            detail = str(error)
            lowered = detail.lower()
            kind = "error"
            if "errno 111" in lowered or "connection refused" in lowered:
                kind = "refused"
            elif "timed out" in lowered or "timeout" in lowered:
                kind = "timeout"
            elif "unauthorized" in lowered or "http 401" in lowered or "http_401" in lowered:
                kind = "unauthorized"
            elif "http 403" in lowered or "forbidden" in lowered:
                kind = "forbidden"
            elif "empty response" in lowered:
                kind = "empty"
            elif "invalid json" in lowered or "non-json" in lowered:
                kind = "invalid_json"
            return {"ok": False, "kind": kind, "detail": detail, "path": normalized}
        if not body.strip():
            if allow_empty:
                return {"ok": True, "kind": "empty_ok", "detail": "", "path": normalized, "bytes": 0}
            return {
                "ok": False,
                "kind": "empty",
                "detail": f"Empty response for {normalized}",
                "path": normalized,
                "bytes": 0,
            }
        try:
            decoded = json.loads(body.decode("utf-8"))
        except Exception as error:
            return {
                "ok": False,
                "kind": "invalid_json",
                "detail": str(error),
                "path": normalized,
                "bytes": len(body),
            }
        if not isinstance(decoded, dict):
            return {
                "ok": False,
                "kind": "invalid_json",
                "detail": f"Unexpected JSON type for {normalized}",
                "path": normalized,
                "bytes": len(body),
            }
        return {
            "ok": True,
            "kind": "ok",
            "detail": "",
            "path": normalized,
            "bytes": len(body),
            "keys": sorted(str(key) for key in decoded.keys())[:12],
        }

    def try_get_setting(self, path: str) -> dict[str, Any] | None:
        try:
            return self.get_setting(path)
        except Exception:
            return None

    def try_get_runtime(self, resource: str) -> dict[str, Any] | None:
        try:
            return self.get_runtime(resource)
        except Exception:
            return None

    def try_get_capability_group(self, group: str) -> dict[str, Any] | None:
        try:
            return self.get_capability_group(group)
        except Exception:
            return None

    def stream_events(self) -> Any:
        url = f"{self.base_url}/events"
        headers = {"Accept": "text/event-stream"}
        if self.token:
            headers["Authorization"] = f"Bearer {self.token}"

        request = urllib.request.Request(url, headers=headers, method="GET")
        open_kwargs: dict[str, Any] = {"timeout": max(self.timeout, 60)}
        if urllib.parse.urlsplit(url).scheme == "https":
            open_kwargs["context"] = ssl._create_unverified_context()

        try:
            with urllib.request.urlopen(request, **open_kwargs) as response:
                event_name = "message"
                data_lines: list[str] = []
                while True:
                    raw_line = response.readline()
                    if raw_line == b"":
                        break

                    line = raw_line.decode("utf-8", errors="replace").rstrip("\r\n")
                    if not line:
                        if data_lines:
                            payload_text = "\n".join(data_lines)
                            try:
                                payload = json.loads(payload_text)
                            except json.JSONDecodeError:
                                payload = payload_text
                            yield {
                                "event": event_name or "message",
                                "data": payload,
                            }
                        event_name = "message"
                        data_lines = []
                        continue

                    if line.startswith(":"):
                        continue
                    if line.startswith("event:"):
                        event_name = line.split(":", 1)[1].strip() or "message"
                        continue
                    if line.startswith("data:"):
                        data_lines.append(line.split(":", 1)[1].lstrip())
        except urllib.error.HTTPError as error:
            body = error.read().decode("utf-8", errors="replace").strip()
            detail = body or error.reason or f"HTTP {error.code}"
            raise CameraApiError(f"GET /events failed: {detail}") from error
        except (urllib.error.URLError, TimeoutError, socket.timeout) as error:
            reason = getattr(error, "reason", None) or str(error)
            raise CameraApiError(f"GET /events failed: {reason}") from error

    def _json_request(
        self,
        method: str,
        path: str,
        payload: dict[str, Any] | None = None,
        timeout: int | None = None,
        *,
        allow_empty: bool = False,
    ) -> dict[str, Any]:
        body, _headers = self._request(method, path, payload=payload, accept="application/json", timeout=timeout)
        if not body.strip():
            if allow_empty:
                return {"status": "accepted"}
            raise CameraApiError(
                f"Empty response for {path} — native API returned no JSON "
                "(agent overloaded, wrong token, or listener not ready)"
            )
        try:
            text = body.decode("utf-8")
        except UnicodeDecodeError as error:
            raise CameraApiError(f"Invalid JSON response for {path}: {error}") from error
        try:
            decoded = json.loads(text)
        except json.JSONDecodeError as error:
            preview = " ".join(text.strip().split())
            if len(preview) > 120:
                preview = preview[:117] + "..."
            raise CameraApiError(f"Invalid JSON response for {path}: {error} (body starts: {preview!r})") from error
        if not isinstance(decoded, dict):
            raise CameraApiError(f"Unexpected JSON response for {path}")
        return decoded

    def _request(
        self,
        method: str,
        path: str,
        payload: dict[str, Any] | None = None,
        accept: str = "application/json",
        timeout: int | None = None,
    ) -> tuple[bytes, Any]:
        url = f"{self.base_url}{path}"
        headers = {"Accept": accept}
        data = None
        if payload is not None:
            data = json.dumps(payload).encode("utf-8")
            headers["Content-Type"] = "application/json"
        if self.token:
            headers["Authorization"] = f"Bearer {self.token}"

        request = urllib.request.Request(url, data=data, headers=headers, method=method)
        try:
            open_kwargs: dict[str, Any] = {"timeout": self.timeout if timeout is None else timeout}
            if urllib.parse.urlsplit(url).scheme == "https":
                open_kwargs["context"] = ssl._create_unverified_context()
            for attempt in range(self._MAX_READ_ATTEMPTS):
                try:
                    with urllib.request.urlopen(request, **open_kwargs) as response:
                        return response.read(), response.headers
                except http.client.IncompleteRead as error:
                    if attempt + 1 >= self._MAX_READ_ATTEMPTS:
                        raise CameraApiError(f"{method} {path} failed: {error}") from error
        except urllib.error.HTTPError as error:
            body = error.read().decode("utf-8", errors="replace").strip()
            detail = body or error.reason or f"HTTP {error.code}"
            raise CameraApiError(f"{method} {path} failed: {detail}") from error
        except urllib.error.URLError as error:
            raise CameraApiError(f"{method} {path} failed: {error.reason}") from error