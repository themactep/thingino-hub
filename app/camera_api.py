import json
import mimetypes
import ssl
import urllib.error
import urllib.parse
import urllib.request
from typing import Any


class CameraApiError(RuntimeError):
    pass


class CameraApiClient:
    def __init__(self, base_url: str, token: str = "", timeout: int = 5) -> None:
        self.base_url = base_url.rstrip("/")
        self.token = token
        self.timeout = timeout

    def get_device(self) -> dict[str, Any]:
        return self._json_request("GET", "/device")

    def get_capabilities(self) -> dict[str, Any]:
        return self._json_request("GET", "/capabilities")

    def get_state(self) -> dict[str, Any]:
        return self._json_request("GET", "/state")

    def get_config(self) -> dict[str, Any]:
        return self._json_request("GET", "/config")

    def patch_config(self, payload: dict[str, Any]) -> dict[str, Any]:
        return self._json_request("PATCH", "/config", payload=payload)

    def control_service(self, service: str, operation: str) -> dict[str, Any]:
        normalized_service = urllib.parse.quote(str(service or "").strip().lower(), safe="")
        normalized_operation = urllib.parse.quote(str(operation or "").strip().lower(), safe="")
        return self._json_request("POST", f"/actions/services/{normalized_service}/{normalized_operation}")

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
        )

    def set_daynight_mode(self, mode: str) -> dict[str, Any]:
        return self._json_request(
            "POST",
            "/actions/daynight",
            payload={"mode": str(mode).strip().lower()},
        )

    def record_clip(self, duration_seconds: int = 10, stream_id: int = 0, path: str = "") -> dict[str, Any]:
        payload: dict[str, Any] = {"duration_seconds": duration_seconds, "stream_id": stream_id}
        if path:
            payload["path"] = path
        return self._json_request("POST", "/actions/record", payload=payload)

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

    def probe(self) -> dict[str, Any]:
        return {
            "device": self.get_device(),
            "capabilities": self.get_capabilities(),
            "state": self.get_state(),
        }

    def _json_request(self, method: str, path: str, payload: dict[str, Any] | None = None) -> dict[str, Any]:
        body, _headers = self._request(method, path, payload=payload, accept="application/json")
        try:
            decoded = json.loads(body.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError) as error:
            raise CameraApiError(f"Invalid JSON response for {path}: {error}") from error
        if not isinstance(decoded, dict):
            raise CameraApiError(f"Unexpected JSON response for {path}")
        return decoded

    def _request(
        self,
        method: str,
        path: str,
        payload: dict[str, Any] | None = None,
        accept: str = "application/json",
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
            open_kwargs: dict[str, Any] = {"timeout": self.timeout}
            if urllib.parse.urlsplit(url).scheme == "https":
                open_kwargs["context"] = ssl._create_unverified_context()
            with urllib.request.urlopen(request, **open_kwargs) as response:
                return response.read(), response.headers
        except urllib.error.HTTPError as error:
            body = error.read().decode("utf-8", errors="replace").strip()
            detail = body or error.reason or f"HTTP {error.code}"
            raise CameraApiError(f"{method} {path} failed: {detail}") from error
        except urllib.error.URLError as error:
            raise CameraApiError(f"{method} {path} failed: {error.reason}") from error