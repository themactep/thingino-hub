import json
import http.client
import unittest
from unittest.mock import patch

from app.camera_api import CameraApiClient, CameraApiError


class _FakeResponse:
    def __init__(self, body: bytes) -> None:
        self._body = body
        self.headers = {"Content-Type": "application/json"}

    def read(self) -> bytes:
        return self._body

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        return None


class CameraApiClientTests(unittest.TestCase):
    def test_get_uses_base_timeout(self) -> None:
        client = CameraApiClient("https://camera/api/v1", token="token", timeout=5)
        body = json.dumps({"status": "ok"}).encode("utf-8")

        with patch("urllib.request.urlopen", return_value=_FakeResponse(body)) as urlopen:
            client.get_device()

        self.assertEqual(urlopen.call_args.kwargs["timeout"], 5)

    def test_patch_uses_control_timeout_floor(self) -> None:
        client = CameraApiClient("https://camera/api/v1", token="token", timeout=5)
        body = json.dumps({"status": "accepted"}).encode("utf-8")

        with patch("urllib.request.urlopen", return_value=_FakeResponse(body)) as urlopen:
            client.patch_config({"motion": {"enabled": True}})

        self.assertEqual(urlopen.call_args.kwargs["timeout"], 15)

    def test_patch_keeps_longer_custom_timeout(self) -> None:
        client = CameraApiClient("https://camera/api/v1", token="token", timeout=20)
        body = json.dumps({"status": "accepted"}).encode("utf-8")

        with patch("urllib.request.urlopen", return_value=_FakeResponse(body)) as urlopen:
            client.patch_config({"motion": {"enabled": True}})

        self.assertEqual(urlopen.call_args.kwargs["timeout"], 20)

    def test_patch_setting_accepts_empty_success_response(self) -> None:
        client = CameraApiClient("https://camera/api/v1", token="token", timeout=5)

        with patch("urllib.request.urlopen", return_value=_FakeResponse(b"")):
            result = client.patch_setting("send2/services/mqtt/send-video", {"send_video": False})

        self.assertEqual(result, {"status": "accepted"})

    def test_patch_setting_still_rejects_non_json_non_empty_response(self) -> None:
        client = CameraApiClient("https://camera/api/v1", token="token", timeout=5)

        with patch("urllib.request.urlopen", return_value=_FakeResponse(b"ok")):
            with self.assertRaisesRegex(RuntimeError, "Invalid JSON response for /settings/send2/services/mqtt/send-video"):
                client.patch_setting("send2/services/mqtt/send-video", {"send_video": False})

    def test_get_retries_on_incomplete_read(self) -> None:
        client = CameraApiClient("https://camera/api/v1", token="token", timeout=5)
        body = json.dumps({"status": "ok"}).encode("utf-8")

        with patch(
            "urllib.request.urlopen",
            side_effect=[http.client.IncompleteRead(b"partial", 10), _FakeResponse(body)],
        ) as urlopen:
            result = client.get_device()

        self.assertEqual(result, {"status": "ok"})
        self.assertEqual(urlopen.call_count, 2)

    def test_get_raises_after_repeated_incomplete_read(self) -> None:
        client = CameraApiClient("https://camera/api/v1", token="token", timeout=5)

        with patch(
            "urllib.request.urlopen",
            side_effect=[
                http.client.IncompleteRead(b"partial", 10),
                http.client.IncompleteRead(b"partial", 10),
                http.client.IncompleteRead(b"partial", 10),
            ],
        ) as urlopen:
            with self.assertRaisesRegex(CameraApiError, r"GET /device failed: IncompleteRead"):
                client.get_device()

        self.assertEqual(urlopen.call_count, 3)

    def test_probe_light_uses_narrow_runtime_routes(self) -> None:
        client = CameraApiClient("https://camera/api/v1", token="token", timeout=5)
        responses = {
            "/device": {"id": "cam1", "name": "Cam"},
            "/runtime/system": {"streamer_running": True},
            "/runtime/network": {"online": True, "ip": "1.2.3.4"},
            "/runtime/motion": {"enabled": False},
            "/runtime/daynight": {"target_mode": "auto", "running_mode": "day"},
            "/runtime/privacy": {"enabled": False},
        }

        def fake_urlopen(request, timeout=None, context=None):
            path = request.full_url.split("/api/v1", 1)[1]
            return _FakeResponse(json.dumps(responses[path]).encode("utf-8"))

        with patch("urllib.request.urlopen", side_effect=fake_urlopen) as urlopen:
            payload = client.probe_light()

        self.assertEqual(payload["device"]["id"], "cam1")
        self.assertTrue(payload["system"]["streamer_running"])
        self.assertEqual(payload["network"]["ip"], "1.2.3.4")
        requested = [call.args[0].full_url for call in urlopen.call_args_list]
        self.assertTrue(any(url.endswith("/device") for url in requested))
        self.assertTrue(any("/runtime/system" in url for url in requested))
        self.assertFalse(any(url.endswith("/state") for url in requested))
        self.assertFalse(any(url.endswith("/config") for url in requested))
        self.assertFalse(any(url.endswith("/capabilities") for url in requested))

    def test_try_get_setting_returns_none_on_failure(self) -> None:
        client = CameraApiClient("https://camera/api/v1", token="token", timeout=5)

        with patch("urllib.request.urlopen", side_effect=RuntimeError("boom")):
            self.assertIsNone(client.try_get_setting("image/hflip"))


if __name__ == "__main__":
    unittest.main()