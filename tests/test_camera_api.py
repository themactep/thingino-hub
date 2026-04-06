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


if __name__ == "__main__":
    unittest.main()