import json
import unittest
from unittest.mock import patch

from app.camera_api import CameraApiClient


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


if __name__ == "__main__":
    unittest.main()