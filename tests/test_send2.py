"""
Tests for send2 configuration and test-action flows.

Regression coverage for:
- send-photo / send-video PATCH payloads must use the field name matching
  the resource leaf, not "enabled" (caught when toggling send_video on telegram)
- test-photo / test-video actions must route to the correct action path
  send2/{service}/test[-photo|-video] (caught when "test video" returned 404)
"""

import json
import sys
import threading
import types
import unittest
from typing import Any
from unittest.mock import MagicMock, call, patch

if "paho" not in sys.modules:
    paho_module = types.ModuleType("paho")
    mqtt_package = types.ModuleType("paho.mqtt")
    client_module = types.ModuleType("paho.mqtt.client")
    client_module.Client = type("Client", (), {})
    client_module.MQTTMessage = type("MQTTMessage", (), {})
    client_module.CallbackAPIVersion = type("CallbackAPIVersion", (), {"VERSION2": object()})
    client_module.MQTT_ERR_SUCCESS = 0
    mqtt_package.client = client_module
    paho_module.mqtt = mqtt_package
    sys.modules["paho"] = paho_module
    sys.modules["paho.mqtt"] = mqtt_package
    sys.modules["paho.mqtt.client"] = client_module

from app.camera_api import CameraApiClient
from app.main import Camera, Hub
from app.web import create_web_app


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

class _FakeResponse:
    """Minimal urllib response mock."""

    def __init__(self, body: bytes) -> None:
        self._body = body
        self.headers: dict[str, str] = {"Content-Type": "application/json"}

    def read(self) -> bytes:
        return self._body

    def __enter__(self) -> "_FakeResponse":
        return self

    def __exit__(self, *_: Any) -> None:
        pass


def _json_response(data: Any) -> _FakeResponse:
    return _FakeResponse(json.dumps(data).encode())


def _make_hub() -> Hub:
    """Minimal Hub wired to a single in-memory camera (no MQTT, no threads)."""
    config: dict[str, Any] = {
        "mqtt": {"host": "127.0.0.1", "port": 1883, "username": "", "password": ""},
        "routing": {
            "command_topic_template": "thingino/cam/{camera_id}/cmd",
            "reply_topic": "thingino/hub/reply",
        },
        "cameras": [
            {
                "id": "cam1",
                "name": "Test Camera",
                "ip": "192.168.1.2",
                "snapshot_url": "http://192.168.1.2/x/ch0.jpg",
                "api_base_url": "https://192.168.1.2:1998/api/v1",
                "api_token": "test-token",
            }
        ],
        "ui": {},
    }
    hub = Hub.__new__(Hub)
    # Initialise only what the send2 methods need
    hub.state_lock = threading.Lock()
    hub.history_lock = threading.Lock()
    hub.api_refreshing: set[str] = set()
    hub.history_store = None
    hub.snapshot_heartbeat_timeout_seconds = 5
    hub.cameras = {
        "cam1": Camera(
            camera_id="cam1",
            name="Test Camera",
            ip="192.168.1.2",
            api_base_url="https://192.168.1.2:1998/api/v1",
            api_token="test-token",
        )
    }
    # Silence persistence / history side-effects
    hub._persist_state = MagicMock()  # type: ignore[method-assign]
    hub._record_native_action = MagicMock()  # type: ignore[method-assign]
    hub._record_history_config_changes = MagicMock()  # type: ignore[method-assign]
    hub._resolve_camera_id = lambda cid: cid  # type: ignore[method-assign]
    return hub


# ---------------------------------------------------------------------------
# CameraApiClient — patch_setting payload keys
# ---------------------------------------------------------------------------

class Send2PatchSettingKeyTests(unittest.TestCase):
    """The field name in the PATCH body must match the resource leaf name,
    not the generic 'enabled' key accepted by other endpoints."""

    def _client_with_mock(self) -> tuple[CameraApiClient, MagicMock]:
        client = CameraApiClient("https://camera/api/v1", token="tok", timeout=15)
        response = _json_response({"status": "accepted", "resource": {"send_photo": True}})
        mock = patch("urllib.request.urlopen", return_value=response).__enter__()
        return client, mock

    def test_send_photo_patch_uses_send_photo_key(self) -> None:
        client = CameraApiClient("https://camera/api/v1", token="tok", timeout=15)
        accepted = _json_response({"status": "accepted"})

        with patch("urllib.request.urlopen", return_value=accepted) as urlopen:
            client.patch_setting("send2/services/telegram/send-photo", {"send_photo": True})

        body = json.loads(urlopen.call_args[0][0].data)
        self.assertIn("send_photo", body, "Payload must contain 'send_photo' key")
        self.assertNotIn("enabled", body, "Payload must NOT contain generic 'enabled' key")

    def test_send_video_patch_uses_send_video_key(self) -> None:
        client = CameraApiClient("https://camera/api/v1", token="tok", timeout=15)
        accepted = _json_response({"status": "accepted"})

        with patch("urllib.request.urlopen", return_value=accepted) as urlopen:
            client.patch_setting("send2/services/telegram/send-video", {"send_video": True})

        body = json.loads(urlopen.call_args[0][0].data)
        self.assertIn("send_video", body, "Payload must contain 'send_video' key")
        self.assertNotIn("enabled", body, "Payload must NOT contain generic 'enabled' key")

    def test_send_photo_false_patch_uses_send_photo_key(self) -> None:
        client = CameraApiClient("https://camera/api/v1", token="tok", timeout=15)
        accepted = _json_response({"status": "accepted"})

        with patch("urllib.request.urlopen", return_value=accepted) as urlopen:
            client.patch_setting("send2/services/email/send-photo", {"send_photo": False})

        body = json.loads(urlopen.call_args[0][0].data)
        self.assertEqual(body.get("send_photo"), False)
        self.assertNotIn("enabled", body)


# ---------------------------------------------------------------------------
# Hub.update_camera_send2_config — correct keys forwarded to patch_setting
# ---------------------------------------------------------------------------

class Send2ConfigUpdateTests(unittest.TestCase):
    """Hub must forward send_photo / send_video keys to the camera, not 'enabled'."""

    def setUp(self) -> None:
        self.hub = _make_hub()

    def _capture_patch_calls(self) -> list[call]:
        """Run update_camera_send2_config and return all patch_setting calls."""
        recorded: list[call] = []
        accepted = _json_response({"status": "accepted"})

        original = CameraApiClient.patch_setting

        def recording_patch(self_inner: CameraApiClient, path: str, payload: dict[str, Any]) -> Any:
            recorded.append(call(path, payload))
            return {"status": "accepted"}

        with patch.object(CameraApiClient, "patch_setting", recording_patch):
            self.hub.update_camera_send2_config("cam1", {
                "telegram": {"send_photo": True, "send_video": False},
            })
        return recorded

    def test_send_photo_payload_key(self) -> None:
        calls = self._capture_patch_calls()
        photo_call = next((c for c in calls if "send-photo" in c.args[0]), None)
        self.assertIsNotNone(photo_call, "Expected a patch_setting call for send-photo")
        self.assertIn("send_photo", photo_call.args[1])
        self.assertNotIn("enabled", photo_call.args[1])

    def test_send_video_payload_key(self) -> None:
        calls = self._capture_patch_calls()
        video_call = next((c for c in calls if "send-video" in c.args[0]), None)
        self.assertIsNotNone(video_call, "Expected a patch_setting call for send-video")
        self.assertIn("send_video", video_call.args[1])
        self.assertNotIn("enabled", video_call.args[1])

    def test_send_photo_value_is_bool(self) -> None:
        calls = self._capture_patch_calls()
        photo_call = next(c for c in calls if "send-photo" in c.args[0])
        self.assertIs(photo_call.args[1]["send_photo"], True)

    def test_send_video_value_is_bool(self) -> None:
        calls = self._capture_patch_calls()
        video_call = next(c for c in calls if "send-video" in c.args[0])
        self.assertIs(video_call.args[1]["send_video"], False)

    def test_motion_output_still_uses_enabled_key(self) -> None:
        """motion/outputs/send2/* legitimately uses 'enabled'."""
        recorded: list[call] = []

        def recording_patch(self_inner: CameraApiClient, path: str, payload: dict[str, Any]) -> Any:
            recorded.append(call(path, payload))
            return {"status": "accepted"}

        with patch.object(CameraApiClient, "patch_setting", recording_patch):
            self.hub.update_camera_send2_config("cam1", {
                "motion": {"send2telegram": True},
            })

        motion_call = next((c for c in recorded if "motion/outputs/send2/" in c.args[0]), None)
        self.assertIsNotNone(motion_call)
        self.assertIn("enabled", motion_call.args[1])


# ---------------------------------------------------------------------------
# Hub.test_camera_send2_service — correct action paths
# ---------------------------------------------------------------------------

class Send2TestActionPathTests(unittest.TestCase):
    """test_camera_send2_service must build the right action path for each type."""

    def setUp(self) -> None:
        self.hub = _make_hub()

    def _run_test(self, send_type: str) -> str:
        """Return the action path that post_action was called with."""
        captured: list[str] = []

        def fake_post_action(self_inner: CameraApiClient, path: str, payload: Any = None, timeout: Any = None) -> Any:
            captured.append(path)
            return {"status": "ok"}

        with patch.object(CameraApiClient, "post_action", fake_post_action):
            self.hub.test_camera_send2_service("cam1", "telegram", send_type=send_type)

        return captured[0]

    def test_plain_test_action_path(self) -> None:
        path = self._run_test("")
        self.assertEqual(path, "send2/telegram/test")

    def test_photo_test_action_path(self) -> None:
        path = self._run_test("photo")
        self.assertEqual(path, "send2/telegram/test-photo")

    def test_video_test_action_path(self) -> None:
        path = self._run_test("video")
        self.assertEqual(path, "send2/telegram/test-video")

    def test_invalid_type_raises(self) -> None:
        with self.assertRaises(RuntimeError):
            self.hub.test_camera_send2_service("cam1", "telegram", send_type="gif")

    def test_invalid_service_raises(self) -> None:
        with self.assertRaises(RuntimeError):
            self.hub.test_camera_send2_service("cam1", "whatsapp")

    def test_timeout_returns_accepted(self) -> None:
        """A timeout on the test action should be treated as success (fire-and-forget)."""
        from app.camera_api import CameraApiError

        def timed_out(self_inner: CameraApiClient, path: str, payload: Any = None, timeout: Any = None) -> Any:
            raise CameraApiError("timed out")

        with patch.object(CameraApiClient, "post_action", timed_out):
            result = self.hub.test_camera_send2_service("cam1", "telegram", send_type="video")

        self.assertEqual(result["status"], "accepted")
        self.assertTrue(result.get("timeout_waiting_for_response"))

    def test_status_error_in_response_raises(self) -> None:
        """Camera returning {"status":"error"} must be surfaced as an exception.

        The agent listener returns HTTP 200 even when the adapter shell exits
        non-zero; the hub must check the status field so the UI shows the
        failure instead of silently swallowing it.
        """
        def error_response(self_inner: CameraApiClient, path: str, payload: Any = None, timeout: Any = None) -> Any:
            return {"status": "error", "message": "send2 test failed"}

        with patch.object(CameraApiClient, "post_action", error_response):
            with self.assertRaises(RuntimeError) as ctx:
                self.hub.test_camera_send2_service("cam1", "telegram", send_type="photo")

        self.assertIn("send2 test failed", str(ctx.exception))


# ---------------------------------------------------------------------------
# Web route — /send2-test/<camera_id>/<service_name>
# ---------------------------------------------------------------------------

class _FakeHubForWeb:
    """Minimal FakeHub for web route tests."""

    def __init__(self) -> None:
        self.last_test_call: dict[str, Any] = {}

    def test_camera_send2_service(self, camera_id: str, service_name: str, *, verbose: bool = True, send_type: str = "") -> dict[str, Any]:
        self.last_test_call = {"camera_id": camera_id, "service": service_name, "verbose": verbose, "send_type": send_type}
        return {"status": "accepted"}

    def get_camera_for_ui(self, camera_id: str) -> dict[str, Any]:
        return {"camera_id": camera_id, "name": "Test", "api_status": "online"}

    # Stubs required by create_web_app internals
    def list_recent_events_for_ui(self, limit: int = 40) -> list:
        return []

    def live_events_since(self, last_sequence: int, limit: int = 20) -> tuple:
        return 0, []


class Send2WebRouteTests(unittest.TestCase):
    def setUp(self) -> None:
        self.hub = _FakeHubForWeb()
        app = create_web_app(self.hub)
        app.config["TESTING"] = True
        app.config["WTF_CSRF_ENABLED"] = False
        self.client = app.test_client()

    def _post(self, camera_id: str, service: str, form: dict[str, str] | None = None) -> Any:
        return self.client.post(
            f"/send2-test/{camera_id}/{service}",
            data=form or {},
            headers={"Accept": "application/json"},
        )

    def test_send2_test_calls_hub_plain(self) -> None:
        resp = self._post("cam1", "telegram")
        self.assertEqual(resp.status_code, 200)
        self.assertEqual(self.hub.last_test_call["send_type"], "")
        self.assertEqual(self.hub.last_test_call["service"], "telegram")

    def test_send2_test_calls_hub_with_photo_type(self) -> None:
        resp = self._post("cam1", "telegram", {"type": "photo"})
        self.assertEqual(resp.status_code, 200)
        self.assertEqual(self.hub.last_test_call["send_type"], "photo")

    def test_send2_test_calls_hub_with_video_type(self) -> None:
        resp = self._post("cam1", "telegram", {"type": "video"})
        self.assertEqual(resp.status_code, 200)
        self.assertEqual(self.hub.last_test_call["send_type"], "video")

    def test_send2_test_returns_json_ok(self) -> None:
        resp = self._post("cam1", "telegram", {"type": "video"})
        data = json.loads(resp.data)
        self.assertTrue(data["ok"])
        self.assertEqual(data["category"], "success")

    def test_send2_test_error_returns_error_json(self) -> None:
        def boom(*a: Any, **kw: Any) -> None:
            raise RuntimeError("camera unavailable")

        self.hub.test_camera_send2_service = boom  # type: ignore[method-assign]
        resp = self._post("cam1", "telegram", {"type": "video"})
        data = json.loads(resp.data)
        self.assertFalse(data["ok"])
        self.assertEqual(data["category"], "error")
        self.assertIn("camera unavailable", data["message"])


# ---------------------------------------------------------------------------
# Hub._camera_send2_controls_for_ui — reads send2 settings from endpoints
# ---------------------------------------------------------------------------

class Send2ControlsForUiTests(unittest.TestCase):
    """_camera_send2_controls_for_ui must read send_photo/send_video from
    /settings/send2/services/{service}/send-photo|send-video, not from
    backend.raw.send2 (which is always absent from the config response)."""

    def setUp(self) -> None:
        self.hub = _make_hub()

    def _make_api_client(
        self,
        capabilities: dict[str, Any],
        settings: dict[str, Any],
    ) -> MagicMock:
        """Return a mock CameraApiClient with canned responses."""
        client = MagicMock(spec=CameraApiClient)
        client.get_config.return_value = {
            "backend": {"raw": {"motion": {"send2telegram": True, "sensitivity": 5}}}
        }
        client.get_capabilities.return_value = capabilities
        def _get_setting(path: str) -> dict[str, Any]:
            return settings.get(path, {})
        client.get_setting.side_effect = _get_setting
        return client

    def _run(self, capabilities: dict[str, Any], settings: dict[str, Any]) -> list[dict[str, Any]]:
        client = self._make_api_client(capabilities, settings)
        with patch.object(self.hub, "_camera_api_client", return_value=client):
            result = self.hub._camera_send2_controls_for_ui(self.hub.cameras["cam1"])
        return result["native_send2_services"]

    def _service(self, services: list[dict[str, Any]], name: str) -> dict[str, Any]:
        return next(s for s in services if s["name"] == name)

    def test_photo_enabled_reads_from_settings_endpoint(self) -> None:
        caps = {"send2": {"telegram": {"send_photo": True, "send_video": False}}}
        settings = {"send2/services/telegram/send-photo": {"send_photo": False}}
        services = self._run(caps, settings)
        self.assertFalse(self._service(services, "telegram")["photo_enabled"])

    def test_video_enabled_reads_from_settings_endpoint(self) -> None:
        caps = {"send2": {"telegram": {"send_photo": True, "send_video": True}}}
        settings = {
            "send2/services/telegram/send-photo": {"send_photo": True},
            "send2/services/telegram/send-video": {"send_video": True},
        }
        services = self._run(caps, settings)
        self.assertTrue(self._service(services, "telegram")["video_enabled"])

    def test_video_disabled_reads_from_settings_endpoint(self) -> None:
        caps = {"send2": {"telegram": {"send_photo": True, "send_video": True}}}
        settings = {
            "send2/services/telegram/send-photo": {"send_photo": True},
            "send2/services/telegram/send-video": {"send_video": False},
        }
        services = self._run(caps, settings)
        self.assertFalse(self._service(services, "telegram")["video_enabled"])

    def test_service_with_no_video_capability_always_has_video_disabled(self) -> None:
        caps = {"send2": {"mqtt": {"send_photo": True, "send_video": False}}}
        settings = {"send2/services/mqtt/send-photo": {"send_photo": True}}
        services = self._run(caps, settings)
        svc = self._service(services, "mqtt")
        self.assertFalse(svc["video_enabled"])

    def test_send2_not_in_config_raw_does_not_affect_result(self) -> None:
        """Even if backend.raw has no send2 key, values come from settings endpoints."""
        caps = {"send2": {"ftp": {"send_photo": True, "send_video": True}}}
        settings = {
            "send2/services/ftp/send-photo": {"send_photo": True},
            "send2/services/ftp/send-video": {"send_video": True},
        }
        services = self._run(caps, settings)
        svc = self._service(services, "ftp")
        self.assertTrue(svc["photo_enabled"])
        self.assertTrue(svc["video_enabled"])

    def test_photo_enabled_defaults_true_when_key_absent_from_send2_json(self) -> None:
        """If send_photo is absent from send2.json, the firmware defaults to true.
        The settings endpoint will return {"send_photo": true}, and hub must honour it."""
        caps = {"send2": {"gphotos": {"send_photo": True, "send_video": True}}}
        # Simulate firmware returning true (its default when key is absent)
        settings = {
            "send2/services/gphotos/send-photo": {"send_photo": True},
            "send2/services/gphotos/send-video": {"send_video": False},
        }
        services = self._run(caps, settings)
        self.assertTrue(self._service(services, "gphotos")["photo_enabled"])


        caps = {"send2": {"telegram": {"send_photo": True, "send_video": False}}}
        settings = {"send2/services/telegram/send-photo": {"send_photo": True}}
        services = self._run(caps, settings)
        # motion.send2telegram = True in our fake config
        self.assertTrue(self._service(services, "telegram")["motion_enabled"])


if __name__ == "__main__":
    unittest.main()
