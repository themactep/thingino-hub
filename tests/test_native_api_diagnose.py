import sys
import types
import unittest
from unittest import mock


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


class NativeApiDiagnoseTests(unittest.TestCase):
    def test_diagnose_path_classifies_empty(self) -> None:
        client = CameraApiClient("https://192.168.1.2:1998/api/v1", token="t", timeout=1)
        with mock.patch.object(client, "_request", return_value=(b"", {})):
            result = client.diagnose_path("/config")
        self.assertFalse(result["ok"])
        self.assertEqual(result["kind"], "empty")

    def test_hub_maps_empty_config_to_wedge(self) -> None:
        hub = object.__new__(Hub)
        camera = Camera(
            camera_id="cam1",
            name="Cam",
            ip="192.168.1.2",
            api_base_url="https://192.168.1.2:1998/api/v1",
            api_token="token",
            api_status="offline",
        )
        hub._camera_api_base_url = lambda cam: cam.api_base_url  # type: ignore[method-assign]
        hub._camera_api_token = lambda cam: cam.api_token  # type: ignore[method-assign]
        hub._camera_registration_status_for_ui = lambda cam: "online"  # type: ignore[method-assign]

        responses = {
            ("", "/device"): {"ok": False, "kind": "unauthorized", "detail": "401", "path": "/device"},
            ("token", "/device"): {"ok": True, "kind": "ok", "detail": "", "path": "/device", "bytes": 10},
            ("token", "/capabilities"): {"ok": True, "kind": "ok", "detail": "", "path": "/capabilities", "bytes": 20},
            ("token", "/config"): {"ok": False, "kind": "empty", "detail": "Empty", "path": "/config", "bytes": 0},
            ("token", "/settings/image/brightness"): {
                "ok": True,
                "kind": "ok",
                "detail": "",
                "path": "/settings/image/brightness",
                "bytes": 8,
            },
        }

        class FakeClient:
            def __init__(self, base_url: str, token: str = "", timeout: int = 5) -> None:
                self.token = token

            def diagnose_path(self, path: str, *, timeout: int | None = None, allow_empty: bool = False) -> dict:
                return dict(responses[(self.token, path)])

        with mock.patch("app.main.CameraApiClient", FakeClient):
            result = Hub._diagnose_camera_native_api(hub, camera)
        self.assertEqual(result["phase"], "config_wedge")
        self.assertEqual(result["auth"], "ok")
        self.assertEqual(result["config"], "empty")

    def test_hub_maps_rejected_token_to_needs_pairing(self) -> None:
        hub = object.__new__(Hub)
        camera = Camera(
            camera_id="cam1",
            name="Cam",
            ip="192.168.1.2",
            api_base_url="https://192.168.1.2:1998/api/v1",
            api_token="bad",
            api_status="offline",
        )
        hub._camera_api_base_url = lambda cam: cam.api_base_url  # type: ignore[method-assign]
        hub._camera_api_token = lambda cam: cam.api_token  # type: ignore[method-assign]
        hub._camera_registration_status_for_ui = lambda cam: "online"  # type: ignore[method-assign]

        class FakeClient:
            def __init__(self, base_url: str, token: str = "", timeout: int = 5) -> None:
                self.token = token

            def diagnose_path(self, path: str, *, timeout: int | None = None, allow_empty: bool = False) -> dict:
                if not self.token:
                    return {"ok": False, "kind": "unauthorized", "detail": "401", "path": path}
                return {"ok": False, "kind": "unauthorized", "detail": "401", "path": path}

        with mock.patch("app.main.CameraApiClient", FakeClient):
            result = Hub._diagnose_camera_native_api(hub, camera)
        self.assertEqual(result["phase"], "needs_pairing")
        self.assertEqual(result["auth"], "rejected")

    def test_hub_maps_empty_device_with_working_caps_to_wedge(self) -> None:
        hub = object.__new__(Hub)
        camera = Camera(
            camera_id="cam1",
            name="Cam",
            ip="192.168.1.2",
            api_base_url="https://192.168.1.2:1998/api/v1",
            api_token="token",
            api_status="offline",
        )
        hub._camera_api_base_url = lambda cam: cam.api_base_url  # type: ignore[method-assign]
        hub._camera_api_token = lambda cam: cam.api_token  # type: ignore[method-assign]
        hub._camera_registration_status_for_ui = lambda cam: "online"  # type: ignore[method-assign]

        responses = {
            ("", "/device"): {"ok": False, "kind": "unauthorized", "detail": "401", "path": "/device"},
            ("token", "/device"): {"ok": False, "kind": "empty", "detail": "Empty response for /device", "path": "/device"},
            ("token", "/capabilities"): {"ok": True, "kind": "ok", "detail": "", "path": "/capabilities", "bytes": 20},
            ("token", "/config"): {"ok": False, "kind": "empty", "detail": "Empty", "path": "/config", "bytes": 0},
            ("token", "/settings/image/brightness"): {
                "ok": True,
                "kind": "ok",
                "detail": "",
                "path": "/settings/image/brightness",
                "bytes": 8,
            },
        }

        class FakeClient:
            def __init__(self, base_url: str, token: str = "", timeout: int = 5) -> None:
                self.token = token

            def diagnose_path(self, path: str, *, timeout: int | None = None, allow_empty: bool = False) -> dict:
                return dict(responses[(self.token, path)])

        with mock.patch("app.main.CameraApiClient", FakeClient):
            result = Hub._diagnose_camera_native_api(hub, camera)
        self.assertEqual(result["phase"], "config_wedge")
        self.assertEqual(result["capabilities"], "ok")


if __name__ == "__main__":
    unittest.main()
