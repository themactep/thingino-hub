import unittest
import sys
import threading
import types


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

from app.main import Camera, Hub


class PairingInstallTests(unittest.TestCase):
    def test_confirmed_mqtt_install_persists_generated_enrollment(self) -> None:
        hub = object.__new__(Hub)
        hub.cameras = {"cam1": Camera(camera_id="cam1", name="Test Camera", ip="192.168.1.2")}
        hub.state_lock = threading.Lock()
        hub.command_reply_timeout_seconds = 5.0

        saved_enrollments: list[dict[str, str]] = []
        history_actions: list[tuple[str, str, str, str]] = []

        hub.generate_pairing_bundle = lambda enrollment: {
            "camera_id": "cam1",
            "api_base_url": "https://192.168.1.2:1998/api/v1",
            "api_token": "generated-token",
            "save_entry": {
                "id": "cam1",
                "name": "Test Camera",
                "ip": "192.168.1.2",
                "snapshot_url": "http://192.168.1.2/x/ch0.jpg",
                "api_base_url": "https://192.168.1.2:1998/api/v1",
                "api_token": "generated-token",
            },
        }
        hub._resolve_camera_id = lambda camera_id: camera_id
        hub._publish_camera_command = lambda *args, **kwargs: {
            "published": True,
            "reply_received": True,
            "reply_ok": True,
            "reply_text": "Agent bootstrap installed",
            "request_id": "req-1",
        }
        hub.enroll_camera = lambda enrollment: saved_enrollments.append(enrollment) or {
            "camera_id": "cam1",
            "updated_existing": True,
        }
        hub._record_history_action = lambda camera_id, action, status, detail, **kwargs: history_actions.append(
            (camera_id, action, status, detail)
        )

        result = Hub.install_pairing_bundle_via_mqtt(hub, {"camera_id": "cam1"})

        self.assertEqual(result["status"], "success")
        self.assertEqual(len(saved_enrollments), 1)
        self.assertEqual(saved_enrollments[0]["api_token"], "generated-token")
        self.assertEqual(saved_enrollments[0]["api_base_url"], "https://192.168.1.2:1998/api/v1")
        self.assertEqual(history_actions[-1], ("cam1", "pairing_install", "success", "Agent bootstrap installed"))

    def test_timed_out_mqtt_install_does_not_persist_generated_enrollment(self) -> None:
        hub = object.__new__(Hub)
        hub.cameras = {"cam1": Camera(camera_id="cam1", name="Test Camera", ip="192.168.1.2")}
        hub.state_lock = threading.Lock()
        hub.command_reply_timeout_seconds = 5.0

        saved_enrollments: list[dict[str, str]] = []

        hub.generate_pairing_bundle = lambda enrollment: {
            "camera_id": "cam1",
            "api_base_url": "https://192.168.1.2:1998/api/v1",
            "api_token": "generated-token",
            "save_entry": {
                "id": "cam1",
                "api_base_url": "https://192.168.1.2:1998/api/v1",
                "api_token": "generated-token",
            },
        }
        hub._resolve_camera_id = lambda camera_id: camera_id
        hub._publish_camera_command = lambda *args, **kwargs: {
            "published": True,
            "reply_received": False,
            "reply_ok": None,
            "reply_text": "",
            "request_id": "req-2",
        }
        hub.enroll_camera = lambda enrollment: saved_enrollments.append(enrollment) or {
            "camera_id": "cam1",
            "updated_existing": True,
        }
        hub._record_history_action = lambda *args, **kwargs: None

        result = Hub.install_pairing_bundle_via_mqtt(hub, {"camera_id": "cam1"})

        self.assertEqual(result["status"], "warning")
        self.assertEqual(saved_enrollments, [])


class StreamControlFormattingTests(unittest.TestCase):
    def test_zero_dimensions_and_fps_render_as_unset(self) -> None:
        hub = object.__new__(Hub)

        self.assertEqual(hub._format_stream_control_value({"width": 0}, "width", zero_means_unset=True), "")
        self.assertEqual(hub._format_stream_control_value({"height": "0"}, "height", zero_means_unset=True), "")
        self.assertEqual(hub._format_stream_control_value({"fps": 0}, "fps", zero_means_unset=True), "")

    def test_positive_stream_values_are_preserved(self) -> None:
        hub = object.__new__(Hub)

        self.assertEqual(hub._format_stream_control_value({"width": 640}, "width", zero_means_unset=True), "640")
        self.assertEqual(hub._format_stream_control_value({"height": "360"}, "height", zero_means_unset=True), "360")
        self.assertEqual(hub._format_stream_control_value({"fps": 20}, "fps", zero_means_unset=True), "20")
        self.assertEqual(hub._format_stream_control_value({"bitrate": 3000}, "bitrate"), "3000")

    def test_zero_dimensions_and_fps_use_live_prudynt_fallback(self) -> None:
        hub = object.__new__(Hub)

        self.assertEqual(
            hub._format_stream_control_value({"width": 0}, "width", zero_means_unset=True, fallback_value=1920),
            "1920",
        )
        self.assertEqual(
            hub._format_stream_control_value({"height": "0"}, "height", zero_means_unset=True, fallback_value="1080"),
            "1080",
        )
        self.assertEqual(
            hub._format_stream_control_value({"fps": 0}, "fps", zero_means_unset=True, fallback_value=30),
            "30",
        )


if __name__ == "__main__":
    unittest.main()