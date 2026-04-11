import json
import time
import io
import unittest
import sys
import threading
import types
import urllib.error


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
        hub.static_camera_ids = set()
        hub.config = {"mqtt": {"host": "192.168.1.10", "port": 1883, "username": "", "password": ""}}
        hub.command_reply_timeout_seconds = 5.0
        hub._persist_state = lambda: None
        hub._camera_accepts_hub_commands = lambda *args, **kwargs: True

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
        hub.static_camera_ids = set()
        hub.config = {"mqtt": {"host": "192.168.1.10", "port": 1883, "username": "", "password": ""}}
        hub.command_reply_timeout_seconds = 5.0
        hub._persist_state = lambda: None
        hub._camera_accepts_hub_commands = lambda *args, **kwargs: True

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


class CameraUiStatusTests(unittest.TestCase):
    def test_registration_staleness_does_not_mark_agentless_camera_offline(self) -> None:
        hub = object.__new__(Hub)
        hub.registration_stale_after_seconds = 30
        hub._camera_api_token = Hub._camera_api_token.__get__(hub, Hub)
        hub._camera_has_agent_for_ui = Hub._camera_has_agent_for_ui.__get__(hub, Hub)
        hub._camera_registration_status_for_ui = Hub._camera_registration_status_for_ui.__get__(hub, Hub)

        camera = Camera(
            camera_id="cam1",
            name="Legacy Camera",
            status="online",
            last_registration_at=time.time() - 3600,
            mqtt_command_status="offline",
            api_token="",
        )

        self.assertEqual(hub._camera_registration_status_for_ui(camera), "online")

    def test_registration_staleness_marks_agent_camera_offline(self) -> None:
        hub = object.__new__(Hub)
        hub.registration_stale_after_seconds = 30
        hub._camera_api_token = Hub._camera_api_token.__get__(hub, Hub)
        hub._camera_has_agent_for_ui = Hub._camera_has_agent_for_ui.__get__(hub, Hub)
        hub._camera_registration_status_for_ui = Hub._camera_registration_status_for_ui.__get__(hub, Hub)

        camera = Camera(
            camera_id="cam1",
            name="Agent Camera",
            status="online",
            last_registration_at=time.time() - 3600,
            mqtt_command_status="online",
            api_token="",
        )

        self.assertEqual(hub._camera_registration_status_for_ui(camera), "offline")


class SnapshotFetchAuthFallbackTests(unittest.TestCase):
    def test_snapshot_fetch_falls_back_to_login_on_401(self) -> None:
        hub = object.__new__(Hub)
        hub.snapshot_heartbeat_timeout_seconds = 5
        hub.default_onvif_username = "thingino"
        hub.default_onvif_password = "thingino"
        hub._camera_api_base_url = lambda camera: ""  # type: ignore[method-assign]

        camera = Camera(
            camera_id="cam1",
            name="Legacy Camera",
            snapshot_url="http://192.168.1.2/x/ch0.jpg",
            api_base_url="",
            onvif_username="thingino",
            onvif_password="thingino",
        )

        auth_error = urllib.error.HTTPError(
            camera.snapshot_url,
            401,
            "Unauthorized",
            hdrs={},
            fp=io.BytesIO(b"unauthorized"),
        )
        with unittest.mock.patch("urllib.request.urlopen", side_effect=auth_error):
            with unittest.mock.patch.object(
                hub,
                "_fetch_snapshot_with_camera_login",
                return_value=(b"\xff\xd8\xff\xe0", "image/jpeg"),
            ) as fallback_fetch:
                photo, filename = Hub._fetch_snapshot(hub, camera)

        self.assertEqual(photo, b"\xff\xd8\xff\xe0")
        self.assertEqual(filename, "cam1.jpg")
        fallback_fetch.assert_called_once_with(camera, "http://192.168.1.2/x/ch0.jpg")


class CameraUrlFallbackTests(unittest.TestCase):
    def test_agent_registration_with_invalid_ip_uses_api_host_for_public_urls(self) -> None:
        hub = object.__new__(Hub)
        camera = Camera(
            camera_id="cam1",
            name="Test Camera",
            ip="src",
            snapshot_url="http://src:1998/api/v1/actions/snapshot?stream_id=0",
            api_base_url="https://192.168.88.160:1998/api/v1",
        )

        self.assertEqual(hub._camera_web_ui_url(camera), "https://192.168.88.160/")
        self.assertEqual(hub._camera_snapshot_url(camera), "http://192.168.88.160/x/ch0.jpg")
        self.assertEqual(hub._camera_mjpeg_url(camera), "http://192.168.88.160/x/ch0.mjpg")
        self.assertEqual(hub._camera_rtsp_url(camera), "rtsp://192.168.88.160:554/ch0")
        self.assertEqual(hub._camera_onvif_endpoint(camera), "http://192.168.88.160/onvif/device_service")

    def test_registration_ignores_invalid_ip_and_rewrites_agent_snapshot_host(self) -> None:
        hub = object.__new__(Hub)
        existing = Camera(
            camera_id="cam1",
            name="Test Camera",
            ip="192.168.88.160",
            snapshot_url="http://192.168.88.160/x/ch0.jpg",
            api_base_url="https://192.168.88.160:1998/api/v1",
        )
        hub.cameras = {"cam1": existing}
        hub.state_lock = threading.Lock()
        hub.static_camera_ids = set()
        hub._camera_id_from_topic = lambda topic: "cam1"
        hub._configured_camera_name = lambda camera_id: ""
        hub._camera_with_runtime_state = lambda camera, current: camera
        hub._persist_state = lambda: None
        hub._schedule_api_refresh = lambda camera_id: False
        hub._schedule_onvif_refresh = lambda camera_id: False
        hub._schedule_supported_controls_refresh = lambda camera_id: False
        hub._schedule_mqtt_command_refresh = lambda camera_id: False
        hub._record_history_action = lambda *args, **kwargs: None

        payload = json.dumps(
            {
                "camera_id": "cam1",
                "name": "Test Camera",
                "hostname": "test-cam",
                "ip": "src",
                "snapshot_url": "http://src:1998/api/v1/actions/snapshot?stream_id=0",
                "api_base_url": "https://192.168.88.160:1998/api/v1",
                "status": "online",
                "timestamp": 1700000000,
            }
        )

        Hub._handle_registration(hub, "thingino/cam/cam1/hello", payload)

        updated = hub.cameras["cam1"]
        self.assertEqual(updated.ip, "192.168.88.160")
        self.assertEqual(updated.snapshot_url, "http://192.168.88.160:1998/api/v1/actions/snapshot?stream_id=0")

    def test_raptor_camera_uses_snap_and_mjpeg_endpoints(self) -> None:
        hub = object.__new__(Hub)
        camera = Camera(
            camera_id="cam1",
            name="Raptor Camera",
            ip="192.168.88.160",
            snapshot_url="http://192.168.88.160/x/ch0.jpg",
            api_base_url="https://192.168.88.160:1998/api/v1",
            api_streamer="raptor",
        )

        self.assertEqual(hub._camera_snapshot_url(camera), "https://192.168.88.160:8080/snap.jpg")
        self.assertEqual(
            hub._camera_snapshot_url(camera, "ch1"),
            "https://192.168.88.160:8080/snap.jpg?stream=1",
        )
        self.assertEqual(hub._camera_mjpeg_url(camera), "https://192.168.88.160:8080/mjpeg")


class AutoPairingTests(unittest.TestCase):
    def test_registration_schedules_auto_pairing_for_unpaired_online_camera(self) -> None:
        hub = object.__new__(Hub)
        hub.cameras = {}
        hub.state_lock = threading.Lock()
        hub.static_camera_ids = set()
        hub.auto_pairing_in_progress = set()
        hub.auto_pairing_next_retry_at = {}
        hub.auto_pairing_enabled = True
        hub.auto_pairing_retry_seconds = 300
        hub.default_onvif_username = "thingino"
        hub.default_onvif_password = "thingino"
        hub._camera_id_from_topic = lambda topic: "cam1"
        hub._configured_camera_name = lambda camera_id: ""
        hub._camera_with_runtime_state = lambda camera, current: camera
        hub._persist_state = lambda: None
        hub._schedule_api_refresh = lambda camera_id: False
        hub._schedule_onvif_refresh = lambda camera_id: False
        hub._schedule_supported_controls_refresh = lambda camera_id: False
        hub._schedule_mqtt_command_refresh = lambda camera_id: False
        hub._record_history_action = lambda *args, **kwargs: None
        hub._resolve_camera_id = lambda camera_id: camera_id

        scheduled: list[str] = []
        hub._schedule_auto_pairing = lambda camera_id: scheduled.append(camera_id) or True

        payload = json.dumps(
            {
                "camera_id": "cam1",
                "name": "Test Camera",
                "hostname": "test-cam",
                "ip": "192.168.88.160",
                "snapshot_url": "http://192.168.88.160/x/ch0.jpg",
                "api_base_url": "http://192.168.88.160:1998/api/v1",
                "status": "online",
                "timestamp": 1700000000,
            }
        )

        Hub._handle_registration(hub, "thingino/cam/cam1/hello", payload)

        self.assertEqual(scheduled, ["cam1"])

    def test_run_auto_pairing_connects_and_pairs_with_default_credentials(self) -> None:
        hub = object.__new__(Hub)
        hub.cameras = {"cam1": Camera(camera_id="cam1", name="Test Camera", ip="192.168.1.2", status="online")}
        hub.state_lock = threading.Lock()
        hub.auto_pairing_in_progress = set()
        hub.auto_pairing_next_retry_at = {}
        hub.auto_pairing_enabled = True
        hub.auto_pairing_retry_seconds = 300
        hub.default_onvif_username = "thingino"
        hub.default_onvif_password = "thingino"
        hub._resolve_camera_id = lambda camera_id: camera_id
        hub._record_history_action = lambda *args, **kwargs: None

        connected: list[dict[str, str]] = []
        paired: list[dict[str, str]] = []
        hub.connect_camera = lambda enrollment: connected.append(dict(enrollment)) or {"status": "success"}
        hub.install_pairing_bundle_via_mqtt = lambda enrollment: paired.append(dict(enrollment)) or {
            "status": "success",
            "status_detail": "Agent bootstrap installed",
        }

        Hub._run_auto_pairing(hub, "cam1")

        self.assertEqual(len(connected), 1)
        self.assertEqual(connected[0]["onvif_username"], "thingino")
        self.assertEqual(connected[0]["onvif_password"], "thingino")
        self.assertEqual(len(paired), 1)
        self.assertNotIn("cam1", hub.auto_pairing_next_retry_at)

    def test_run_auto_pairing_sets_retry_after_failure(self) -> None:
        hub = object.__new__(Hub)
        hub.cameras = {"cam1": Camera(camera_id="cam1", name="Test Camera", ip="192.168.1.2", status="online")}
        hub.state_lock = threading.Lock()
        hub.auto_pairing_in_progress = set()
        hub.auto_pairing_next_retry_at = {}
        hub.auto_pairing_enabled = True
        hub.auto_pairing_retry_seconds = 120
        hub.default_onvif_username = "thingino"
        hub.default_onvif_password = "thingino"
        hub._resolve_camera_id = lambda camera_id: camera_id
        hub._record_history_action = lambda *args, **kwargs: None
        hub.connect_camera = lambda enrollment: (_ for _ in ()).throw(RuntimeError("MQTT commands offline"))
        hub.install_pairing_bundle_via_mqtt = lambda enrollment: {"status": "success"}

        Hub._run_auto_pairing(hub, "cam1")

        self.assertIn("cam1", hub.auto_pairing_next_retry_at)
        self.assertGreater(hub.auto_pairing_next_retry_at["cam1"], 0.0)


class MqttRegistrationAndEventTests(unittest.TestCase):
    def test_unchanged_registration_heartbeat_does_not_trigger_refreshes(self) -> None:
        hub = object.__new__(Hub)
        existing = Camera(
            camera_id="cam1",
            name="Test Camera",
            hostname="test-cam",
            ip="192.168.88.160",
            snapshot_url="http://192.168.88.160/x/ch0.jpg",
            api_base_url="https://192.168.88.160:1998/api/v1",
            status="online",
            last_registration_at=1699999999,
        )
        hub.cameras = {"cam1": existing}
        hub.state_lock = threading.Lock()
        hub.static_camera_ids = set()
        hub._camera_id_from_topic = lambda topic: "cam1"
        hub._configured_camera_name = lambda camera_id: ""
        hub._camera_with_runtime_state = lambda camera, current: camera
        hub._persist_state = lambda: None
        hub._schedule_auto_pairing = lambda camera_id: False
        hub._record_history_action = lambda *args, **kwargs: None

        scheduled: list[tuple[str, str]] = []
        hub._schedule_api_refresh = lambda camera_id: scheduled.append(("api", camera_id)) or True
        hub._schedule_onvif_refresh = lambda camera_id: scheduled.append(("onvif", camera_id)) or True
        hub._schedule_supported_controls_refresh = lambda camera_id: scheduled.append(("controls", camera_id)) or True
        hub._schedule_mqtt_command_refresh = lambda camera_id: scheduled.append(("mqtt", camera_id)) or True

        payload = json.dumps(
            {
                "camera_id": "cam1",
                "name": "Test Camera",
                "hostname": "test-cam",
                "ip": "192.168.88.160",
                "snapshot_url": "http://192.168.88.160/x/ch0.jpg",
                "api_base_url": "https://192.168.88.160:1998/api/v1",
                "status": "online",
                "timestamp": 1700000000,
            }
        )

        Hub._handle_registration(hub, "thingino/cam/cam1/hello", payload)

        self.assertEqual(scheduled, [])
        self.assertEqual(hub.cameras["cam1"].last_registration_at, 1700000000)

    def test_mqtt_event_message_routes_to_camera_event_handler(self) -> None:
        hub = object.__new__(Hub)
        hub._handle_registration = lambda topic, payload: None
        hub._format_reply = lambda topic, payload: (None, None)

        handled: list[tuple[str, dict[str, object]]] = []
        hub._ensure_camera = lambda camera_id: None
        hub._handle_camera_stream_event = lambda camera_id, event: handled.append((camera_id, event))

        message = types.SimpleNamespace(
            topic="thingino/cam/cam1/event",
            payload=b'{"event":"motion.started","data":{"active":true}}',
        )

        Hub._on_mqtt_message(hub, None, None, message)

        self.assertEqual(
            handled,
            [("cam1", {"event": "motion.started", "data": {"active": True}})],
        )

    def test_mqtt_state_message_updates_camera_runtime_status(self) -> None:
        hub = object.__new__(Hub)
        hub.cameras = {
            "cam1": Camera(camera_id="cam1", name="cam1", status="unknown")
        }
        hub.state_lock = threading.Lock()
        hub._persist_state = lambda: None
        recorded: list[tuple[str, str, dict[str, object], dict[str, object], int]] = []
        hub._record_history_state_sample = lambda camera_id, sample_type, sample, normalized=None, recorded_at=None: recorded.append(
            (camera_id, sample_type, sample, normalized or {}, int(recorded_at or 0))
        )
        hub._coerce_int = Hub._coerce_int.__get__(hub, Hub)
        hub._normalized_camera_ip = Hub._normalized_camera_ip.__get__(hub, Hub)

        payload = json.dumps(
            {
                "camera_id": "cam1",
                "timestamp": 1700000000,
                "device": {
                    "name": "Test Camera",
                    "hostname": "test-cam",
                    "model": "wyze-cam3",
                    "streamer": "prudynt",
                    "firmware_version": "0-local",
                },
                "state": {
                    "system": {"streamer_running": True},
                    "network": {"online": True, "ip": "192.168.88.160"},
                    "motion": {"enabled": True},
                    "privacy": {"enabled": False},
                    "daynight": {"target_mode": "auto", "running_mode": "day"},
                },
            }
        )

        Hub._handle_mqtt_state(hub, "thingino/cam/cam1/state", payload)

        camera = hub.cameras["cam1"]
        self.assertEqual(camera.name, "Test Camera")
        self.assertEqual(camera.hostname, "test-cam")
        self.assertEqual(camera.ip, "192.168.88.160")
        self.assertEqual(camera.status, "online")
        self.assertEqual(camera.probe_status, "online")
        self.assertEqual(camera.api_status, "online")
        self.assertEqual(camera.api_streamer, "prudynt")
        self.assertEqual(camera.api_version, "0-local")
        self.assertEqual(recorded[0][0], "cam1")
        self.assertEqual(recorded[0][1], "mqtt_state")


if __name__ == "__main__":
    unittest.main()
