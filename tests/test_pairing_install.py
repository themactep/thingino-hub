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


class CredentialsFirstEnrollTests(unittest.TestCase):
    def test_discover_identity_uses_native_device_id_when_roster_empty(self) -> None:
        hub = object.__new__(Hub)
        hub.cameras = {}
        hub.state_lock = threading.Lock()
        hub.default_onvif_username = "thingino"
        hub.default_onvif_password = "thingino"

        class FakeClient:
            def get_device(self) -> dict[str, str]:
                return {"id": "0203823f5533", "name": "ptz-cam-01", "hostname": "ptz-cam-01"}

        hub._camera_api_client = lambda camera: FakeClient()  # type: ignore[method-assign]
        hub._fetch_onvif_device_information = lambda camera: (_ for _ in ()).throw(RuntimeError("skip"))  # type: ignore[method-assign]
        hub._resolve_enrollment_camera_identity = Hub._resolve_enrollment_camera_identity.__get__(hub, Hub)

        camera_id, name = Hub._discover_enrollment_identity(
            hub,
            {"ip": "192.168.140.11", "api_token": "tok"},
        )

        self.assertEqual(camera_id, "0203823f5533")
        self.assertEqual(name, "ptz-cam-01")

    def test_discover_identity_prefers_roster_match_over_native_api(self) -> None:
        hub = object.__new__(Hub)
        hub.cameras = {
            "aabbccddeeff": Camera(camera_id="aabbccddeeff", name="front-door", ip="192.168.140.11"),
        }
        hub.state_lock = threading.Lock()
        hub.default_onvif_username = "thingino"
        hub.default_onvif_password = "thingino"
        hub._resolve_enrollment_camera_identity = Hub._resolve_enrollment_camera_identity.__get__(hub, Hub)
        hub._camera_api_client = lambda camera: (_ for _ in ()).throw(AssertionError("native API should not be used"))  # type: ignore[method-assign]

        camera_id, name = Hub._discover_enrollment_identity(hub, {"ip": "192.168.140.11"})

        self.assertEqual(camera_id, "aabbccddeeff")
        self.assertEqual(name, "front-door")

    def test_discover_identity_falls_back_to_onvif_serial(self) -> None:
        hub = object.__new__(Hub)
        hub.cameras = {}
        hub.state_lock = threading.Lock()
        hub.default_onvif_username = "thingino"
        hub.default_onvif_password = "thingino"
        hub._resolve_enrollment_camera_identity = Hub._resolve_enrollment_camera_identity.__get__(hub, Hub)
        hub._camera_api_client = lambda camera: (_ for _ in ()).throw(RuntimeError("api down"))  # type: ignore[method-assign]
        hub._fetch_onvif_device_information = lambda camera: {  # type: ignore[method-assign]
            "serial_number": "SN123456",
            "model": "ThinginoCam",
        }

        camera_id, name = Hub._discover_enrollment_identity(hub, {"ip": "192.168.140.11"})

        self.assertEqual(camera_id, "sn123456")
        self.assertEqual(name, "ThinginoCam")

    def test_connect_camera_enrolls_from_native_api_without_mqtt_roster(self) -> None:
        hub = object.__new__(Hub)
        hub.cameras = {}
        hub.state_lock = threading.Lock()
        hub.static_camera_ids = set()
        hub.default_onvif_username = "thingino"
        hub.default_onvif_password = "thingino"

        class FakeClient:
            def get_device(self) -> dict[str, str]:
                return {"id": "0203823f5533", "name": "ptz-cam-01"}

        hub._camera_api_client = lambda camera: FakeClient()  # type: ignore[method-assign]
        hub._fetch_onvif_device_information = lambda camera: (_ for _ in ()).throw(RuntimeError("skip"))  # type: ignore[method-assign]
        hub._resolve_camera_id = Hub._resolve_camera_id.__get__(hub, Hub)
        hub._resolve_enrollment_camera_identity = Hub._resolve_enrollment_camera_identity.__get__(hub, Hub)
        hub._discover_enrollment_identity = Hub._discover_enrollment_identity.__get__(hub, Hub)
        hub._normalized_enrollment_entry = Hub._normalized_enrollment_entry.__get__(hub, Hub)

        saved: list[dict[str, str]] = []
        hub.enroll_camera = lambda enrollment: saved.append(dict(enrollment)) or {  # type: ignore[method-assign]
            "camera_id": enrollment["id"],
            "updated_existing": False,
        }
        hub._record_history_action = lambda *args, **kwargs: None  # type: ignore[method-assign]

        result = Hub.connect_camera(
            hub,
            {
                "ip": "192.168.140.11",
                "api_token": "tok-123",
                "onvif_username": "thingino",
                "onvif_password": "thingino",
            },
        )

        self.assertEqual(result["camera_id"], "0203823f5533")
        self.assertEqual(result["api_token"], "tok-123")
        self.assertEqual(len(saved), 1)
        self.assertEqual(saved[0]["id"], "0203823f5533")
        self.assertEqual(saved[0]["api_token"], "tok-123")
        self.assertEqual(saved[0]["api_base_url"], "https://192.168.140.11:1998/api/v1")

    def test_connect_camera_with_explicit_id_still_requires_mqtt_commands(self) -> None:
        hub = object.__new__(Hub)
        hub.cameras = {
            "cam1": Camera(camera_id="cam1", name="Test Camera", ip="192.168.1.2", mqtt_command_status="offline"),
        }
        hub.state_lock = threading.Lock()
        hub.static_camera_ids = set()
        hub._resolve_camera_id = Hub._resolve_camera_id.__get__(hub, Hub)
        hub._camera_accepts_hub_commands = lambda *args, **kwargs: False  # type: ignore[method-assign]
        hub._camera_hub_command_error = lambda camera_id: "Camera did not respond to hub MQTT commands."  # type: ignore[method-assign]

        with self.assertRaisesRegex(RuntimeError, "Camera did not respond to hub MQTT commands"):
            Hub.connect_camera(
                hub,
                {
                    "camera_id": "cam1",
                    "ip": "192.168.1.2",
                    "onvif_username": "thingino",
                    "onvif_password": "thingino",
                },
            )


class PairingInstallTests(unittest.TestCase):
    def test_confirmed_mqtt_install_persists_generated_enrollment(self) -> None:
        hub = object.__new__(Hub)
        hub.cameras = {"cam1": Camera(camera_id="cam1", name="Test Camera", ip="192.168.1.2")}
        hub.state_lock = threading.Lock()
        hub.static_camera_ids = set()
        hub.config = {"mqtt": {"host": "192.168.1.10", "port": 1883, "username": "", "password": ""}}
        hub.command_reply_timeout_seconds = 5.0
        hub.history_store = None
        hub._persist_state = lambda: None
        hub._camera_accepts_hub_commands = lambda *args, **kwargs: True
        hub._schedule_camera_config_backup = lambda *args, **kwargs: None
        hub._refresh_camera_state_after_pairing = lambda *args, **kwargs: None

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
        self.assertFalse(result.get("config_restore_available"))

    def test_timed_out_mqtt_install_does_not_persist_generated_enrollment(self) -> None:
        hub = object.__new__(Hub)
        hub.cameras = {"cam1": Camera(camera_id="cam1", name="Test Camera", ip="192.168.1.2")}
        hub.state_lock = threading.Lock()
        hub.static_camera_ids = set()
        hub.config = {"mqtt": {"host": "192.168.1.10", "port": 1883, "username": "", "password": ""}}
        hub.command_reply_timeout_seconds = 5.0
        hub.history_store = None
        hub._persist_state = lambda: None
        hub._camera_accepts_hub_commands = lambda *args, **kwargs: True
        hub._schedule_camera_config_backup = lambda *args, **kwargs: None
        hub._confirm_pairing_install_via_api = lambda *args, **kwargs: False

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
        self.assertFalse(result.get("config_restore_available"))


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

        self.assertEqual(hub._camera_snapshot_url(camera), "https://192.168.88.160:8443/snap.jpg")
        self.assertEqual(
            hub._camera_snapshot_url(camera, "ch1"),
            "https://192.168.88.160:8443/snap.jpg?stream=1",
        )
        self.assertEqual(hub._camera_mjpeg_url(camera), "https://192.168.88.160:8443/mjpeg")


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


class NativeConfigSettingsSplitTests(unittest.TestCase):
    def test_split_stream_osd_into_settings_leaf_patches(self) -> None:
        hub = object.__new__(Hub)
        patches, residual = Hub._split_native_config_patch_for_settings(
            hub,
            {
                "image": {"brightness": 128},
                "action": {"restart_thread": 3},
                "stream0": {
                    "fps": 25,
                    "osd": {
                        "enabled": True,
                        "time": {"enabled": True},
                        "usertext": {"enabled": True, "format": "Bird Box 01"},
                    },
                },
            },
        )

        self.assertEqual(residual, {})
        self.assertIn(("image/brightness", {"brightness": 128}), patches)
        self.assertIn(("streams/0/fps", {"fps": 25}), patches)
        self.assertIn(("streams/0/osd/enabled", {"enabled": True}), patches)
        self.assertIn(("streams/0/osd/time/enabled", {"enabled": True}), patches)
        self.assertIn(("streams/0/osd/usertext/enabled", {"enabled": True}), patches)
        self.assertIn(("streams/0/osd/usertext/format", {"format": "Bird Box 01"}), patches)

    def test_split_osd_position_via_writable_settings_catalog(self) -> None:
        hub = object.__new__(Hub)
        patches, residual = Hub._split_native_config_patch_for_settings(
            hub,
            {
                "stream0": {
                    "osd": {
                        "time": {"position": "top_left"},
                        "usertext": {"position": "bottom_right", "format": "Garden"},
                        "privacy": {"position": "middle_center", "text": "PRIVATE"},
                    },
                },
            },
        )

        self.assertEqual(residual, {})
        self.assertIn(("streams/0/osd/time/position", {"position": "top_left"}), patches)
        self.assertIn(("streams/0/osd/usertext/position", {"position": "bottom_right"}), patches)
        self.assertIn(("streams/0/osd/usertext/format", {"format": "Garden"}), patches)
        self.assertIn(("streams/0/osd/privacy/position", {"position": "middle_center"}), patches)
        self.assertIn(("streams/0/osd/privacy/text", {"text": "PRIVATE"}), patches)

    def test_writable_settings_catalog_marks_position_for_ui(self) -> None:
        hub = object.__new__(Hub)
        catalog = Hub._native_writable_settings_catalog(hub, stream_ids=[0])
        by_path = {entry["config_path"]: entry for entry in catalog}
        self.assertEqual(by_path["stream0.osd.time.position"]["settings_path"], "streams/0/osd/time/position")
        self.assertTrue(by_path["stream0.osd.time.position"]["ui"])
        self.assertIn("top_left", by_path["stream0.osd.usertext.position"]["enum"])

    def test_patch_camera_config_writes_osd_via_settings_not_omnibus(self) -> None:
        hub = object.__new__(Hub)
        hub.state_lock = threading.Lock()
        hub.cameras = {
            "cam1": Camera(camera_id="cam1", name="Cam", ip="192.168.1.2", api_base_url="https://192.168.1.2:1998/api/v1"),
        }
        calls: list[tuple[str, str, dict]] = []

        class FakeClient:
            def patch_setting(self, path, payload):
                calls.append(("setting", path, payload))
                return {"status": "accepted", "applied": [f"settings.{path.replace('/', '.')}"]}

            def patch_config(self, payload):
                calls.append(("config", "", payload))
                return {"status": "accepted", "applied": ["config"]}

        hub._camera_api_client = lambda camera: FakeClient()
        hub._record_native_action = lambda *args, **kwargs: None
        hub._record_history_config_changes = lambda *args, **kwargs: None
        hub._record_optimistic_supported_controls = lambda *args, **kwargs: None
        hub._schedule_api_refresh = lambda camera_id: False
        hub._schedule_supported_controls_refresh = lambda camera_id: False
        hub._schedule_camera_config_backup = lambda *args, **kwargs: None

        result = Hub.patch_camera_config(
            hub,
            "cam1",
            {
                "stream0": {"osd": {"usertext": {"format": "Bird Box 01"}}},
                "image": {"anti_flicker": "1"},
                "action": {"restart_thread": 3},
            },
            refresh_after=False,
        )

        self.assertEqual(result["status"], "accepted")
        self.assertEqual(
            calls,
            [
                ("setting", "image/anti-flicker", {"anti_flicker": "1"}),
                ("setting", "streams/0/osd/usertext/format", {"format": "Bird Box 01"}),
            ],
        )


if __name__ == "__main__":
    unittest.main()
