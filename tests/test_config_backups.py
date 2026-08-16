import hashlib
import json
import sys
import tempfile
import threading
import time
import types
import unittest
from pathlib import Path
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

from app.history_store import HistoryStore
from app.main import Camera, Hub


class ConfigSnapshotStoreTests(unittest.TestCase):
    def setUp(self) -> None:
        self._tmpdir = tempfile.TemporaryDirectory()
        self.db_path = str(Path(self._tmpdir.name) / "history.sqlite3")
        self.store = HistoryStore(
            self.db_path,
            max_config_snapshots_per_camera=2,
            config_snapshot_max_age_days=0,
        )

    def tearDown(self) -> None:
        self.store.close()
        self._tmpdir.cleanup()

    def test_dedup_skips_identical_consecutive_hash(self) -> None:
        first = self.store.record_config_snapshot(
            recorded_at=100,
            camera_id="cam1",
            source="manual",
            config={"image": {"hflip": True}},
            capabilities={"image": {}},
            content_hash="abc",
        )
        second = self.store.record_config_snapshot(
            recorded_at=101,
            camera_id="cam1",
            source="hub_write",
            config={"image": {"hflip": True}},
            capabilities={"image": {}},
            content_hash="abc",
        )
        self.assertTrue(first["stored"])
        self.assertFalse(second["stored"])
        self.assertTrue(second["skipped_duplicate"])
        self.assertEqual(len(self.store.list_config_snapshots("cam1")), 1)

    def test_retention_prunes_by_count(self) -> None:
        for index in range(4):
            self.store.record_config_snapshot(
                recorded_at=100 + index,
                camera_id="cam1",
                source="manual",
                config={"n": index},
                content_hash=f"hash-{index}",
            )
        rows = self.store.list_config_snapshots("cam1")
        self.assertEqual(len(rows), 2)
        self.assertEqual(rows[0]["content_hash"], "hash-3")
        self.assertEqual(rows[1]["content_hash"], "hash-2")


class _ConfigHubTestMixin:
    def _hub_with_store(self, *, max_snapshots: int = 20) -> Hub:
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        hub = object.__new__(Hub)
        hub.state_lock = threading.Lock()
        hub.cameras = {
            "aabbccddeeff": Camera(
                camera_id="aabbccddeeff",
                name="birdbox",
                ip="192.168.1.10",
                api_base_url="https://192.168.1.10:1998/api/v1",
                api_token="tok",
                api_status="online",
                api_streamer="raptor",
            )
        }
        hub.history_store = HistoryStore(
            str(Path(tmp.name) / "h.sqlite3"),
            max_config_snapshots_per_camera=max_snapshots,
            config_snapshot_max_age_days=0,
        )
        self.addCleanup(hub.history_store.close)
        hub.history_enabled = True
        hub.history_db_path = str(Path(tmp.name) / "h.sqlite3")
        hub.history_max_config_snapshots_per_camera = max_snapshots
        hub.history_config_snapshot_max_age_days = 0
        hub.optimistic_supported_controls_by_camera = {}
        hub._resolve_camera_id = lambda camera_id: str(camera_id or "").strip().lower()  # type: ignore[method-assign]
        hub._camera_api_base_url = lambda camera: str(camera.api_base_url or "")  # type: ignore[method-assign]
        hub._record_history_action = lambda *args, **kwargs: None  # type: ignore[method-assign]
        hub._record_native_action = lambda *args, **kwargs: None  # type: ignore[method-assign]
        hub._record_history_config_changes = lambda *args, **kwargs: None  # type: ignore[method-assign]
        hub._record_optimistic_supported_controls = lambda *args, **kwargs: None  # type: ignore[method-assign]
        hub._schedule_api_refresh = lambda camera_id: None  # type: ignore[method-assign]
        hub._schedule_supported_controls_refresh = lambda camera_id: None  # type: ignore[method-assign]
        hub.refresh_camera_api_details = lambda camera_id: True  # type: ignore[method-assign]
        hub._format_timestamp = Hub._format_timestamp.__get__(hub, Hub)
        hub._coerce_int = Hub._coerce_int.__get__(hub, Hub)
        hub._config_snapshot_summary_for_ui = Hub._config_snapshot_summary_for_ui.__get__(hub, Hub)
        hub._config_snapshot_detail_for_ui = Hub._config_snapshot_detail_for_ui.__get__(hub, Hub)
        hub._build_config_restore_plan = Hub._build_config_restore_plan.__get__(hub, Hub)
        hub._camera_config_read_timeout = Hub._camera_config_read_timeout.__get__(hub, Hub)
        hub._normalize_native_api_error = Hub._normalize_native_api_error.__get__(hub, Hub)
        hub._schedule_camera_config_backup = lambda *args, **kwargs: None  # type: ignore[method-assign]
        hub._strip_restore_secrets = Hub._strip_restore_secrets.__get__(hub, Hub)
        hub._restorable_config_groups = Hub._restorable_config_groups.__get__(hub, Hub)
        hub._restore_capability_group_for_key = Hub._restore_capability_group_for_key.__get__(hub, Hub)
        hub._restore_value_conflict = Hub._restore_value_conflict.__get__(hub, Hub)
        hub._restore_value_summary = Hub._restore_value_summary.__get__(hub, Hub)
        hub._flatten_config_leaves = Hub._flatten_config_leaves.__get__(hub, Hub)
        hub._unflatten_config_leaves = Hub._unflatten_config_leaves.__get__(hub, Hub)
        hub._normalize_config_for_field_ops = Hub._normalize_config_for_field_ops.__get__(hub, Hub)
        hub._nested_value_at = Hub._nested_value_at.__get__(hub, Hub)
        hub._read_camera_live_config_context = Hub._read_camera_live_config_context.__get__(hub, Hub)
        hub._resolve_config_clone_source = Hub._resolve_config_clone_source.__get__(hub, Hub)
        hub._build_config_clone_field_plan = Hub._build_config_clone_field_plan.__get__(hub, Hub)
        hub._restore_leaf_conflict = Hub._restore_leaf_conflict.__get__(hub, Hub)
        hub._payload_from_clone_selection = Hub._payload_from_clone_selection.__get__(hub, Hub)
        hub._split_native_config_patch_for_settings = Hub._split_native_config_patch_for_settings.__get__(hub, Hub)
        hub._native_writable_settings_catalog = Hub._native_writable_settings_catalog.__get__(hub, Hub)
        hub._osd_position_choices = Hub._osd_position_choices.__get__(hub, Hub)
        hub._stream_setting_path = Hub._stream_setting_path.__get__(hub, Hub)
        hub._native_config_stage_plan = Hub._native_config_stage_plan.__get__(hub, Hub)
        hub._setting_values_match = Hub._setting_values_match.__get__(hub, Hub)
        hub._confirm_native_config_stage = Hub._confirm_native_config_stage.__get__(hub, Hub)
        hub._coerce_bool = Hub._coerce_bool.__get__(hub, Hub)
        hub.backup_camera_config = Hub.backup_camera_config.__get__(hub, Hub)
        hub._maybe_backup_camera_config = Hub._maybe_backup_camera_config.__get__(hub, Hub)
        hub.preview_camera_config_restore = Hub.preview_camera_config_restore.__get__(hub, Hub)
        hub.restore_camera_config_backup = Hub.restore_camera_config_backup.__get__(hub, Hub)
        hub.iter_restore_camera_config_backup = Hub.iter_restore_camera_config_backup.__get__(hub, Hub)
        hub.preview_camera_config_clone = Hub.preview_camera_config_clone.__get__(hub, Hub)
        hub.apply_camera_config_clone = Hub.apply_camera_config_clone.__get__(hub, Hub)
        hub.iter_apply_camera_config_clone = Hub.iter_apply_camera_config_clone.__get__(hub, Hub)
        hub.preview_camera_config_clone_push = Hub.preview_camera_config_clone_push.__get__(hub, Hub)
        hub.apply_camera_config_clone_push = Hub.apply_camera_config_clone_push.__get__(hub, Hub)
        hub.iter_apply_camera_config_clone_push = Hub.iter_apply_camera_config_clone_push.__get__(hub, Hub)
        hub.patch_camera_config = Hub.patch_camera_config.__get__(hub, Hub)
        hub.list_camera_config_backups = Hub.list_camera_config_backups.__get__(hub, Hub)
        hub.get_camera_config_backup = Hub.get_camera_config_backup.__get__(hub, Hub)
        return hub


class ConfigBackupHubTests(_ConfigHubTestMixin, unittest.TestCase):
    def test_backup_after_successful_patch(self) -> None:
        hub = self._hub_with_store()
        patched: list[tuple[str, dict]] = []

        class FakeClient:
            def patch_setting(self, path: str, body: dict) -> dict:
                patched.append((path, body))
                return {"applied": [path]}

            def patch_config(self, payload: dict) -> dict:
                return {"status": "accepted", "applied": list(payload.keys())}

            def get_config(self, timeout: int | None = None) -> dict:
                return {"image": {"hflip": True}, "stream0": {"width": 1920}, "agent": {"token": "secret"}}

            def _control_timeout(self) -> int:
                return 15

            def get_capabilities(self) -> dict:
                return {"image": {}, "streams": {}}

            def get_device(self) -> dict:
                return {"software": {"firmware_version": "fw-1", "streamer": "raptor"}}

        hub._camera_api_client = lambda camera: FakeClient()  # type: ignore[method-assign]

        # Avoid racing the background backup thread in this unit test.
        hub._schedule_camera_config_backup = lambda *args, **kwargs: None  # type: ignore[method-assign]
        Hub.patch_camera_config(hub, "aabbccddeeff", {"image": {"hflip": True}}, refresh_after=False)
        # Simulate the scheduled backup path directly.
        hub.backup_camera_config("aabbccddeeff", source="hub_write", label="After settings save")
        backups = hub.list_camera_config_backups("aabbccddeeff")
        self.assertEqual(len(backups), 1)
        self.assertEqual(backups[0]["source"], "hub_write")

    def test_failed_patch_does_not_backup(self) -> None:
        hub = self._hub_with_store()
        hub._schedule_camera_config_backup = lambda *args, **kwargs: (_ for _ in ()).throw(  # type: ignore[method-assign]
            AssertionError("backup must not be scheduled after failed patch")
        )

        class FakeClient:
            def patch_setting(self, path: str, body: dict) -> dict:
                raise RuntimeError("write failed")

            def patch_config(self, payload: dict) -> dict:
                raise RuntimeError("write failed")

            def get_config(self) -> dict:
                raise AssertionError("should not backup")

        hub._camera_api_client = lambda camera: FakeClient()  # type: ignore[method-assign]
        with self.assertRaises(RuntimeError):
            Hub.patch_camera_config(hub, "aabbccddeeff", {"image": {"hflip": True}}, refresh_after=False)
        self.assertEqual(hub.list_camera_config_backups("aabbccddeeff"), [])

    def test_restore_preview_classifies_secrets_dropped_and_conflicts(self) -> None:
        hub = self._hub_with_store()
        snapshot_id = hub.history_store.record_config_snapshot(
            recorded_at=int(time.time()),
            camera_id="aabbccddeeff",
            source="manual",
            firmware_id="fw-old",
            streamer="raptor",
            config={
                "agent": {"token": "secret", "listen": "0.0.0.0"},
                "mqtt_sub": {"host": "broker", "password": "pw"},
                "image": {"hflip": True},
                "legacy_thing": {"x": 1},
                "stream0": {"format": "h265"},
            },
            capabilities={"image": {}, "streams": {}},
            content_hash="snap-1",
        )["snapshot_id"]

        class FakeClient:
            def get_capabilities(self) -> dict:
                return {
                    "image": {},
                    "streams": {"fields": {"format": {"enum": ["h264", "h265"]}}},
                }

            def get_config(self, timeout: int | None = None) -> dict:
                return {"image": {"hflip": False}, "stream0": {"format": "h264"}}

        hub._camera_api_client = lambda camera: FakeClient()  # type: ignore[method-assign]
        preview = hub.preview_camera_config_restore("aabbccddeeff", snapshot_id)
        self.assertTrue(preview["restore_ready"])
        self.assertTrue(preview["live_capabilities_ok"])
        secret_paths = {item["path"] for item in preview["skipped_secrets"]}
        self.assertIn("agent", secret_paths)
        self.assertIn("mqtt_sub.password", secret_paths)
        compatible_paths = {item["path"] for item in preview["compatible"]}
        self.assertIn("image", compatible_paths)
        dropped_paths = {item["path"] for item in preview["dropped"]}
        self.assertIn("legacy_thing", dropped_paths)
        self.assertIn("image", preview["compatible_payload"])
        self.assertNotIn("agent", preview["compatible_payload"])

    def test_restore_preview_blocks_when_live_api_unreadable(self) -> None:
        hub = self._hub_with_store()
        snapshot_id = hub.history_store.record_config_snapshot(
            recorded_at=int(time.time()),
            camera_id="aabbccddeeff",
            source="manual",
            config={"image": {"hflip": True}, "stream0": {"width": 1920}},
            capabilities={"image": {}, "streams": {}},
            content_hash="snap-blocked",
        )["snapshot_id"]

        class FakeClient:
            def get_capabilities(self) -> dict:
                raise RuntimeError("Invalid JSON response for /capabilities: Expecting value: line 1 column 1 (char 0)")

            def get_config(self, timeout: int | None = None) -> dict:
                raise RuntimeError("Empty response for /config — native API returned no JSON")

        hub._camera_api_client = lambda camera: FakeClient()  # type: ignore[method-assign]
        preview = hub.preview_camera_config_restore("aabbccddeeff", snapshot_id)
        self.assertFalse(preview["restore_ready"])
        self.assertEqual(preview["compatible_count"], 0)
        self.assertGreater(preview["dropped_count"], 0)
        with self.assertRaisesRegex(RuntimeError, "non-JSON|empty|not ready|Could not read"):
            hub.restore_camera_config_backup("aabbccddeeff", snapshot_id, mode="compatible")

    def test_restore_preview_allows_when_capabilities_ok_but_config_empty(self) -> None:
        hub = self._hub_with_store()
        snapshot_id = hub.history_store.record_config_snapshot(
            recorded_at=int(time.time()),
            camera_id="aabbccddeeff",
            source="manual",
            config={"image": {"hflip": True}, "stream0": {"width": 1920}},
            capabilities={"image": {}, "streams": {}},
            content_hash="snap-empty-config",
        )["snapshot_id"]

        class FakeClient:
            def get_capabilities(self) -> dict:
                return {"image": {}, "streams": {}}

            def get_config(self, timeout: int | None = None) -> dict:
                raise RuntimeError("Empty response for /config — native API returned no JSON")

        hub._camera_api_client = lambda camera: FakeClient()  # type: ignore[method-assign]
        hub._camera_api_token = lambda camera: "token"  # type: ignore[method-assign]
        hub._fetch_camera_api_details = lambda camera: {  # type: ignore[method-assign]
            "device_name": "cam",
            "streamer": "raptor",
            "version": "1",
        }
        hub._record_api_result = lambda *args, **kwargs: None  # type: ignore[method-assign]
        preview = hub.preview_camera_config_restore("aabbccddeeff", snapshot_id)
        self.assertTrue(preview["restore_ready"])
        self.assertTrue(preview["live_capabilities_ok"])
        self.assertFalse(preview["live_config_ok"])
        self.assertGreater(preview["compatible_count"], 0)

    def test_restore_apply_uses_settings_peel_for_stream_osd(self) -> None:
        hub = self._hub_with_store()
        snapshot_id = hub.history_store.record_config_snapshot(
            recorded_at=int(time.time()),
            camera_id="aabbccddeeff",
            source="manual",
            config={
                "stream0": {
                    "width": 1280,
                    "osd": {"enabled": True, "usertext": {"enabled": True, "format": "%hostname"}},
                }
            },
            capabilities={"streams": {}},
            content_hash="snap-osd",
        )["snapshot_id"]

        setting_calls: list[tuple[str, dict]] = []
        live_settings: dict[str, dict] = {}

        class FakeClient:
            def get_capabilities(self) -> dict:
                return {"streams": {}}

            def get_config(self, timeout: int | None = None) -> dict:
                return {"stream0": {"width": 640}}

            def patch_setting(self, path: str, body: dict) -> dict:
                setting_calls.append((path, body))
                live_settings[path] = dict(body)
                return {"applied": [path]}

            def get_setting(self, path: str) -> dict:
                return dict(live_settings.get(path) or {})

            def patch_config(self, payload: dict) -> dict:
                raise AssertionError(f"omnibus should not be required for peeled fields: {payload}")

            def get_device(self) -> dict:
                return {"software": {"firmware_version": "fw", "streamer": "raptor"}}

        hub._camera_api_client = lambda camera: FakeClient()  # type: ignore[method-assign]
        hub.refresh_camera_api_details = lambda camera_id: (_ for _ in ()).throw(  # type: ignore[method-assign]
            AssertionError("restore must not sync-refresh API details")
        )
        hub._schedule_api_refresh = lambda camera_id: None  # type: ignore[method-assign]
        hub._schedule_supported_controls_refresh = lambda camera_id: None  # type: ignore[method-assign]
        hub._record_optimistic_supported_controls = lambda *args, **kwargs: None  # type: ignore[method-assign]
        hub.backup_camera_config = lambda *args, **kwargs: {  # type: ignore[method-assign]
            "status": "success",
            "status_detail": "Stored config backup #42",
            "snapshot_id": 42,
        }

        with mock.patch("app.main.time.sleep", return_value=None):
            result = hub.restore_camera_config_backup("aabbccddeeff", snapshot_id, mode="compatible")
        self.assertEqual(result["status"], "success")
        paths = [path for path, _body in setting_calls]
        self.assertTrue(any(path.endswith("width") or "width" in path for path in paths), paths)
        self.assertTrue(any("osd" in path for path in paths), paths)

    def test_confirm_skips_advanced_image_leaves(self) -> None:
        hub = self._hub_with_store()
        get_paths: list[str] = []

        class FakeClient:
            def get_setting(self, path: str) -> dict:
                get_paths.append(path)
                if path in {"image/ae-compensation", "image/core-wb-mode"}:
                    raise RuntimeError(f"Empty response for /settings/{path}")
                return {"brightness": 128}

        hub._camera_api_client = lambda camera: FakeClient()  # type: ignore[method-assign]
        with mock.patch("app.main.time.sleep", return_value=None):
            ok, detail = hub._confirm_native_config_stage(
                hub.cameras["aabbccddeeff"],
                {
                    "image": {
                        "brightness": 128,
                        "ae_compensation": 5,
                        "core_wb_mode": 0,
                    }
                },
            )
        self.assertTrue(ok, detail)
        self.assertEqual(get_paths, ["image/brightness"])

    def test_restore_stages_emit_plan_and_confirm(self) -> None:
        hub = self._hub_with_store()
        snapshot_id = hub.history_store.record_config_snapshot(
            recorded_at=int(time.time()),
            camera_id="aabbccddeeff",
            source="manual",
            config={"image": {"hflip": True}, "stream0": {"width": 1280}},
            capabilities={"image": {}, "streams": {}},
            content_hash="snap-stages",
        )["snapshot_id"]
        live_settings: dict[str, dict] = {}

        class FakeClient:
            def get_capabilities(self) -> dict:
                return {"image": {}, "streams": {}}

            def get_config(self, timeout: int | None = None) -> dict:
                return {"image": {"hflip": False}, "stream0": {"width": 640}}

            def patch_setting(self, path: str, body: dict) -> dict:
                live_settings[path] = dict(body)
                return {"applied": [path]}

            def get_setting(self, path: str) -> dict:
                return dict(live_settings.get(path) or {})

            def patch_config(self, payload: dict) -> dict:
                return {"status": "accepted", "applied": list(payload.keys())}

            def get_device(self) -> dict:
                return {"software": {"firmware_version": "fw", "streamer": "raptor"}}

        hub._camera_api_client = lambda camera: FakeClient()  # type: ignore[method-assign]
        hub.backup_camera_config = lambda *args, **kwargs: {  # type: ignore[method-assign]
            "status": "success",
            "status_detail": "Stored config backup #99",
            "snapshot_id": 99,
        }
        events: list[dict] = []
        with mock.patch("app.main.time.sleep", return_value=None):
            for event in hub.iter_restore_camera_config_backup("aabbccddeeff", snapshot_id, mode="compatible"):
                events.append(event)
        kinds = [event.get("event") for event in events]
        self.assertEqual(kinds[0], "plan")
        self.assertIn("stage_confirming", kinds)
        self.assertEqual(kinds[-1], "complete")
        self.assertTrue(events[-1].get("ok"))
        self.assertGreaterEqual(len(events[0].get("stages") or []), 2)


class ConfigCloneHubTests(_ConfigHubTestMixin, unittest.TestCase):
    def _hub_two_cameras(self) -> Hub:
        hub = self._hub_with_store()
        hub.cameras["112233445566"] = Camera(
            camera_id="112233445566",
            name="garden",
            ip="192.168.1.11",
            api_base_url="https://192.168.1.11:1998/api/v1",
            api_token="tok2",
            api_status="online",
            api_streamer="raptor",
        )
        return hub

    def test_flatten_and_unflatten_roundtrip(self) -> None:
        hub = self._hub_two_cameras()
        original = {"image": {"hflip": True, "osd": {"x": 10}}, "stream0": {"width": 1280}}
        flat = hub._flatten_config_leaves(original)
        self.assertEqual(flat["image.hflip"], True)
        self.assertEqual(flat["image.osd.x"], 10)
        self.assertEqual(flat["stream0.width"], 1280)
        self.assertEqual(hub._unflatten_config_leaves(flat), original)

    def test_streams_list_expands_into_stream_fields(self) -> None:
        hub = self._hub_two_cameras()
        source_config = {
            "streams": [
                {
                    "id": 0,
                    "width": 1920,
                    "height": 1080,
                    "fps": 25,
                    "osd": {"enabled": True, "usertext": {"format": "%hostname", "pos_x": 12}},
                },
                {"id": 1, "width": 640, "height": 360, "fps": 15},
            ]
        }

        class SourceClient:
            def get_capabilities(self) -> dict:
                return {"image": {}, "streams": {}}

            def get_config(self, timeout: int | None = None) -> dict:
                return source_config

            def get_device(self) -> dict:
                return {"software": {"firmware_version": "fw", "streamer": "raptor"}}

        class TargetClient:
            def get_capabilities(self) -> dict:
                return {"image": {}, "streams": {}}

            def get_config(self, timeout: int | None = None) -> dict:
                return {"streams": [{"id": 0, "width": 640}, {"id": 1, "width": 320}]}

            def get_device(self) -> dict:
                return {"software": {"firmware_version": "fw", "streamer": "raptor"}}

        def client_for(camera):
            return SourceClient() if camera.camera_id == "aabbccddeeff" else TargetClient()

        hub._camera_api_client = client_for  # type: ignore[method-assign]
        preview = hub.preview_camera_config_clone(
            "112233445566",
            source_camera_id="aabbccddeeff",
            source_kind="live",
        )
        paths = {item["path"] for item in preview["compatible"]}
        self.assertIn("stream0.width", paths)
        self.assertIn("stream0.height", paths)
        self.assertIn("stream0.fps", paths)
        self.assertIn("stream0.osd.enabled", paths)
        self.assertIn("stream0.osd.usertext.format", paths)
        self.assertIn("stream0.osd.usertext.pos_x", paths)
        self.assertIn("stream1.width", paths)
        self.assertNotIn("streams", paths)

    def test_pull_preview_is_field_level_and_skips_secrets(self) -> None:
        hub = self._hub_two_cameras()
        source_config = {
            "agent": {"token": "secret"},
            "image": {"hflip": True, "vflip": False},
            "stream0": {"width": 1920, "osd": {"enabled": True, "pos_x": 8}},
        }

        class SourceClient:
            def get_capabilities(self) -> dict:
                return {"image": {}, "streams": {}}

            def get_config(self, timeout: int | None = None) -> dict:
                return source_config

            def get_device(self) -> dict:
                return {"software": {"firmware_version": "fw-src", "streamer": "raptor"}}

        class TargetClient:
            def get_capabilities(self) -> dict:
                return {"image": {}, "streams": {}}

            def get_config(self, timeout: int | None = None) -> dict:
                return {"image": {"hflip": False}, "stream0": {"width": 640}}

            def get_device(self) -> dict:
                return {"software": {"firmware_version": "fw-dst", "streamer": "raptor"}}

        def client_for(camera):
            return SourceClient() if camera.camera_id == "aabbccddeeff" else TargetClient()

        hub._camera_api_client = client_for  # type: ignore[method-assign]
        preview = hub.preview_camera_config_clone(
            "112233445566",
            source_camera_id="aabbccddeeff",
            source_kind="live",
        )
        paths = {item["path"] for item in preview["compatible"]}
        self.assertIn("image.hflip", paths)
        self.assertIn("stream0.osd.pos_x", paths)
        self.assertNotIn("agent.token", paths)
        self.assertTrue(any(item["path"] == "agent" or item["path"].startswith("agent") for item in preview["skipped_secrets"]))

    def test_pull_apply_respects_selected_paths(self) -> None:
        hub = self._hub_two_cameras()
        patched: list[dict] = []

        class SourceClient:
            def get_capabilities(self) -> dict:
                return {"image": {}, "streams": {}}

            def get_config(self, timeout: int | None = None) -> dict:
                return {"image": {"hflip": True, "vflip": True}, "stream0": {"width": 1920}}

            def get_device(self) -> dict:
                return {"software": {"firmware_version": "fw", "streamer": "raptor"}}

        class TargetClient:
            def __init__(self) -> None:
                self.live_settings: dict[str, dict] = {}

            def get_capabilities(self) -> dict:
                return {"image": {}, "streams": {}}

            def get_config(self, timeout: int | None = None) -> dict:
                return {"image": {"hflip": False, "vflip": False}, "stream0": {"width": 640}}

            def get_device(self) -> dict:
                return {"software": {"firmware_version": "fw", "streamer": "raptor"}}

            def patch_setting(self, path: str, body: dict) -> dict:
                patched.append({"path": path, "body": body})
                self.live_settings[path] = dict(body)
                return {"applied": [path]}

            def get_setting(self, path: str) -> dict:
                return dict(self.live_settings.get(path) or {})

            def patch_config(self, payload: dict) -> dict:
                patched.append({"omnibus": payload})
                return {"status": "accepted", "applied": list(payload.keys())}

        def client_for(camera):
            return SourceClient() if camera.camera_id == "aabbccddeeff" else target_client

        target_client = TargetClient()
        hub._camera_api_client = client_for  # type: ignore[method-assign]
        hub.backup_camera_config = lambda *args, **kwargs: {"stored": True}  # type: ignore[method-assign]

        with mock.patch("app.main.time.sleep", return_value=None):
            result = hub.apply_camera_config_clone(
                "112233445566",
                source_camera_id="aabbccddeeff",
                source_kind="live",
                selected_paths=["image.hflip", "stream0.width"],
                mode="compatible",
            )
        self.assertEqual(result["status"], "success")
        applied = hub._flatten_config_leaves(result["applied_payload"])
        self.assertEqual(set(applied.keys()), {"image.hflip", "stream0.width"})
        self.assertTrue(patched)

    def test_push_applies_to_multiple_targets(self) -> None:
        hub = self._hub_two_cameras()
        hub.cameras["998877665544"] = Camera(
            camera_id="998877665544",
            name="driveway",
            ip="192.168.1.12",
            api_base_url="https://192.168.1.12:1998/api/v1",
            api_token="tok3",
            api_status="online",
            api_streamer="raptor",
        )
        applied_by_camera: dict[str, list[str]] = {}

        class FakeClient:
            def __init__(self, camera_id: str, *, is_source: bool = False) -> None:
                self.camera_id = camera_id
                self.is_source = is_source
                self.live_settings: dict[str, dict] = {}

            def get_capabilities(self) -> dict:
                return {"image": {}, "streams": {}}

            def get_config(self, timeout: int | None = None) -> dict:
                if self.is_source:
                    return {"image": {"hflip": True}, "stream0": {"width": 1600}}
                return {"image": {"hflip": False}, "stream0": {"width": 640}}

            def get_device(self) -> dict:
                return {"software": {"firmware_version": "fw", "streamer": "raptor"}}

            def patch_setting(self, path: str, body: dict) -> dict:
                applied_by_camera.setdefault(self.camera_id, []).append(path)
                self.live_settings[path] = dict(body)
                return {"applied": [path]}

            def get_setting(self, path: str) -> dict:
                return dict(self.live_settings.get(path) or {})

            def patch_config(self, payload: dict) -> dict:
                applied_by_camera.setdefault(self.camera_id, []).extend(payload.keys())
                return {"status": "accepted", "applied": list(payload.keys())}

        clients: dict[str, FakeClient] = {}

        def client_for(camera):
            existing = clients.get(camera.camera_id)
            if existing is None:
                existing = FakeClient(camera.camera_id, is_source=camera.camera_id == "aabbccddeeff")
                clients[camera.camera_id] = existing
            return existing

        hub._camera_api_client = client_for  # type: ignore[method-assign]
        hub.backup_camera_config = lambda *args, **kwargs: {"stored": True}  # type: ignore[method-assign]

        with mock.patch("app.main.time.sleep", return_value=None):
            result = hub.apply_camera_config_clone_push(
                "aabbccddeeff",
                target_camera_ids=["112233445566", "998877665544"],
                source_kind="live",
                selected_paths=["image.hflip"],
                mode="compatible",
            )
        self.assertEqual(result["status"], "success")
        self.assertEqual(result["success_count"], 2)
        self.assertIn("112233445566", applied_by_camera)
        self.assertIn("998877665544", applied_by_camera)

    def test_clone_from_peer_backup(self) -> None:
        hub = self._hub_two_cameras()
        snapshot_id = hub.history_store.record_config_snapshot(
            recorded_at=int(time.time()),
            camera_id="aabbccddeeff",
            source="manual",
            config={"image": {"hflip": True}, "stream0": {"osd": {"pos_x": 12}}},
            capabilities={"image": {}, "streams": {}},
            content_hash="clone-backup",
        )["snapshot_id"]

        class TargetClient:
            def get_capabilities(self) -> dict:
                return {"image": {}, "streams": {}}

            def get_config(self, timeout: int | None = None) -> dict:
                return {"image": {"hflip": False}, "stream0": {"osd": {"pos_x": 0}}}

            def get_device(self) -> dict:
                return {"software": {"firmware_version": "fw", "streamer": "raptor"}}

            def patch_setting(self, path: str, body: dict) -> dict:
                return {"applied": [path]}

            def patch_config(self, payload: dict) -> dict:
                return {"status": "accepted", "applied": list(payload.keys())}

        hub._camera_api_client = lambda camera: TargetClient()  # type: ignore[method-assign]
        hub.backup_camera_config = lambda *args, **kwargs: {"stored": True}  # type: ignore[method-assign]

        preview = hub.preview_camera_config_clone(
            "112233445566",
            source_camera_id="aabbccddeeff",
            source_kind="backup",
            snapshot_id=snapshot_id,
        )
        self.assertEqual(preview["source_kind"], "backup")
        self.assertIn("stream0.osd.pos_x", preview["default_selected_paths"])

        result = hub.apply_camera_config_clone(
            "112233445566",
            source_camera_id="aabbccddeeff",
            source_kind="backup",
            snapshot_id=snapshot_id,
            selected_paths=["stream0.osd.pos_x"],
            mode="compatible",
        )
        self.assertEqual(result["applied_payload"], {"stream0": {"osd": {"pos_x": 12}}})


if __name__ == "__main__":
    unittest.main()
