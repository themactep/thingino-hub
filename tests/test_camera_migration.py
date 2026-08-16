import sys
import tempfile
import threading
import time
import types
import unittest
from pathlib import Path


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


class CameraMigrationTests(unittest.TestCase):
    def _hub(self) -> Hub:
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        hub = object.__new__(Hub)
        hub.state_lock = threading.Lock()
        hub.reply_lock = threading.Lock()
        hub.config_path = str(Path(tmp.name) / "config.yaml")
        hub.state_path = Path(tmp.name) / "state.yaml"
        hub.config = {"cameras": []}
        hub.static_camera_ids = {"oldmac000001"}
        hub.last_chat_by_camera = {}
        hub.native_action_history_by_camera = {}
        hub.pending_by_request = {}
        hub.cameras = {
            "oldmac000001": Camera(
                camera_id="oldmac000001",
                name="Bird Box",
                ip="192.168.140.11",
                api_token="old-token",
                api_status="offline",
                hostname="birdbox",
                onvif_serial_number="SN-ABC",
                last_registration_at=1000,
            ),
            "newmac000002": Camera(
                camera_id="newmac000002",
                name="newmac000002",
                ip="192.168.140.11",
                api_token="",
                api_status="online",
                hostname="birdbox",
                onvif_serial_number="SN-ABC",
                last_registration_at=2000,
            ),
        }
        hub.history_store = HistoryStore(
            str(Path(tmp.name) / "history.sqlite3"),
            max_config_snapshots_per_camera=20,
            config_snapshot_max_age_days=0,
        )
        self.addCleanup(hub.history_store.close)
        hub.history_enabled = True
        hub._resolve_camera_id = lambda camera_id: str(camera_id or "").strip().lower()  # type: ignore[method-assign]
        hub._camera_api_token = lambda camera: str(camera.api_token or "")  # type: ignore[method-assign]
        hub._coerce_int = Hub._coerce_int.__get__(hub, Hub)
        hub._format_timestamp = Hub._format_timestamp.__get__(hub, Hub)
        hub._config_snapshot_summary_for_ui = Hub._config_snapshot_summary_for_ui.__get__(hub, Hub)
        hub._config_snapshot_detail_for_ui = Hub._config_snapshot_detail_for_ui.__get__(hub, Hub)
        hub.list_camera_config_backups = Hub.list_camera_config_backups.__get__(hub, Hub)
        hub.get_camera_migration_offer = Hub.get_camera_migration_offer.__get__(hub, Hub)
        hub._camera_peers_sharing_ip = Hub._camera_peers_sharing_ip.__get__(hub, Hub)
        hub._camera_identity_soft_match = Hub._camera_identity_soft_match.__get__(hub, Hub)
        hub.migrate_camera_identity = Hub.migrate_camera_identity.__get__(hub, Hub)
        hub._record_history_action = lambda *args, **kwargs: None  # type: ignore[method-assign]
        hub.export_config = lambda: {"cameras": [{"id": "oldmac000001", "name": "Bird Box", "ip": "192.168.140.11"}]}  # type: ignore[method-assign]
        hub.save_config = lambda config: None  # type: ignore[method-assign]
        hub._persist_state = lambda: None  # type: ignore[method-assign]
        hub.reload_config = lambda: None  # type: ignore[method-assign]
        hub._publish_control_command = lambda *args, **kwargs: False  # type: ignore[method-assign]
        hub._connect_mqtt = lambda: False  # type: ignore[method-assign]
        hub.mqtt_client = None
        hub._registration_topic_for_camera = lambda camera_id: f"thingino/cam/{camera_id}/hello"  # type: ignore[method-assign]
        return hub

    def test_rebind_moves_config_snapshots(self) -> None:
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        store = HistoryStore(str(Path(tmp.name) / "h.sqlite3"), config_snapshot_max_age_days=0)
        self.addCleanup(store.close)
        store.record_config_snapshot(
            recorded_at=100,
            camera_id="oldmac000001",
            source="manual",
            config={"image": {"hflip": True}},
            content_hash="h1",
        )
        counts = store.rebind_camera_id("oldmac000001", "newmac000002")
        self.assertEqual(counts["config_snapshots"], 1)
        self.assertEqual(store.list_config_snapshots("oldmac000001"), [])
        self.assertEqual(len(store.list_config_snapshots("newmac000002")), 1)

    def test_migration_offer_detects_ip_and_serial(self) -> None:
        hub = self._hub()
        offer = hub.get_camera_migration_offer("oldmac000001")
        self.assertIsNotNone(offer)
        assert offer is not None
        self.assertEqual(offer["stale_camera_id"], "oldmac000001")
        self.assertEqual(offer["live_camera_id"], "newmac000002")
        self.assertIn("ip", offer["match_reasons"])
        self.assertIn("hostname", offer["match_reasons"])
        self.assertIn("onvif_serial", offer["match_reasons"])
        self.assertEqual(offer["viewing"], "stale")

    def test_migrate_rebinds_backups_and_removes_stale(self) -> None:
        hub = self._hub()
        hub.history_store.record_config_snapshot(
            recorded_at=int(time.time()),
            camera_id="oldmac000001",
            source="manual",
            label="pre-ota",
            config={"image": {"brightness": 128}, "stream0": {"width": 1920}},
            content_hash="pre-ota",
        )
        result = hub.migrate_camera_identity(
            from_camera_id="oldmac000001",
            to_camera_id="newmac000002",
            restore_latest_backup=False,
        )
        self.assertEqual(result["status"], "success")
        self.assertEqual(result["rebind"]["config_snapshots"], 1)
        self.assertNotIn("oldmac000001", hub.cameras)
        self.assertIn("newmac000002", hub.cameras)
        self.assertEqual(hub.cameras["newmac000002"].name, "Bird Box")
        backups = hub.list_camera_config_backups("newmac000002")
        self.assertEqual(len(backups), 1)
        self.assertEqual(backups[0]["label"], "pre-ota")
        self.assertTrue(result["restore_available"])

    def test_migrate_does_not_reload_config_when_removing_stale(self) -> None:
        """Regression: unregister used to reload_config and drop the live MQTT identity."""
        hub = self._hub()
        hub.config = {
            "cameras": [
                {"id": "oldmac000001", "name": "Bird Box", "ip": "192.168.140.11"},
            ]
        }
        hub.export_config = lambda: {  # type: ignore[method-assign]
            "cameras": [dict(item) for item in (hub.config.get("cameras") or [])]
        }
        saved: list[dict] = []

        def save_config(config: dict) -> None:
            saved.append(
                {
                    "cameras": [dict(item) for item in (config.get("cameras") or [])],
                }
            )
            hub.config = {
                "cameras": [dict(item) for item in (config.get("cameras") or [])],
            }

        reloads: list[str] = []
        hub.save_config = save_config  # type: ignore[method-assign]
        hub.reload_config = lambda: reloads.append("reload")  # type: ignore[method-assign]
        hub.unregister_camera = Hub.unregister_camera.__get__(hub, Hub)
        hub.history_store.record_config_snapshot(
            recorded_at=int(time.time()),
            camera_id="oldmac000001",
            source="manual",
            label="pre-ota",
            config={"image": {"brightness": 128}},
            content_hash="pre-ota-3",
        )
        result = hub.migrate_camera_identity(
            from_camera_id="oldmac000001",
            to_camera_id="newmac000002",
            restore_latest_backup=False,
        )
        self.assertEqual(result["status"], "success")
        self.assertEqual(reloads, [])
        self.assertIn("newmac000002", hub.cameras)
        self.assertNotIn("oldmac000001", hub.cameras)
        self.assertTrue(any("newmac000002" in {str(c.get("id") or "").lower() for c in cfg.get("cameras", [])} for cfg in saved))

    def test_migrate_defers_restore_until_paired(self) -> None:
        hub = self._hub()
        hub.history_store.record_config_snapshot(
            recorded_at=int(time.time()),
            camera_id="oldmac000001",
            source="manual",
            label="pre-ota",
            config={"image": {"brightness": 128}},
            content_hash="pre-ota-2",
        )
        result = hub.migrate_camera_identity(
            from_camera_id="oldmac000001",
            to_camera_id="newmac000002",
            restore_latest_backup=True,
        )
        self.assertEqual(result["status"], "success")
        self.assertIsNone(result.get("restore_result"))
        self.assertIsNotNone(result.get("restore_deferred"))
        self.assertIn("pairing", str(result["restore_deferred"]["status_detail"]).lower())
        self.assertTrue(result["restore_available"])


if __name__ == "__main__":
    unittest.main()
