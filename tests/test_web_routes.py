import unittest

from app.web import create_web_app


class FakeHub:
    def __init__(self) -> None:
        self.recent_events = [
            {
                "sequence": 3,
                "camera_id": "cam1",
                "camera_name": "Test Camera",
                "timestamp": "1700000000",
                "at": "now",
                "source": "camera_event",
                "name": "motion started",
                "status": "success",
                "detail": "Motion detected",
            }
        ]
        self.camera = {
            "camera_id": "cam1",
            "name": "Test Camera",
            "status": "online",
            "camera_image_id": "wyze_cam3_t31x_gc2053_atbm6031",
            "ota_upgrade_command": "CAMERA=wyze_cam3_t31x_gc2053_atbm6031 IP=192.168.1.2 make cleanbuild upgrade_ota",
            "api_status": "online",
            "api_last_ok_at": "now",
            "api_last_error": "",
            "api_device_name": "test-cam",
            "api_device_model": "wyze",
            "api_streamer": "prudynt",
            "api_version": "0-local",
            "hostname": "test-cam",
            "ip": "192.168.1.2",
            "snapshot_url": "http://192.168.1.2/x/ch0.jpg",
            "web_ui_url": "http://192.168.1.2/",
            "api_base_url": "https://192.168.1.2:1998/api/v1",
            "api_key": "",
            "last_registration_at": "now",
            "last_probe_at": "now",
            "last_snapshot_ok_at": "now",
            "last_probe_error": "",
            "identity_conflict_error": "",
            "onvif_manufacturer": "Thingino",
            "onvif_model": "wyze",
            "onvif_firmware_version": "master",
            "onvif_serial_number": "123",
            "onvif_hardware_id": "t31",
            "onvif_last_ok_at": "now",
            "onvif_last_error": "",
            "preview_version": "1",
            "override_name": "",
            "override_ip": "",
            "override_snapshot_url": "",
            "override_api_key": "",
            "override_api_base_url": "",
            "override_api_token": "",
            "override_onvif_endpoint": "",
            "override_onvif_username": "",
            "override_onvif_password": "",
            "native_action_history": [],
        }
        self.controls = {
            "native_controls_available": True,
            "native_daynight_action_supported": True,
            "native_daynight_requested_mode": "auto",
            "native_privacy_supported": True,
            "native_privacy_enabled": False,
        }

    def get_camera_for_ui(self, camera_id: str):
        if camera_id != "cam1":
            raise RuntimeError("Unknown camera")
        return dict(self.camera)

    def get_camera_supported_controls_for_ui(self, camera_id: str):
        if camera_id != "cam1":
            raise RuntimeError("Unknown camera")
        return dict(self.controls)

    def list_recent_events_for_ui(self, limit: int = 40):
        return list(self.recent_events[:limit])

    def live_events_since(self, last_sequence: int, limit: int = 20):
        events = [entry for entry in self.recent_events if int(entry["sequence"]) > int(last_sequence)]
        return 3, events[:limit]

    def queue_camera_api_refresh(self, camera_id: str) -> str:
        if camera_id != "cam1":
            raise RuntimeError("Unknown camera")
        return "scheduled"

    def queue_camera_onvif_refresh(self, camera_id: str) -> str:
        if camera_id != "cam1":
            raise RuntimeError("Unknown camera")
        return "already_running"

    def set_camera_daynight_mode(self, camera_id: str, mode: str, *, refresh_after: bool = True):
        if camera_id != "cam1":
            raise RuntimeError("Unknown camera")
        if refresh_after:
            raise AssertionError("day/night route should be non-blocking")
        return {"status": "accepted", "mode": mode}

    def set_camera_privacy(self, camera_id: str, enabled: bool, channel: str = "all", *, refresh_after: bool = True):
        if camera_id != "cam1":
            raise RuntimeError("Unknown camera")
        if refresh_after:
            raise AssertionError("privacy route should be non-blocking")
        return {"status": "accepted", "enabled": enabled, "channel": channel}

    def patch_camera_config(self, camera_id: str, payload, *, refresh_after: bool = True):
        if camera_id != "cam1":
            raise RuntimeError("Unknown camera")
        if refresh_after:
            raise AssertionError("patch config route should be non-blocking")
        if "motion" in payload:
            motion = payload.get("motion") or {}
            self.controls["native_motion_enabled"] = bool(motion.get("enabled"))
        history = list(self.camera.get("native_action_history") or [])
        history.insert(
            0,
            {
                "action": "patch config",
                "at": "now",
                "detail": ", ".join(sorted(payload.keys())) or "accepted",
                "source": "native_api",
                "status": "success",
            },
        )
        self.camera["native_action_history"] = history[:8]
        return {"status": "accepted"}

    def perform_bulk_action(self, camera_ids, action: str):
        if action == "restart-streaming":
            return {
                "action": action,
                "total": len(camera_ids),
                "success_count": 0,
                "error_count": len(camera_ids),
                "results": [{"camera_id": camera_id, "status": "error", "detail": "camera offline"} for camera_id in camera_ids],
            }
        return {
            "action": action,
            "total": len(camera_ids),
            "success_count": len(camera_ids),
            "error_count": 0,
            "results": [{"camera_id": camera_id, "status": "success", "detail": "scheduled"} for camera_id in camera_ids],
        }

    def unregister_camera(self, camera_id: str):
        if camera_id != "cam1":
            raise RuntimeError("Unknown camera")
        return {
            "config_removed": True,
            "retained_cleared": True,
            "command_published": True,
            "retained_error": "",
        }

    def connect_camera(self, enrollment):
        camera_id = enrollment.get("camera_id") or enrollment.get("id") or "cam2"
        return {
            "camera_id": camera_id,
            "status": "success",
            "status_detail": f"Connected {camera_id} to the hub.",
            "api_base_url": f"https://{enrollment.get('ip') or '192.168.1.2'}:1998/api/v1",
            "api_token": "connected-token-123",
        }

    def enroll_camera(self, enrollment):
        return {
            "camera_id": "cam2",
            "updated_existing": False,
            "rescan_requested": True,
            "api_refresh": "scheduled",
            "onvif_refresh": "not_configured",
            "controls_refresh": "scheduled",
        }

    def probe_camera_enrollment(self, enrollment):
        return {
            "camera_id": "cam2",
            "name": "Back Door",
            "ip": enrollment.get("ip") or "192.168.1.3",
            "conflicts": {},
            "snapshot_url": "http://192.168.1.3/x/ch0.jpg",
            "can_save": True,
            "api": {
                "configured": True,
                "ok": True,
                "base_url": "https://192.168.1.3:1998/api/v1",
                "error": "",
                "device_name": "Back Door",
                "device_model": "wyze",
                "streamer": "prudynt",
                "version": "1.0",
            },
            "onvif": {
                "configured": True,
                "ok": False,
                "endpoint": "http://192.168.1.3/onvif/device_service",
                "error": "authentication failed",
                "manufacturer": "",
                "model": "",
                "firmware_version": "",
            },
        }

    def generate_pairing_bundle(self, enrollment):
        return {
            "camera_id": "cam2",
            "name": "Back Door",
            "ip": enrollment.get("ip") or "192.168.1.3",
            "api_base_url": "https://192.168.1.3:1998/api/v1",
            "api_token": "pairing-token-123",
            "bootstrap_payload": {
                "agent": {
                    "enabled": True,
                    "tls": True,
                    "listen": "0.0.0.0",
                    "port": 1998,
                    "token": "pairing-token-123",
                }
            },
            "bootstrap_json": '{\n  "agent": {\n    "enabled": true,\n    "listen": "0.0.0.0",\n    "port": 1998,\n    "tls": true,\n    "token": "pairing-token-123"\n  }\n}',
            "commands": [
                "jct /etc/thingino.json set agent.enabled true",
                "jct /etc/thingino.json set agent.tls true",
            ],
            "bootstrap_install_commands": [
                "printf '%s\\n' '{\"agent\":{}}' > /etc/thingino-agent-bootstrap.json",
                "/etc/init.d/S95thingino-agent restart",
            ],
            "save_entry": {
                "id": "cam2",
                "api_base_url": "https://192.168.1.3:1998/api/v1",
                "api_token": "pairing-token-123",
            },
        }

    def install_pairing_bundle_via_mqtt(self, enrollment):
        camera_id = enrollment.get("camera_id") or enrollment.get("id") or "cam2"
        result = self.generate_pairing_bundle(enrollment)
        result.update(
            {
                "status": "success",
                "status_detail": "Agent bootstrap installed via MQTT",
                "mqtt": {
                    "camera_id": camera_id,
                    "published": True,
                    "reply_received": True,
                    "reply_ok": True,
                    "reply_text": "Agent bootstrap installed",
                    "request_id": "request-123",
                },
            }
        )
        return result

    def snapshot_status(self):
        return {
            "telegram_ok": True,
            "telegram_last_ok": "now",
            "telegram_last_error": "",
            "mqtt_connected": True,
            "mqtt_host": "mqtt.local",
            "mqtt_port": 1883,
            "mqtt_last_error": "",
            "config_path": "/tmp/config.yaml",
            "history_enabled": True,
            "history_db_path": "/tmp/history.sqlite3",
            "history_recent_actions_limit": 20,
            "api_known": 1,
            "api_ready": 1,
            "api_errors": 0,
            "api_last_ok": "now",
            "onvif_known": 0,
            "onvif_ready": 0,
            "onvif_errors": 0,
            "onvif_last_ok": "",
            "last_reload_at": "now",
        }

    def list_cameras_for_ui(self):
        return [
            {
                "camera_id": "cam1",
                "name": "Test Camera",
                "ip": "192.168.1.2",
                "snapshot_url": "http://192.168.1.2/x/ch0.jpg",
                "status": "online",
                "api_status": "online",
                "api_streamer": "prudynt",
                "preview_state": "placeholder",
                "hostname": "test-cam",
                "onvif_label": "",
                "last_registration_at": "now",
                "last_probe_at": "now",
                "last_snapshot_ok_at": "now",
                "last_probe_error": "",
                "preview_version": "1",
            }
        ]

    def get_camera_history_for_ui(self, camera_id: str, limit: int = 100, kind_filter: str = "all", sample_type_filter: str = "all"):
        if camera_id != "cam1":
            raise RuntimeError("Unknown camera")
        return {
            "camera_id": "cam1",
            "name": "Test Camera",
            "ip": "192.168.1.2",
            "status": "online",
            "api_status": "online",
            "history_enabled": True,
            "history_db_path": "/tmp/history.sqlite3",
            "history_limit": limit,
            "history_kind_filter": kind_filter,
            "history_sample_type_filter": sample_type_filter,
            "available_sample_types": ["all", "api_probe"],
            "timeline_action_count": 1,
            "timeline_state_count": 0,
            "timeline_config_count": 1,
            "latest_api_probe": None,
            "latest_snapshot_probe": None,
            "charts": [
                {
                    "title": "Network Reachability",
                    "summary": "100% positive across 1 known samples",
                    "latest": "online",
                    "width": 320,
                    "height": 44,
                    "bars": [
                        {"x": 0, "y": 6, "width": 319, "height": 32, "fill": "#198754", "title": "now: online"},
                    ],
                    "range_start": "now",
                    "range_end": "now",
                    "sample_count": 1,
                    "legend": [
                        {"label": "online", "fill": "#198754"},
                    ],
                }
            ],
            "timeline": [
                {
                    "at": "now",
                    "timestamp": "1700000000",
                    "kind": "config",
                    "name": "enrollment create",
                    "status": "info",
                    "source": "hub",
                    "detail": "/hub/enrollment/ip: null -> \"192.168.1.2\" (Camera enrollment)",
                }
            ],
        }


class WebRouteTests(unittest.TestCase):
    def setUp(self) -> None:
        self.hub = FakeHub()
        self.app = create_web_app(self.hub)
        self.client = self.app.test_client()
        self.json_headers = {
            "Accept": "application/json",
            "X-Requested-With": "fetch",
        }

    def test_hydrate_returns_camera_payload_and_refresh_state(self) -> None:
        response = self.client.post("/camera/cam1/hydrate", headers=self.json_headers)

        self.assertEqual(response.status_code, 200)
        payload = response.get_json()
        self.assertTrue(payload["ok"])
        self.assertEqual(payload["refreshes"], {"api": "scheduled", "onvif": "already_running"})
        self.assertEqual(payload["camera"]["camera_id"], "cam1")
        self.assertTrue(payload["camera"]["native_controls_available"])

    def test_payload_route_returns_full_camera_payload(self) -> None:
        response = self.client.get("/camera/cam1/payload", headers=self.json_headers)

        self.assertEqual(response.status_code, 200)
        payload = response.get_json()
        self.assertTrue(payload["ok"])
        self.assertEqual(payload["camera"]["camera_id"], "cam1")
        self.assertIn("native_daynight_requested_mode", payload["camera"])

    def test_camera_detail_renders_copyable_ota_command(self) -> None:
        response = self.client.get("/camera/cam1")

        self.assertEqual(response.status_code, 200)
        body = response.get_data(as_text=True)
        self.assertIn("Firmware Rebuild and OTA Command", body)
        self.assertIn("CAMERA=wyze_cam3_t31x_gc2053_atbm6031 IP=192.168.1.2 make cleanbuild upgrade_ota", body)
        self.assertIn("/pair/cam1", body)
        self.assertIn(">Pair<", body)
        self.assertIn("/connect/cam1", body)
        self.assertIn("Connect to Hub", body)

    def test_pair_camera_returns_success_summary(self) -> None:
        response = self.client.post("/pair/cam1", headers=self.json_headers)

        self.assertEqual(response.status_code, 200)
        payload = response.get_json()
        self.assertTrue(payload["ok"])
        self.assertIn("Pairing installed for cam1", payload["message"])
        self.assertEqual(payload["redirect_url"], "/camera/cam1")

    def test_connect_camera_returns_success_summary(self) -> None:
        response = self.client.post(
            "/connect/cam1",
            data={"onvif_username": "thingino", "onvif_password": "thingino"},
            headers=self.json_headers,
        )

        self.assertEqual(response.status_code, 200)
        payload = response.get_json()
        self.assertTrue(payload["ok"])
        self.assertIn("Connected cam1 to the hub.", payload["message"])
        self.assertEqual(payload["redirect_url"], "/camera/cam1")

    def test_hydrate_payload_route_returns_delta_payload(self) -> None:
        response = self.client.get("/camera/cam1/hydrate-payload", headers=self.json_headers)

        self.assertEqual(response.status_code, 200)
        payload = response.get_json()
        self.assertTrue(payload["ok"])
        self.assertEqual(payload["camera"]["api_status"], "online")
        self.assertIn("native_daynight_requested_mode", payload["camera"])
        self.assertNotIn("camera_id", payload["camera"])

    def test_daynight_returns_minimal_delta(self) -> None:
        response = self.client.post(
            "/daynight/cam1",
            data={"daynight_mode": "day"},
            headers=self.json_headers,
        )

        self.assertEqual(response.status_code, 200)
        payload = response.get_json()
        self.assertEqual(payload["message"], "Day/night set to day.")
        self.assertEqual(payload["camera"], {"native_daynight_requested_mode": "day"})

    def test_privacy_returns_minimal_delta(self) -> None:
        response = self.client.post(
            "/privacy/cam1",
            data={"privacy_enabled": "true", "privacy_channel": "all"},
            headers=self.json_headers,
        )

        self.assertEqual(response.status_code, 200)
        payload = response.get_json()
        self.assertEqual(payload["message"], "Privacy enabled.")
        self.assertEqual(payload["camera"], {"native_privacy_enabled": True})

    def test_apply_supported_config_returns_latest_action_delta(self) -> None:
        response = self.client.post(
            "/apply-supported-config/cam1",
            data={"motion_enabled_present": "1", "motion_enabled": "on"},
            headers=self.json_headers,
        )

        self.assertEqual(response.status_code, 200)
        payload = response.get_json()
        self.assertEqual(payload["message"], "Settings applied: native config: accepted")
        self.assertTrue(payload["camera"]["native_motion_enabled"])
        self.assertIn("native_action_history_latest", payload["camera"])
        self.assertNotIn("native_action_history", payload["camera"])
        self.assertEqual(payload["camera"]["native_action_history_latest"]["detail"], "motion")

    def test_refresh_api_is_queued_and_has_no_camera_blob(self) -> None:
        response = self.client.post("/refresh-api/cam1", headers=self.json_headers)

        self.assertEqual(response.status_code, 200)
        payload = response.get_json()
        self.assertTrue(payload["ok"])
        self.assertEqual(payload["message"], "Native API refresh queued.")
        self.assertNotIn("camera", payload)

    def test_event_feed_returns_recent_entries(self) -> None:
        response = self.client.get("/events/feed", headers=self.json_headers)

        self.assertEqual(response.status_code, 200)
        payload = response.get_json()
        self.assertTrue(payload["ok"])
        self.assertEqual(len(payload["events"]), 1)
        self.assertEqual(payload["events"][0]["name"], "motion started")

    def test_dashboard_links_to_status_events_and_enroll_pages(self) -> None:
        response = self.client.get("/")

        self.assertEqual(response.status_code, 200)
        body = response.get_data(as_text=True)
        self.assertIn("href=\"/status\"", body)
        self.assertIn("href=\"/events\"", body)
        self.assertIn("href=\"/enroll\"", body)
        self.assertIn("bulk-action-form", body)
        self.assertNotIn("action=\"/delete/cam1\"", body)
        self.assertNotIn(">Delete<", body)
        self.assertNotIn("enroll-camera-form", body)
        self.assertNotIn("dashboard-event-feed", body)
        self.assertNotIn("Polling OK", body)
        self.assertNotIn("Connected", body)

    def test_delete_camera_returns_success_summary(self) -> None:
        response = self.client.post("/delete/cam1", headers=self.json_headers)

        self.assertEqual(response.status_code, 200)
        payload = response.get_json()
        self.assertTrue(payload["ok"])
        self.assertIn("Removed cam1 from the roster", payload["message"])
        self.assertEqual(payload["redirect_url"], "/")

    def test_status_page_renders_status_cards(self) -> None:
        response = self.client.get("/status")

        self.assertEqual(response.status_code, 200)
        body = response.get_data(as_text=True)
        self.assertIn("System Status", body)
        self.assertIn("Telegram", body)
        self.assertIn("MQTT", body)
        self.assertIn("Native API", body)
        self.assertIn("History", body)
        self.assertIn("Reload Config", body)

    def test_events_page_renders_live_event_feed(self) -> None:
        response = self.client.get("/events")

        self.assertEqual(response.status_code, 200)
        body = response.get_data(as_text=True)
        self.assertIn("Live Event Feed", body)
        self.assertIn("dashboard-event-feed", body)
        self.assertIn("motion started", body)
        self.assertIn("new EventSource", body)

    def test_enroll_page_renders_enrollment_form(self) -> None:
        response = self.client.get("/enroll")

        self.assertEqual(response.status_code, 200)
        body = response.get_data(as_text=True)
        self.assertIn("Enroll Camera", body)
        self.assertIn("enroll-camera-form", body)
        self.assertIn("enroll-pairing-button", body)
        self.assertIn("enroll-pairing-install-button", body)
        self.assertIn("enroll-probe-button", body)
        self.assertNotIn("enroll_camera_id", body)
        self.assertNotIn("enroll_name", body)

    def test_dashboard_renders_and_clears_bulk_action_results(self) -> None:
        with self.client.session_transaction() as session:
            session["dashboard_bulk_action_result"] = {
                "action": "restart-streaming",
                "total": 1,
                "success_count": 0,
                "error_count": 1,
                "results": [{"camera_id": "cam1", "status": "error", "detail": "camera offline"}],
            }

        response = self.client.get("/")

        self.assertEqual(response.status_code, 200)
        body = response.get_data(as_text=True)
        self.assertIn("Bulk Results", body)
        self.assertIn("camera offline", body)
        self.assertIn("Retry", body)

        second_response = self.client.get("/")
        second_body = second_response.get_data(as_text=True)
        self.assertNotIn("Bulk Results", second_body)

    def test_camera_history_renders_config_change_entries(self) -> None:
        response = self.client.get("/camera/cam1/history")

        self.assertEqual(response.status_code, 200)
        body = response.get_data(as_text=True)
        self.assertIn("Network Reachability", body)
        self.assertIn("Config changes only", body)
        self.assertIn("enrollment create", body)
        self.assertIn("/hub/enrollment/ip", body)

    def test_bulk_action_returns_summary(self) -> None:
        response = self.client.post(
            "/bulk-action",
            data={"bulk_action": "refresh-api", "camera_ids": ["cam1"]},
            headers=self.json_headers,
        )

        self.assertEqual(response.status_code, 200)
        payload = response.get_json()
        self.assertTrue(payload["ok"])
        self.assertEqual(payload["result"]["success_count"], 1)
        self.assertEqual(payload["result"]["action"], "refresh-api")

    def test_bulk_action_redirect_sets_dashboard_results(self) -> None:
        response = self.client.post(
            "/bulk-action",
            data={"bulk_action": "restart-streaming", "camera_ids": ["cam1"]},
            follow_redirects=True,
        )

        self.assertEqual(response.status_code, 200)
        body = response.get_data(as_text=True)
        self.assertIn("Bulk Results", body)
        self.assertIn("camera offline", body)
        self.assertIn("Retry", body)

    def test_enroll_camera_returns_new_camera_summary(self) -> None:
        response = self.client.post(
            "/enroll",
            data={
                "ip": "192.168.1.3",
            },
            headers=self.json_headers,
        )

        self.assertEqual(response.status_code, 200)
        payload = response.get_json()
        self.assertTrue(payload["ok"])
        self.assertEqual(payload["result"]["camera_id"], "cam2")
        self.assertIn("Connected cam2 to the hub.", payload["message"])

    def test_enroll_probe_returns_probe_summary(self) -> None:
        response = self.client.post(
            "/enroll/probe",
            data={
                "ip": "192.168.1.3",
            },
            headers=self.json_headers,
        )

        self.assertEqual(response.status_code, 200)
        payload = response.get_json()
        self.assertTrue(payload["ok"])
        self.assertEqual(payload["result"]["camera_id"], "cam2")
        self.assertTrue(payload["result"]["api"]["ok"])
        self.assertFalse(payload["result"]["onvif"]["ok"])

    def test_pairing_bundle_returns_bootstrap_payload(self) -> None:
        response = self.client.post(
            "/enroll/pairing-bundle",
            data={
                "ip": "192.168.1.3",
            },
            headers=self.json_headers,
        )

        self.assertEqual(response.status_code, 200)
        payload = response.get_json()
        self.assertTrue(payload["ok"])
        self.assertEqual(payload["result"]["camera_id"], "cam2")
        self.assertEqual(payload["result"]["api_token"], "pairing-token-123")
        self.assertEqual(payload["result"]["bootstrap_payload"]["agent"]["listen"], "0.0.0.0")

    def test_pairing_install_returns_mqtt_confirmation(self) -> None:
        response = self.client.post(
            "/enroll/pairing-install",
            data={
                "ip": "192.168.1.3",
            },
            headers=self.json_headers,
        )

        self.assertEqual(response.status_code, 200)
        payload = response.get_json()
        self.assertTrue(payload["ok"])
        self.assertEqual(payload["result"]["status"], "success")
        self.assertTrue(payload["result"]["mqtt"]["reply_received"])
        self.assertEqual(payload["result"]["mqtt"]["reply_text"], "Agent bootstrap installed")


if __name__ == "__main__":
    unittest.main()