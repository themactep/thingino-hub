import unittest

from fastapi.testclient import TestClient

from app.api_v2 import create_api_v2_app


class FakeHub:
    def __init__(self) -> None:
        self._status = {
            "mqtt_connected": True,
            "telegram_ok": True,
            "mqtt_host": "broker.local",
        }
        self._cameras = [
            {
                "camera_id": "cam1",
                "name": "Front Door",
                "status": "online",
                "ip": "192.168.1.10",
                "api_status": "online",
                "api_last_error": "",
                "onvif_model": "wyze",
                "setup_status": "ready",
                "mqtt_command_status": "online",
                "mqtt_command_capable": True,
                "present_on_mqtt_broker": True,
                "onvif_last_error": "",
            },
            {
                "camera_id": "cam2",
                "name": "Garage",
                "status": "offline",
                "ip": "192.168.1.11",
                "api_status": "offline",
                "api_last_error": "timeout",
                "onvif_model": "t31",
                "setup_status": "connect",
                "mqtt_command_status": "offline",
                "mqtt_command_capable": True,
                "present_on_mqtt_broker": False,
                "onvif_last_error": "auth failed",
            },
        ]
        self._events = [
            {"sequence": 1, "camera_id": "cam1", "name": "motion started"},
            {"sequence": 2, "camera_id": "cam2", "name": "mqtt disconnected"},
        ]
        self._refresh_api_result = "scheduled"
        self._refresh_onvif_result = "scheduled"
        self._refresh_snapshot_result = "scheduled"
        self._service_result = {"status": "accepted"}
        self._rescan_total = 1
        self._rescan_published = 1
        self._privacy_result = {"status": "accepted"}
        self._daynight_result = {"status": "accepted"}
        self._record_result = {"status": "accepted", "result": {"path": "/tmp/cam1.mp4"}}
        self._patch_result = {"status": "accepted"}
        self._send2_update_result = {"status": "accepted", "applied": ["motion.sensitivity"]}
        self._send2_test_result = {"status": "accepted", "message": "queued"}
        self._enroll_probe_result = {"camera_id": "cam1", "reachable": True}
        self._pairing_bundle_result = {"camera_id": "cam1", "api_token": "pairing-token-123"}
        self._pairing_install_result = {"status": "success", "camera_id": "cam1"}
        self._connect_result = {"camera_id": "cam1", "status": "success", "status_detail": "Connected cam1 to the hub."}
        self._bulk_action_result = {
            "action": "refresh-api",
            "total": 2,
            "success_count": 2,
            "error_count": 0,
            "results": [
                {"camera_id": "cam1", "status": "success", "detail": "scheduled"},
                {"camera_id": "cam2", "status": "success", "detail": "scheduled"},
            ],
        }
        self._unregister_result = {
            "camera_id": "cam1",
            "command_published": True,
            "config_removed": True,
            "retained_cleared": True,
            "retained_error": "",
        }

    def snapshot_status(self) -> dict:
        return dict(self._status)

    def list_cameras_for_ui(self) -> list[dict]:
        return [dict(entry) for entry in self._cameras]

    def list_recent_events_for_ui(self, *, limit: int) -> list[dict]:
        return [dict(entry) for entry in self._events[:limit]]

    def get_camera_for_ui(self, camera_id: str) -> dict:
        for camera in self._cameras:
            if camera["camera_id"] == camera_id:
                return dict(camera)
        raise RuntimeError(f"camera not found: {camera_id}")

    def queue_camera_api_refresh(self, camera_id: str) -> str:
        if camera_id == "missing":
            raise RuntimeError("camera not found")
        return self._refresh_api_result

    def queue_camera_onvif_refresh(self, camera_id: str) -> str:
        if camera_id == "missing":
            raise RuntimeError("camera not found")
        return self._refresh_onvif_result

    def queue_camera_detail_hydration_refresh(self, camera_id: str) -> dict:
        return {
            "api": self.queue_camera_api_refresh(camera_id),
            "onvif": self.queue_camera_onvif_refresh(camera_id),
        }

    def queue_snapshot_refresh(self, camera_id: str) -> str:
        if camera_id == "missing":
            raise RuntimeError("camera not found")
        return self._refresh_snapshot_result

    def control_camera_service(self, camera_id: str, service: str, operation: str, *, refresh_after: bool = True) -> dict:
        if camera_id == "missing":
            raise RuntimeError("camera not found")
        return {
            "status": self._service_result.get("status", "accepted"),
            "service": service,
            "operation": operation,
            "refresh_after": refresh_after,
        }

    def rescan_cameras(self, camera_id: str | None = None) -> tuple[int, int]:
        if camera_id == "missing":
            raise RuntimeError("camera not found")
        return self._rescan_total, self._rescan_published

    def set_camera_privacy(self, camera_id: str, enabled: bool, channel: str = "all", *, refresh_after: bool = True) -> dict:
        if camera_id == "missing":
            raise RuntimeError("camera not found")
        return {
            **self._privacy_result,
            "enabled": enabled,
            "channel": channel,
            "refresh_after": refresh_after,
        }

    def set_camera_daynight_mode(self, camera_id: str, mode: str, *, refresh_after: bool = True) -> dict:
        if camera_id == "missing":
            raise RuntimeError("camera not found")
        return {
            **self._daynight_result,
            "mode": mode,
            "refresh_after": refresh_after,
        }

    def record_camera_clip(self, camera_id: str, duration_seconds: int = 10, stream_id: int = 0, path: str = "") -> dict:
        if camera_id == "missing":
            raise RuntimeError("camera not found")
        return {
            **self._record_result,
            "duration_seconds": duration_seconds,
            "stream_id": stream_id,
            "path": path,
        }

    def patch_camera_config(self, camera_id: str, payload: dict, *, refresh_after: bool = True) -> dict:
        if camera_id == "missing":
            raise RuntimeError("camera not found")
        return {
            **self._patch_result,
            "payload": dict(payload),
            "refresh_after": refresh_after,
        }

    def update_camera_send2_config(self, camera_id: str, payload: dict) -> dict:
        if camera_id == "missing":
            raise RuntimeError("camera not found")
        return {**self._send2_update_result, "payload": dict(payload)}

    def test_camera_send2_service(
        self,
        camera_id: str,
        service_name: str,
        *,
        verbose: bool = True,
        send_type: str = "",
    ) -> dict:
        if camera_id == "missing":
            raise RuntimeError("camera not found")
        if service_name == "badservice":
            raise RuntimeError("Unknown send2 service")
        return {
            **self._send2_test_result,
            "service": service_name,
            "verbose": verbose,
            "send_type": send_type,
        }

    def probe_camera_enrollment(self, enrollment: dict[str, str]) -> dict:
        if not enrollment.get("ip"):
            raise RuntimeError("ip is required")
        return dict(self._enroll_probe_result)

    def generate_pairing_bundle(self, enrollment: dict[str, str]) -> dict:
        if not enrollment.get("ip"):
            raise RuntimeError("ip is required")
        return dict(self._pairing_bundle_result)

    def install_pairing_bundle_via_mqtt(self, enrollment: dict[str, str]) -> dict:
        camera_id = enrollment.get("camera_id") or ""
        if camera_id == "missing":
            raise RuntimeError("camera not found")
        if not camera_id and not enrollment.get("ip"):
            raise RuntimeError("camera id or ip is required")
        return dict(self._pairing_install_result)

    def connect_camera(self, enrollment: dict[str, str]) -> dict:
        camera_id = str(enrollment.get("camera_id") or "").strip() or "cam1"
        if camera_id == "missing":
            raise RuntimeError("camera not found")
        if not enrollment.get("ip"):
            raise RuntimeError("ip is required")
        return {**self._connect_result, "camera_id": camera_id}

    def perform_bulk_action(self, camera_ids: list[str], action: str) -> dict:
        if not camera_ids:
            raise RuntimeError("Select at least one known camera")
        if action == "unsupported":
            raise RuntimeError("Unsupported bulk action: unsupported")
        result = dict(self._bulk_action_result)
        result["action"] = action or result.get("action", "")
        return result

    def unregister_camera(self, camera_id: str) -> dict:
        if camera_id == "missing":
            raise RuntimeError("camera not found")
        return dict(self._unregister_result)


class ApiV2RoutesTest(unittest.TestCase):
    def setUp(self) -> None:
        self.app = create_api_v2_app(FakeHub())
        self.client = TestClient(self.app)

    def test_health_route_returns_hub_snapshot(self) -> None:
        response = self.client.get("/api/v2/health")
        self.assertEqual(response.status_code, 200)
        payload = response.json()
        self.assertEqual(payload["ok"], True)
        self.assertEqual(payload["component"], "api-v2")
        self.assertEqual(payload["hub"]["mqtt_connected"], True)

    def test_cameras_route_supports_limit(self) -> None:
        response = self.client.get("/api/v2/cameras?limit=1")
        self.assertEqual(response.status_code, 200)
        payload = response.json()
        self.assertEqual(payload["ok"], True)
        self.assertEqual(payload["count"], 1)
        self.assertEqual(payload["cameras"][0]["camera_id"], "cam1")

    def test_events_route_returns_list(self) -> None:
        response = self.client.get("/api/v2/events?limit=2")
        self.assertEqual(response.status_code, 200)
        payload = response.json()
        self.assertEqual(payload["ok"], True)
        self.assertEqual(len(payload["events"]), 2)

    def test_attention_route_returns_actionable_camera(self) -> None:
        response = self.client.get("/api/v2/cameras/attention")
        self.assertEqual(response.status_code, 200)
        payload = response.json()
        self.assertEqual(payload["ok"], True)
        self.assertEqual(payload["count"], 1)
        self.assertEqual(payload["cameras"][0]["camera_id"], "cam2")
        self.assertGreaterEqual(payload["cameras"][0]["score"], 1)
        issue_codes = {issue["code"] for issue in payload["cameras"][0]["issues"]}
        self.assertIn("camera-offline", issue_codes)
        self.assertIn("native-api-problem", issue_codes)

    def test_attention_route_filters_by_severity(self) -> None:
        response = self.client.get("/api/v2/cameras/attention?minimum_severity=critical")
        self.assertEqual(response.status_code, 200)
        payload = response.json()
        self.assertEqual(payload["count"], 1)
        for issue in payload["cameras"][0]["issues"]:
            self.assertEqual(issue["severity"], "critical")

    def test_attention_route_can_include_ready_cameras(self) -> None:
        response = self.client.get("/api/v2/cameras/attention?include_ready=true&minimum_severity=low")
        self.assertEqual(response.status_code, 200)
        payload = response.json()
        camera_ids = {camera["camera_id"] for camera in payload["cameras"]}
        self.assertEqual(camera_ids, {"cam1", "cam2"})

    def test_camera_payload_route_returns_single_camera(self) -> None:
        response = self.client.get("/api/v2/cameras/cam1/payload")
        self.assertEqual(response.status_code, 200)
        payload = response.json()
        self.assertEqual(payload["ok"], True)
        self.assertEqual(payload["camera"]["camera_id"], "cam1")

    def test_hydrate_route_returns_refresh_states(self) -> None:
        response = self.client.post("/api/v2/cameras/cam2/hydrate")
        self.assertEqual(response.status_code, 200)
        payload = response.json()
        self.assertEqual(payload["ok"], True)
        self.assertEqual(payload["camera"]["camera_id"], "cam2")
        self.assertEqual(payload["refreshes"]["api"], "scheduled")
        self.assertEqual(payload["refreshes"]["onvif"], "scheduled")

    def test_refresh_api_route_returns_action_status(self) -> None:
        response = self.client.post("/api/v2/cameras/cam1/refresh/api")
        self.assertEqual(response.status_code, 200)
        payload = response.json()
        self.assertEqual(payload["ok"], True)
        self.assertEqual(payload["camera_id"], "cam1")
        self.assertEqual(payload["action"], "refresh-api")
        self.assertEqual(payload["result"], "scheduled")

    def test_refresh_snapshot_route_handles_already_running(self) -> None:
        fake_hub = FakeHub()
        fake_hub._refresh_snapshot_result = "already-running"
        client = TestClient(create_api_v2_app(fake_hub))
        response = client.post("/api/v2/cameras/cam1/refresh/snapshot")
        self.assertEqual(response.status_code, 200)
        payload = response.json()
        self.assertEqual(payload["result"], "already-running")
        self.assertIn("already running", payload["message"])

    def test_refresh_onvif_route_returns_500_on_error(self) -> None:
        response = self.client.post("/api/v2/cameras/missing/refresh/onvif")
        self.assertEqual(response.status_code, 500)
        payload = response.json()
        self.assertIn("ONVIF refresh failed", payload["detail"])

    def test_service_action_route_returns_status_and_details(self) -> None:
        response = self.client.post("/api/v2/cameras/cam1/service/streaming/restart")
        self.assertEqual(response.status_code, 200)
        payload = response.json()
        self.assertEqual(payload["ok"], True)
        self.assertEqual(payload["action"], "service:streaming:restart")
        self.assertEqual(payload["result"], "accepted")
        self.assertEqual(payload["details"]["service"], "streaming")
        self.assertEqual(payload["details"]["operation"], "restart")

    def test_streaming_shortcut_route_works(self) -> None:
        response = self.client.post("/api/v2/cameras/cam1/streaming/start")
        self.assertEqual(response.status_code, 200)
        payload = response.json()
        self.assertEqual(payload["action"], "service:streaming:start")

    def test_service_action_route_returns_500_on_error(self) -> None:
        response = self.client.post("/api/v2/cameras/missing/service/streaming/start")
        self.assertEqual(response.status_code, 500)
        payload = response.json()
        self.assertIn("Streaming start failed", payload["detail"])

    def test_rescan_route_returns_ok_when_all_published(self) -> None:
        response = self.client.post("/api/v2/cameras/cam1/rescan")
        self.assertEqual(response.status_code, 200)
        payload = response.json()
        self.assertEqual(payload["ok"], True)
        self.assertEqual(payload["total"], 1)
        self.assertEqual(payload["published"], 1)

    def test_rescan_route_returns_not_ok_when_partial(self) -> None:
        fake_hub = FakeHub()
        fake_hub._rescan_total = 2
        fake_hub._rescan_published = 1
        client = TestClient(create_api_v2_app(fake_hub))
        response = client.post("/api/v2/cameras/cam1/rescan")
        self.assertEqual(response.status_code, 200)
        payload = response.json()
        self.assertEqual(payload["ok"], False)
        self.assertEqual(payload["total"], 2)
        self.assertEqual(payload["published"], 1)

    def test_privacy_route_updates_state(self) -> None:
        response = self.client.post("/api/v2/cameras/cam1/privacy", json={"enabled": True, "channel": "all"})
        self.assertEqual(response.status_code, 200)
        payload = response.json()
        self.assertEqual(payload["action"], "privacy")
        self.assertEqual(payload["result"], "accepted")
        self.assertEqual(payload["details"]["enabled"], True)

    def test_daynight_route_updates_mode(self) -> None:
        response = self.client.post("/api/v2/cameras/cam1/daynight", json={"mode": "night"})
        self.assertEqual(response.status_code, 200)
        payload = response.json()
        self.assertEqual(payload["action"], "daynight")
        self.assertEqual(payload["details"]["mode"], "night")

    def test_daynight_route_rejects_invalid_mode(self) -> None:
        response = self.client.post("/api/v2/cameras/cam1/daynight", json={"mode": "sunset"})
        self.assertEqual(response.status_code, 422)

    def test_record_route_returns_clip_path(self) -> None:
        response = self.client.post(
            "/api/v2/cameras/cam1/record",
            json={"duration_seconds": 8, "stream_id": 1, "path": "/tmp/out.mp4"},
        )
        self.assertEqual(response.status_code, 200)
        payload = response.json()
        self.assertEqual(payload["action"], "record")
        self.assertIn("/tmp/cam1.mp4", payload["message"])

    def test_record_route_returns_500_on_error(self) -> None:
        response = self.client.post("/api/v2/cameras/missing/record", json={})
        self.assertEqual(response.status_code, 500)
        payload = response.json()
        self.assertIn("Clip recording failed", payload["detail"])

    def test_patch_config_route_applies_payload(self) -> None:
        response = self.client.post(
            "/api/v2/cameras/cam1/config/patch",
            json={"patch": {"image": {"hflip": True}}},
        )
        self.assertEqual(response.status_code, 200)
        payload = response.json()
        self.assertEqual(payload["action"], "patch-config")
        self.assertEqual(payload["details"]["payload"]["image"]["hflip"], True)

    def test_patch_config_route_rejects_empty_payload(self) -> None:
        response = self.client.post("/api/v2/cameras/cam1/config/patch", json={"patch": {}})
        self.assertEqual(response.status_code, 400)
        payload = response.json()
        self.assertIn("empty", payload["detail"])

    def test_apply_supported_config_route_accepts_native_and_send2(self) -> None:
        response = self.client.post(
            "/api/v2/cameras/cam1/apply-supported-config",
            json={
                "native_patch": {"image": {"hflip": True}},
                "send2_patch": {"motion": {"sensitivity": 30}},
            },
        )
        self.assertEqual(response.status_code, 200)
        payload = response.json()
        self.assertEqual(payload["action"], "apply-supported-config")
        self.assertIn("Settings applied", payload["message"])
        self.assertIn("native", payload["details"])
        self.assertIn("send2", payload["details"])

    def test_apply_supported_config_route_rejects_empty_request(self) -> None:
        response = self.client.post("/api/v2/cameras/cam1/apply-supported-config", json={})
        self.assertEqual(response.status_code, 400)
        payload = response.json()
        self.assertIn("No supported settings", payload["detail"])

    def test_send2_test_route_accepts_defaults_without_body(self) -> None:
        response = self.client.post("/api/v2/cameras/cam1/send2-test/telegram")
        self.assertEqual(response.status_code, 200)
        payload = response.json()
        self.assertEqual(payload["action"], "send2-test")
        self.assertEqual(payload["details"]["service"], "telegram")
        self.assertEqual(payload["details"]["verbose"], True)
        self.assertEqual(payload["details"]["send_type"], "")

    def test_send2_test_route_handles_explicit_type(self) -> None:
        response = self.client.post(
            "/api/v2/cameras/cam1/send2-test/telegram",
            json={"verbose": False, "send_type": "photo"},
        )
        self.assertEqual(response.status_code, 200)
        payload = response.json()
        self.assertEqual(payload["details"]["verbose"], False)
        self.assertEqual(payload["details"]["send_type"], "photo")

    def test_send2_test_route_returns_500_on_error(self) -> None:
        response = self.client.post("/api/v2/cameras/missing/send2-test/telegram")
        self.assertEqual(response.status_code, 500)
        payload = response.json()
        self.assertIn("Send2 test failed", payload["detail"])

    def test_refresh_routes_parity_running_semantics(self) -> None:
        cases = [
            ("/api/v2/cameras/cam1/refresh/api", "_refresh_api_result"),
            ("/api/v2/cameras/cam1/refresh/onvif", "_refresh_onvif_result"),
            ("/api/v2/cameras/cam1/refresh/snapshot", "_refresh_snapshot_result"),
        ]
        for route, attr in cases:
            fake_hub = FakeHub()
            setattr(fake_hub, attr, "already_running")
            client = TestClient(create_api_v2_app(fake_hub))
            response = client.post(route)
            self.assertEqual(response.status_code, 200)
            payload = response.json()
            self.assertEqual(payload["result"], "already_running")
            self.assertIn("already running", payload["message"])

    def test_enroll_probe_route_returns_result(self) -> None:
        response = self.client.post("/api/v2/enroll/probe", json={"ip": "192.168.1.10"})
        self.assertEqual(response.status_code, 200)
        payload = response.json()
        self.assertEqual(payload["ok"], True)
        self.assertEqual(payload["result"]["camera_id"], "cam1")

    def test_enroll_route_returns_connected_camera(self) -> None:
        response = self.client.post("/api/v2/enroll", json={"camera_id": "cam1", "ip": "192.168.1.10"})
        self.assertEqual(response.status_code, 200)
        payload = response.json()
        self.assertEqual(payload["action"], "enroll")
        self.assertEqual(payload["camera_id"], "cam1")
        self.assertEqual(payload["result"], "success")

    def test_enroll_route_returns_400_on_error(self) -> None:
        response = self.client.post("/api/v2/enroll", json={"camera_id": "cam1"})
        self.assertEqual(response.status_code, 400)
        payload = response.json()
        self.assertIn("ip is required", payload["detail"])

    def test_connect_route_returns_camera_connect_result(self) -> None:
        response = self.client.post(
            "/api/v2/cameras/cam1/connect",
            json={"onvif_username": "thingino", "onvif_password": "thingino"},
        )
        self.assertEqual(response.status_code, 200)
        payload = response.json()
        self.assertEqual(payload["action"], "connect")
        self.assertEqual(payload["camera_id"], "cam1")
        self.assertEqual(payload["result"], "success")

    def test_connect_route_returns_500_on_error(self) -> None:
        response = self.client.post("/api/v2/cameras/missing/connect", json={})
        self.assertEqual(response.status_code, 500)
        payload = response.json()
        self.assertIn("Connect failed for missing", payload["detail"])

    def test_bulk_action_route_returns_success_summary(self) -> None:
        response = self.client.post(
            "/api/v2/bulk-action",
            json={"camera_ids": ["cam1", "cam2"], "action": "refresh-api"},
        )
        self.assertEqual(response.status_code, 200)
        payload = response.json()
        self.assertEqual(payload["ok"], True)
        self.assertEqual(payload["action"], "refresh-api")
        self.assertIn("finished for 2 of 2", payload["message"])

    def test_bulk_action_route_returns_partial_failure_summary(self) -> None:
        fake_hub = FakeHub()
        fake_hub._bulk_action_result = {
            "action": "refresh-api",
            "total": 2,
            "success_count": 1,
            "error_count": 1,
            "results": [
                {"camera_id": "cam1", "status": "success", "detail": "scheduled"},
                {"camera_id": "cam2", "status": "error", "detail": "camera not found"},
            ],
        }
        client = TestClient(create_api_v2_app(fake_hub))
        response = client.post(
            "/api/v2/bulk-action",
            json={"camera_ids": ["cam1", "cam2"], "action": "refresh-api"},
        )
        self.assertEqual(response.status_code, 200)
        payload = response.json()
        self.assertEqual(payload["ok"], False)
        self.assertEqual(payload["result"]["error_count"], 1)

    def test_bulk_action_route_returns_400_on_error(self) -> None:
        response = self.client.post(
            "/api/v2/bulk-action",
            json={"camera_ids": ["cam1"], "action": "unsupported"},
        )
        self.assertEqual(response.status_code, 400)
        payload = response.json()
        self.assertIn("Unsupported bulk action", payload["detail"])

    def test_pairing_bundle_route_returns_token(self) -> None:
        response = self.client.post("/api/v2/enroll/pairing-bundle", json={"ip": "192.168.1.10"})
        self.assertEqual(response.status_code, 200)
        payload = response.json()
        self.assertEqual(payload["ok"], True)
        self.assertEqual(payload["result"]["api_token"], "pairing-token-123")

    def test_pairing_install_route_handles_warning(self) -> None:
        fake_hub = FakeHub()
        fake_hub._pairing_install_result = {"status": "warning", "camera_id": "cam1"}
        client = TestClient(create_api_v2_app(fake_hub))
        response = client.post("/api/v2/enroll/pairing-install", json={"camera_id": "cam1", "ip": "192.168.1.10"})
        self.assertEqual(response.status_code, 200)
        payload = response.json()
        self.assertEqual(payload["ok"], True)
        self.assertIn("timed out", payload["message"])

    def test_pair_camera_route_success(self) -> None:
        response = self.client.post("/api/v2/cameras/cam1/pair")
        self.assertEqual(response.status_code, 200)
        payload = response.json()
        self.assertEqual(payload["action"], "pair")
        self.assertIn("Pairing installed for cam1", payload["message"])

    def test_pair_camera_route_warning(self) -> None:
        fake_hub = FakeHub()
        fake_hub._pairing_install_result = {"status": "warning", "camera_id": "cam1"}
        client = TestClient(create_api_v2_app(fake_hub))
        response = client.post("/api/v2/cameras/cam1/pair")
        self.assertEqual(response.status_code, 200)
        payload = response.json()
        self.assertEqual(payload["result"], "warning")
        self.assertIn("did not confirm before the timeout", payload["message"])

    def test_pair_camera_route_failure(self) -> None:
        response = self.client.post("/api/v2/cameras/missing/pair")
        self.assertEqual(response.status_code, 500)
        payload = response.json()
        self.assertIn("Pairing failed for missing", payload["detail"])

    def test_delete_camera_route_success(self) -> None:
        response = self.client.post("/api/v2/cameras/cam1/delete")
        self.assertEqual(response.status_code, 200)
        payload = response.json()
        self.assertEqual(payload["ok"], True)
        self.assertIn("Removed cam1 from the roster", payload["message"])

    def test_delete_camera_route_partial_failure(self) -> None:
        fake_hub = FakeHub()
        fake_hub._unregister_result = {
            "camera_id": "cam1",
            "command_published": False,
            "config_removed": True,
            "retained_cleared": False,
            "retained_error": "publish rc=4",
        }
        client = TestClient(create_api_v2_app(fake_hub))
        response = client.post("/api/v2/cameras/cam1/delete")
        self.assertEqual(response.status_code, 500)
        payload = response.json()
        self.assertIn("retained unregister did not complete", payload["detail"])
