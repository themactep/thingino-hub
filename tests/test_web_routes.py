import unittest

from app.web import create_web_app


class FakeHub:
    def __init__(self) -> None:
        self.camera = {
            "camera_id": "cam1",
            "name": "Test Camera",
            "status": "online",
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

    def test_refresh_api_is_queued_and_has_no_camera_blob(self) -> None:
        response = self.client.post("/refresh-api/cam1", headers=self.json_headers)

        self.assertEqual(response.status_code, 200)
        payload = response.get_json()
        self.assertTrue(payload["ok"])
        self.assertEqual(payload["message"], "Native API refresh queued.")
        self.assertNotIn("camera", payload)


if __name__ == "__main__":
    unittest.main()