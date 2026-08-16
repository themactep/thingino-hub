import logging
import threading
from typing import TYPE_CHECKING, Any, Literal

from fastapi import FastAPI, HTTPException, Query
from pydantic import BaseModel
import uvicorn

from .action_result_adapter import delete_outcome, pairing_outcome

if TYPE_CHECKING:
    from .main import Hub


LOG = logging.getLogger("telegrambothub.api_v2")
_SEVERITY_RANK = {"low": 0, "medium": 1, "high": 2, "critical": 3}


class AttentionIssue(BaseModel):
    code: str
    severity: Literal["low", "medium", "high", "critical"]
    message: str
    suggested_action: str


class CameraAttentionItem(BaseModel):
    camera_id: str
    name: str
    status: str
    setup_status: str
    score: int
    issues: list[AttentionIssue]


class CameraAttentionResponse(BaseModel):
    ok: bool
    count: int
    cameras: list[CameraAttentionItem]


class CameraActionResponse(BaseModel):
    ok: bool
    camera_id: str
    action: str
    result: str
    message: str


class CameraServiceActionResponse(CameraActionResponse):
    details: dict[str, Any] | None = None


class CameraRescanResponse(BaseModel):
    ok: bool
    camera_id: str
    total: int
    published: int
    message: str


class PrivacyRequest(BaseModel):
    enabled: bool
    channel: str = "all"


class DaynightRequest(BaseModel):
    mode: Literal["auto", "day", "night"] = "auto"


class RecordClipRequest(BaseModel):
    duration_seconds: int = 10
    stream_id: int = 0
    path: str = ""


class ConfigPatchRequest(BaseModel):
    patch: dict[str, Any]


class ApplySupportedConfigRequest(BaseModel):
    native_patch: dict[str, Any] | None = None
    send2_patch: dict[str, Any] | None = None


class Send2TestRequest(BaseModel):
    verbose: bool = True
    send_type: Literal["", "photo", "video"] = ""


class EnrollmentRequest(BaseModel):
    camera_id: str = ""
    ip: str = ""
    api_token: str = ""
    onvif_username: str = ""
    onvif_password: str = ""


class BulkActionRequest(BaseModel):
    camera_ids: list[str]
    action: str


class PairCameraRequest(BaseModel):
    backup_before: bool = True


class ConfigBackupCreateRequest(BaseModel):
    label: str = "Manual backup"
    source: str = "manual"


class ConfigRestoreRequest(BaseModel):
    mode: Literal["compatible", "best_effort"] = "compatible"


class ConfigClonePreviewRequest(BaseModel):
    source_camera_id: str
    source_kind: Literal["live", "backup"] = "live"
    snapshot_id: int | None = None


class ConfigCloneApplyRequest(BaseModel):
    source_camera_id: str
    source_kind: Literal["live", "backup"] = "live"
    snapshot_id: int | None = None
    selected_paths: list[str] | None = None
    mode: Literal["compatible", "best_effort"] = "compatible"


class ConfigClonePushPreviewRequest(BaseModel):
    target_camera_ids: list[str]
    source_kind: Literal["live", "backup"] = "live"
    snapshot_id: int | None = None


class ConfigClonePushApplyRequest(BaseModel):
    target_camera_ids: list[str]
    source_kind: Literal["live", "backup"] = "live"
    snapshot_id: int | None = None
    selected_paths: list[str] | None = None
    mode: Literal["compatible", "best_effort"] = "compatible"


class CameraMigrateRequest(BaseModel):
    from_camera_id: str
    to_camera_id: str
    restore_latest_backup: bool = False


def _camera_teaser_payload(camera: dict[str, Any]) -> dict[str, Any]:
    return {
        "camera_id": str(camera.get("camera_id") or ""),
        "name": str(camera.get("name") or ""),
        "status": str(camera.get("status") or "unknown"),
        "ip": str(camera.get("ip") or ""),
        "api_status": str(camera.get("api_status") or "unknown"),
        "onvif_model": str(camera.get("onvif_model") or ""),
        "setup_status": str(camera.get("setup_status") or ""),
    }


def _camera_attention_issues(camera: dict[str, Any]) -> list[AttentionIssue]:
    issues: list[AttentionIssue] = []
    status = str(camera.get("status") or "unknown").strip().lower()
    setup_status = str(camera.get("setup_status") or "").strip().lower()
    api_status = str(camera.get("api_status") or "unknown").strip().lower()
    api_last_error = str(camera.get("api_last_error") or "").strip()
    mqtt_status = str(camera.get("mqtt_command_status") or "unknown").strip().lower()
    mqtt_capable = bool(camera.get("mqtt_command_capable"))
    onvif_last_error = str(camera.get("onvif_last_error") or "").strip()
    present_on_mqtt_broker = camera.get("present_on_mqtt_broker")

    if status not in {"online", "ready"}:
        issues.append(
            AttentionIssue(
                code="camera-offline",
                severity="critical",
                message=f"Camera status is {status}.",
                suggested_action="Check camera power, network reachability, and broker connectivity.",
            )
        )

    if setup_status in {"connect", "pair", "enroll"}:
        issues.append(
            AttentionIssue(
                code="setup-incomplete",
                severity="high",
                message=f"Camera setup status is {setup_status}.",
                suggested_action="Complete enrollment/pairing from the dashboard or retry from /enroll.",
            )
        )

    if api_status in {"offline", "error", "unauthorized"} or api_last_error:
        issues.append(
            AttentionIssue(
                code="native-api-problem",
                severity="high",
                message=f"Native API status is {api_status}.",
                suggested_action="Refresh API details and verify api_base_url/api_token for the camera.",
            )
        )

    if mqtt_capable and mqtt_status in {"offline", "error", "unknown"}:
        issues.append(
            AttentionIssue(
                code="mqtt-command-problem",
                severity="medium",
                message=f"MQTT command status is {mqtt_status}.",
                suggested_action="Verify MQTT command topics and check broker ACL/credentials.",
            )
        )

    if present_on_mqtt_broker is False:
        issues.append(
            AttentionIssue(
                code="not-present-on-broker",
                severity="high",
                message="Camera is not currently present on the MQTT broker roster.",
                suggested_action="Confirm the camera is publishing hello/state topics to the expected broker.",
            )
        )

    if onvif_last_error:
        issues.append(
            AttentionIssue(
                code="onvif-problem",
                severity="medium",
                message="Recent ONVIF probe returned an error.",
                suggested_action="Verify ONVIF endpoint and credentials, then refresh ONVIF details.",
            )
        )

    return issues


def create_api_v2_app(hub: "Hub") -> FastAPI:
    app = FastAPI(
        title="Thingino Hub API v2 (Teaser)",
        version="0.1.0",
        docs_url="/api/v2/docs",
        redoc_url="/api/v2/redoc",
        openapi_url="/api/v2/openapi.json",
    )

    @app.get("/api/v2/health")
    def api_v2_health() -> dict[str, Any]:
        return {
            "ok": True,
            "component": "api-v2",
            "hub": hub.snapshot_status(),
        }

    @app.get("/api/v2/cameras")
    def api_v2_cameras(limit: int = Query(default=0, ge=0, le=500)) -> dict[str, Any]:
        cameras = hub.list_cameras_for_ui()
        if limit > 0:
            cameras = cameras[:limit]
        teaser = [_camera_teaser_payload(camera) for camera in cameras]
        return {
            "ok": True,
            "count": len(teaser),
            "cameras": teaser,
        }

    @app.get("/api/v2/events")
    def api_v2_events(limit: int = Query(default=40, ge=1, le=500)) -> dict[str, Any]:
        return {
            "ok": True,
            "events": hub.list_recent_events_for_ui(limit=limit),
        }

    @app.get("/api/v2/cameras/{camera_id}/payload")
    def api_v2_camera_payload(camera_id: str) -> dict[str, Any]:
        return {
            "ok": True,
            "camera": hub.get_camera_for_ui(camera_id),
        }

    @app.post("/api/v2/cameras/{camera_id}/hydrate")
    def api_v2_hydrate_camera(camera_id: str) -> dict[str, Any]:
        refreshes = {"api": "skipped", "onvif": "skipped"}
        try:
            refreshes = hub.queue_camera_detail_hydration_refresh(camera_id)
        except Exception as error:
            refreshes = {"api": f"error: {error}", "onvif": f"error: {error}"}

        return {
            "ok": True,
            "camera": hub.get_camera_for_ui(camera_id),
            "refreshes": refreshes,
        }

    def _camera_action_response(camera_id: str, action: str, result: str, queued_message: str, running_message: str) -> CameraActionResponse:
        normalized = str(result).strip().lower().replace("-", "_")
        if normalized == "scheduled":
            message = queued_message
        else:
            message = running_message
        return CameraActionResponse(
            ok=True,
            camera_id=camera_id,
            action=action,
            result=result,
            message=message,
        )

    def _camera_mutation_response(
        camera_id: str,
        action: str,
        result: str,
        message: str,
        details: dict[str, Any] | None = None,
    ) -> CameraServiceActionResponse:
        return CameraServiceActionResponse(
            ok=True,
            camera_id=camera_id,
            action=action,
            result=result,
            message=message,
            details=details,
        )

    @app.post("/api/v2/cameras/{camera_id}/service/{service_name}/{operation}", response_model=CameraServiceActionResponse)
    def api_v2_service_action(camera_id: str, service_name: str, operation: str) -> CameraServiceActionResponse:
        try:
            details = hub.control_camera_service(camera_id, service_name, operation, refresh_after=False)
            return CameraServiceActionResponse(
                ok=True,
                camera_id=camera_id,
                action=f"service:{service_name}:{operation}",
                result=str(details.get("status") or "accepted"),
                message=f"{service_name.replace('_', ' ').title()} {operation} requested.",
                details=details,
            )
        except Exception as error:
            pretty_service = service_name.replace("_", " ").title()
            raise HTTPException(
                status_code=500,
                detail=f"{pretty_service} {operation} failed: {error}",
            ) from error

    @app.post("/api/v2/cameras/{camera_id}/streaming/restart", response_model=CameraServiceActionResponse)
    def api_v2_restart_streaming(camera_id: str) -> CameraServiceActionResponse:
        return api_v2_service_action(camera_id, "streaming", "restart")

    @app.post("/api/v2/cameras/{camera_id}/streaming/start", response_model=CameraServiceActionResponse)
    def api_v2_start_streaming(camera_id: str) -> CameraServiceActionResponse:
        return api_v2_service_action(camera_id, "streaming", "start")

    @app.post("/api/v2/cameras/{camera_id}/streaming/stop", response_model=CameraServiceActionResponse)
    def api_v2_stop_streaming(camera_id: str) -> CameraServiceActionResponse:
        return api_v2_service_action(camera_id, "streaming", "stop")

    @app.post("/api/v2/cameras/{camera_id}/rescan", response_model=CameraRescanResponse)
    def api_v2_rescan_camera(camera_id: str) -> CameraRescanResponse:
        try:
            total, published = hub.rescan_cameras(camera_id)
        except Exception as error:
            raise HTTPException(status_code=500, detail=f"Metadata refresh failed: {error}") from error

        if published == total:
            message = "Metadata refresh requested."
        else:
            message = "Metadata refresh request failed."
        return CameraRescanResponse(
            ok=published == total,
            camera_id=camera_id,
            total=total,
            published=published,
            message=message,
        )

    @app.post("/api/v2/cameras/{camera_id}/apply-supported-config", response_model=CameraServiceActionResponse)
    def api_v2_apply_supported_config(
        camera_id: str,
        request: ApplySupportedConfigRequest,
    ) -> CameraServiceActionResponse:
        native_payload = request.native_patch or {}
        send2_payload = request.send2_patch or {}
        if not native_payload and not send2_payload:
            raise HTTPException(status_code=400, detail="No supported settings were provided")

        results: list[str] = []
        details: dict[str, Any] = {}
        try:
            if native_payload:
                native_result = hub.patch_camera_config(camera_id, native_payload, refresh_after=False)
                results.append(f"native config: {native_result.get('status', 'ok')}")
                details["native"] = native_result
            if send2_payload:
                send2_result = hub.update_camera_send2_config(camera_id, send2_payload)
                results.append(f"send2: {send2_result.get('result', send2_result.get('status', 'ok'))}")
                details["send2"] = send2_result
        except Exception as error:
            raise HTTPException(status_code=500, detail=f"Settings update failed: {error}") from error

        return _camera_mutation_response(
            camera_id=camera_id,
            action="apply-supported-config",
            result="accepted",
            message=f"Settings applied: {'; '.join(results)}",
            details=details,
        )

    @app.post("/api/v2/cameras/{camera_id}/send2-test/{service_name}", response_model=CameraServiceActionResponse)
    def api_v2_send2_test(
        camera_id: str,
        service_name: str,
        request: Send2TestRequest | None = None,
    ) -> CameraServiceActionResponse:
        payload = request or Send2TestRequest()
        send_type = str(payload.send_type or "").strip().lower()
        try:
            result = hub.test_camera_send2_service(
                camera_id,
                service_name,
                verbose=bool(payload.verbose),
                send_type=send_type,
            )
        except Exception as error:
            label = f"{service_name} {send_type}".strip()
            raise HTTPException(status_code=500, detail=f"Send2 test failed for {label}: {error}") from error

        label = f"{service_name} {send_type}".strip()
        return _camera_mutation_response(
            camera_id=camera_id,
            action="send2-test",
            result=str(result.get("status") or "accepted"),
            message=f"Send2 test finished for {label}.",
            details=result,
        )

    def _enrollment_payload(request: EnrollmentRequest) -> dict[str, str]:
        return {
            "camera_id": str(request.camera_id or "").strip(),
            "ip": str(request.ip or "").strip(),
            "api_token": str(request.api_token or "").strip(),
            "onvif_username": str(request.onvif_username or "").strip(),
            "onvif_password": str(request.onvif_password or ""),
        }

    @app.post("/api/v2/enroll", response_model=CameraServiceActionResponse)
    def api_v2_enroll(request: EnrollmentRequest) -> CameraServiceActionResponse:
        enrollment = _enrollment_payload(request)
        try:
            result = hub.connect_camera(enrollment)
        except Exception as error:
            raise HTTPException(status_code=400, detail=str(error)) from error

        camera_id = str(result.get("camera_id") or enrollment.get("camera_id") or "")
        detail = str(result.get("status_detail") or "").strip()
        message = detail or f"Connected {camera_id} to the hub."
        return _camera_mutation_response(
            camera_id=camera_id,
            action="enroll",
            result=str(result.get("status") or "success"),
            message=message,
            details=result,
        )

    @app.post("/api/v2/cameras/{camera_id}/connect", response_model=CameraServiceActionResponse)
    def api_v2_connect_camera(camera_id: str, request: EnrollmentRequest) -> CameraServiceActionResponse:
        try:
            camera = hub.get_camera_for_ui(camera_id)
            enrollment = _enrollment_payload(request)
            enrollment["camera_id"] = camera_id
            enrollment["ip"] = str(camera.get("ip") or "").strip()
            result = hub.connect_camera(enrollment)
        except Exception as error:
            raise HTTPException(status_code=500, detail=f"Connect failed for {camera_id}: {error}") from error

        detail = str(result.get("status_detail") or "").strip()
        message = detail or f"Connected {camera_id} to the hub."
        return _camera_mutation_response(
            camera_id=camera_id,
            action="connect",
            result=str(result.get("status") or "success"),
            message=message,
            details=result,
        )

    @app.post("/api/v2/bulk-action")
    def api_v2_bulk_action(request: BulkActionRequest) -> dict[str, Any]:
        selected_ids = [str(camera_id or "").strip() for camera_id in request.camera_ids]
        action = str(request.action or "").strip()
        try:
            result = hub.perform_bulk_action(selected_ids, action)
        except Exception as error:
            raise HTTPException(status_code=400, detail=str(error)) from error

        message = (
            f"{str(result.get('action') or action).replace('-', ' ').title()} "
            f"finished for {result['success_count']} of {result['total']} camera(s)."
        )
        return {
            "ok": int(result.get("error_count", 0)) == 0,
            "action": str(result.get("action") or action),
            "message": message,
            "result": result,
        }

    @app.post("/api/v2/enroll/probe")
    def api_v2_enroll_probe(request: EnrollmentRequest) -> dict[str, Any]:
        enrollment = _enrollment_payload(request)
        try:
            result = hub.probe_camera_enrollment(enrollment)
        except Exception as error:
            raise HTTPException(status_code=400, detail=str(error)) from error
        return {
            "ok": True,
            "message": "Enrollment probe finished.",
            "result": result,
        }

    @app.post("/api/v2/enroll/pairing-bundle")
    def api_v2_pairing_bundle(request: EnrollmentRequest) -> dict[str, Any]:
        enrollment = _enrollment_payload(request)
        try:
            result = hub.generate_pairing_bundle(enrollment)
        except Exception as error:
            raise HTTPException(status_code=400, detail=str(error)) from error
        return {
            "ok": True,
            "message": "Pairing bundle prepared.",
            "result": result,
        }

    @app.post("/api/v2/enroll/pairing-install")
    def api_v2_pairing_install(request: EnrollmentRequest) -> dict[str, Any]:
        enrollment = _enrollment_payload(request)
        try:
            result = hub.install_pairing_bundle_via_mqtt(enrollment)
        except Exception as error:
            raise HTTPException(status_code=400, detail=str(error)) from error
        message = "Pairing installed over MQTT."
        if result.get("status") == "warning":
            message = "Pairing install was published over MQTT, but camera confirmation timed out."
        return {
            "ok": True,
            "message": message,
            "result": result,
        }

    @app.post("/api/v2/cameras/{camera_id}/pair", response_model=CameraServiceActionResponse)
    def api_v2_pair_camera(camera_id: str, request: PairCameraRequest = PairCameraRequest()) -> CameraServiceActionResponse:
        try:
            camera = hub.get_camera_for_ui(camera_id)
            enrollment = {
                "camera_id": camera_id,
                "ip": str(camera.get("ip") or "").strip(),
            }
            backup_before = bool(request.backup_before)
            result = hub.install_pairing_bundle_via_mqtt(enrollment, backup_before=backup_before)
            outcome = pairing_outcome(camera_id, result)
            message = str(outcome["message"])
            if result.get("config_restore_available"):
                message = f"{message} Config backup available — review restore from Config Backups."
            return _camera_mutation_response(
                camera_id=camera_id,
                action="pair",
                result=str(outcome["result"]),
                message=message,
                details=result,
            )
        except Exception as error:
            raise HTTPException(status_code=500, detail=f"Pairing failed for {camera_id}: {error}") from error

    @app.get("/api/v2/cameras/{camera_id}/config-backups")
    def api_v2_list_config_backups(camera_id: str, limit: int = Query(50, ge=1, le=200)) -> dict[str, Any]:
        try:
            backups = hub.list_camera_config_backups(camera_id, limit=limit)
        except Exception as error:
            raise HTTPException(status_code=400, detail=str(error)) from error
        return {"ok": True, "camera_id": camera_id, "backups": backups, "count": len(backups)}

    @app.post("/api/v2/cameras/{camera_id}/config-backups")
    def api_v2_create_config_backup(camera_id: str, request: ConfigBackupCreateRequest) -> dict[str, Any]:
        try:
            result = hub.backup_camera_config(
                camera_id,
                source=str(request.source or "manual"),
                label=str(request.label or "Manual backup"),
            )
        except Exception as error:
            raise HTTPException(status_code=400, detail=str(error)) from error
        return {"ok": True, "message": str(result.get("status_detail") or "Config backup stored."), "result": result}

    @app.get("/api/v2/cameras/{camera_id}/config-backups/{snapshot_id}")
    def api_v2_get_config_backup(camera_id: str, snapshot_id: int) -> dict[str, Any]:
        try:
            backup = hub.get_camera_config_backup(camera_id, snapshot_id)
        except Exception as error:
            raise HTTPException(status_code=404, detail=str(error)) from error
        return {"ok": True, "backup": backup}

    @app.post("/api/v2/cameras/{camera_id}/config-backups/{snapshot_id}/restore-preview")
    def api_v2_restore_preview(camera_id: str, snapshot_id: int) -> dict[str, Any]:
        try:
            preview = hub.preview_camera_config_restore(camera_id, snapshot_id)
        except Exception as error:
            raise HTTPException(status_code=400, detail=str(error)) from error
        return {"ok": True, "preview": preview}

    @app.post("/api/v2/cameras/{camera_id}/config-backups/{snapshot_id}/restore")
    def api_v2_restore_config_backup(camera_id: str, snapshot_id: int, request: ConfigRestoreRequest) -> dict[str, Any]:
        try:
            result = hub.restore_camera_config_backup(camera_id, snapshot_id, mode=request.mode)
        except Exception as error:
            raise HTTPException(status_code=400, detail=str(error)) from error
        return {"ok": True, "message": str(result.get("status_detail") or "Config restore finished."), "result": result}

    @app.delete("/api/v2/cameras/{camera_id}/config-backups/{snapshot_id}")
    def api_v2_delete_config_backup(camera_id: str, snapshot_id: int) -> dict[str, Any]:
        try:
            result = hub.delete_camera_config_backup(camera_id, snapshot_id)
        except Exception as error:
            raise HTTPException(status_code=404, detail=str(error)) from error
        return {"ok": True, "message": f"Deleted config backup #{snapshot_id}.", "result": result}

    @app.post("/api/v2/cameras/{camera_id}/config-clone/pull/preview")
    def api_v2_config_clone_pull_preview(camera_id: str, request: ConfigClonePreviewRequest) -> dict[str, Any]:
        try:
            preview = hub.preview_camera_config_clone(
                camera_id,
                source_camera_id=request.source_camera_id,
                source_kind=request.source_kind,
                snapshot_id=request.snapshot_id,
            )
        except Exception as error:
            raise HTTPException(status_code=400, detail=str(error)) from error
        return {"ok": True, "preview": preview}

    @app.post("/api/v2/cameras/{camera_id}/config-clone/pull")
    def api_v2_config_clone_pull(camera_id: str, request: ConfigCloneApplyRequest) -> dict[str, Any]:
        try:
            result = hub.apply_camera_config_clone(
                camera_id,
                source_camera_id=request.source_camera_id,
                source_kind=request.source_kind,
                snapshot_id=request.snapshot_id,
                selected_paths=request.selected_paths,
                mode=request.mode,
            )
        except Exception as error:
            raise HTTPException(status_code=400, detail=str(error)) from error
        return {"ok": True, "message": str(result.get("status_detail") or "Config clone finished."), "result": result}

    @app.post("/api/v2/cameras/{camera_id}/config-clone/push/preview")
    def api_v2_config_clone_push_preview(camera_id: str, request: ConfigClonePushPreviewRequest) -> dict[str, Any]:
        try:
            preview = hub.preview_camera_config_clone_push(
                camera_id,
                target_camera_ids=request.target_camera_ids,
                source_kind=request.source_kind,
                snapshot_id=request.snapshot_id,
            )
        except Exception as error:
            raise HTTPException(status_code=400, detail=str(error)) from error
        return {"ok": True, "preview": preview}

    @app.post("/api/v2/cameras/{camera_id}/config-clone/push")
    def api_v2_config_clone_push(camera_id: str, request: ConfigClonePushApplyRequest) -> dict[str, Any]:
        try:
            result = hub.apply_camera_config_clone_push(
                camera_id,
                target_camera_ids=request.target_camera_ids,
                source_kind=request.source_kind,
                snapshot_id=request.snapshot_id,
                selected_paths=request.selected_paths,
                mode=request.mode,
            )
        except Exception as error:
            raise HTTPException(status_code=400, detail=str(error)) from error
        ok = result.get("status") != "error"
        return {
            "ok": ok,
            "message": str(result.get("status_detail") or "Config clone push finished."),
            "result": result,
        }

    @app.get("/api/v2/cameras/{camera_id}/migration-offer")
    def api_v2_camera_migration_offer(camera_id: str) -> dict[str, Any]:
        try:
            offer = hub.get_camera_migration_offer(camera_id)
        except Exception as error:
            raise HTTPException(status_code=400, detail=str(error)) from error
        return {"ok": True, "camera_id": camera_id, "offer": offer}

    @app.post("/api/v2/cameras/{camera_id}/migrate")
    def api_v2_camera_migrate(camera_id: str, request: CameraMigrateRequest) -> dict[str, Any]:
        try:
            result = hub.migrate_camera_identity(
                from_camera_id=request.from_camera_id,
                to_camera_id=request.to_camera_id,
                restore_latest_backup=bool(request.restore_latest_backup),
            )
        except Exception as error:
            raise HTTPException(status_code=400, detail=str(error)) from error
        return {
            "ok": True,
            "message": str(result.get("status_detail") or "Camera identity migrated."),
            "result": result,
        }

    @app.post("/api/v2/cameras/{camera_id}/delete")
    def api_v2_delete_camera(camera_id: str) -> dict[str, Any]:
        try:
            result = hub.unregister_camera(camera_id)
            outcome = delete_outcome(camera_id, result)
        except Exception as error:
            raise HTTPException(status_code=500, detail=f"Delete failed for {camera_id}: {error}") from error

        if int(outcome["http_status"]) >= 400:
            raise HTTPException(status_code=int(outcome["http_status"]), detail=str(outcome["message"]))
        return {
            "ok": bool(outcome["ok"]),
            "camera_id": camera_id,
            "action": "delete",
            "result": str(outcome["result"]),
            "message": str(outcome["message"]),
            "details": result,
        }

    @app.post("/api/v2/cameras/{camera_id}/privacy", response_model=CameraServiceActionResponse)
    def api_v2_set_privacy(camera_id: str, request: PrivacyRequest) -> CameraServiceActionResponse:
        try:
            details = hub.set_camera_privacy(
                camera_id,
                enabled=request.enabled,
                channel=request.channel,
                refresh_after=False,
            )
            state = "enabled" if request.enabled else "disabled"
            return _camera_mutation_response(
                camera_id=camera_id,
                action="privacy",
                result=str(details.get("status") or "accepted"),
                message=f"Privacy {state}.",
                details=details,
            )
        except Exception as error:
            raise HTTPException(status_code=500, detail=f"Privacy update failed: {error}") from error

    @app.post("/api/v2/cameras/{camera_id}/daynight", response_model=CameraServiceActionResponse)
    def api_v2_set_daynight(camera_id: str, request: DaynightRequest) -> CameraServiceActionResponse:
        try:
            details = hub.set_camera_daynight_mode(camera_id, mode=request.mode, refresh_after=False)
            return _camera_mutation_response(
                camera_id=camera_id,
                action="daynight",
                result=str(details.get("status") or "accepted"),
                message=f"Day/night set to {request.mode}.",
                details=details,
            )
        except Exception as error:
            raise HTTPException(status_code=500, detail=f"Day/night update failed: {error}") from error

    @app.post("/api/v2/cameras/{camera_id}/record", response_model=CameraServiceActionResponse)
    def api_v2_record_clip(camera_id: str, request: RecordClipRequest) -> CameraServiceActionResponse:
        try:
            details = hub.record_camera_clip(
                camera_id,
                duration_seconds=request.duration_seconds,
                stream_id=request.stream_id,
                path=request.path.strip(),
            )
        except Exception as error:
            raise HTTPException(status_code=500, detail=f"Clip recording failed: {error}") from error

        clip_path = str(((details.get("result") or {}).get("path") or "")).strip() if isinstance(details, dict) else ""
        suffix = f" -> {clip_path}" if clip_path else ""
        return _camera_mutation_response(
            camera_id=camera_id,
            action="record",
            result=str(details.get("status") or "accepted"),
            message=f"Clip recording requested{suffix}",
            details=details,
        )

    @app.post("/api/v2/cameras/{camera_id}/config/patch", response_model=CameraServiceActionResponse)
    def api_v2_patch_config(camera_id: str, request: ConfigPatchRequest) -> CameraServiceActionResponse:
        if not isinstance(request.patch, dict) or not request.patch:
            raise HTTPException(status_code=400, detail="Native config patch payload is empty.")
        try:
            details = hub.patch_camera_config(camera_id, request.patch, refresh_after=False)
            return _camera_mutation_response(
                camera_id=camera_id,
                action="patch-config",
                result=str(details.get("status") or "accepted"),
                message="Config patch applied.",
                details=details,
            )
        except Exception as error:
            raise HTTPException(status_code=500, detail=f"Config patch failed: {error}") from error

    @app.post("/api/v2/cameras/{camera_id}/refresh/api", response_model=CameraActionResponse)
    def api_v2_refresh_api(camera_id: str) -> CameraActionResponse:
        try:
            result = hub.queue_camera_api_refresh(camera_id)
            return _camera_action_response(
                camera_id,
                "refresh-api",
                str(result),
                "Native API refresh queued.",
                "Native API refresh is already running.",
            )
        except Exception as error:
            raise HTTPException(status_code=500, detail=f"Native API refresh failed: {error}") from error

    @app.post("/api/v2/cameras/{camera_id}/refresh/onvif", response_model=CameraActionResponse)
    def api_v2_refresh_onvif(camera_id: str) -> CameraActionResponse:
        try:
            result = hub.queue_camera_onvif_refresh(camera_id)
            return _camera_action_response(
                camera_id,
                "refresh-onvif",
                str(result),
                "ONVIF refresh queued.",
                "ONVIF refresh is already running.",
            )
        except Exception as error:
            raise HTTPException(status_code=500, detail=f"ONVIF refresh failed: {error}") from error

    @app.post("/api/v2/cameras/{camera_id}/refresh/snapshot", response_model=CameraActionResponse)
    def api_v2_refresh_snapshot(camera_id: str) -> CameraActionResponse:
        try:
            result = hub.queue_snapshot_refresh(camera_id)
            return _camera_action_response(
                camera_id,
                "refresh-snapshot",
                str(result),
                "Snapshot refresh queued.",
                "Snapshot refresh is already running.",
            )
        except Exception as error:
            raise HTTPException(status_code=500, detail=f"Snapshot refresh failed: {error}") from error

    @app.get("/api/v2/cameras/attention", response_model=CameraAttentionResponse)
    def api_v2_camera_attention(
        limit: int = Query(default=25, ge=1, le=500),
        minimum_severity: Literal["low", "medium", "high", "critical"] = Query(default="medium"),
        include_ready: bool = Query(default=False),
    ) -> CameraAttentionResponse:
        threshold = _SEVERITY_RANK[minimum_severity]
        items: list[CameraAttentionItem] = []
        for camera in hub.list_cameras_for_ui():
            issues = [
                issue
                for issue in _camera_attention_issues(camera)
                if _SEVERITY_RANK[issue.severity] >= threshold
            ]
            if not issues and not include_ready:
                continue
            score = sum(_SEVERITY_RANK[issue.severity] + 1 for issue in issues)
            items.append(
                CameraAttentionItem(
                    camera_id=str(camera.get("camera_id") or ""),
                    name=str(camera.get("name") or ""),
                    status=str(camera.get("status") or "unknown"),
                    setup_status=str(camera.get("setup_status") or ""),
                    score=score,
                    issues=issues,
                )
            )

        items.sort(key=lambda item: (item.score, item.name.lower()), reverse=True)
        clipped = items[:limit]
        return CameraAttentionResponse(ok=True, count=len(clipped), cameras=clipped)

    return app


class ApiV2Server:
    def __init__(self, app: FastAPI, host: str, port: int) -> None:
        self.app = app
        self.host = host
        self.port = port
        self._server = uvicorn.Server(
            uvicorn.Config(
                app,
                host=host,
                port=port,
                log_level="info",
                access_log=False,
            )
        )
        def _skip_signal_handlers() -> None:
            return None

        self._server.install_signal_handlers = _skip_signal_handlers
        self._thread = threading.Thread(target=self._server.run, name="telegrambothub-api-v2", daemon=True)

    def start(self) -> None:
        LOG.info("Starting FastAPI teaser on http://%s:%s/api/v2/docs", self.host, self.port)
        self._thread.start()

    def stop(self) -> None:
        self._server.should_exit = True
        self._thread.join(timeout=5)
