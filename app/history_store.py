import json
import logging
import sqlite3
import threading
import time
from pathlib import Path
from typing import Any


LOG = logging.getLogger("telegrambothub.history")


class HistoryStore:
    def __init__(
        self,
        db_path: str,
        *,
        max_action_events_per_camera: int = 1000,
        max_state_samples_per_camera: int = 5000,
        max_config_snapshots_per_camera: int = 20,
        config_snapshot_max_age_days: int = 90,
    ) -> None:
        self.db_path = Path(db_path)
        self.max_action_events_per_camera = max(1, int(max_action_events_per_camera))
        self.max_state_samples_per_camera = max(1, int(max_state_samples_per_camera))
        self.max_config_snapshots_per_camera = max(1, int(max_config_snapshots_per_camera))
        self.config_snapshot_max_age_days = max(0, int(config_snapshot_max_age_days))
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._lock = threading.RLock()
        self._conn = sqlite3.connect(self.db_path, timeout=5, check_same_thread=False)
        self._conn.row_factory = sqlite3.Row
        self._configure()
        self._migrate()

    def _configure(self) -> None:
        with self._lock:
            self._conn.execute("PRAGMA journal_mode=WAL")
            self._conn.execute("PRAGMA synchronous=NORMAL")
            self._conn.execute("PRAGMA foreign_keys=ON")

    def _migrate(self) -> None:
        with self._lock:
            self._conn.executescript(
                """
                CREATE TABLE IF NOT EXISTS action_events (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    recorded_at INTEGER NOT NULL,
                    camera_id TEXT NOT NULL,
                    source TEXT NOT NULL,
                    action TEXT NOT NULL,
                    status TEXT NOT NULL,
                    detail TEXT NOT NULL,
                    payload_summary TEXT NOT NULL DEFAULT ''
                );

                CREATE INDEX IF NOT EXISTS idx_action_events_camera_time
                ON action_events(camera_id, recorded_at DESC, id DESC);

                CREATE TABLE IF NOT EXISTS state_samples (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    recorded_at INTEGER NOT NULL,
                    camera_id TEXT NOT NULL,
                    sample_type TEXT NOT NULL,
                    sample_json TEXT NOT NULL,
                    api_status TEXT,
                    network_online INTEGER,
                    streamer_running INTEGER,
                    motion_enabled INTEGER,
                    privacy_enabled INTEGER,
                    daynight_target_mode TEXT,
                    daynight_running_mode TEXT,
                    ip TEXT,
                    has_cached_snapshot INTEGER
                );

                CREATE INDEX IF NOT EXISTS idx_state_samples_camera_time
                ON state_samples(camera_id, recorded_at DESC, id DESC);

                CREATE TABLE IF NOT EXISTS config_changes (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    recorded_at INTEGER NOT NULL,
                    camera_id TEXT NOT NULL,
                    source TEXT NOT NULL,
                    change_type TEXT NOT NULL,
                    path TEXT NOT NULL,
                    previous_json TEXT NOT NULL,
                    new_json TEXT NOT NULL,
                    detail TEXT NOT NULL DEFAULT ''
                );

                CREATE INDEX IF NOT EXISTS idx_config_changes_camera_time
                ON config_changes(camera_id, recorded_at DESC, id DESC);

                CREATE TABLE IF NOT EXISTS config_snapshots (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    recorded_at INTEGER NOT NULL,
                    camera_id TEXT NOT NULL,
                    source TEXT NOT NULL,
                    label TEXT NOT NULL DEFAULT '',
                    firmware_id TEXT NOT NULL DEFAULT '',
                    streamer TEXT NOT NULL DEFAULT '',
                    capabilities_json TEXT NOT NULL,
                    config_json TEXT NOT NULL,
                    content_hash TEXT NOT NULL
                );

                CREATE INDEX IF NOT EXISTS idx_config_snapshots_camera_time
                ON config_snapshots(camera_id, recorded_at DESC, id DESC);
                """
            )
            self._ensure_column("state_samples", "api_status", "TEXT")
            self._ensure_column("state_samples", "network_online", "INTEGER")
            self._ensure_column("state_samples", "streamer_running", "INTEGER")
            self._ensure_column("state_samples", "motion_enabled", "INTEGER")
            self._ensure_column("state_samples", "privacy_enabled", "INTEGER")
            self._ensure_column("state_samples", "daynight_target_mode", "TEXT")
            self._ensure_column("state_samples", "daynight_running_mode", "TEXT")
            self._ensure_column("state_samples", "ip", "TEXT")
            self._ensure_column("state_samples", "has_cached_snapshot", "INTEGER")
            self._conn.commit()

    def rebind_camera_id(self, old_camera_id: str, new_camera_id: str) -> dict[str, int]:
        """Move history rows from one camera identity to another (OTA / reflash)."""
        old_id = str(old_camera_id or "").strip().lower()
        new_id = str(new_camera_id or "").strip().lower()
        if not old_id or not new_id:
            raise ValueError("old_camera_id and new_camera_id are required")
        if old_id == new_id:
            return {
                "action_events": 0,
                "state_samples": 0,
                "config_changes": 0,
                "config_snapshots": 0,
            }

        tables = (
            "action_events",
            "state_samples",
            "config_changes",
            "config_snapshots",
        )
        counts: dict[str, int] = {}
        with self._lock:
            for table in tables:
                cursor = self._conn.execute(
                    f"UPDATE {table} SET camera_id = ? WHERE camera_id = ?",
                    (new_id, old_id),
                )
                counts[table] = int(cursor.rowcount or 0)
            self._prune_action_events(new_id)
            self._prune_state_samples(new_id)
            self._prune_config_snapshots(new_id)
            self._conn.commit()
        return counts

    def _ensure_column(self, table_name: str, column_name: str, column_type: str) -> None:
        columns = {
            str(row["name"])
            for row in self._conn.execute(f"PRAGMA table_info({table_name})").fetchall()
        }
        if column_name in columns:
            return
        self._conn.execute(f"ALTER TABLE {table_name} ADD COLUMN {column_name} {column_type}")

    def close(self) -> None:
        with self._lock:
            self._conn.close()

    def record_action_event(
        self,
        *,
        recorded_at: int,
        camera_id: str,
        source: str,
        action: str,
        status: str,
        detail: str,
        payload_summary: str = "",
    ) -> None:
        with self._lock:
            self._conn.execute(
                """
                INSERT INTO action_events (
                    recorded_at, camera_id, source, action, status, detail, payload_summary
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (recorded_at, camera_id, source, action, status, detail, payload_summary),
            )
            self._prune_action_events(camera_id)
            self._conn.commit()

    def recent_action_events(
        self,
        camera_id: str,
        limit: int = 20,
        *,
        sources: list[str] | None = None,
    ) -> list[dict[str, Any]]:
        query = [
            """
            SELECT recorded_at, source, action, status, detail, payload_summary
            FROM action_events
            WHERE camera_id = ?
            """
        ]
        params: list[Any] = [camera_id]
        if sources:
            placeholders = ", ".join("?" for _ in sources)
            query.append(f"AND source IN ({placeholders})")
            params.extend(sources)
        query.append("ORDER BY recorded_at DESC, id DESC LIMIT ?")
        params.append(max(1, int(limit)))

        with self._lock:
            rows = self._conn.execute("\n".join(query), tuple(params)).fetchall()
        return [dict(row) for row in rows]

    def recent_global_action_events(
        self,
        limit: int = 50,
        *,
        sources: list[str] | None = None,
    ) -> list[dict[str, Any]]:
        query = [
            """
            SELECT recorded_at, camera_id, source, action, status, detail, payload_summary
            FROM action_events
            WHERE 1 = 1
            """
        ]
        params: list[Any] = []
        if sources:
            placeholders = ", ".join("?" for _ in sources)
            query.append(f"AND source IN ({placeholders})")
            params.extend(sources)
        query.append("ORDER BY recorded_at DESC, id DESC LIMIT ?")
        params.append(max(1, int(limit)))

        with self._lock:
            rows = self._conn.execute("\n".join(query), tuple(params)).fetchall()
        return [dict(row) for row in rows]

    def recent_state_samples(self, camera_id: str, limit: int = 100) -> list[dict[str, Any]]:
        with self._lock:
            rows = self._conn.execute(
                """
                SELECT recorded_at, sample_type, sample_json,
                       api_status, network_online, streamer_running,
                      motion_enabled, privacy_enabled, daynight_target_mode,
                      daynight_running_mode,
                       ip, has_cached_snapshot
                FROM state_samples
                WHERE camera_id = ?
                ORDER BY recorded_at DESC, id DESC
                LIMIT ?
                """,
                (camera_id, max(1, int(limit))),
            ).fetchall()
        return [dict(row) for row in rows]

    def record_config_change(
        self,
        *,
        recorded_at: int,
        camera_id: str,
        source: str,
        change_type: str,
        path: str,
        previous_value: Any,
        new_value: Any,
        detail: str = "",
    ) -> None:
        with self._lock:
            self._conn.execute(
                """
                INSERT INTO config_changes (
                    recorded_at, camera_id, source, change_type, path, previous_json, new_json, detail
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    recorded_at,
                    camera_id,
                    source,
                    change_type,
                    path,
                    json.dumps(previous_value, sort_keys=True),
                    json.dumps(new_value, sort_keys=True),
                    detail,
                ),
            )
            self._prune_config_changes(camera_id)
            self._conn.commit()

    def recent_config_changes(self, camera_id: str, limit: int = 100) -> list[dict[str, Any]]:
        with self._lock:
            rows = self._conn.execute(
                """
                SELECT recorded_at, source, change_type, path, previous_json, new_json, detail
                FROM config_changes
                WHERE camera_id = ?
                ORDER BY recorded_at DESC, id DESC
                LIMIT ?
                """,
                (camera_id, max(1, int(limit))),
            ).fetchall()
        return [dict(row) for row in rows]

    def record_config_snapshot(
        self,
        *,
        recorded_at: int,
        camera_id: str,
        source: str,
        label: str = "",
        firmware_id: str = "",
        streamer: str = "",
        capabilities: dict[str, Any] | None = None,
        config: dict[str, Any] | None = None,
        content_hash: str = "",
        skip_duplicate: bool = True,
    ) -> dict[str, Any]:
        capabilities_json = json.dumps(capabilities or {}, sort_keys=True)
        config_json = json.dumps(config or {}, sort_keys=True)
        normalized_hash = str(content_hash or "").strip()
        if not normalized_hash:
            normalized_hash = str(hash((capabilities_json, config_json)))

        with self._lock:
            if skip_duplicate:
                newest = self._conn.execute(
                    """
                    SELECT id, content_hash
                    FROM config_snapshots
                    WHERE camera_id = ?
                    ORDER BY recorded_at DESC, id DESC
                    LIMIT 1
                    """,
                    (camera_id,),
                ).fetchone()
                if newest is not None and str(newest["content_hash"] or "") == normalized_hash:
                    return {
                        "stored": False,
                        "skipped_duplicate": True,
                        "snapshot_id": int(newest["id"]),
                        "content_hash": normalized_hash,
                    }

            cursor = self._conn.execute(
                """
                INSERT INTO config_snapshots (
                    recorded_at, camera_id, source, label, firmware_id, streamer,
                    capabilities_json, config_json, content_hash
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    recorded_at,
                    camera_id,
                    source,
                    str(label or ""),
                    str(firmware_id or ""),
                    str(streamer or ""),
                    capabilities_json,
                    config_json,
                    normalized_hash,
                ),
            )
            snapshot_id = int(cursor.lastrowid)
            self._prune_config_snapshots(camera_id)
            self._conn.commit()
            return {
                "stored": True,
                "skipped_duplicate": False,
                "snapshot_id": snapshot_id,
                "content_hash": normalized_hash,
            }

    def list_config_snapshots(self, camera_id: str, limit: int = 50) -> list[dict[str, Any]]:
        with self._lock:
            rows = self._conn.execute(
                """
                SELECT id, recorded_at, source, label, firmware_id, streamer, content_hash,
                       length(config_json) AS config_bytes,
                       length(capabilities_json) AS capabilities_bytes
                FROM config_snapshots
                WHERE camera_id = ?
                ORDER BY recorded_at DESC, id DESC
                LIMIT ?
                """,
                (camera_id, max(1, int(limit))),
            ).fetchall()
        return [dict(row) for row in rows]

    def get_config_snapshot(self, camera_id: str, snapshot_id: int) -> dict[str, Any] | None:
        with self._lock:
            row = self._conn.execute(
                """
                SELECT id, recorded_at, camera_id, source, label, firmware_id, streamer,
                       capabilities_json, config_json, content_hash
                FROM config_snapshots
                WHERE camera_id = ? AND id = ?
                """,
                (camera_id, int(snapshot_id)),
            ).fetchone()
        if row is None:
            return None
        entry = dict(row)
        try:
            entry["capabilities"] = json.loads(str(entry.pop("capabilities_json") or "{}"))
        except json.JSONDecodeError:
            entry["capabilities"] = {}
        try:
            entry["config"] = json.loads(str(entry.pop("config_json") or "{}"))
        except json.JSONDecodeError:
            entry["config"] = {}
        return entry

    def delete_config_snapshot(self, camera_id: str, snapshot_id: int) -> bool:
        with self._lock:
            cursor = self._conn.execute(
                "DELETE FROM config_snapshots WHERE camera_id = ? AND id = ?",
                (camera_id, int(snapshot_id)),
            )
            self._conn.commit()
            return cursor.rowcount > 0

    def latest_config_snapshot_summary(self, camera_id: str) -> dict[str, Any] | None:
        rows = self.list_config_snapshots(camera_id, limit=1)
        return rows[0] if rows else None

    def record_state_sample(
        self,
        *,
        recorded_at: int,
        camera_id: str,
        sample_type: str,
        sample: dict[str, Any],
        normalized: dict[str, Any] | None = None,
    ) -> None:
        metrics = normalized or {}
        with self._lock:
            self._conn.execute(
                """
                INSERT INTO state_samples (
                    recorded_at, camera_id, sample_type, sample_json,
                    api_status, network_online, streamer_running,
                    motion_enabled, privacy_enabled, daynight_target_mode,
                    daynight_running_mode,
                    ip, has_cached_snapshot
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    recorded_at,
                    camera_id,
                    sample_type,
                    json.dumps(sample, sort_keys=True),
                    metrics.get("api_status"),
                    self._bool_to_int(metrics.get("network_online")),
                    self._bool_to_int(metrics.get("streamer_running")),
                    self._bool_to_int(metrics.get("motion_enabled")),
                    self._bool_to_int(metrics.get("privacy_enabled")),
                    metrics.get("daynight_target_mode"),
                    metrics.get("daynight_running_mode"),
                    metrics.get("ip"),
                    self._bool_to_int(metrics.get("has_cached_snapshot")),
                ),
            )
            self._prune_state_samples(camera_id)
            self._conn.commit()

    def _prune_action_events(self, camera_id: str) -> None:
        self._conn.execute(
            """
            DELETE FROM action_events
            WHERE camera_id = ?
              AND id NOT IN (
                  SELECT id
                  FROM action_events
                  WHERE camera_id = ?
                  ORDER BY recorded_at DESC, id DESC
                  LIMIT ?
              )
            """,
            (camera_id, camera_id, self.max_action_events_per_camera),
        )

    def _prune_state_samples(self, camera_id: str) -> None:
        self._conn.execute(
            """
            DELETE FROM state_samples
            WHERE camera_id = ?
              AND id NOT IN (
                  SELECT id
                  FROM state_samples
                  WHERE camera_id = ?
                  ORDER BY recorded_at DESC, id DESC
                  LIMIT ?
              )
            """,
            (camera_id, camera_id, self.max_state_samples_per_camera),
        )

    def _prune_config_changes(self, camera_id: str) -> None:
        self._conn.execute(
            """
            DELETE FROM config_changes
            WHERE camera_id = ?
              AND id NOT IN (
                  SELECT id
                  FROM config_changes
                  WHERE camera_id = ?
                  ORDER BY recorded_at DESC, id DESC
                  LIMIT ?
              )
            """,
            (camera_id, camera_id, self.max_action_events_per_camera),
        )

    def _prune_config_snapshots(self, camera_id: str) -> None:
        if self.config_snapshot_max_age_days > 0:
            cutoff = int(time.time()) - (self.config_snapshot_max_age_days * 86400)
            self._conn.execute(
                """
                DELETE FROM config_snapshots
                WHERE camera_id = ? AND recorded_at < ?
                """,
                (camera_id, cutoff),
            )
        self._conn.execute(
            """
            DELETE FROM config_snapshots
            WHERE camera_id = ?
              AND id NOT IN (
                  SELECT id
                  FROM config_snapshots
                  WHERE camera_id = ?
                  ORDER BY recorded_at DESC, id DESC
                  LIMIT ?
              )
            """,
            (camera_id, camera_id, self.max_config_snapshots_per_camera),
        )

    def _bool_to_int(self, value: Any) -> int | None:
        if value is None:
            return None
        return 1 if bool(value) else 0
