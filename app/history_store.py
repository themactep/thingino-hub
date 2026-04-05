import json
import logging
import sqlite3
import threading
from pathlib import Path
from typing import Any


LOG = logging.getLogger("telegrambothub.history")


class HistoryStore:
    def __init__(self, db_path: str, *, max_action_events_per_camera: int = 1000, max_state_samples_per_camera: int = 5000) -> None:
        self.db_path = Path(db_path)
        self.max_action_events_per_camera = max(1, int(max_action_events_per_camera))
        self.max_state_samples_per_camera = max(1, int(max_state_samples_per_camera))
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

    def _bool_to_int(self, value: Any) -> int | None:
        if value is None:
            return None
        return 1 if bool(value) else 0