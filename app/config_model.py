from typing import Any


def load_config_dict(config: dict[str, Any]) -> dict[str, Any]:
    for section in ("telegram", "mqtt", "routing"):
        if section not in config:
            raise ValueError(f"Missing config section: {section}")
    config.setdefault("ui", {})
    config.setdefault("history", {})
    config["ui"].setdefault("username", "")
    config["ui"].setdefault("password", "")
    config["ui"].setdefault("competency_level", "basic")
    config["ui"].setdefault("registration_stale_after_seconds", 0)
    config["ui"].setdefault("snapshot_heartbeat_interval_seconds", 60)
    config["ui"].setdefault("snapshot_heartbeat_timeout_seconds", 5)
    config["ui"].setdefault("snapshot_cache_stale_after_seconds", 3600)
    config["history"].setdefault("enabled", True)
    config["history"].setdefault("path", "")
    config["history"].setdefault("recent_actions_limit", 20)
    config["history"].setdefault("max_action_events_per_camera", 1000)
    config["history"].setdefault("max_state_samples_per_camera", 5000)
    if not config["telegram"].get("token"):
        raise ValueError("telegram.token is required")
    if not config["mqtt"].get("host"):
        raise ValueError("mqtt.host is required")
    if not config["routing"].get("command_topic"):
        raise ValueError("routing.command_topic is required")
    if not config["routing"].get("reply_topic"):
        raise ValueError("routing.reply_topic is required")
    return config