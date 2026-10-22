from __future__ import annotations

import os
from dataclasses import dataclass, field
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent


def _load_dotenv() -> None:
    path = ROOT / ".env"
    if not path.exists():
        return
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        os.environ.setdefault(key.strip(), value.strip().strip('"').strip("'"))


_load_dotenv()


def _csv_keys(raw: str) -> list[str]:
    return [part.strip() for part in raw.split(",") if part.strip()]


def _env_flag(name: str, default: bool) -> bool:
    raw = os.getenv(name)
    if raw is None or not raw.strip():
        return default
    return raw.strip().lower() in {"1", "true", "yes", "on"}


@dataclass(frozen=True)
class Settings:
    dfs_login: str = os.getenv("DFS_LOGIN", "")
    dfs_password: str = os.getenv("DFS_PASSWORD", "")
    dfs_api_keys: str = os.getenv("DFS_API_KEYS", "")
    nvidia_api_keys: list[str] = field(
        default_factory=lambda: _csv_keys(os.getenv("NVIDIA_API_KEYS", ""))
    )
    nvidia_base_url: str = os.getenv(
        "NVIDIA_BASE_URL", "https://integrate.api.nvidia.com/v1"
    )
    nvidia_model: str = os.getenv("NVIDIA_MODEL", "openai/gpt-oss-20b")
    duckdb_path: str = os.getenv("DUCKDB_PATH", str(ROOT / "data" / "anid2le.duckdb"))
    worker_count: int = int(os.getenv("WORKER_COUNT", "4"))
    nvidia_max_workers: int = int(os.getenv("NVIDIA_MAX_WORKERS", "8"))
    dfs_poll_delay_sec: float = float(os.getenv("DFS_POLL_DELAY_SEC", "8"))
    dfs_poll_max_attempts: int = int(os.getenv("DFS_POLL_MAX_ATTEMPTS", "60"))
    task_stale_seconds: int = int(os.getenv("TASK_STALE_SECONDS", "300"))
    cache_ttl_seconds: int = int(os.getenv("CACHE_TTL_SECONDS", str(7 * 24 * 3600)))
    dfs_keyword_max_chars: int = int(os.getenv("DFS_KEYWORD_MAX_CHARS", "698"))
    host: str = os.getenv("HOST", "127.0.0.1")
    port: int = int(os.getenv("PORT", "8080"))
    log_level: str = os.getenv("LOG_LEVEL", "INFO")
    scheduler_tick_sec: float = float(os.getenv("SCHEDULER_TICK_SEC", "5"))
    dfs_location_code: int = int(os.getenv("DFS_LOCATION_CODE", "2840"))
    dfs_language_code: str = os.getenv("DFS_LANGUAGE_CODE", "en")
    dfs_priority: int = int(os.getenv("DFS_PRIORITY", "1"))
    hide_status_final: bool = _env_flag("HIDE_STATUS_FINAL", True)


def get_settings() -> Settings:
    return Settings()
