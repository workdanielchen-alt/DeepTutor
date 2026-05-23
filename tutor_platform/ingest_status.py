"""
tutor_platform/ingest_status.py — 文件入库状态追踪 (v7.0)

使用 JSON 文件持久化入库任务的进度状态。
"""

import json
import os
import time
import logging
from pathlib import Path

logger = logging.getLogger("tutor_platform.ingest_status")

_STATUS_DIR = os.environ.get("INGEST_STATUS_DIR", "/data/ingest_status")


def _ensure_dir():
    os.makedirs(_STATUS_DIR, exist_ok=True)


def _path(trace_id: str) -> str:
    return os.path.join(_STATUS_DIR, f"{trace_id}.json")


class IngestStatusTracker:
    """文件入库状态追踪器。"""

    STATUS_DIR = _STATUS_DIR

    @staticmethod
    def mark(trace_id: str, stage: str, metadata: dict = None):
        """标记入库进度。"""
        _ensure_dir()
        entry = {
            "trace_id": trace_id,
            "stage": stage,
            "ts": time.time(),
            "metadata": metadata or {},
        }
        try:
            with open(_path(trace_id), "w") as f:
                json.dump(entry, f)
        except Exception as e:
            logger.warning("Failed to mark ingest status %s: %s", trace_id, e)

    @staticmethod
    def get(trace_id: str) -> dict | None:
        """获取入库状态。"""
        try:
            with open(_path(trace_id)) as f:
                return json.load(f)
        except (FileNotFoundError, json.JSONDecodeError):
            return None

    @staticmethod
    def get_orphaned(max_age: float = 3600) -> list[dict]:
        """获取卡在 processing 阶段的孤立条目。"""
        _ensure_dir()
        orphans = []
        now = time.time()
        try:
            for name in os.listdir(_STATUS_DIR):
                if not name.endswith(".json"):
                    continue
                try:
                    with open(os.path.join(_STATUS_DIR, name)) as f:
                        entry = json.load(f)
                    if entry.get("stage") in ("processing",) and now - entry.get("ts", 0) > max_age:
                        orphans.append(entry)
                except Exception:
                    pass
        except Exception:
            pass
        return orphans

    @staticmethod
    def clean(max_age: float = 86400):
        """清理超过 max_age 的旧条目。"""
        _ensure_dir()
        now = time.time()
        try:
            for name in os.listdir(_STATUS_DIR):
                p = os.path.join(_STATUS_DIR, name)
                if name.endswith(".json") and now - os.path.getmtime(p) > max_age:
                    os.remove(p)
        except Exception:
            pass
