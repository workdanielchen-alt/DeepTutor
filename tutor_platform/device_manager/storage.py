"""
tutor_platform/device_manager/storage.py — 存储空间检测

读取磁盘分区使用情况。Docker 开发环境返回模拟数据。
"""

import logging
import os
import shutil
from dataclasses import dataclass

logger = logging.getLogger("tutor_platform.device_manager.storage")

_STORAGE_PATHS = ["/", "/data", "/var/log"]

DEFAULT_TOTAL = 64_000_000_000   # 64 GB
DEFAULT_USED = 12_000_000_000    # 12 GB


@dataclass
class StorageInfo:
    total: int = DEFAULT_TOTAL
    used: int = DEFAULT_USED
    free: int = DEFAULT_TOTAL - DEFAULT_USED
    used_percent: float = round(DEFAULT_USED / DEFAULT_TOTAL * 100, 1)


def _get_partition_usage(path: str) -> dict | None:
    """获取指定路径所在分区的磁盘使用情况。"""
    try:
        usage = shutil.disk_usage(path)
        return {
            "total": usage.total,
            "used": usage.used,
            "free": usage.free,
            "used_percent": round(usage.used / usage.total * 100, 1),
        }
    except (FileNotFoundError, OSError):
        return None


def get_storage() -> StorageInfo:
    """获取存储空间信息。"""
    for path in _STORAGE_PATHS:
        info = _get_partition_usage(path)
        if info:
            return StorageInfo(**info)
    return StorageInfo()
