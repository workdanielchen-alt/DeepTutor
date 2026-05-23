"""
tutor_platform/device_manager/cleanup.py — 系统清理

安全清理旧日志、临时文件、上传暂存文件。
"""

import logging
import os
import shutil
import time
from dataclasses import dataclass

logger = logging.getLogger("tutor_platform.device_manager.cleanup")

# 清理目标: (路径, 文件 glob 模式, 保留时间秒)
_CLEANUP_TARGETS: list[tuple[str, str, int]] = [
    ("/var/log", "*.log", 7 * 86400),        # 日志保留 7 天
    ("/tmp", "upload_*", 3600),              # 上传暂存保留 1 小时
    ("/data/uploads", "*", 7 * 86400),       # 上传文件保留 7 天
    ("/data/chromadb", "*.tmp", 3600),       # ChromaDB 临时文件
]

# 最大清理字节数 (安全上限, 避免一次清理过多)
_MAX_CLEANUP_BYTES = 500 * 1024 * 1024  # 500 MB


@dataclass
class CleanupResult:
    freed_bytes: int = 0
    freed_files: int = 0
    errors: list[str] = None

    def __post_init__(self):
        if self.errors is None:
            self.errors = []


def cleanup() -> CleanupResult:
    """执行系统清理。"""
    result = CleanupResult()
    now = time.time()
    total_freed = 0
    total_files = 0

    for base_path, pattern, max_age in _CLEANUP_TARGETS:
        if not os.path.isdir(base_path):
            continue
        try:
            freed, files = _cleanup_dir(base_path, pattern, now - max_age, _MAX_CLEANUP_BYTES - total_freed)
            total_freed += freed
            total_files += files
            if total_freed >= _MAX_CLEANUP_BYTES:
                logger.info("Cleanup reached max bytes (%d), stopping", _MAX_CLEANUP_BYTES)
                break
        except Exception as e:
            result.errors.append(f"{base_path}: {e}")
            logger.warning("Cleanup error in %s: %s", base_path, e)

    result.freed_bytes = total_freed
    result.freed_files = total_files
    logger.info("Cleanup completed: freed %d bytes across %d files", total_freed, total_files)
    return result


def _cleanup_dir(base_path: str, pattern: str, cutoff: float, max_bytes: int) -> tuple[int, int]:
    """清理目录中符合条件的旧文件。"""
    import fnmatch
    freed_bytes = 0
    freed_files = 0

    for root, dirs, files in os.walk(base_path):
        if freed_bytes >= max_bytes:
            break
        for fname in files:
            if not fnmatch.fnmatch(fname, pattern):
                continue
            fpath = os.path.join(root, fname)
            try:
                stat = os.stat(fpath)
                if stat.st_mtime < cutoff:
                    size = stat.st_size
                    os.remove(fpath)
                    freed_bytes += size
                    freed_files += 1
                    if freed_bytes >= max_bytes:
                        break
            except (FileNotFoundError, OSError):
                continue

        # Remove empty directories
        for d in dirs[:]:
            dpath = os.path.join(root, d)
            try:
                if not os.listdir(dpath):
                    os.rmdir(dpath)
            except OSError:
                continue

    return freed_bytes, freed_files
