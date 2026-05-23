"""
tutor_platform/device_manager/ssd.py — SSD 健康状态

通过 smartctl 或 nvme-cli 读取 SSD 健康数据。Docker 开发环境返回模拟数据。
"""

import logging
import os
import re
import subprocess
from dataclasses import dataclass

logger = logging.getLogger("tutor_platform.device_manager.ssd")


@dataclass
class SsdHealth:
    life_remaining: float | None = 95.0
    temp: float = 40.0
    total_bytes_written: int = 0
    power_on_hours: int = 0
    media_errors: int = 0


def _run_cmd(cmd: list[str]) -> str:
    """运行命令并返回 stdout。"""
    try:
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=5)
        return result.stdout
    except (FileNotFoundError, subprocess.TimeoutExpired, OSError):
        return ""


def _parse_smartctl() -> SsdHealth | None:
    """通过 smartctl 解析 SSD 健康信息。"""
    output = _run_cmd(["smartctl", "-a", "/dev/mmcblk0"])
    if not output:
        return None

    life = None
    temp = 40.0
    hours = 0

    for line in output.splitlines():
        m = re.search(r"Percentage Used:\s+(\d+)", line)
        if m:
            life = 100 - int(m.group(1))
        m = re.search(r"Temperature:\s+(\d+)", line)
        if m:
            temp = float(m.group(1))
        m = re.search(r"Power On Hours:\s+(\d+)", line)
        if m:
            hours = int(m.group(1))

    return SsdHealth(life_remaining=life, temp=temp, power_on_hours=hours)


def get_ssd_health() -> SsdHealth:
    """获取 SSD / eMMC 健康状态。"""
    result = _parse_smartctl()
    if result:
        return result
    return SsdHealth()
