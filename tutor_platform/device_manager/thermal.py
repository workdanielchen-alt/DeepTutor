"""
tutor_platform/device_manager/thermal.py — 温度检测

读取 RK3576 设备温度传感器。Docker 开发环境返回模拟数据。
"""

import logging
import os
import re
from dataclasses import dataclass

logger = logging.getLogger("tutor_platform.device_manager.thermal")

_THERMAL_ZONE_BASE = "/sys/class/thermal"


@dataclass
class TempReading:
    cpu: float = 45.0
    gpu: float = 40.0
    board: float = 42.0


def _read_thermal_zone(index: int) -> float | None:
    """读取指定 thermal zone 的温度 (毫°C → °C)。"""
    path = os.path.join(_THERMAL_ZONE_BASE, f"thermal_zone{index}", "temp")
    try:
        with open(path) as f:
            raw = f.read().strip()
            return int(raw) / 1000.0
    except (FileNotFoundError, ValueError, OSError):
        return None


def get_temp() -> TempReading:
    """读取设备温度。"""
    if not os.path.exists(_THERMAL_ZONE_BASE):
        return TempReading()

    cpu_temps = []
    gpu_temps = []

    for i in range(8):
        t = _read_thermal_zone(i)
        if t is not None:
            cpu_temps.append(t)
            if len(cpu_temps) >= 4:
                break

    for i in range(8, 12):
        t = _read_thermal_zone(i)
        if t is not None:
            gpu_temps.append(t)

    cpu = max(cpu_temps) if cpu_temps else 45.0
    gpu = max(gpu_temps) if gpu_temps else 40.0
    board = cpu  # board temp ≈ CPU package

    return TempReading(cpu=round(cpu, 1), gpu=round(gpu, 1), board=round(board, 1))
