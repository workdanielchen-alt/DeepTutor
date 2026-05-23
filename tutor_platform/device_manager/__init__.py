"""
tutor_platform/device_manager — 设备管理器 (v7.0)

封装 RK3576 设备状态查询: 温度、存储、SSD、WiFi、系统清理。
所有模块在非 Linux 环境 (Docker 开发) 中返回模拟数据。
"""

import logging
from dataclasses import dataclass, asdict
from typing import Any

from . import thermal, storage, ssd, wifi, cleanup

logger = logging.getLogger("tutor_platform.device_manager")


class DeviceManager:
    """设备管理器 — 聚合各子模块状态。"""

    def __init__(self):
        self._thermal = thermal
        self._storage = storage
        self._ssd = ssd
        self._wifi = wifi
        self._cleanup = cleanup

    def full_status(self) -> dict[str, Any]:
        """返回设备综合状态。"""
        return {
            "temp": asdict(self._thermal.get_temp()),
            "storage": asdict(self._storage.get_storage()),
            "ssd": asdict(self._ssd.get_ssd_health()),
            "wifi": asdict(self._wifi.wifi_status()),
        }

    def check_alerts(self) -> list[dict]:
        """检查设备告警。"""
        alerts = []

        temp = self._thermal.get_temp()
        if temp.cpu > 75:
            alerts.append({"severity": "warning", "type": "temp_high",
                           "message": f"CPU 温度 {temp.cpu}°C 偏高", "value": temp.cpu})

        store = self._storage.get_storage()
        pct = store.used_percent
        if pct > 90:
            alerts.append({"severity": "critical", "type": "storage_full",
                           "message": f"存储使用 {pct}% 即将满", "value": pct})
        elif pct > 80:
            alerts.append({"severity": "warning", "type": "storage_high",
                           "message": f"存储使用 {pct}% 偏高", "value": pct})

        health = self._ssd.get_ssd_health()
        if health.life_remaining is not None and health.life_remaining < 20:
            alerts.append({"severity": "warning", "type": "ssd_life",
                           "message": f"SSD 剩余寿命 {health.life_remaining}%", "value": health.life_remaining})

        return alerts
