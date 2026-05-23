"""
tutor_platform/device_manager/wifi.py — WiFi 管理

通过 NetworkManager (nmcli) 管理 WiFi 连接。
Docker 开发环境返回模拟数据。
"""

from dataclasses import dataclass
import logging
import os
import subprocess

logger = logging.getLogger("tutor_platform.device_manager.wifi")


@dataclass
class WifiStatus:
    connected: bool = False
    ssid: str = ""
    signal: int = 0
    ip: str = ""


@dataclass
class NetworkInfo:
    ssid: str
    signal: int
    security: str = "WPA2"


def _nmcli(args: list[str]) -> str:
    """运行 nmcli 命令。"""
    try:
        result = subprocess.run(
            ["nmcli"] + args,
            capture_output=True,
            text=True,
            timeout=10,
        )
        return result.stdout
    except (FileNotFoundError, subprocess.TimeoutExpired, OSError) as e:
        logger.debug("nmcli %s failed: %s", " ".join(args), e)
        return ""


def wifi_status() -> WifiStatus:
    """获取当前 WiFi 连接状态。"""
    if not os.path.exists("/usr/bin/nmcli"):
        return WifiStatus()

    output = _nmcli(["-t", "-f", "ACTIVE,SSID,SIGNAL,IP4", "device", "wifi", "list"])
    for line in output.splitlines():
        parts = line.strip().split(":")
        if len(parts) >= 4 and parts[0] == "yes":
            return WifiStatus(
                connected=True,
                ssid=parts[1],
                signal=int(parts[2]) if parts[2].isdigit() else 0,
                ip=parts[3],
            )
    return WifiStatus()


def wifi_scan() -> list[NetworkInfo]:
    """扫描可用 WiFi 网络。"""
    if not os.path.exists("/usr/bin/nmcli"):
        return [
            NetworkInfo(ssid="模拟WiFi_5G", signal=85, security="WPA2"),
            NetworkInfo(ssid="模拟WiFi_2.4G", signal=60, security="WPA2"),
        ]

    output = _nmcli(["-t", "-f", "SSID,SIGNAL,SECURITY", "device", "wifi", "list"])
    networks = []
    seen = set()
    for line in output.splitlines():
        parts = line.strip().split(":")
        if len(parts) >= 3 and parts[0] and parts[0] not in seen:
            seen.add(parts[0])
            signal = int(parts[1]) if parts[1].isdigit() else 0
            networks.append(
                NetworkInfo(
                    ssid=parts[0],
                    signal=signal,
                    security=parts[2] or "开放",
                )
            )
    return sorted(networks, key=lambda n: n.signal, reverse=True)[:50]


def _get_wifi_ip() -> str:
    """获取当前 WiFi 连接的 IP 地址 (自动检测无线接口名)."""
    output = _nmcli(["-t", "-f", "NAME,UUID,TYPE,DEVICE", "connection", "show", "--active"])
    for line in output.splitlines():
        parts = line.strip().split(":")
        if len(parts) >= 4 and "wireless" in parts[2].lower():
            dev = parts[3]
            if dev:
                ip_out = _nmcli(["-t", "-f", "IP4", "device", "show", dev])
                for ip_line in ip_out.splitlines():
                    if "/" in ip_line:
                        ip = ip_line.strip().split("/")[0]
                        if ip.count(".") == 3:
                            return ip
    return ""


def wifi_connect(ssid: str, password: str) -> dict:
    """连接 WiFi 网络。"""
    if not os.path.exists("/usr/bin/nmcli"):
        return {"success": True, "message": f"已连接 {ssid} (模拟)"}

    try:
        output = _nmcli(
            [
                "device",
                "wifi",
                "connect",
                ssid,
                "password",
                password,
            ]
        )
        if "successfully" in output.lower():
            logger.info("WiFi connected to %s", ssid)
            ip = _get_wifi_ip()
            result = {"success": True, "message": f"已成功连接到 {ssid}"}
            if ip:
                result["ip"] = ip
            return result
        return {"success": False, "error": output.strip() or "连接失败"}
    except Exception as e:
        logger.warning("WiFi connect failed: %s", e)
        return {"success": False, "error": str(e)}


def wifi_forget(ssid: str) -> dict:
    """忘记已保存的 WiFi 网络。"""
    if not os.path.exists("/usr/bin/nmcli"):
        return {"success": True, "message": f"已忘记 {ssid} (模拟)"}

    try:
        # List connections to find the one matching this SSID
        output = _nmcli(["-t", "-f", "NAME", "connection", "show"])
        for line in output.splitlines():
            if line.strip() == ssid:
                _nmcli(["connection", "delete", ssid])
                logger.info("WiFi forgotten: %s", ssid)
                return {"success": True, "message": f"已忘记网络 {ssid}"}
        return {"success": True, "message": f"未找到 {ssid} 的已保存配置"}
    except Exception as e:
        logger.warning("WiFi forget failed: %s", e)
        return {"success": False, "error": str(e)}
