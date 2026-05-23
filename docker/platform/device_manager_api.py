"""
platform/device_manager_api.py — Device Manager API (port 8101)

Serves device status, WiFi management, and system cleanup endpoints
consumed by the MCP layer running inside provider_api.py (port 8100).
"""

from dataclasses import asdict
import logging
import os
import sys

sys.path.insert(0, "/tutor_platform")
sys.path.insert(0, "/")

from fastapi import FastAPI
from pydantic import BaseModel
import uvicorn

from tutor_platform.device_manager import DeviceManager

logger = logging.getLogger("device_manager_api")
logging.basicConfig(level=logging.INFO, format="[device] %(asctime)s %(message)s")

_manager = DeviceManager()

app = FastAPI(title="Device Manager API", version="7.0.0")


class WiFiConnectRequest(BaseModel):
    ssid: str
    password: str


class WiFiForgetRequest(BaseModel):
    ssid: str


# ── Status ──


@app.get("/health")
async def health():
    return {"status": "ok", "service": "device_manager"}


@app.get("/api/device/status")
async def full_status():
    return {"ok": True, "data": _manager.full_status()}


@app.get("/api/device/temp")
async def device_temp():
    return {"ok": True, "data": asdict(_manager._thermal.get_temp())}


@app.get("/api/device/storage")
async def device_storage():
    return {"ok": True, "data": asdict(_manager._storage.get_storage())}


@app.get("/api/device/ssd")
async def device_ssd():
    return {"ok": True, "data": asdict(_manager._ssd.get_ssd_health())}


@app.get("/api/device/alerts")
async def device_alerts():
    return {"ok": True, "data": _manager.check_alerts()}


# ── WiFi ──


@app.get("/api/device/wifi/status")
async def wifi_status():
    return {"ok": True, "data": asdict(_manager._wifi.wifi_status())}


@app.get("/api/device/wifi/scan")
async def wifi_scan():
    networks = _manager._wifi.wifi_scan()
    return {"ok": True, "data": [asdict(n) for n in networks]}


@app.post("/api/device/wifi/connect")
async def wifi_connect(req: WiFiConnectRequest):
    result = _manager._wifi.wifi_connect(req.ssid, req.password)
    return {"ok": result.get("success", False), "data": result}


@app.post("/api/device/wifi/forget")
async def wifi_forget(req: WiFiForgetRequest):
    result = _manager._wifi.wifi_forget(req.ssid)
    return {"ok": result.get("success", False), "data": result}


# ── Cleanup ──


@app.post("/api/device/cleanup")
async def device_cleanup():
    result = _manager._cleanup.cleanup()
    return {"ok": True, "data": asdict(result)}


def run_device_manager(port: int = 8101):
    print(f"[device_manager_api] starting on port {port}")
    uvicorn.run(app, host="0.0.0.0", port=port, log_level="info")


if __name__ == "__main__":
    port = int(os.getenv("DEVICE_MANAGER_PORT", "8101"))
    run_device_manager(port)
