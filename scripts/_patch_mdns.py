"""Insert mdns status endpoint after health endpoint."""

path = "docker/platform/provider_api.py"
content = open(path, "r", encoding="utf-8").read()

old = "    return response\n\n\n\n\ndef run_provider_api(port: int = 8100):"
new = """    return response


@app.get("/api/device/mdns/status")
async def mdns_status():
    import subprocess
    avahi_alive = False
    try:
        r = subprocess.run(
            ["pgrep", "-f", "avahi-publish-service"],
            capture_output=True, timeout=5,
        )
        avahi_alive = r.returncode == 0
    except Exception:
        pass
    import threading
    zeroconf_alive = any(
        t.name == "mdns-zeroconf" and t.is_alive()
        for t in threading.enumerate()
    )
    return {
        "ok": True,
        "hostname": _MDNS_HOSTNAME,
        "ip": _DEVICE_IP or "",
        "engines": {"avahi": avahi_alive, "zeroconf": zeroconf_alive},
    }


def run_provider_api(port: int = 8100):"""

if old in content:
    content = content.replace(old, new, 1)
    open(path, "w", encoding="utf-8").write(content)
    print("OK: mdns_status endpoint inserted")
else:
    print("ERROR: pattern not found")
