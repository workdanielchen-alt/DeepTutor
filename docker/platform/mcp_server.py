"""
platform/mcp_server.py — MCP Server (v7.0, 从 deeptutor_mcp 合并)

改编自 docker/deeptutor_mcp/server.py:
  - PLATFORM_URL 默认指向 http://localhost:8100 (原 hermes_ingest:8005)
  - DEVICE_MANAGER_URL 默认指向 http://localhost:8101 (原 device_manager:8006)
  - 保留全部 33 个工具 + 熔断器 + fast/slow path
"""

import os
import json
import base64
import asyncio
import time
import logging
import contextvars
import httpx

logger = logging.getLogger(__name__)
from mcp.server.fastmcp import FastMCP
from mcp.types import TextContent, ImageContent

DEEPTUTOR_URL = os.getenv("DEEPTUTOR_API_URL", "http://deeptutor:8003")
PLATFORM_URL = os.getenv("PLATFORM_API_URL", "http://localhost:8100")
HERMES_URL = os.getenv("HERMES_AGENT_URL", "http://hermes_agent:8004")
DEVICE_MANAGER_URL = os.getenv("DEVICE_MANAGER_URL", "http://localhost:8101")

SOURCES_DIR = os.getenv("SOURCES_DIR", "/data/sources")

MCP_TOOL_TIMEOUT = int(os.getenv("MCP_TOOL_TIMEOUT", "180"))

_TIMEOUT_DT_GET = 30
_TIMEOUT_DT_POST = 60
_TIMEOUT_PLATFORM_GET = 30
_TIMEOUT_PLATFORM_POST = 120
_TIMEOUT_DM_FAST = 15
_TIMEOUT_DM_SCAN = 25
_TIMEOUT_DM_CONFIGURE = 35
_TIMEOUT_DM_CLEANUP = 90

_current_tool: contextvars.ContextVar[str] = contextvars.ContextVar("current_tool", default="")

# ── Phase B: direct mode (merged into Provider API process) ──
# When _USE_DIRECT_PROVIDER is True, _platform_post/_platform_get use
# ASGI transport (httpx.AsyncClient(app=...)) instead of real HTTP.
# This avoids the localhost loop deadlock when FastMCP is mounted
# on the same FastAPI app that handles /api/* routes.
_USE_DIRECT_PROVIDER: bool = False
_fastapi_app = None  # set by _set_direct_mode(app)


def _set_direct_mode(app) -> None:
    """Enable ASGI transport for platform API calls (same-process merge)."""
    global _USE_DIRECT_PROVIDER, _fastapi_app
    _USE_DIRECT_PROVIDER = True
    _fastapi_app = app


from mcp.server.streamable_http import TransportSecuritySettings

mcp = FastMCP(
    "deeptutor",
    instructions=(
        "DeepTutor 学习平台 MCP Server (纯 REST 代理)。\n"
        "本服务只代理 DeepTutor 的 REST API，不做 LLM 推理。\n"
        "LLM 推理由 HermesAgent agent loop 统一管理。\n"
        "Provider (OCR/视觉/入库) 由 HermesAgent 直接调用，不经 MCP 代理。"
    ),
    # 禁用 DNS rebinding protection: hermes_agent 容器以宿主域名 "platform" 访问,
    # 默认只放行 127.0.0.1/localhost, 跨容器请求全部被 421 拒绝
    transport_security=TransportSecuritySettings(enable_dns_rebinding_protection=False),
)

_scene: str = "full"

# ── 角色权限门禁 (方案C: 单 HA + 身份门禁) ──
# Layer 2 防御: 孩子无法调用设备管理工具
_CHILD_BLOCKED_TOOLS: set[str] = {
    "device_status", "device_temp", "storage_info", "ssd_health",
    "wifi_scan", "wifi_configure", "wifi_status", "wifi_forget", "device_cleanup",
    "device_command", "device_alerts",
    "get_circuit_status", "reset_circuit", "get_bot_qrcode",
}

_PARENT_ROLE = "parent"

def _check_tool_permission(tool_name: str, learner_id: str = "") -> str | None:
    """检查工具调用权限. 返回 None=放行, 返回 str=拒绝原因."""
    if tool_name not in _CHILD_BLOCKED_TOOLS:
        return None
    if learner_id and learner_id == _PARENT_ROLE:
        return None
    return f"需要家长权限才能使用 {tool_name}，请让家长来操作"

def _check_device_permission(agent_role: str = "") -> str | None:
    """检查设备工具调用权限 (B 类工具, 无 learner_id 参数). v7.2: 大小写不敏感."""
    if agent_role.lower() == _PARENT_ROLE:
        return None
    return "需要家长权限才能管理设备，请让家长来操作"

_SLOW_TOOLS: set[str] = {
    "process_file",
    "kb_upload_file",
    "kb_upload_text",
    "book_create",
    "notebook_execute",
    "tutor_chat",
    "wifi_scan",
    "wifi_configure",
    "wifi_forget",
    "device_cleanup",
    "cowriter_edit",
    "generate_practice",
    "deep_solve",
    "vision_solve",
}
_slow_queue = asyncio.Semaphore(1)
_slow_pending: int = 0
_SLOW_BUSY_THRESHOLD: int = 5

_SCENE_TOOLS: dict[str, set[str]] = {
    "practice": {"quiz_review", "generate_practice", "generate_exam_paper", "deep_solve", "vision_solve",
                  "record_quiz_result", "question_notebook_list"},
    "reading": {"book_create", "book_read", "book_list"},
}

_TOOL_DESCRIPTIONS: dict[str, str] = {
    "kb_search":           "知识库搜索",
    "kb_list":             "列出知识库",
    "kb_upload_text":      "文本入库",
    "kb_upload_file":      "文件入库",
    "quiz_review":         "错题回顾",
    "generate_practice":   "根据错题生成针对性练习题",
    "generate_exam_paper": "根据学情生成完整强化训练试卷 — 覆盖全部薄弱知识点",
    "deep_solve":          "深度解题 — 用户明确要求时调用, OCR自动内容用 tutor_chat",
    "vision_solve":        "拍照解题 — 将题目图片发给解题引擎返回图文解答",
    "update_memory":       "更新学习者画像 (summary/profile)",
    "record_quiz_result":  "记录答题结果到学习会话",
    "question_notebook_list": "查询学习历史中的所有答题记录（含纠错本）",
    "generate_report":      "生成学习报告 (日报/周报/月报) — 返回报告文本",
    "book_create":         "生成教材",
    "book_read":           "阅读教材",
    "book_list":           "列出教材",
    "get_memory":          "学习者画像",
    "process_file":        "文件处理 (归一化→OCR→意图→分发)",
    "set_scene":           "场景切换",
    "detect_scene":        "场景识别",
    "get_circuit_status":  "熔断器状态",
    "reset_circuit":       "重置熔断器",
    "device_status":       "设备综合状态",
    "storage_info":        "存储空间详情",
    "view_source":         "查看原始资料（根据 trace_id 检索归档文件）",
    "ssd_health":          "SSD 健康状态",
    "device_temp":         "设备温度",
    "wifi_scan":           "WiFi 扫描",
    "device_command":      "设备管理统一入口：先规则匹配，未分类时 LLM 兜底",
    "device_alerts":       "查询设备告警 (温度/存储/SSD/服务异常)",
    "wifi_configure":      "WiFi 连接",
    "wifi_forget":         "忘记已保存的 WiFi 网络",
    "wifi_status":         "WiFi 状态",
    "device_cleanup":      "安全清理旧日志和上传暂存文件",
    "cowriter_edit":       "AI 协作文本编辑 (润色/缩短/扩展)",
    "cowriter_documents":  "列出/创建 Co-Writer 文档",
    "cowriter_history":    "Co-Writer 编辑历史",
    "get_scene":           "场景状态和可用工具",
    "notebook_create":     "创建交互式笔记",
    "notebook_read":       "读取笔记内容",
    "notebook_add_cell":   "添加单元格 (Markdown/代码)",
    "notebook_execute":    "执行代码单元格",
    "notebook_list":       "列出所有笔记",
    "tutor_chat":          "引导式教学 (将消息转发给 DeepTutor 教学引擎)",
    "session_read":        "读取 Web 端学习会话历史",
    "get_bot_qrcode":      "警告: 仅家长可调用。当家长说【加孩子】或【加孩子学习】时，必须立即调用此工具生成二维码。",
    "bind_child_bot":      "警告: 仅家长可调用，仅首次。当孩子没有专属机器人时调用此工具创建并返回二维码。已绑定请用 get_bot_qrcode。",
}

_SCENE_KEYWORDS: dict[str, list[tuple[list[str], int]]] = {
    "practice": [
        (["出题", "做题", "刷题", "练习题", "测验", "考试", "题目", "批改", "错题",
          "解题", "求解", "答案", "对错", "步骤", "运算",
          "因式分解", "解方程", "计算", "证明", "几何", "函数"], 2),
    ],
    "reading": [
        (["教材", "课本", "阅读", "看书", "学习资料", "课程", "章节",
          "可视化", "图表", "画图", "概念图", "思维导图",
          "解释概念", "什么是", "讲解", "原理", "定义"], 2),
    ],
}

_DEVICE_KEYWORDS: list[str] = [
    "温度", "多少度", "烫", "散热", "发热",
    "存储", "空间", "磁盘", "硬盘", "满了", "还剩",
    "ssd", "寿命", "磨损", "固态",
    "wifi", "wi-fi", "网络", "连网", "无线", "扫描wifi", "扫描网络", "搜wifi", "搜网络", "搜无线",
    "设备状态", "系统状态", "运行状态",
    "ip", "信号",
    "清理", "释放", "腾空间",
    "加孩子", "绑定孩子", "子网关",  # 子网关绑定 (规则优先, LLM 兜底)
]

_DEVICE_IP: str = ""


def _get_device_ip() -> str:
    """获取设备 LAN IP, 用于 /bind-qr 页面展示.

    优先级:
      1. DEVICE_IP 环境变量 (docker-compose 传入宿主机 LAN IP)
      2. UDP 连接 8.8.8.8 取出口 IP (bridge 模式下也可用)
      3. hostname -I (host 网络模式)
      4. socket.gethostbyname 兜底
    """
    global _DEVICE_IP
    if _DEVICE_IP:
        return _DEVICE_IP

    # 1) 环境变量
    env_ip = os.environ.get("DEVICE_IP", "").strip()
    if env_ip and env_ip.count(".") == 3:
        _DEVICE_IP = env_ip
        return env_ip

    # 2) UDP 出口 IP (连接外部地址获取本机出口 IP)
    import socket
    for target in ("8.8.8.8", "223.5.5.5", "114.114.114.114"):
        try:
            s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            s.settimeout(3)
            s.connect((target, 80))
            ip = s.getsockname()[0]
            s.close()
            if ip.count(".") == 3 and not ip.startswith("127.") and not ip.startswith("172."):
                _DEVICE_IP = ip
                return ip
        except Exception:
            pass

    # 3) hostname -I (host 网络模式)
    import subprocess
    try:
        r = subprocess.run(
            ["hostname", "-I"], capture_output=True, text=True, timeout=5,
        )
        if r.returncode == 0 and r.stdout.strip():
            ips = r.stdout.strip().split()
            for ip in ips:
                if ip.count(".") == 3 and not ip.startswith("127.") and not ip.startswith("172."):
                    _DEVICE_IP = ip
                    return ip
            _DEVICE_IP = ips[0]
            return ips[0]
    except Exception:
        pass

    # 4) 兜底: gethostbyname
    try:
        hostname = socket.gethostname()
        _DEVICE_IP = socket.gethostbyname(hostname) or ""
    except Exception:
        _DEVICE_IP = ""
    return _DEVICE_IP


_ALWAYS_AVAILABLE: set[str] = {
    "device_command", "device_status", "device_temp", "storage_info", "ssd_health",
    "wifi_scan", "wifi_configure", "wifi_status", "device_cleanup",
}


def _detect_scene_internal(user_message: str) -> dict:
    msg_lower = user_message.lower()
    for kw in _DEVICE_KEYWORDS:
        if kw.lower() in msg_lower:
            return {"scene": "full", "confidence": "device",
                    "scores": {"practice": 0, "reading": 0}, "threshold": 3}
    scores = {"practice": 0, "reading": 0}
    for scene, rule_groups in _SCENE_KEYWORDS.items():
        for keywords, weight in rule_groups:
            for kw in keywords:
                if kw.lower() in msg_lower:
                    scores[scene] += weight
                    break
    if scores["practice"] >= 3 and scores["practice"] > scores["reading"]:
        detected = "practice"
    elif scores["reading"] >= 3 and scores["reading"] > scores["practice"]:
        detected = "reading"
    elif scores["practice"] >= 3 and scores["practice"] == scores["reading"]:
        detected = _scene if _scene != "full" else "full"
    else:
        detected = "full"
    total = scores["practice"] + scores["reading"]
    confidence = "high" if total >= 5 else ("medium" if total >= 2 else "low")
    return {"scene": detected, "confidence": confidence,
            "scores": scores, "threshold": 3}


def _check_scene(tool_name: str) -> str | None:
    if _scene == "full":
        return None
    if tool_name in _ALWAYS_AVAILABLE:
        return None
    allowed = _SCENE_TOOLS.get(_scene, set())
    if tool_name in ("set_scene", "detect_scene", "get_scene",
                     "get_circuit_status", "reset_circuit"):
        return None
    if tool_name not in allowed:
        scene_cn = {"practice": "练习模式", "reading": "阅读模式"}.get(_scene, _scene)
        return f"工具 {tool_name} 在 {scene_cn} 下不可用。当前可用: {', '.join(sorted(allowed))}"
    return None


class CircuitBreaker:
    def __init__(self, name: str, failure_threshold: int = 3, recovery_timeout: float = 60.0):
        self.name = name
        self.failure_threshold = failure_threshold
        self.recovery_timeout = recovery_timeout
        self._failure_count: int = 0
        self._last_failure_time: float = 0.0
        self._state: str = "closed"

    @property
    def is_open(self) -> bool:
        if self._state == "closed":
            return False
        if self._state == "half_open":
            return False
        if time.time() - self._last_failure_time > self.recovery_timeout:
            self._state = "half_open"
            return False
        return True

    def record_success(self):
        self._failure_count = 0
        self._state = "closed"

    def record_failure(self):
        self._failure_count += 1
        self._last_failure_time = time.time()
        if self._failure_count >= self.failure_threshold:
            self._state = "open"

    def status(self) -> dict:
        age = round(time.time() - self._last_failure_time, 1) if self._last_failure_time else 0.0
        return {
            "name": self.name,
            "state": self._state,
            "failure_count": self._failure_count,
            "threshold": self.failure_threshold,
            "last_failure_age_s": age or None,
            "recovery_timeout_s": self.recovery_timeout,
            "retry_after_s": max(0, round(self.recovery_timeout - age, 0)) if self._last_failure_time else 0,
        }


_dt_circuit = CircuitBreaker(name="deeptutor", failure_threshold=3, recovery_timeout=60.0)


def _circuit_open_response(service: str) -> dict:
    st = _dt_circuit.status()
    age = st.get("last_failure_age_s", 0) or 0
    retry_after = max(0, round(st["recovery_timeout_s"] - age, 0))
    return {
        "ok": False,
        "error": "后端大脑正在重启中，请稍候片刻后重试。",
        "circuit": "open",
        "retry_after_s": retry_after,
        "service": service,
    }


_http: httpx.AsyncClient | None = None


def _get_http() -> httpx.AsyncClient:
    global _http
    if _http is None:
        _http = httpx.AsyncClient(
            timeout=httpx.Timeout(MCP_TOOL_TIMEOUT),
            limits=httpx.Limits(max_keepalive_connections=5, keepalive_expiry=60.0),
        )
    return _http


async def _close_http():
    global _http
    if _http is not None:
        await _http.aclose()
        _http = None


async def _guarded_call(tool_name: str, coro):
    error = _check_scene(tool_name)
    if error:
        return json.dumps({"ok": False, "error": error, "scene": _scene}, ensure_ascii=False)
    _current_tool.set(tool_name)
    if tool_name in _SLOW_TOOLS:
        global _slow_pending
        _slow_pending += 1
        try:
            async with _slow_queue:
                return await _call_with_retry(coro, tool_name)
        finally:
            _slow_pending -= 1
    return await _call_with_retry(coro, tool_name)


async def _call_with_retry(coro, tool_name: str, max_retries: int = 1):
    """执行 coro 并重试瞬态错误.

    不包裹 asyncio.wait_for — 内层 HTTP 调用有各自的超时 (DT_GET=30, DT_POST=60, PLATFORM_POST=120),
    外层 wait_for 会与内层冲突导致慢操作被误判超时.
    MCP_TOOL_TIMEOUT 仅作为总控上限传给底层 httpx client.
    """
    last_exc = None
    for attempt in range(max_retries + 1):
        try:
            return await coro
        except (httpx.TransportError, httpx.ConnectError, httpx.RemoteProtocolError) as e:
            last_exc = e
            if attempt < max_retries:
                logger.warning("瞬态错误, 重试 %s (attempt %d/%d): %s", tool_name, attempt + 1, max_retries, e)
                await asyncio.sleep(0.5 * (attempt + 1))
                continue
            raise
    raise last_exc


async def _dt_get(path: str) -> dict:
    if _dt_circuit.is_open:
        return _circuit_open_response("deeptutor")
    try:
        client = _get_http()
        r = await client.get(f"{DEEPTUTOR_URL}{path}", timeout=_TIMEOUT_DT_GET)
        if r.status_code == 200:
            _dt_circuit.record_success()
            return r.json()
        elif r.status_code >= 500:
            _dt_circuit.record_failure()
            return {"ok": False, "error": f"Deeptutor HTTP {r.status_code}"}
        else:
            return {"ok": False, "error": f"HTTP {r.status_code}"}
    except Exception as e:
        _dt_circuit.record_failure()
        return {"ok": False, "error": f"连接失败: {str(e)}"}


async def _dt_put(path: str, data: dict = None) -> dict:
    if _dt_circuit.is_open:
        return _circuit_open_response("deeptutor")
    try:
        client = _get_http()
        r = await client.put(f"{DEEPTUTOR_URL}{path}", json=data or {}, timeout=_TIMEOUT_DT_POST)
        if r.status_code == 200:
            _dt_circuit.record_success()
            return r.json()
        elif r.status_code >= 500:
            _dt_circuit.record_failure()
            return {"ok": False, "error": f"Deeptutor HTTP {r.status_code}"}
        else:
            return {"ok": False, "error": f"HTTP {r.status_code}"}
    except Exception as e:
        _dt_circuit.record_failure()
        return {"ok": False, "error": f"连接失败: {str(e)}"}


async def _dt_post(path: str, data: dict = None) -> dict:
    if _dt_circuit.is_open:
        return _circuit_open_response("deeptutor")
    try:
        client = _get_http()
        r = await client.post(f"{DEEPTUTOR_URL}{path}", json=data or {}, timeout=_TIMEOUT_DT_POST)
        if r.status_code == 200:
            _dt_circuit.record_success()
            return r.json()
        elif r.status_code >= 500:
            _dt_circuit.record_failure()
            return {"ok": False, "error": f"Deeptutor HTTP {r.status_code}"}
        else:
            return {"ok": False, "error": f"HTTP {r.status_code}"}
    except Exception as e:
        _dt_circuit.record_failure()
        return {"ok": False, "error": f"连接失败: {str(e)}"}


_platform_circuit = CircuitBreaker(name="platform", failure_threshold=3, recovery_timeout=60.0)


def _platform_circuit_open_response() -> dict:
    age = time.time() - _platform_circuit._last_failure_time
    retry_after = max(0, round(_platform_circuit.recovery_timeout - age, 0))
    return {
        "ok": False,
        "error": "平台服务正在重启中，请稍候片刻后重试。",
        "circuit": "open",
        "retry_after_s": retry_after,
        "service": "platform",
    }


async def _platform_get(path: str) -> dict:
    global _platform_circuit
    if _platform_circuit.is_open:
        return _platform_circuit_open_response()
    if _USE_DIRECT_PROVIDER and _fastapi_app is not None:
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=_fastapi_app), base_url="http://localhost:8100") as _c:
            try:
                _r = await _c.get(path, timeout=_TIMEOUT_PLATFORM_GET)
                if _r.status_code == 200:
                    _platform_circuit.record_success()
                    return _r.json()
                elif _r.status_code >= 500:
                    _platform_circuit.record_failure()
                return {"ok": False, "error": f"HTTP {_r.status_code}"}
            except Exception as e:
                _platform_circuit.record_failure()
                return {"ok": False, "error": f"平台连接失败: {str(e)}"}
    try:
        client = _get_http()
        r = await client.get(f"{PLATFORM_URL}{path}", timeout=_TIMEOUT_PLATFORM_GET)
        if r.status_code == 200:
            _platform_circuit.record_success()
            return r.json()
        elif r.status_code >= 500:
            _platform_circuit.record_failure()
        return {"ok": False, "error": f"HTTP {r.status_code}"}
    except Exception as e:
        _platform_circuit.record_failure()
        return {"ok": False, "error": f"平台连接失败: {str(e)}"}


async def _platform_post(path: str, data: dict = None) -> dict:
    global _platform_circuit
    if _platform_circuit.is_open:
        return _platform_circuit_open_response()
    headers = {}
    tool_name = _current_tool.get("")
    if tool_name:
        headers["X-Tool-Name"] = tool_name
    if _USE_DIRECT_PROVIDER and _fastapi_app is not None:
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=_fastapi_app), base_url="http://localhost:8100") as _c:
            try:
                _r = await _c.post(path, json=data or {}, headers=headers, timeout=_TIMEOUT_PLATFORM_POST)
                if _r.status_code == 200:
                    _platform_circuit.record_success()
                    return _r.json()
                elif _r.status_code >= 500:
                    _platform_circuit.record_failure()
                return {"ok": False, "error": f"HTTP {_r.status_code}"}
            except Exception as e:
                _platform_circuit.record_failure()
                return {"ok": False, "error": f"平台连接失败: {str(e)}"}
    try:
        client = _get_http()
        r = await client.post(f"{PLATFORM_URL}{path}", json=data or {}, headers=headers, timeout=_TIMEOUT_PLATFORM_POST)
        if r.status_code == 200:
            _platform_circuit.record_success()
            return r.json()
        elif r.status_code >= 500:
            _platform_circuit.record_failure()
        return {"ok": False, "error": f"HTTP {r.status_code}"}
    except Exception as e:
        _platform_circuit.record_failure()
        return {"ok": False, "error": f"平台连接失败: {str(e)}"}


async def _platform_upload(path: str, data: dict, file_path: str, filename: str, headers: dict = None) -> dict:
    """Multipart file upload to platform API. Direct mode uses ASGI transport."""
    global _platform_circuit
    if _platform_circuit.is_open:
        return _platform_circuit_open_response()
    _headers = headers or {}
    if _USE_DIRECT_PROVIDER and _fastapi_app is not None:
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=_fastapi_app), base_url="http://localhost:8100") as _c:
            with open(file_path, "rb") as _f:
                try:
                    _r = await _c.post(path, data=data, files={"file": (filename, _f, "application/octet-stream")}, headers=_headers, timeout=_TIMEOUT_PLATFORM_POST)
                    if _r.status_code == 200:
                        _platform_circuit.record_success()
                        return json.loads(_r.text) if _r.text.strip() else {}
                    elif _r.status_code >= 500:
                        _platform_circuit.record_failure()
                    return {"ok": False, "error": f"HTTP {_r.status_code}"}
                except Exception as e:
                    _platform_circuit.record_failure()
                    return {"ok": False, "error": f"平台连接失败: {str(e)}"}
    client = _get_http()
    with open(file_path, "rb") as f:
        r = await client.post(f"{PLATFORM_URL}{path}", data=data, files={"file": (filename, f, "application/octet-stream")}, headers=_headers, timeout=_TIMEOUT_PLATFORM_POST)
    if r.status_code == 200:
        _platform_circuit.record_success()
        return json.loads(r.text) if r.text.strip() else {}
    elif r.status_code >= 500:
        _platform_circuit.record_failure()
    return {"ok": False, "error": f"HTTP {r.status_code}"}


_dm_circuit = CircuitBreaker(name="device_manager", failure_threshold=3, recovery_timeout=60.0)


def _dm_circuit_open_response() -> dict:
    age = time.time() - _dm_circuit._last_failure_time
    retry_after = max(0, round(_dm_circuit.recovery_timeout - age, 0))
    return {
        "ok": False,
        "error": "设备管理服务暂时不可用，请稍候片刻后重试。",
        "circuit": "open",
        "retry_after_s": retry_after,
        "service": "device_manager",
    }


async def _dm_get(path: str, timeout: int = 15) -> dict:
    if _dm_circuit.is_open:
        return _dm_circuit_open_response()
    try:
        client = _get_http()
        r = await client.get(f"{DEVICE_MANAGER_URL}{path}", timeout=timeout)
        if r.status_code == 200:
            _dm_circuit.record_success()
            return r.json()
        elif r.status_code >= 500:
            _dm_circuit.record_failure()
        return {"ok": False, "error": f"HTTP {r.status_code}"}
    except Exception as e:
        _dm_circuit.record_failure()
        return {"ok": False, "error": f"设备管理连接失败: {str(e)}"}


async def _dm_post(path: str, data: dict = None, timeout: int = 35) -> dict:
    if _dm_circuit.is_open:
        return _dm_circuit_open_response()
    try:
        client = _get_http()
        r = await client.post(f"{DEVICE_MANAGER_URL}{path}", json=data or {}, timeout=timeout)
        if r.status_code == 200:
            _dm_circuit.record_success()
            return r.json()
        elif r.status_code >= 500:
            _dm_circuit.record_failure()
        return {"ok": False, "error": f"HTTP {r.status_code}"}
    except Exception as e:
        _dm_circuit.record_failure()
        return {"ok": False, "error": f"设备管理连接失败: {str(e)}"}


# ═══════════════════════════════════════════════
# MCP Tools — unchanged from deeptutor_mcp
# ═══════════════════════════════════════════════

@mcp.tool()
async def get_scene() -> str:
    allowed = _SCENE_TOOLS.get(_scene, set()) if _scene != "full" else set(_TOOL_DESCRIPTIONS.keys())
    result = {
        "scene": _scene,
        "available_tools": sorted(allowed),
        "total_tools": len(_TOOL_DESCRIPTIONS),
        "slow_queue_pending": _slow_pending,
    }
    if _slow_pending >= _SLOW_BUSY_THRESHOLD:
        result["busy_message"] = (
            "主人，刚才拍的图片我正在一张张仔细看，可能需要几分钟，"
            "但我现在可以陪你先复习之前的错题。"
        )
    return json.dumps(result, ensure_ascii=False)


@mcp.tool()
async def detect_scene(user_message: str) -> str:
    result = _detect_scene_internal(user_message)
    return json.dumps(result, ensure_ascii=False)


@mcp.tool()
async def set_scene(mode: str, user_message: str = "") -> str:
    global _scene
    if mode == "auto":
        result = _detect_scene_internal(user_message)
        _scene = result["scene"]
    elif mode in ("practice", "reading", "full"):
        _scene = mode
    else:
        return json.dumps({"ok": False, "error": f"Unknown mode: {mode}"}, ensure_ascii=False)
    return json.dumps({"ok": True, "scene": _scene,
                       "tools": sorted(_SCENE_TOOLS.get(_scene, set())) if _scene != "full" else "all"},
                      ensure_ascii=False)


@mcp.tool()
async def get_circuit_status(agent_role: str = "") -> str:
    perm = _check_device_permission(agent_role)
    if perm:
        return json.dumps({"ok": False, "error": perm}, ensure_ascii=False)
    dt_s = _dt_circuit.status()
    plat_s = _platform_circuit.status()
    dm_s = _dm_circuit.status()
    return json.dumps({"deeptutor": dt_s, "platform": plat_s, "device_manager": dm_s}, ensure_ascii=False)


@mcp.tool()
async def reset_circuit(target: str = "deeptutor", agent_role: str = "") -> str:
    perm = _check_device_permission(agent_role)
    if perm:
        return json.dumps({"ok": False, "error": perm}, ensure_ascii=False)
    results = []
    for name, cb in [("deeptutor", _dt_circuit), ("platform", _platform_circuit), ("device_manager", _dm_circuit)]:
        if target in (name, "all"):
            prev = cb._state
            cb._failure_count = 0
            cb._state = "closed"
            results.append({"circuit": name, "previous_state": prev, "current_state": "closed"})
    return json.dumps({"ok": True, "action": "circuit_reset", "results": results}, ensure_ascii=False)


@mcp.tool()
async def kb_search(query: str, kb_name: str = "tutoring", top_k: int = 5) -> str:
    async def _run():
        chroma_data = []
        try:
            result = await _platform_get(
                f"/api/kb/search?query={query}&kb_name={kb_name}&top_k={top_k}"
            )
            data = result if isinstance(result, dict) else {}
            chroma_data = data.get("results", [])
            for item in chroma_data:
                item["_source"] = "chromadb"
        except Exception:
            logger.warning("kb_search: ChromaDB search failed", exc_info=True)

        dt_data = []
        try:
            files = await _dt_get(f"/api/v1/knowledge/{kb_name}/files")
            if isinstance(files, list):
                import re
                q_lower = query.lower()
                for f in files:
                    fname = (f.get("filename") or f.get("name", "")).lower()
                    if q_lower in fname or any(kw in fname for kw in q_lower.split()):
                        dt_data.append({
                            "_source": "llamaindex",
                            "filename": f.get("filename") or f.get("name", ""),
                            "score": 0.9,
                            "text": f"文件: {f.get('filename') or f.get('name', '')}",
                        })
        except Exception:
            pass

        seen = set()
        merged = []
        for item in chroma_data + dt_data:
            key = item.get("filename") or item.get("text", "")[:100]
            if key not in seen:
                seen.add(key)
                merged.append(item)

        merged = merged[:top_k]
        if merged:
            return json.dumps({"ok": True, "results": merged, "total": len(merged)}, ensure_ascii=False)

        return json.dumps({
            "ok": True, "results": [], "total": 0,
            "message": "知识库中暂无相关内容, 请先上传学习资料",
        }, ensure_ascii=False)

    return await _guarded_call("kb_search", _run())


@mcp.tool()
async def kb_list() -> str:
    async def _run():
        result = await _dt_get("/api/v1/knowledge/")
        return json.dumps(result, ensure_ascii=False)
    return await _guarded_call("kb_list", _run())


@mcp.tool()
async def kb_upload_text(kb_name: str, content: str, filename: str = "",
                         learner_id: str = "default") -> str:
    perm = _check_tool_permission("kb_upload_text", learner_id)
    if perm:
        return json.dumps({"ok": False, "error": perm}, ensure_ascii=False)
    async def _run():
        result = await _platform_post("/api/ingest/text", {
            "kb_name": kb_name, "content": content, "filename": filename,
            "source": "mcp", "learner_id": learner_id,
        })
        return json.dumps(result, ensure_ascii=False)
    return await _guarded_call("kb_upload_text", _run())


@mcp.tool()
async def kb_upload_file(kb_name: str, file_path: str, learner_id: str = "default") -> str:
    perm = _check_tool_permission("kb_upload_file", learner_id)
    if perm:
        return json.dumps({"ok": False, "error": perm}, ensure_ascii=False)
    async def _run():
        actual_path = file_path
        if not os.path.exists(actual_path):
            # v0.14.0 hermes-agent: /opt/data/cache/...
            # 平台容器共享卷: /data/hermes/cache/...
            translated = file_path
            for prefix, replacement in (
                ("/opt/data/child", "/data/hermes_child"),
                ("/opt/data", "/data/hermes"),
                ("/root/.hermes", "/data/hermes"),
            ):
                if file_path.startswith(prefix + "/") or file_path == prefix:
                    translated = replacement + file_path[len(prefix):]
                    break
            if translated != file_path and os.path.exists(translated):
                logger.warning("kb_upload_file path translated: %s -> %s", file_path, translated)
                actual_path = translated
            else:
                alt = os.path.join("/data/uploads", os.path.basename(file_path))
                if os.path.exists(alt):
                    logger.warning("kb_upload_file path fallback: %s -> %s", file_path, alt)
                    actual_path = alt
        if not os.path.exists(actual_path):
            return json.dumps({"ok": False, "error": f"File not found: {file_path}"}, ensure_ascii=False)
        filename = os.path.basename(actual_path)
        headers = {}
        tool_name = _current_tool.get("")
        if tool_name:
            headers["X-Tool-Name"] = tool_name
        data = await _platform_upload(
            "/api/ingest/file",
            {"kb_name": kb_name, "learner_id": learner_id},
            actual_path, filename, headers,
        )
        return json.dumps(data, ensure_ascii=False)
    return await _guarded_call("kb_upload_file", _run())


@mcp.tool()
async def session_read(learner_id: str = "", limit: int = 10) -> str:
    perm = _check_tool_permission("session_read", learner_id)
    if perm:
        return json.dumps({"ok": False, "error": perm}, ensure_ascii=False)
    async def _run():
        results = []
        try:
            sessions = await _dt_get(f"/api/v1/chat/sessions?limit={limit}")
            if isinstance(sessions, list):
                for s in sessions:
                    entry = {
                        "session_id": s.get("id", ""),
                        "title": s.get("title", s.get("topic", "")),
                        "created_at": s.get("created_at", s.get("timestamp", "")),
                        "message_count": s.get("message_count", 0),
                        "source": "deeptutor_web",
                    }
                    if learner_id:
                        entry["learner_id"] = learner_id
                    results.append(entry)
        except Exception:
            logger.debug("session_read: DeepTutor chat sessions not available")

        if not results:
            try:
                solve_sessions = await _dt_get(f"/api/v1/solve/sessions?limit={limit}")
                if isinstance(solve_sessions, list):
                    for s in solve_sessions:
                        results.append({
                            "session_id": s.get("id", ""),
                            "title": s.get("title", s.get("problem", "")),
                            "created_at": s.get("created_at", s.get("timestamp", "")),
                            "source": "deeptutor_solve",
                        })
            except Exception:
                pass

        if results:
            return json.dumps({"ok": True, "sessions": results[:limit], "total": len(results)}, ensure_ascii=False)
        return json.dumps({"ok": True, "sessions": [], "total": 0, "message": "暂无学习会话记录"})

    return await _guarded_call("session_read", _run())


@mcp.tool()
async def view_source(trace_id: str) -> str:
    """查看原始资料 — 根据 trace_id 检索归档的原始文件.

    场景三 (docs/business_scenarios.md 第87行):
      用户要求查看原始资料时, HA 调用此工具获取文件内容和 source_url.
    """
    sources_dir = SOURCES_DIR
    if not os.path.isdir(sources_dir):
        return json.dumps({"ok": False, "error": "归档目录不存在"}, ensure_ascii=False)

    prefix = f"{trace_id}_"
    try:
        matches = [f for f in os.listdir(sources_dir) if f.startswith(prefix)]
    except OSError as e:
        return json.dumps({"ok": False, "error": f"读取归档目录失败: {e}"}, ensure_ascii=False)

    if not matches:
        return json.dumps({"ok": False, "error": f"未找到 trace_id={trace_id} 的归档文件"}, ensure_ascii=False)

    from datetime import datetime
    import base64

    results = []
    for filename in sorted(matches):
        filepath = os.path.join(sources_dir, filename)
        try:
            st = os.stat(filepath)
            ext = os.path.splitext(filename)[1].lower()
            is_image = ext in (".jpg", ".jpeg", ".png", ".webp", ".bmp")
            is_text = ext in (".txt", ".md", ".html", ".htm", ".json", ".csv", ".xml", ".yaml", ".yml")

            info: dict = {
                "filename": filename,
                "size": st.st_size,
                "size_mb": round(st.st_size / 1024 / 1024, 2),
                "mtime": datetime.fromtimestamp(st.st_mtime).isoformat(),
                "type": "image" if is_image else "text" if is_text else "document",
                "source_url": f"/sources/{filename}",
            }

            if is_image:
                with open(filepath, "rb") as f:
                    info["content_base64"] = base64.b64encode(f.read()).decode()
                    info["content_type"] = f"image/{ext.lstrip('.')}"
            elif is_text:
                with open(filepath, "r", encoding="utf-8", errors="replace") as f:
                    info["content_text"] = f.read()

            results.append(info)
        except OSError as e:
            results.append({"filename": filename, "error": str(e)})

    return json.dumps({"ok": True, "trace_id": trace_id, "files": results}, ensure_ascii=False)


@mcp.tool()
async def quiz_review(learner_id: str, kp_id: str = "", limit: int = 10) -> str:
    perm = _check_tool_permission("quiz_review", learner_id)
    if perm:
        return json.dumps({"ok": False, "error": perm}, ensure_ascii=False)
    async def _run():
        url = f"/api/mastery/{learner_id}/wrong?limit={limit}"
        if kp_id:
            url += f"&kp_id={kp_id}"
        result = await _platform_get(url)
        return json.dumps(result, ensure_ascii=False)
    return await _guarded_call("quiz_review", _run())


@mcp.tool()
async def generate_practice(learner_id: str, kp_id: str = "", count: int = 3) -> str:
    """根据错题生成针对性练习题，帮助学生巩固薄弱知识点。

    调用时机: quiz_review 返回错题后调用此工具，基于错题知识点生成类似题目加强训练。

    参数:
    - learner_id: 学习者标识
    - kp_id: 知识点ID (如 "math/basic_arithmetic")，为空时覆盖所有薄弱点
    - count: 生成题目数量 (1-10，默认3)
    """
    perm = _check_tool_permission("generate_practice", learner_id)
    if perm:
        return json.dumps({"ok": False, "error": perm}, ensure_ascii=False)
    async def _run():
        if _platform_circuit.is_open:
            return json.dumps(_platform_circuit_open_response(), ensure_ascii=False)
        result = await _platform_post("/api/practice/generate", {
            "learner_id": learner_id, "kp_id": kp_id, "count": count,
        })
        return json.dumps(result, ensure_ascii=False)
    return await _guarded_call("generate_practice", _run())


@mcp.tool()
async def generate_exam_paper(learner_id: str, kp_id: str = "", count: int = 10) -> str:
    """根据学情自动生成完整强化训练试卷，覆盖全部薄弱知识点。

    生成包含选择题、填空题、解答题的完整试卷，可注入教学流程。
    自动分析学习者的错题记录和薄弱点，针对性出题。

    参数:
    - learner_id: 学习者标识
    - kp_id: 可选，限定到特定知识点
    - count: 题目总数 (5-30，默认10)
    """
    perm = _check_tool_permission("generate_exam_paper", learner_id)
    if perm:
        return json.dumps({"ok": False, "error": perm}, ensure_ascii=False)
    async def _run():
        if _platform_circuit.is_open:
            return json.dumps(_platform_circuit_open_response(), ensure_ascii=False)
        result = await _platform_post("/api/practice/exam", {
            "learner_id": learner_id, "kp_id": kp_id, "count": count,
        })
        return json.dumps(result, ensure_ascii=False)
    return await _guarded_call("generate_exam_paper", _run())


@mcp.tool()
async def deep_solve(learner_id: str, problem: str, detailed: bool = False) -> str:
    """深度解题 — 将题目全文发给解题引擎，返回解答。

    仅当用户明确请求"解题"、"怎么做"、"求解"时调用。
    禁止对系统自动提取的 OCR 试卷内容调用此工具 — 那种情况用 tutor_chat。

    参数:
    - learner_id: 学习者标识
    - problem: 题目全文
    - detailed: 是否返回更详细的解题步骤 (默认 false)
    """
    perm = _check_tool_permission("deep_solve", learner_id)
    if perm:
        return json.dumps({"ok": False, "error": perm}, ensure_ascii=False)
    async def _run():
        if _platform_circuit.is_open:
            return json.dumps(_platform_circuit_open_response(), ensure_ascii=False)
        result = await _platform_post("/api/solve", {
            "learner_id": learner_id, "question": problem, "detailed": detailed,
        })
        return json.dumps(result, ensure_ascii=False)
    return await _guarded_call("deep_solve", _run())


@mcp.tool()
async def vision_solve(learner_id: str, file_path: str, question: str = "") -> str:
    """拍照解题 — 将题目图片发给解题引擎，返回图文解答。

    当学生发来题目图片并问"这题怎么做"时调用。
    注意：必须先调用 process_file 进行 OCR，如果未返回 tutor_content 再调用此工具。

    参数:
    - learner_id: 学习者标识
    - file_path: 图片文件路径 (process_file 中使用的原图路径)
    - question: 可选的问题描述
    """
    perm = _check_tool_permission("vision_solve", learner_id)
    if perm:
        return json.dumps({"ok": False, "error": perm}, ensure_ascii=False)
    async def _run():
        if _platform_circuit.is_open:
            return json.dumps(_platform_circuit_open_response(), ensure_ascii=False)
        result = await _platform_post("/api/vision/solve", {
            "learner_id": learner_id, "image_data": file_path, "question": question,
        })
        return json.dumps(result, ensure_ascii=False)
    return await _guarded_call("vision_solve", _run())


@mcp.tool()
async def update_memory(learner_id: str, file: str, content: str) -> str:
    """更新学习者画像 — 保存学习总结或个人档案信息。

    当了解到学生的重要信息时调用：
    - 学习习惯、偏好、薄弱点 → file="summary"
    - 个人特点、称呼、学习目标 → file="profile"

    参数:
    - learner_id: 学习者标识
    - file: 文件类型 ("summary" 或 "profile")
    - content: 要保存的内容（markdown 格式）
    """
    if file not in ("summary", "profile"):
        return json.dumps({"ok": False, "error": "file 参数必须是 summary 或 profile"}, ensure_ascii=False)
    perm = _check_tool_permission("update_memory", learner_id)
    if perm:
        return json.dumps({"ok": False, "error": perm}, ensure_ascii=False)
    async def _run():
        if _dt_circuit.is_open:
            return _circuit_open_response("deeptutor")
        result = await _dt_put("/api/v1/memory", {"file": file, "content": content})
        return json.dumps(result, ensure_ascii=False)
    return await _guarded_call("update_memory", _run())


@mcp.tool()
async def record_quiz_result(session_id: str, results: str, learner_id: str = "") -> str:
    """记录答题结果到学习会话。

    当学生完成一组练习题后调用此工具记录答题结果。
    答题结果会被写入 DeepTutor 的学习记录，用于后续掌握度分析。

    参数:
    - session_id: 学习会话 ID（可从 session_read 获取）
    - results: JSON 字符串，格式为 [{"question": "...", "user_answer": "...",
               "correct_answer": "...", "is_correct": true/false, ...}]
    - learner_id: 学习者标识（可选）
    """
    perm = _check_tool_permission("record_quiz_result", learner_id)
    if perm:
        return json.dumps({"ok": False, "error": perm}, ensure_ascii=False)
    async def _run():
        if _dt_circuit.is_open:
            return _circuit_open_response("deeptutor")
        try:
            import json as _json
            answers = _json.loads(results) if isinstance(results, str) else results
        except Exception as e:
            return json.dumps({"ok": False, "error": f"results 格式错误: {e}"}, ensure_ascii=False)
        if not isinstance(answers, list):
            return json.dumps({"ok": False, "error": "results 必须是数组"}, ensure_ascii=False)
        result = await _dt_post(f"/api/v1/sessions/{session_id}/quiz-results",
                                {"answers": answers})
        return json.dumps(result, ensure_ascii=False)
    return await _guarded_call("record_quiz_result", _run())


@mcp.tool()
async def question_notebook_list(learner_id: str = "", limit: int = 20,
                                  is_correct: bool | None = None) -> str:
    """查询学习历史中的所有答题记录（含纠错本）。

    查看学生历史的答题记录，可按是否正确过滤。
    区别于 quiz_review（只返回错题掌握度分析），此工具返回完整答题历史。

    参数:
    - learner_id: 学习者标识（可选）
    - limit: 返回条数上限 (1-200，默认 20)
    - is_correct: 按是否正确过滤（true=只返回正确，false=只返回错误，null=全部）
    """
    perm = _check_tool_permission("question_notebook_list", learner_id)
    if perm:
        return json.dumps({"ok": False, "error": perm}, ensure_ascii=False)
    async def _run():
        if _dt_circuit.is_open:
            return _circuit_open_response("deeptutor")
        params = f"?limit={limit}"
        if is_correct is not None:
            params += f"&is_correct={'true' if is_correct else 'false'}"
        result = await _dt_get(f"/api/v1/question-notebook/entries{params}")
        return json.dumps(result, ensure_ascii=False)
    return await _guarded_call("question_notebook_list", _run())


@mcp.tool()
async def book_create(topic: str, kb_name: str = "") -> str:
    async def _run():
        data = {"topic": topic}
        if kb_name:
            data["kb_name"] = kb_name
        result = await _dt_post("/api/v1/book/books", data)
        return json.dumps(result, ensure_ascii=False)
    return await _guarded_call("book_create", _run())


@mcp.tool()
async def book_read(book_id: str, page_id: str = "") -> str:
    async def _run():
        if page_id:
            result = await _dt_get(f"/api/v1/book/books/{book_id}/pages/{page_id}")
        else:
            result = await _dt_get(f"/api/v1/book/books/{book_id}")
        return json.dumps(result, ensure_ascii=False)
    return await _guarded_call("book_read", _run())


@mcp.tool()
async def book_list() -> str:
    async def _run():
        result = await _dt_get("/api/v1/book/books")
        return json.dumps(result, ensure_ascii=False)
    return await _guarded_call("book_list", _run())


@mcp.tool()
async def get_memory(learner_id: str) -> str:
    perm = _check_tool_permission("get_memory", learner_id)
    if perm:
        return json.dumps({"ok": False, "error": perm}, ensure_ascii=False)
    async def _run():
        result = await _dt_get(f"/api/v1/memory/?learner_id={learner_id}")
        # Signal new learner when global memory is empty (no summary/profile data)
        result["is_new_learner"] = not (result.get("summary") or result.get("profile"))
        return json.dumps(result, ensure_ascii=False)
    return await _guarded_call("get_memory", _run())


@mcp.tool()
async def process_file(file_path: str, kb_name: str = "tutoring", learner_id: str = "default") -> str:
    perm = _check_tool_permission("process_file", learner_id)
    if perm:
        return json.dumps({"ok": False, "error": perm}, ensure_ascii=False)
    async def _run():
        actual_path = file_path
        if not os.path.exists(actual_path):
            # v0.14.0 hermes-agent: /opt/data/cache/... (新默认 HERMES_HOME)
            # v0.13.x hermes-agent: /root/.hermes/cache/... (旧默认)
            # 平台容器: /data/hermes/cache/... (共享卷挂载, /data/hermes)
            translated = file_path
            for prefix, replacement in (
                ("/opt/data/child", "/data/hermes_child"),
                ("/opt/data", "/data/hermes"),
                ("/root/.hermes", "/data/hermes"),
            ):
                if file_path.startswith(prefix + "/") or file_path == prefix:
                    translated = replacement + file_path[len(prefix):]
                    break
            if translated != file_path and os.path.exists(translated):
                logger.warning("process_file path translated: %s -> %s", file_path, translated)
                actual_path = translated
            else:
                alt = os.path.join("/data/uploads", os.path.basename(file_path))
                if os.path.exists(alt):
                    logger.warning("process_file path fallback: %s -> %s", file_path, alt)
                    actual_path = alt

        if os.path.exists(actual_path) and os.path.isfile(actual_path):
            filename = os.path.basename(actual_path)
            headers = {}
            tool_name = _current_tool.get("")
            if tool_name:
                headers["X-Tool-Name"] = tool_name
            data = await _platform_upload(
                "/api/process/file",
                {"kb_name": kb_name, "learner_id": learner_id},
                actual_path, filename, headers,
            )
            if data.get("tutor_content"):
                return json.dumps({"tutor_content": data["tutor_content"]}, ensure_ascii=False)
            return json.dumps(data, ensure_ascii=False)
        else:
            result = await _platform_post("/api/process/file", {
                "file_path": file_path,
                "kb_name": kb_name,
                "learner_id": learner_id,
            })
            if result.get("tutor_content"):
                return json.dumps({"tutor_content": result["tutor_content"]}, ensure_ascii=False)
            return json.dumps(result, ensure_ascii=False)

    return await _guarded_call("process_file", _run())


@mcp.tool()
async def notebook_create(title: str, kb_name: str = "") -> str:
    async def _run():
        data = {"title": title}
        if kb_name:
            data["kb_name"] = kb_name
        result = await _dt_post("/api/v1/notebook/notebooks", data)
        return json.dumps(result, ensure_ascii=False)
    return await _guarded_call("notebook_create", _run())


@mcp.tool()
async def notebook_read(notebook_id: str, cell_id: str = "") -> str:
    async def _run():
        if cell_id:
            result = await _dt_get(f"/api/v1/notebook/notebooks/{notebook_id}/cells/{cell_id}")
        else:
            result = await _dt_get(f"/api/v1/notebook/notebooks/{notebook_id}")
        return json.dumps(result, ensure_ascii=False)
    return await _guarded_call("notebook_read", _run())


@mcp.tool()
async def notebook_add_cell(notebook_id: str, cell_type: str = "markdown",
                             content: str = "", language: str = "python") -> str:
    async def _run():
        data = {"cell_type": cell_type, "content": content, "language": language}
        result = await _dt_post(f"/api/v1/notebook/notebooks/{notebook_id}/cells", data)
        return json.dumps(result, ensure_ascii=False)
    return await _guarded_call("notebook_add_cell", _run())


@mcp.tool()
async def notebook_execute(notebook_id: str, cell_id: str) -> str:
    async def _run():
        result = await _dt_post(f"/api/v1/notebook/notebooks/{notebook_id}/cells/{cell_id}/execute", {})
        return json.dumps(result, ensure_ascii=False)
    return await _guarded_call("notebook_execute", _run())


@mcp.tool()
async def notebook_list() -> str:
    async def _run():
        result = await _dt_get("/api/v1/notebook/notebooks")
        return json.dumps(result, ensure_ascii=False)
    return await _guarded_call("notebook_list", _run())


@mcp.tool()
async def tutor_chat(message: str, learner_id: str = "default", context: str = "", mode: str = "guide") -> str:
    """将教学消息转发给 DeepTutor 教学引擎。学生问学习问题、做题、对答案时必须调用此工具。

    两种教学模式:
    - guide (默认): 苏格拉底式引导教学 — 逐题引导、不直接给答案，适合孩子做题场景
    - explain: 直接讲解模式 — 给答案 + 分步解析 + 知识点说明，适合家长先学再教

    参数说明:
    - message: 学生的问题/回答内容
    - context: 首轮调用时传入作业/试题全文，后续轮次不传 (教学引擎自动继承上下文)
    - learner_id: 学习者标识
    - mode: 教学模式 ("guide" 或 "explain")，默认 guide

    教学引擎自动处理：
    - 引导式教学（逐题引导、不直接给答案）/ 直接讲解（家长自学）
    - 难度适配（根据掌握度调整）
    - 错题关注（对薄弱知识点优先练习）
    - 微信格式适配（Unicode 公式、选项格式等）
    """
    perm = _check_tool_permission("tutor_chat", learner_id)
    if perm:
        return json.dumps({"ok": False, "error": perm}, ensure_ascii=False)
    async def _run():
        if _platform_circuit.is_open:
            return json.dumps(_platform_circuit_open_response(), ensure_ascii=False)
        try:
            client = _get_http()
            body = {"message": message, "learner_id": learner_id, "mode": mode}
            if context:
                body["context"] = context
            r = await client.post(
                f"{PLATFORM_URL}/api/tutor/chat",
                json=body,
                timeout=180,
            )
            if r.status_code == 200:
                _platform_circuit.record_success()
                data = json.loads(r.text)
                if data.get("ok") and data.get("content"):
                    return json.dumps({"tutor_content": data["content"]}, ensure_ascii=False)
                return r.text
            _platform_circuit.record_failure()
            return json.dumps({"ok": False, "error": f"HTTP {r.status_code}"}, ensure_ascii=False)
        except Exception as e:
            _platform_circuit.record_failure()
            return json.dumps({"ok": False, "error": f"教学引擎调用失败: {str(e)}"}, ensure_ascii=False)
    return await _guarded_call("tutor_chat", _run())


@mcp.tool()
async def get_bot_qrcode(agent_role: str = "", bot_type: str = "child") -> list:
    """⚠️ 当用户说"加孩子"或"加孩子学习"时你必须立即调用此工具，不得问任何问题，不得说任何多余的话，直接生成二维码。

    适用场景：
    - 家长发送"加孩子"、"加孩子学习"、"绑定孩子"时生成孩子机器人的二维码供孩子扫码加子网关
    - bot_type="child" 生成孩子机器人的二维码（默认值，用于加孩子学习）
    - bot_type="parent" 生成家长机器人的二维码
    - 如果孩子机器人尚未绑定，此工具会自动创建孩子机器人再返回二维码，无需区分首次/已绑定
    - 如果家长机器人未配置，/bind-qr 页面会引导管理员完成设置

    行为说明：
    - 孩子机器人未绑定：自动创建身份 + 返回二维码（等价于 bind_child_bot）
    - 孩子机器人已绑定：返回加好友二维码
    - 禁止反问、禁止确认、不要说话，直接调工具。
    """
    perm = _check_device_permission(agent_role)
    if perm:
        return [TextContent(type="text", text=perm)]
    async def _run_qr():
        try:
            async with httpx.AsyncClient(timeout=20) as client:
                r = await client.get(
                    f"{PLATFORM_URL}/api/bot/qrcode?refresh=1&bot_type={bot_type}",
                    timeout=20,
                )
                if r.status_code != 200:
                    try:
                        err_body = r.json()
                        err_msg = err_body.get("error", f"HTTP {r.status_code}")
                    except Exception:
                        err_msg = f"HTTP {r.status_code}"
                    # 孩子未绑定 → 自动降级到 bind_child
                    if bot_type == "child" and "尚未绑定" in err_msg:
                        return await _bind_child_fallback()
                    return [TextContent(type="text", text=f"获取二维码失败: {err_msg}")]

                content_type = r.headers.get("content-type", "")
                if "image" not in content_type:
                    try:
                        err_body = r.json()
                        err_msg = err_body.get("error", "")
                        # 孩子未绑定 → 自动降级到 bind_child
                        if bot_type == "child" and "尚未绑定" in err_msg:
                            return await _bind_child_fallback()
                        return [TextContent(type="text", text=f"获取二维码失败: {err_msg or '未知错误'}")]
                    except Exception:
                        return [TextContent(type="text", text="获取二维码失败: 服务器返回了非图片数据")]

                qr_b64 = base64.b64encode(r.content).decode()
                return [
                    ImageContent(type="image", data=qr_b64, mimeType="image/png"),
                    TextContent(type="text", text="二维码已生成"),
                ]
        except Exception as e:
            return [TextContent(type="text", text=f"获取二维码失败: {str(e)}")]

    async def _bind_child_fallback():
        """自动降级调 bind_child_bot 创建孩子机器人并返回二维码."""
        try:
            async with httpx.AsyncClient(timeout=120) as client:
                r = await client.post(
                    f"{PLATFORM_URL}/api/bot/bind_child?force=0",
                    timeout=120,
                )
                if r.status_code != 200:
                    try:
                        err_body = r.json()
                        return [TextContent(type="text", text=f"绑定孩子机器人失败: {err_body.get('error', f'HTTP {r.status_code}')}")]
                    except Exception:
                        return [TextContent(type="text", text=f"绑定孩子机器人失败: HTTP {r.status_code}")]
                if "image" in r.headers.get("content-type", ""):
                    qr_b64 = base64.b64encode(r.content).decode()
                    return [
                        ImageContent(type="image", data=qr_b64, mimeType="image/png"),
                        TextContent(type="text", text="孩子机器人已创建，请保存二维码并转发给孩子扫码"),
                    ]
                try:
                    body = r.json()
                    return [TextContent(type="text", text=f"绑定孩子机器人失败: {body.get('error', '未知错误')}")]
                except Exception:
                    return [TextContent(type="text", text="绑定孩子机器人失败: 服务器返回了非图片数据")]
        except Exception as e:
            return [TextContent(type="text", text=f"绑定孩子机器人失败: {str(e)}")]

    return await _guarded_call("get_bot_qrcode", _run_qr())


@mcp.tool()
async def bind_child_bot(agent_role: str = "", force: bool = False) -> list:
    """⚠️ 当用户说"加孩子"或"加孩子学习"时，如果孩子机器人尚未绑定，调用此工具创建孩子专属机器人并生成二维码。

    适用场景：
    - 家长说"加孩子"、"加孩子学习"、"绑定孩子"，且孩子还没有专属机器人时
    - 此工具会在设备上创建新的孩子机器人身份，返回二维码供孩子扫码绑定
    - force=true 时即使已经绑定也会创建新身份（用于重新绑定/换绑场景）
    - 如果孩子机器人已经绑定且未设 force，会告知绑定状态并引导使用 get_bot_qrcode

    注意：正常绑定后请使用 get_bot_qrcode 生成子网关二维码。只有需要重新绑定时才用 force=true。
    """
    perm = _check_device_permission(agent_role)
    if perm:
        return [TextContent(type="text", text=perm)]

    async def _run():
        try:
            async with httpx.AsyncClient(timeout=120) as client:
                force_param = "1" if force else "0"
                r = await client.post(
                    f"{PLATFORM_URL}/api/bot/bind_child?force={force_param}",
                    timeout=120,
                )
                if r.status_code != 200:
                    try:
                        err_body = r.json()
                        err_msg = err_body.get("error", f"HTTP {r.status_code}")
                    except Exception:
                        err_msg = f"HTTP {r.status_code}"
                    return [TextContent(type="text", text=f"绑定孩子机器人失败: {err_msg}")]

                content_type = r.headers.get("content-type", "")
                if "image" in content_type:
                    qr_b64 = base64.b64encode(r.content).decode()
                    return [
                        ImageContent(type="image", data=qr_b64, mimeType="image/png"),
                        TextContent(type="text", text="二维码已生成，请保存并转发给孩子扫码"),
                    ]

                try:
                    body = r.json()
                    if body.get("ok") and body.get("text_only"):
                        return [TextContent(type="text", text=f"请使用以下链接生成二维码:\n{body.get('url', '')}")]
                    return [TextContent(type="text", text=f"绑定孩子机器人失败: {body.get('error', '未知错误')}")]
                except Exception:
                    return [TextContent(type="text", text=f"绑定孩子机器人失败: 服务器返回了非图片数据")]
        except Exception as e:
            return [TextContent(type="text", text=f"绑定孩子机器人失败: {str(e)}")]

    return await _guarded_call("bind_child_bot", _run())


@mcp.tool()
async def cowriter_edit(text: str, instruction: str, action: str = "rewrite",
                        source: str = "", kb_name: str = "") -> str:
    async def _run():
        body = {"text": text, "instruction": instruction, "action": action}
        if source in ("rag", "web"):
            body["source"] = source
            if kb_name:
                body["kb_name"] = kb_name
        result = await _dt_post("/api/v1/co_writer/edit", body)
        return json.dumps(result, ensure_ascii=False)
    return await _guarded_call("cowriter_edit", _run())


@mcp.tool()
async def cowriter_documents(list_only: bool = True, title: str = "",
                             content: str = "") -> str:
    async def _run():
        if list_only:
            result = await _dt_get("/api/v1/co_writer/documents")
            return json.dumps(result, ensure_ascii=False)
        else:
            body = {}
            if title:
                body["title"] = title
            if content:
                body["content"] = content
            result = await _dt_post("/api/v1/co_writer/documents", body)
            return json.dumps(result, ensure_ascii=False)
    return await _guarded_call("cowriter_documents", _run())


@mcp.tool()
async def cowriter_history(operation_id: str = "") -> str:
    async def _run():
        if operation_id:
            result = await _dt_get(f"/api/v1/co_writer/history/{operation_id}")
        else:
            result = await _dt_get("/api/v1/co_writer/history")
        return json.dumps(result, ensure_ascii=False)
    return await _guarded_call("cowriter_history", _run())


@mcp.tool()
async def generate_report(learner_id: str, report_type: str = "daily") -> str:
    """生成学习报告 (日报/周报/月报). 用于定时报告推送.

    Args:
        learner_id: 学习者标识
        report_type: 报告类型 "daily" 日报 / "weekly" 周报 / "monthly" 月报
    """
    perm = _check_tool_permission("generate_report", learner_id)
    if perm:
        return json.dumps({"ok": False, "error": perm}, ensure_ascii=False)
    async def _run():
        client = _get_http()
        try:
            r = await client.post(
                f"{PLATFORM_URL}/api/report/generate",
                json={"learner_id": learner_id, "type": report_type},
                timeout=_TIMEOUT_PLATFORM_POST,
            )
            if r.status_code == 200:
                return r.text
            return json.dumps({"ok": False, "error": f"HTTP {r.status_code}"}, ensure_ascii=False)
        except Exception as e:
            return json.dumps({"ok": False, "error": str(e)}, ensure_ascii=False)
    return await _guarded_call("generate_report", _run())


@mcp.tool()
async def push_report(report_type: str = "daily") -> str:
    """生成并推送学习报告到微信 (日报/周报/月报). 用于定时报告推送.

    为所有学习者生成报告，通过 Hermes Agent 推送到微信。
    Args:
        report_type: 报告类型 "daily" 日报 / "weekly" 周报 / "monthly" 月报
    """
    perm = _check_tool_permission("push_report", "")
    if perm:
        return json.dumps({"ok": False, "error": perm}, ensure_ascii=False)
    async def _run():
        client = _get_http()
        try:
            r = await client.post(
                f"{PLATFORM_URL}/api/report/push",
                json={"type": report_type},
                timeout=_TIMEOUT_PLATFORM_POST,
            )
            if r.status_code == 200:
                data = r.json()
                return json.dumps(data, ensure_ascii=False)
            return json.dumps({"ok": False, "error": f"HTTP {r.status_code}"}, ensure_ascii=False)
        except Exception as e:
            return json.dumps({"ok": False, "error": str(e)}, ensure_ascii=False)
    return await _guarded_call("push_report", _run())


# Device tools

@mcp.tool()
async def device_status(agent_role: str = "") -> str:
    perm = _check_device_permission(agent_role)
    if perm:
        return json.dumps({"ok": False, "error": perm}, ensure_ascii=False)
    async def _run():
        result = await _dm_get("/api/device/status", timeout=_TIMEOUT_DM_FAST)
        return json.dumps(result, ensure_ascii=False)
    return await _guarded_call("device_status", _run())


@mcp.tool()
async def storage_info(agent_role: str = "") -> str:
    perm = _check_device_permission(agent_role)
    if perm:
        return json.dumps({"ok": False, "error": perm}, ensure_ascii=False)
    async def _run():
        result = await _dm_get("/api/device/storage", timeout=_TIMEOUT_DM_FAST)
        return json.dumps(result, ensure_ascii=False)
    return await _guarded_call("storage_info", _run())


@mcp.tool()
async def ssd_health(agent_role: str = "") -> str:
    perm = _check_device_permission(agent_role)
    if perm:
        return json.dumps({"ok": False, "error": perm}, ensure_ascii=False)
    async def _run():
        result = await _dm_get("/api/device/ssd", timeout=_TIMEOUT_DM_FAST)
        return json.dumps(result, ensure_ascii=False)
    return await _guarded_call("ssd_health", _run())


@mcp.tool()
async def device_temp(agent_role: str = "") -> str:
    perm = _check_device_permission(agent_role)
    if perm:
        return json.dumps({"ok": False, "error": perm}, ensure_ascii=False)
    async def _run():
        result = await _dm_get("/api/device/temp", timeout=_TIMEOUT_DM_FAST)
        return json.dumps(result, ensure_ascii=False)
    return await _guarded_call("device_temp", _run())


@mcp.tool()
async def device_alerts(agent_role: str = "") -> str:
    """查询设备当前告警 (温度/存储/SSD/服务异常). 返回活跃告警列表及严重程度."""
    perm = _check_device_permission(agent_role)
    if perm:
        return json.dumps({"ok": False, "error": perm}, ensure_ascii=False)
    async def _run():
        result = await _dm_get("/api/device/alerts", timeout=_TIMEOUT_DM_FAST)
        return json.dumps(result, ensure_ascii=False)
    return await _guarded_call("device_alerts", _run())


# ── 规则优先的设备管理入口 ──

_PARAMETERLESS_DEVICE_TOOLS: dict[str, tuple[str, str, int]] = {
    # tool_name: (http_method, api_path, timeout)
    "wifi_scan":      ("GET",  "/api/device/wifi/scan",    _TIMEOUT_DM_SCAN),
    "wifi_status":    ("GET",  "/api/device/wifi/status",  _TIMEOUT_DM_FAST),
    "device_status":  ("GET",  "/api/device/status",       _TIMEOUT_DM_FAST),
    "device_alerts":  ("GET",  "/api/device/alerts",       _TIMEOUT_DM_FAST),
    "storage_info":   ("GET",  "/api/device/storage",      _TIMEOUT_DM_FAST),
    "ssd_health":     ("GET",  "/api/device/ssd",          _TIMEOUT_DM_FAST),
    "device_temp":    ("GET",  "/api/device/temp",         _TIMEOUT_DM_FAST),
    "device_cleanup": ("POST", "/api/device/cleanup",      _TIMEOUT_DM_CLEANUP),
}

# 需要 LLM 提取参数的工具 (规则只负责分类, 不执行)
_PARAM_REQUIRED_DEVICE_TOOLS: set[str] = {
    "wifi_configure",  # 需 ssid + password
    "wifi_forget",     # 需 ssid
    "get_bot_qrcode",  # 需 bot_type + agent_role (规则识别后 LLM 提取参数)
}

_HIGH_CONFIDENCE_THRESHOLD = 0.5  # device intent 匹配即视为高置信


@mcp.tool()
async def device_command(text: str, agent_role: str = "") -> str:
    """设备管理统一入口：规则优先分类，未匹配时由 LLM 兜底。

    使用场景:
      用户发出设备管理相关请求时，优先调用此工具进行规则匹配。
      规则能处理的参数化工具(连接WiFi/忘记WiFi)返回 intent 信息，LLM 再调用对应具体工具；
      规则能处理的免参工具直接执行并返回结果；
      规则无法分类时返回 unclassified，由 LLM 自行判断。

    Args:
      text: 用户的原始消息文本
      agent_role: 调用者身份 ("parent" / "child1" / …)
    """
    perm = _check_device_permission(agent_role)
    if perm:
        return json.dumps({"ok": False, "error": perm}, ensure_ascii=False)

    try:
        from tutor_platform.tools.intent_rules import classify_device_intent, get_device_tool_name
    except ImportError:
        return json.dumps({"intent": "unclassified", "reason": "规则引擎不可用"}, ensure_ascii=False)

    classification = classify_device_intent(text)
    intent = classification.get("intent", "none")
    confidence = classification.get("confidence", 0.0) if classification.get("confidence") else 0.0
    description = classification.get("description", "")

    if intent == "none" or confidence < _HIGH_CONFIDENCE_THRESHOLD:
        return json.dumps({
            "intent": "unclassified",
            "confidence": confidence,
            "hint": "规则未匹配，由 LLM 自行判断",
        }, ensure_ascii=False)

    tool_name = get_device_tool_name(intent)

    if tool_name in _PARAM_REQUIRED_DEVICE_TOOLS:
        return json.dumps({
            "intent": intent,
            "tool": tool_name,
            "confidence": confidence,
            "description": description,
            "requires_params": True,
            "instruction": f"规则已识别为「{description}」，请从对话中提取参数后调用 {tool_name}",
        }, ensure_ascii=False)

    if tool_name in _PARAMETERLESS_DEVICE_TOOLS:
        method, path, timeout = _PARAMETERLESS_DEVICE_TOOLS[tool_name]
        try:
            if method == "GET":
                result = await _dm_get(path, timeout=timeout)
            else:
                result = await _dm_post(path, timeout=timeout)
            result["_classified"] = True
            result["_intent"] = intent
            return json.dumps(result, ensure_ascii=False)
        except Exception as e:
            return json.dumps({"ok": False, "error": str(e)}, ensure_ascii=False)

    return json.dumps({
        "intent": "unclassified",
        "confidence": confidence,
        "hint": f"已识别为 {intent} 但无对应工具实现，由 LLM 自行判断",
    }, ensure_ascii=False)


@mcp.tool()
async def wifi_scan(agent_role: str = "") -> str:
    perm = _check_device_permission(agent_role)
    if perm:
        return json.dumps({"ok": False, "error": perm}, ensure_ascii=False)
    async def _run():
        result = await _dm_get("/api/device/wifi/scan", timeout=_TIMEOUT_DM_SCAN)
        return json.dumps(result, ensure_ascii=False)
    return await _guarded_call("wifi_scan", _run())


@mcp.tool()
async def wifi_configure(ssid: str, password: str, agent_role: str = "") -> str:
    perm = _check_device_permission(agent_role)
    if perm:
        return json.dumps({"ok": False, "error": perm}, ensure_ascii=False)
    async def _run():
        result = await _dm_post(
            "/api/device/wifi/connect",
            data={"ssid": ssid, "password": password},
            timeout=_TIMEOUT_DM_CONFIGURE,
        )
        return json.dumps(result, ensure_ascii=False)
    return await _guarded_call("wifi_configure", _run())


@mcp.tool()
async def wifi_status(agent_role: str = "") -> str:
    perm = _check_device_permission(agent_role)
    if perm:
        return json.dumps({"ok": False, "error": perm}, ensure_ascii=False)
    async def _run():
        result = await _dm_get("/api/device/wifi/status", timeout=_TIMEOUT_DM_FAST)
        return json.dumps(result, ensure_ascii=False)
    return await _guarded_call("wifi_status", _run())


@mcp.tool()
async def wifi_forget(ssid: str, agent_role: str = "") -> str:
    """忘记已保存的 WiFi 网络 (删除 NetworkManager 连接配置). 仅家长可用."""
    perm = _check_device_permission(agent_role)
    if perm:
        return json.dumps({"ok": False, "error": perm}, ensure_ascii=False)
    async def _run():
        result = await _dm_post(
            "/api/device/wifi/forget",
            data={"ssid": ssid},
            timeout=_TIMEOUT_DM_CONFIGURE,
        )
        return json.dumps(result, ensure_ascii=False)
    return await _guarded_call("wifi_forget", _run())


@mcp.tool()
async def device_cleanup(agent_role: str = "") -> str:
    perm = _check_device_permission(agent_role)
    if perm:
        return json.dumps({"ok": False, "error": perm}, ensure_ascii=False)
    async def _run():
        result = await _dm_post("/api/device/cleanup", timeout=_TIMEOUT_DM_CLEANUP)
        return json.dumps(result, ensure_ascii=False)
    return await _guarded_call("device_cleanup", _run())


# ═══════════════════════════════════════════════
# Health check & status endpoints
# ═══════════════════════════════════════════════

_STATUS_CSS = """\
* { margin:0; padding:0; box-sizing:border-box; }
body { font-family:-apple-system,BlinkMacSystemFont,"Segoe UI",Roboto,sans-serif;
       background:#f5f5f5; display:flex; justify-content:center; align-items:center;
       min-height:100vh; }
.container { background:#fff; border-radius:16px; padding:40px 32px;
             margin:20px; max-width:420px; width:100%; text-align:center;
             box-shadow:0 2px 12px rgba(0,0,0,0.08); }
h1 { font-size:22px; font-weight:600; color:#1a1a1a; margin-bottom:8px; }
.hint { font-size:14px; color:#666; margin-bottom:20px; }
.qr-wrapper { background:#fff; border:2px solid #e8e8e8; border-radius:12px;
              padding:16px; display:inline-block; margin-bottom:20px; }
.qr { width:260px; height:260px; display:block; }
.footer { font-size:12px; color:#999; line-height:1.5; }
.steps { text-align:left; background:#f8f9fa; border-radius:8px; padding:16px;
         margin-bottom:20px; font-size:13px; color:#444; line-height:1.8; }
.steps strong { color:#1a1a1a; }
.status-grid { text-align:left; margin-bottom:20px; font-size:13px; }
.status-row { display:flex; justify-content:space-between; padding:8px 12px;
              border-radius:6px; margin-bottom:4px; }
.status-row .label { color:#666; }
.status-row .value { font-weight:600; }
.status-ok { background:#e8f5e9; }
.status-ok .value { color:#2e7d32; }
.status-warn { background:#fff3e0; }
.status-warn .value { color:#e65100; }
.status-err { background:#ffebee; }
.status-err .value { color:#c62828; }
.status-info { background:#e3f2fd; }
.status-info .value { color:#1565c0; }
.refresh-btn { display:inline-block; margin-top:16px; padding:10px 28px;
               background:#1976d2; color:#fff; border:none; border-radius:8px;
               font-size:14px; cursor:pointer; text-decoration:none; }
.refresh-btn:hover { background:#1565c0; }
.retry-hint { font-size:13px; color:#999; margin-top:8px; }
"""



_BOUND_STATUS = """\
<div class="status-row status-ok">
  <span class="label">系统状态</span>
  <span class="value">✓ 运行中</span>
</div>
<div class="status-row status-ok">
  <span class="label">微信账号</span>
  <span class="value">✓ 已绑定</span>
</div>"""

_BOUND_HINT = "设备已就绪，打开微信即可与 AI 教助对话"

_BOUND_INSTRUCTION = """<div class="steps">
  <strong>如何使用</strong><br>
  ① 打开微信，找到 "AI 教学助手" 联系人<br>
  ② 发送作业照片即可获得 AI 辅导<br>
  ③ 发送"加孩子学习"可为孩子生成专属学习账号<br><br>
  <span style="color:#999;">如需更换绑定账号，请使用下方功能重置。</span>
</div>"""

_REBIND_SECTION = """<div class="rebind-warn">
  <a href="#" onclick="return clearCreds()" class="rebind-btn">⚠ 清除绑定信息并重新设置</a>
  <div id="rebind-msg" style="display:none;margin-top:8px;font-size:13px;text-align:center;"></div>
</div>
<script>
function clearCreds() {
  if (!confirm('⚠ 确定清除微信账号\\n\\n清除后当前账号将无法使用，设备需要重新设置。\\n\\n操作步骤：\\n1. 点击「确定」清除当前账号\\n2. 断开设备电源\\n3. 重新插电启动\\n4. 设备启动后会自动显示二维码\\n5. 用微信扫描二维码重新绑定')) return false;
  document.querySelector('.rebind-btn').style.display = 'none';
  var m = document.getElementById('rebind-msg');
  m.style.display = 'block';
  m.innerHTML = '<span style="color:#666;">⟳ 正在清除...</span>';
  fetch('/api/bot/rebind_parent', { method: 'POST' })
    .then(function(r) { return r.json(); })
    .then(function(d) {
      if (d.ok) {
        m.innerHTML = '<span style="color:#4caf50;">✓ 已清除，请关闭此页面，断开设备电源后重新上电。</span>';
      } else {
        m.innerHTML = '<span style="color:#f44336;">✗ 操作失败: ' + (d.error || '请稍后重试') + '</span>';
        document.querySelector('.rebind-btn').style.display = 'block';
      }
    })
    .catch(function(e) {
      m.innerHTML = '<span style="color:#f44336;">✗ 网络错误，请稍后重试</span>';
      document.querySelector('.rebind-btn').style.display = 'block';
    });
  return false;
}
</script>"""

_BOUND_BODY = _BOUND_INSTRUCTION + _REBIND_SECTION


async def _serve_bind_qr(scope, receive, send):
    """Serve a mobile-friendly bootstrapping page for headless RK3576.

    States (checked in order):
      1. ✅ Bound — .parent_identity.json exists, QR displayed
      2. ⟳ Bootstrap — .bootstrap_qr.txt exists, QR generated in-memory, wait for scan
      3. ⏳ Starting — HermesAgent up but no QR yet
      4. ❌ Not configured — containers not started or missing credentials
    """
    import base64
    from io import BytesIO

    BOOTSTRAP_QR = "/data/hermes/.bootstrap_qr.txt"
    PARENT_ID = "/data/hermes/.parent_identity.json"
    png_b64: str | None = None
    refresh_sec: int | None = None

    # ── State 1: Already bound — fetch QR from Provider API ──
    if os.path.exists(PARENT_ID):
        try:
            async with httpx.AsyncClient(timeout=20) as client:
                r = await client.get(f"{PLATFORM_URL}/api/bot/qrcode?refresh=1", timeout=20)
                if r.status_code == 200 and "image/png" in r.headers.get("content-type", ""):
                    png_b64 = base64.b64encode(r.content).decode()
        except Exception:
            pass

        if png_b64:
            status_rows = _BOUND_STATUS
            hint = _BOUND_HINT
            body_html = _BOUND_BODY
        else:
            refresh_sec = 10
            status_rows = """
<div class="status-row status-ok">
  <span class="label">系统状态</span>
  <span class="value">✓ 运行中</span>
</div>
<div class="status-row status-info">
  <span class="label">微信服务</span>
  <span class="value">⟳ 激活中</span>
</div>
<div class="status-row status-info">
  <span class="label">二维码</span>
  <span class="value">⟳ 生成中</span>
</div>"""
            hint = "正在获取二维码，请稍候..."
            body_html = '<div class="steps" style="background:#fff3e0;"><strong>⏳ 二维码加载中</strong><br>页面将自动刷新，请稍候。</div>'

    # ── State 2: Bootstrap in progress — QR file from gateway_start.sh ──
    elif os.path.exists(BOOTSTRAP_QR):
        try:
            with open(BOOTSTRAP_QR, "r") as f:
                qr_url = f.read().strip()
        except Exception:
            qr_url = ""

        if qr_url:
            import qrcode as _qrlib
            _qr = _qrlib.QRCode(box_size=8, border=2)
            _qr.add_data(qr_url)
            _qr.make(fit=True)
            _img = _qr.make_image(fill_color="black", back_color="white")
            _buf = BytesIO()
            _img.save(_buf, format="PNG")
            png_b64 = base64.b64encode(_buf.getvalue()).decode()

            status_rows = """
<div class="status-row status-ok">
  <span class="label">系统状态</span>
  <span class="value">✓ 运行中</span>
</div>
<div class="status-row status-ok">
  <span class="label">微信服务</span>
  <span class="value">✓ 待绑定</span>
</div>
<div class="status-row status-ok">
  <span class="label">二维码</span>
  <span class="value">✓ 可用</span>
</div>"""
            hint = '请使用微信"扫一扫"扫描下方二维码'
            body_html = f"""
<div class="qr-wrapper"><img src="data:image/png;base64,{png_b64}" alt="QR Code" class="qr"></div>
<div class="steps">
  <strong>操作步骤</strong><br>
  ① 打开微信「扫一扫」<br>
  ② 扫描上方二维码<br>
  ③ 确认绑定 AI 教学助手<br>
  ④ 页面将自动刷新进入完成状态
</div>
<p class="footer">正在等待扫码... 页面将自动刷新</p>"""
            # QR ready → 10s refresh to /bind-qr (will switch to State 1 after scan)
            refresh_sec = 10
        else:
            status_rows = ""
            hint = "正在获取二维码..."
            body_html = '<div class="steps" style="background:#fff3e0;"><strong>⏳ 正在获取二维码</strong><br>请稍候...</div>'
            refresh_sec = 5

    # ── State 3: Try env-var QR, then check HermesAgent ──
    else:
        # 尝试从 Provider API 获取二维码 (WEIXIN_TOKEN 通过 env 注入)
        env_qr_ok = False
        try:
            async with httpx.AsyncClient(timeout=10) as _qc:
                _r = await _qc.get(f"{PLATFORM_URL}/api/bot/qrcode?refresh=1", timeout=10)
                if _r.status_code == 200 and "image/png" in _r.headers.get("content-type", ""):
                    png_b64 = base64.b64encode(_r.content).decode()
                    env_qr_ok = True
        except Exception:
            pass

        if env_qr_ok:
            status_rows = _BOUND_STATUS
            hint = _BOUND_HINT
            body_html = _BOUND_BODY
        # ── State 4: Check HermesAgent health ──
        else:
            ha_ok = False
            try:
                async with httpx.AsyncClient(timeout=5) as client:
                    r = await client.get(f"{HERMES_URL}/health", timeout=5)
                    ha_ok = r.status_code == 200
            except Exception:
                pass

            if ha_ok:
                refresh_sec = 10
                status_rows = """
<div class="status-row status-ok">
  <span class="label">系统状态</span>
  <span class="value">✓ 运行中</span>
</div>
<div class="status-row status-warn">
  <span class="label">微信服务</span>
  <span class="value">⟳ 注册中...</span>
</div>
<div class="status-row status-warn">
  <span class="label">二维码</span>
  <span class="value">⟳ 等待中</span>
</div>"""
                hint = "正在准备二维码，请稍候..."
                body_html = """
<div class="steps" style="background:#fff3e0;">
  <strong>⏳ 正在准备二维码</strong><br>
  正在准备微信二维码，<br>
  通常需要 30-60 秒，请稍候。
</div>"""
            else:
                refresh_sec = 10
                status_rows = """
<div class="status-row status-err">
  <span class="label">系统状态</span>
  <span class="value">✗ 启动中</span>
</div>
<div class="status-row status-err">
  <span class="label">AI 教学助手</span>
  <span class="value">✗ 等待中</span>
</div>
<div class="status-row status-warn">
  <span class="label">二维码</span>
  <span class="value">—</span>
</div>"""
                hint = "正在启动，请稍候..."
                body_html = """
<div class="steps" style="background:#ffebee;">
  <strong>⏳ 系统正在启动中</strong><br>
  系统正在启动中，通常需要 1-2 分钟完成初始化。<br><br>
  如果长时间停留在此页面：<br>
  ① 确认设备已连接电源和网线<br>
  ② 拔出网线重新插入，等待 1 分钟后刷新
</div>
<p class="footer">配置完成后刷新此页面</p>"""

    html = f"""<!DOCTYPE html>
<html lang="zh-CN">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1.0,maximum-scale=1.0,user-scalable=no">
{f'  <meta http-equiv="refresh" content="{refresh_sec}">' if refresh_sec is not None else ''}
<title>绑定 AI 教学助手</title>
<style>{_STATUS_CSS}
.rebind-warn {{ margin-top:24px; padding-top:16px; border-top:2px solid #ffcdd2; }}
.rebind-btn {{ display:block; width:100%; padding:12px; background:#fff; color:#c62828;
              border:1px solid #ef9a9a; border-radius:8px; font-size:14px;
              cursor:pointer; text-align:center; text-decoration:none; }}
.rebind-btn:hover {{ background:#ffebee; }}
</style>
</head>
<body>
<div class="container">
  <h1>📚 绑定 AI 教学助手</h1>
  <p class="hint">{hint}</p>

  <div class="status-grid">{status_rows}</div>

  <div style="text-align:center;font-size:12px;color:#999;margin:0 0 12px 0;">
    设备 IP: <strong>{_get_device_ip()}</strong>
    &nbsp;|&nbsp; 手机访问 <strong>http://{_get_device_ip()}:8100</strong>
  </div>

  {body_html}

  {f'<div><a href="/bind-qr" class="refresh-btn">⟳ 刷新页面</a></div>' if refresh_sec is not None else ''}
  <p class="footer">首次绑定后即可通过微信与 AI 家庭教师对话</p>
</div>
</body>
</html>"""
    body = html.encode("utf-8")
    headers = [
        (b"content-type", b"text/html; charset=utf-8"),
        (b"cache-control", b"no-cache, no-store, must-revalidate"),
    ]
    await send({"type": "http.response.start", "status": 200, "headers": headers})
    await send({"type": "http.response.body", "body": body})


async def _serve_source_file(scope, receive, send):
    """服务 /sources/ 静态文件 (替代 v6.x nginx 反代).

    场景三 (docs/business_scenarios.md 第89行):
      HA 通过微信发送 /sources/{trace_id}_{filename} 时，需要 HTTP 服务返回原文件.
    """
    path = scope.get("path", "")
    filename = path[len("/sources/"):]
    if not filename or ".." in filename or "/" in filename:
        body = b"{\"error\":\"invalid path\"}"
        await send({"type": "http.response.start", "status": 400,
                     "headers": [(b"content-type", b"application/json")]})
        await send({"type": "http.response.body", "body": body})
        return

    filepath = os.path.join(SOURCES_DIR, filename)
    if not os.path.isfile(filepath):
        body = b"{\"error\":\"file not found\"}"
        await send({"type": "http.response.start", "status": 404,
                     "headers": [(b"content-type", b"application/json")]})
        await send({"type": "http.response.body", "body": body})
        return

    import mimetypes
    content_type, _ = mimetypes.guess_type(filename)
    headers = [(b"content-type", (content_type or "application/octet-stream").encode()),
               (b"cache-control", b"private, max-age=3600")]

    try:
        with open(filepath, "rb") as f:
            data = f.read()
    except OSError as e:
        body = json.dumps({"error": str(e)}).encode()
        await send({"type": "http.response.start", "status": 500,
                     "headers": [(b"content-type", b"application/json")]})
        await send({"type": "http.response.body", "body": body})
        return

    await send({"type": "http.response.start", "status": 200, "headers": headers})
    await send({"type": "http.response.body", "body": data})


async def _health_check(scope, receive, send):
    """Health check endpoint returning MCP server status."""
    dt_cb = _dt_circuit.status()
    plat_cb = _platform_circuit.status()
    dm_cb = _dm_circuit.status()
    body = json.dumps({
        "status": "ok",
        "service": "platform_mcp",
        "tools_registered": len(_TOOL_DESCRIPTIONS),
        "scene": _scene,
        "slow_queue_pending": _slow_pending,
        "circuit_breakers": {
            "deeptutor": {"state": dt_cb["state"], "failure_count": dt_cb["failure_count"]},
            "platform": {"state": plat_cb["state"], "failure_count": plat_cb["failure_count"]},
            "device_manager": {"state": dm_cb["state"], "failure_count": dm_cb["failure_count"]},
        },
    }, ensure_ascii=False).encode()

    await send({
        "type": "http.response.start",
        "status": 200,
        "headers": [(b"content-type", b"application/json")],
    })
    await send({"type": "http.response.body", "body": body})


async def _proxy_json_post(scope, receive, send, target_url: str):
    """代理 POST JSON 请求到内部 API，返回 JSON 响应."""
    body_bytes = b""
    while True:
        msg = await receive()
        if msg["type"] == "http.request":
            body_bytes += msg.get("body", b"")
            if not msg.get("more_body", False):
                break
        elif msg["type"] == "http.disconnect":
            return
    try:
        async with httpx.AsyncClient(timeout=30) as client:
            resp = await client.post(target_url, content=body_bytes or None,
                                     headers={"content-type": "application/json"})
            resp_body = resp.content
            status = resp.status_code
    except Exception as e:
        resp_body = json.dumps({"ok": False, "error": str(e)}).encode()
        status = 502
    headers = [
        (b"content-type", b"application/json"),
        (b"cache-control", b"no-cache"),
    ]
    await send({"type": "http.response.start", "status": status, "headers": headers})
    await send({"type": "http.response.body", "body": resp_body})


# ═══════════════════════════════════════════════
# Entry point — called by entry.py
# ═══════════════════════════════════════════════

def _build_app():
    """构建带 health + /sources/ 包装的 ASGI app.

    对 MCP POST 请求 (`/mcp`), 拦截 404 "Session not found" 响应,
    转为 200 并填入正确 request ID, 使客户端能识别 session 过期
    (消息 "session not found" 匹配 hermest-agent 的 _SESSION_EXPIRED_MARKERS),
    从而触发 _handle_session_expired_and_retry 自动重连 + 重试,
    避免工具调用因响应消息不匹配 ("Session terminated") 直接陷入熔断.
    """
    raw_app = mcp.streamable_http_app()

    async def wrapper(scope, receive, send):
        if scope["type"] != "http":
            await raw_app(scope, receive, send)
            return
        path = scope.get("path", "")
        if path == "/health":
            await _health_check(scope, receive, send)
        elif path in ("/", "/bind-qr", "/bind-qr/"):
            await _serve_bind_qr(scope, receive, send)
        elif path.startswith("/sources/"):
            await _serve_source_file(scope, receive, send)
        elif path == "/api/bot/rebind_parent" and scope.get("method") == "POST":
            await _proxy_json_post(scope, receive, send, f"{PLATFORM_URL}{path}")
        elif path == "/mcp" and scope.get("method") == "POST":
            await _handle_mcp_post(scope, receive, send, raw_app)
        else:
            await raw_app(scope, receive, send)

    return wrapper


async def _handle_mcp_post(scope, receive, send, raw_app):
    """处理 MCP POST, 拦截 404 → 200 使 session 过期可恢复.

    MCP 客户端收到 404 后会自已注入 "Session terminated" 错误消息,
    该消息不匹配 hermes-agent 的 _SESSION_EXPIRED_MARKERS,
    导致工具调用无法触发 session 过期恢复流程 (_handle_session_expired_and_retry).

    本函数拦截 404 响应, 将状态码改为 200 并用请求中的真实 request ID
    替换占位 ID "server-error", 使客户端通过 _handle_json_response
    收到原始 "Session not found" 错误, 匹配 markers 从而触发自动重连 + 重试.
    """
    # ── 1. 缓冲请求体 ──
    chunks: list[bytes] = []
    more = True
    while more:
        event = await receive()
        if event["type"] == "http.request":
            if event.get("body"):
                chunks.append(event["body"])
            more = event.get("more_body", False)

    body = b"".join(chunks)

    # ── 2. 提取 JSON-RPC request ID ──
    request_id: int | str | None = None
    if body:
        try:
            req = json.loads(body)
            request_id = req.get("id")
        except (json.JSONDecodeError, ValueError):
            pass

    # ── 3. 创建 replay receive (一次交出完整 body, 后续阻塞等待)
    # 不能返回 http.disconnect, 否则 Starlette 会提前中断 SSE 响应。
    body_delivered = False

    async def replay_receive():
        nonlocal body_delivered
        if not body_delivered:
            body_delivered = True
            return {"type": "http.request", "body": body, "more_body": False}
        await asyncio.Event().wait()  # 阻塞而非断开, 防止 SSE 流被截断

    # ── 4. 包装 send 拦截 404 ──
    intercepted: list[dict] = []
    resp_body = bytearray()

    async def intercept_send(msg):
        if msg["type"] == "http.response.start":
            if msg["status"] == 404 and request_id is not None:
                intercepted.append(msg)
                return
            await send(msg)
        elif msg["type"] == "http.response.body":
            if intercepted:
                resp_body.extend(msg.get("body", b""))
                if not msg.get("more_body", False):
                    try:
                        err = json.loads(resp_body)
                        if isinstance(err, dict) and "error" in err:
                            err["id"] = request_id
                            modified = json.dumps(err, ensure_ascii=False).encode()
                            hdr = [
                                (b"content-type", b"application/json"),
                                (b"content-length", str(len(modified)).encode()),
                            ]
                            await send({
                                "type": "http.response.start",
                                "status": 200,
                                "headers": hdr,
                            })
                            await send({"type": "http.response.body", "body": modified})
                            return
                    except (json.JSONDecodeError, Exception):
                        pass
                    await send(intercepted[0])
                    await send(msg)
                return
            await send(msg)
        else:
            await send(msg)

    await raw_app(scope, replay_receive, intercept_send)


# ═══════════════════════════════════════════════
# mDNS 自动发现 — 双引擎: avahi-publish + zeroconf 兜底
# ═══════════════════════════════════════════════

_MDNS_HOSTNAME = os.getenv("MDNS_HOSTNAME", "ai-tutor").strip().rstrip(".local")


def _start_mdns(port: int = 8003):
    """在后台注册 mDNS 服务.

    双引擎并行:
      1. avahi-publish — D-Bus → 宿主 avahi-daemon → 全 LAN 可见
         崩溃后自动重启, 防止静默丢失
      2. zeroconf 兜底 — 纯 Python, 注册宿主机 IP (桥接模式仍有意义)
    """
    import threading
    hostname = _MDNS_HOSTNAME
    host_ip = _get_device_ip()

    def _run_avahi():
        """avahi-publish-service 后台运行, 崩溃自动重启."""
        import subprocess
        import time as _time

        while True:
            try:
                proc = subprocess.Popen(
                    ["avahi-publish-service",
                     "-H", f"{hostname}.local",
                     "-s", "AI Tutor",
                     "_http._tcp",
                     str(port),
                     "path=/"],
                    stdout=subprocess.DEVNULL,
                    stderr=subprocess.DEVNULL,
                )
                _time.sleep(0.5)
                if proc.poll() is not None:
                    logger.info("avahi-publish 不可用 (宿主机无 Avahi 或 D-Bus), 仅用 zeroconf")
                    return  # 彻底放弃, zeroconf 已在运行

                logger.info(
                    "mDNS (avahi): http://%s.local:%d/bind-qr  (全 LAN 可见)",
                    hostname, port,
                )
                proc.wait()  # 阻塞直到进程退出
                logger.warning("avahi-publish 进程已退出, 5s 后重启...")
                _time.sleep(5)
            except FileNotFoundError:
                logger.info("avahi-utils 未安装, mDNS 仅依赖 zeroconf")
                return
            except Exception as e:
                logger.warning("avahi-publish 异常: %s, 10s 后重试", e)
                _time.sleep(10)

    def _run_zeroconf():
        """zeroconf 后台注册, 携带宿主机 IP 使桥接模式下仍可指向正确地址."""
        import socket as _socket

        try:
            from zeroconf import Zeroconf, ServiceInfo
        except ImportError:
            logger.warning("zeroconf 未安装, mDNS 仅依赖 avahi")
            return

        # 尝试获取宿主机 IP 作为注册地址 (非桥接容器 IP)
        addresses = None
        if host_ip:
            try:
                addresses = [_socket.inet_aton(host_ip)]
            except Exception:
                pass

        info = ServiceInfo(
            type_="_http._tcp.local.",
            name=f"AI Tutor ({hostname})._http._tcp.local.",
            server=f"{hostname}.local.",
            addresses=addresses,
            port=port,
            properties={"path": "/bind-qr", "vendor": "home-ai-tutor"},
        )
        try:
            zc = Zeroconf()
            zc.register_service(info)
            ip_hint = f"IP={host_ip}" if addresses else "接口自动"
            logger.info(
                "mDNS (zeroconf): http://%s.local:%d/bind-qr  (%s)",
                hostname, port, ip_hint,
            )
        except Exception as e:
            logger.warning("zeroconf 注册失败: %s", e)

    # 并行启动双引擎: avahi 全 LAN + zeroconf 兜底
    t1 = threading.Thread(target=_run_avahi, name="mdns-avahi", daemon=True)
    t1.start()

    t2 = threading.Thread(target=_run_zeroconf, name="mdns-zeroconf", daemon=True)
    t2.start()


# 模块级 ASGI app，支持 uvicorn -m mcp_server 直接启动（开发用）
app = _build_app()

# 模块加载时自动启动 mDNS (仅当以 uvicorn 方式直接加载时, run_mcp_server 会自己调)
if not os.environ.get("_MCP_DISABLE_MDNS") and not os.environ.get("_MCP_MDNS_STARTED"):
    os.environ["_MCP_MDNS_STARTED"] = "1"
    _start_mdns(int(os.getenv("MCP_PORT", "8003")))


def run_mcp_server(port: int = 8003):
    import uvicorn
    if not os.environ.get("_MCP_MDNS_STARTED"):
        os.environ["_MCP_MDNS_STARTED"] = "1"
        _start_mdns(port)
    uvicorn.run(_build_app(), host="0.0.0.0", port=port, log_level="info")


if __name__ == "__main__":
    port = int(os.getenv("MCP_PORT", "8003"))
    run_mcp_server(port)
