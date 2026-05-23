"""
platform/provider_api.py — Provider API (v7.0, 从 hermes_ingest 合并)

改编自 docker/hermes_ingest/server.py:
  - 默认端口: 8100 (内部 localhost)
  - ChromaDB PersistentClient (替代 HttpClient)
  - 简化 /health (Chromadb 内嵌后不再独立跟踪)
"""

import asyncio
import atexit
import base64
import glob
import json
import logging
import os
import random
import re
import struct
import subprocess
import sys
import time
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path

sys.path.insert(0, "/tutor_platform")
sys.path.insert(0, "/")

from contextlib import asynccontextmanager
from fastapi import FastAPI, Request, UploadFile, File, Form
from fastapi.responses import Response
from pydantic import BaseModel
import uvicorn
import httpx

from markitdown import MarkItDown

from tutor_platform.unified_provider import UnifiedLocalProvider, get_provider_instance, reset_provider_instance
from tutor_platform.storage import validate_provider_config
from domains.tutoring.mastery import (
    get_mastery, get_wrong_answers, update_mastery, weak_points, get_report,
    generate_parent_report, generate_daily_report, get_due_reviews, schedule_review, _load,
    get_weekly_stats, get_monthly_stats, get_answer_history,
)

# ── Phase B: MCP Server merge ──
# Prevent mcp_server module from auto-starting mDNS on import;
# we call _start_mdns manually from lifespan with the merged :8100 port.
os.environ["_MCP_MDNS_STARTED"] = "1"
from mcp_server import (
    mcp as mcp_fastmcp,
    _set_direct_mode,
    _start_mdns,
    _serve_source_file,
    _handle_mcp_post,
    _MDNS_HOSTNAME,
)

logger = logging.getLogger("provider_api")
logging.basicConfig(level=logging.INFO, format="[provider] %(asctime)s %(message)s")


def _ws_is_alive(ws) -> bool:
    """Check if a websockets connection is still open.

    websockets v13+ uses ClientConnection.state;
    older versions use the .closed bool property.
    """
    try:
        return not ws.closed
    except AttributeError:
        import websockets.protocol
        return ws.state is websockets.protocol.State.OPEN

HERMES_AGENT_URL = os.getenv("HERMES_AGENT_URL", "http://hermes_agent:8004")
DEEPTUTOR_URL = os.getenv("DEEPTUTOR_API_URL", "http://deeptutor:8001")
UPLOADS_DIR = os.getenv("UPLOADS_DIR", "/data/uploads")
SOURCES_DIR = os.getenv("SOURCES_DIR", "/data/sources")

_provider_init_time: float = 0.0
_provider_error: str | None = None

# Session cleanup threshold: only send /new to teacher bot when this many
# tutor_chat calls have accumulated since last cleanup.  Each call adds ~2-5
# session messages (~1-2 KB).  2000 calls ≈ 2-4 MB of session data in memory.
# Reset on container restart — the bot is also fresh at that point.
_session_msg_since_cleanup = 0
SESSION_CLEANUP_THRESHOLD = int(os.getenv("SESSION_CLEANUP_THRESHOLD", "2000"))

# Tutor context cache: keys learner_id → last non-empty context string.
# When _tutor_chat_core receives a follow-up call (context="") from the
# Phase C protocol handler (e.g. student answers "B"), the cached context
# is auto-injected so DT TutorBot has the teaching context.
_last_tutor_context: dict[str, str] = {}
_MAX_CACHED_CONTEXTS = 100

# Question number tracker per learner: tracks the last question number DT
# output (e.g. "第5题"). Used by auto-next to send explicit "请出第N+1题"
# instead of ambiguous "下一题" — prevents LLM miscounting/skipping.
_last_question_num: dict[str, int] = {}

# DT LLM profile cache: tracks currently active (profile_id, model_id) so
# _tutor_chat_core can skip redundant catalog switches + bot restarts.
_last_llm_profile: tuple[str, str] | None = None

# Per-learner DT WebSocket session pool: reuse WS connections across
# follow-up calls to avoid ~2-5s bot cold start on every turn.
_MAX_WS_SESSIONS = 100  # 最大并发 WS 连接数


class _DTTutorSession:
    """Persistent DT TutorBot WS connection per learner, with auto-reconnect."""
    _sessions: dict[str, '_DTTutorSession'] = {}
    _lock = asyncio.Lock()

    def __init__(self, learner_id: str):
        self.learner_id = learner_id
        self.ws: 'websockets.WebSocketClientProtocol | None' = None
        self.last_used: float = time.time()
        self._ws_lock = asyncio.Lock()

    async def send_and_recv(self, payload: str, trace_id: str) -> dict:
        import websockets
        self.last_used = time.time()
        try:
            ws = await self._ensure_ws()
            return await self._do_send_recv(ws, payload, trace_id)
        except (websockets.ConnectionClosed, OSError, asyncio.TimeoutError) as e:
            logger.warning("[%s] DT WS disconnected, reconnecting: %s", trace_id, e)
            await self._close_ws()
            ws = await self._ensure_ws()
            return await self._do_send_recv(ws, payload, trace_id)

    async def _ensure_ws(self):
        if self.ws is not None and _ws_is_alive(self.ws):
            return self.ws
        import websockets
        self.ws = await asyncio.wait_for(
            websockets.connect("ws://deeptutor:8001/api/v1/tutorbot/teacher/ws", close_timeout=10),
            timeout=30,
        )
        return self.ws

    async def _do_send_recv(self, ws, payload: str, trace_id: str) -> dict:
        import websockets
        await asyncio.wait_for(
            ws.send(json.dumps({"content": payload, "chat_id": self.learner_id})),
            timeout=30,
        )
        final_content = ""
        proactive_content = ""
        ws_error = ""
        while True:
            raw = await asyncio.wait_for(ws.recv(), timeout=180)
            data = json.loads(raw)
            msg_type = data.get("type", "")
            c = data.get("content", "")
            if msg_type == "content" and c:
                final_content = c
            elif msg_type == "proactive" and c:
                proactive_content = c
            elif msg_type == "done":
                break
            elif msg_type == "error":
                ws_error = c or "unknown error"
                break

        if not final_content and proactive_content:
            final_content = proactive_content
        if final_content:
            _qn_m = re.search(r'第\s*(\d+)\s*题', final_content)
            if _qn_m:
                _last_question_num[self.learner_id] = int(_qn_m.group(1))
            logger.info("[%s] TutorBot: %s", trace_id, final_content[:300])
            return {"ok": True, "content": final_content.strip(), "trace_id": trace_id}

        if ws_error != "timeout":
            try:
                raw = await asyncio.wait_for(ws.recv(), timeout=2)
                data = json.loads(raw)
                if data.get("type") == "proactive":
                    c = data.get("content", "")
                    if c:
                        return {"ok": True, "content": c.strip(), "trace_id": trace_id}
            except (asyncio.TimeoutError, json.JSONDecodeError):
                pass
        return {"ok": False, "error": ws_error or "empty"}

    async def _close_ws(self):
        if self.ws is not None and _ws_is_alive(self.ws):
            try:
                await self.ws.close()
            except Exception:
                pass
        self.ws = None

    async def close(self):
        await self._close_ws()

    @classmethod
    async def get(cls, learner_id: str) -> '_DTTutorSession':
        async with cls._lock:
            if learner_id not in cls._sessions:
                # 超过上限时踢掉最久未使用的
                if len(cls._sessions) >= _MAX_WS_SESSIONS:
                    oldest = min(cls._sessions.items(), key=lambda x: x[1].last_used)
                    logger.warning("[dt_session] Capacity %d reached, evicting %s",
                                   _MAX_WS_SESSIONS, oldest[0])
                    await oldest[1].close()
                    del cls._sessions[oldest[0]]
                cls._sessions[learner_id] = cls(learner_id)
            return cls._sessions[learner_id]

    @classmethod
    async def close_all(cls):
        async with cls._lock:
            for session in cls._sessions.values():
                await session.close()
            cls._sessions.clear()


async def _dt_session_cleanup_loop():
    """每 5 分钟关闭空闲 >30 分钟的 DT WS 会话."""
    while True:
        await asyncio.sleep(300)
        now = time.time()
        async with _DTTutorSession._lock:
            idle = [sid for sid, sess in _DTTutorSession._sessions.items()
                    if now - sess.last_used > 1800]
            for sid in idle:
                await _DTTutorSession._sessions[sid].close()
                del _DTTutorSession._sessions[sid]
            if idle:
                logger.info("[dt_session] Closed %d idle WS session(s)", len(idle))


# SOUL.md 更新锁: 全局锁 (所有 learner 共享同一个 "teacher" bot workspace,
# 不能用 per-learner 锁, 否则 learner A/B 并发更新时会互相覆盖)
_soul_global_lock = asyncio.Lock()
_soul_version: int = 0  # 每次 SOUL.md 更新递增, 用于检测过期写入

# Local LLM resource lock: provider 统一调度本地 LLM (rkllama) 访问.
# HA 的 OCR 通过 HTTP acquire/release 申请锁; DT 后续如需本地 LLM 也通过此锁.
_llm_lock = asyncio.Lock()

# OCR 并发控制: 最多 2 个并发 OCR 请求, 防止 NPU OOM.
_ocr_semaphore = asyncio.Semaphore(2)

# OCR warm-once: 首次图片请求时 warm, 后续跳过 (模型加载后常驻).
# 持久化到文件, 容器重启后跳过 warm (模型在 rkllama 容器中常驻).
_ocr_warmed: bool = False
_OCR_WARM_FLAG = "/data/mastery/.ocr_warmed"


def _load_ocr_warmed() -> bool:
    try:
        return os.path.exists(_OCR_WARM_FLAG)
    except OSError:
        return False


def _save_ocr_warmed():
    try:
        Path(_OCR_WARM_FLAG).touch()
    except OSError:
        pass

# Last SOUL.md content per learner: skip redundant updates when unchanged.
_last_soul_content: dict[str, str] = {}

_CONTEXT_PERSIST_DIR = os.getenv("MASTERY_DIR", "/data/mastery")

def _persist_path(learner_id: str) -> str:
    # URL-safe base64 编码避免文件系统路径冲突
    safe = base64.urlsafe_b64encode(learner_id.encode("utf-8")).decode("ascii")
    return os.path.join(_CONTEXT_PERSIST_DIR, f"{safe}_session.json")

def _save_context_to_disk(learner_id: str):
    """持久化教学上下文到 JSON 文件, 容器重启后恢复."""
    ctx = _last_tutor_context.get(learner_id, "")
    qnum = _last_question_num.get(learner_id, 0)
    if not ctx:
        return
    path = _persist_path(learner_id)
    try:
        os.makedirs(os.path.dirname(path), exist_ok=True)
        with open(path, "w", encoding="utf-8") as f:
            json.dump({"learner_id": learner_id, "context": ctx, "question_num": qnum,
                       "updated_at": time.time()}, f, ensure_ascii=False)
    except OSError as e:
        logger.warning("Failed to persist context for %s: %s", learner_id, e)

def _load_context_from_disk(learner_id: str) -> tuple[str, int]:
    """从磁盘恢复教学上下文."""
    path = _persist_path(learner_id)
    try:
        with open(path, encoding="utf-8") as f:
            data = json.load(f)
        return data.get("context", ""), data.get("question_num", 0)
    except (FileNotFoundError, json.JSONDecodeError):
        return "", 0

async def _switch_dt_profile(profile_id: str, model_id: str, trace_id: str) -> bool:
    """Switch DT's active LLM profile via GET+PUT /api/settings/catalog.

    Returns True on success, False on any failure (network, parse, etc).
    """
    try:
        async with httpx.AsyncClient(timeout=10) as client:
            resp = await client.get(f"{DEEPTUTOR_URL}/api/v1/settings/catalog")
            if resp.status_code != 200:
                logger.warning("[%s] _switch_dt_profile GET failed: %s", trace_id, resp.status_code)
                return False
            catalog_wrapper = resp.json()
            catalog = catalog_wrapper.get("catalog", catalog_wrapper)
            if "services" not in catalog or "llm" not in catalog["services"]:
                logger.warning("[%s] _switch_dt_profile bad catalog: %s", trace_id, list(catalog.keys()))
                return False
            catalog["services"]["llm"]["active_profile_id"] = profile_id
            catalog["services"]["llm"]["active_model_id"] = model_id
            payload = {"catalog": catalog} if "catalog" in catalog_wrapper else catalog
            resp = await client.put(f"{DEEPTUTOR_URL}/api/v1/settings/catalog", json=payload)
            if resp.status_code != 200:
                logger.warning("[%s] _switch_dt_profile PUT failed: %s", trace_id, resp.status_code)
                return False
            logger.info("[%s] DT LLM switched to %s/%s", trace_id, profile_id, model_id)
            return True
    except Exception as e:
        logger.warning("[%s] _switch_dt_profile error: %s", trace_id, e)
        return False


async def _get_soul_lock() -> asyncio.Lock:
    """获取全局 SOUL.md 锁 (所有 learner 共享同一 bot workspace)."""
    return _soul_global_lock


async def _init_provider() -> UnifiedLocalProvider:
    global _provider_init_time, _provider_error
    try:
        logger.info("Initializing UnifiedLocalProvider singleton...")
        provider = get_provider_instance()
        _provider_init_time = time.time()
        _provider_error = None
        logger.info("UnifiedLocalProvider initialized successfully")
        return provider
    except Exception as e:
        _provider_error = str(e)
        logger.error("Failed to initialize UnifiedLocalProvider: %s", e)
        raise


async def _get_provider() -> UnifiedLocalProvider:
    try:
        return get_provider_instance()
    except Exception:
        return await _init_provider()


ALLOWED_EXTENSIONS = frozenset({
    ".pdf", ".docx", ".doc", ".pptx", ".ppt",
    ".txt", ".md", ".html", ".htm", ".rst",
    ".xlsx", ".xls", ".csv",
    ".jpg", ".jpeg", ".png", ".webp", ".bmp",
})
MAX_FILE_SIZE = 100 * 1024 * 1024


def _sanitize_html_content(html: str) -> str:
    """Remove dangerous HTML elements, keeping only safe text content.

    Strips scripts, event handlers, iframes, objects, and style blocks
    to prevent XSS when content is displayed in any web context.
    """
    import re as _re
    cleaned = _re.sub(r'<script[^>]*>.*?</script>', '', html, flags=_re.DOTALL | _re.IGNORECASE)
    cleaned = _re.sub(r'<style[^>]*>.*?</style>', '', cleaned, flags=_re.DOTALL | _re.IGNORECASE)
    cleaned = _re.sub(r'<iframe[^>]*>.*?</iframe>', '', cleaned, flags=_re.DOTALL | _re.IGNORECASE)
    cleaned = _re.sub(r'<object[^>]*>.*?</object>', '', cleaned, flags=_re.DOTALL | _re.IGNORECASE)
    cleaned = _re.sub(r'<embed[^>]*>.*?</embed>', '', cleaned, flags=_re.DOTALL | _re.IGNORECASE)
    cleaned = _re.sub(r'\s+on\w+\s*=\s*["\'][^"\']*["\']', '', cleaned, flags=_re.IGNORECASE)
    cleaned = _re.sub(r'\s+on\w+\s*=\s*\S+', '', cleaned, flags=_re.IGNORECASE)
    return cleaned


def _validate_file(filename: str | None, size: int) -> str | None:
    ext = os.path.splitext(filename or "")[1].lower()
    if ext not in ALLOWED_EXTENSIONS:
        return f"不支持的文件类型: {ext or '未知'}"
    if size > MAX_FILE_SIZE:
        return f"文件过大 ({size / 1024 / 1024:.1f} MB > 100 MB)"
    return None


_EDUCATIONAL_KEYWORDS = frozenset({
    "题", "分", "解", "方程", "计算", "证明", "化简", "求值", "判断",
    "选择", "填空", "解答", "应用", "阅读", "理解", "分析", "论述",
    "实验", "观察", "归纳", "推理", "论证", "求解", "画", "列出",
    "写出", "找出", "确定", "比较", "分类", "概括", "解释", "说明",
    "练习", "测试", "考试", "作业", "试卷", "答题", "得分", "评卷",
    "知识点", "考点", "公式", "定理", "定义", "法则", "性质",
    "class", "exercise", "homework", "exam", "test", "quiz",
    "calculate", "prove", "solve", "equation", "function", "graph",
    "一、", "二、", "三、", "1.", "①", "考点", "单元", "学期",
    "年级", "科目", "数学", "语文", "英语", "物理", "化学", "生物",
    "政治", "历史", "地理", "科学", "algebra", "geometry", "physics",
})


def _is_educational_content(text: str, min_chars: int = 20) -> bool:
    """快速启发式判断内容是否为教育/学习材料.

    检查文本中是否包含教育类关键词或试卷/题目格式特征.
    长度不足 min_chars 的内容直接判定为非教育 (可能是噪声 OCR).
    """
    if not text or len(text.strip()) < min_chars:
        return False
    text_lower = text.lower()
    # 关键词匹配
    hits = sum(1 for kw in _EDUCATIONAL_KEYWORDS if kw.lower() in text_lower)
    if hits >= 2:
        return True
    # 试卷格式特征: 编号+题号 或 分数标注
    if re.search(r'(?:^|\n)\s*(?:一|二|三|四|五|六)\.\s*(?:选择|填空|判断|解答|计算)', text):
        return True
    if re.search(r'[（(]\s*[1-9]\d*\s*分\s*[)）]', text):
        return True
    # 数学符号密集型文本
    math_symbols = sum(1 for ch in text if ch in '=×÷±√∞∫πΔ²³∑∏')
    if len(text) > 50 and math_symbols / len(text) > 0.02:
        return True
    return False


def _cleanup_orphaned_uploads():
    try:
        now = time.time()
        for pattern in [os.path.join(UPLOADS_DIR, "*")]:
            for f in glob.glob(pattern):
                if os.path.isfile(f) and (now - os.path.getmtime(f)) > 3600:
                    try:
                        os.unlink(f)
                    except OSError:
                        pass
    except Exception:
        pass


atexit.register(_cleanup_orphaned_uploads)


def _ensure_uploads_dir():
    os.makedirs(UPLOADS_DIR, exist_ok=True)
    _cleanup_orphaned_uploads()


_trace_id_counter: int = 0


async def _warm_ocr_model(trace_id: str = ""):
    try:
        rkllama_url = os.getenv("RKLLAMA_URL", "http://rkllama:8080")
        async with httpx.AsyncClient(timeout=10) as client:
            resp = await client.post(f"{rkllama_url}/v1/warm/deepseekocr-3b")
            if resp.status_code == 200:
                data = resp.json()
                logger.info("[%s] OCR warm: %s (loaded=%s, %.0fms)",
                            trace_id, data.get("display", "?"),
                            data.get("loaded"), data.get("warm_ms", 0))
            else:
                logger.debug("[%s] OCR warm skipped (HTTP %s)", trace_id, resp.status_code)
    except Exception as e:
        logger.debug("[%s] OCR warm failed (non-critical): %s", trace_id, e)


_IMAGE_EXTENSIONS = frozenset({".jpg", ".jpeg", ".png", ".webp", ".bmp"})
_TEXT_EXTENSIONS = frozenset({".txt", ".md", ".html", ".htm", ".rst", ".csv"})
_OFFICE_EXTENSIONS = frozenset({".pdf", ".docx", ".doc", ".pptx", ".ppt", ".xlsx", ".xls"})


async def _handle_inbound_file(
    file_path: str,
    metadata: dict | None = None,
) -> dict:
    """Handle inbound file: classify → route → extract text.

    Classification pipeline:

        File
        ├─ Image (.jpg/.png/…) → OpenCV preprocess → rkllama OCR
        ├─ Text (.txt/.md/…)   → direct read
        ├─ PDF
        │   ├─ Has text layer  → markitdown extract
        │   └─ Scanned         → render page(s) → OpenCV → rkllama OCR
        ├─ Office (.docx/.pptx/…) → markitdown extract
        ├─ Old .doc            → antiword extract
        └─ Unknown             → metadata placeholder
    """
    meta = metadata or {}
    trace_id = meta.get("trace_id", _generate_trace_id())
    learner_id = meta.get("learner_id", "default")
    tool_name = meta.get("tool_name", "")
    ext = os.path.splitext(file_path)[1].lower() if file_path else ""

    if not os.path.isfile(file_path):
        return {"ok": False, "error": f"File not found: {file_path}"}

    try:
        # ── Image files: OpenCV preprocess → rkllama OCR ──
        if ext in _IMAGE_EXTENSIONS:
            content = await _ocr_image_file(file_path, trace_id)
            if content:
                logger.info("[%s] OCR success: %d chars from %s",
                            trace_id, len(content), file_path)
                return {
                    "ok": True, "content": content,
                    "intent": "EDUCATION", "route": "ocr",
                    "storage": {"ok": False},
                }
            # OCR failed — descriptive fallback so DT Bot can guide user
            content = "用户通过微信发送了一张图片，但图片中的文字未能被自动识别。请用友好的语气请学生将题目文字直接输入发送过来。"
            logger.warning("[%s] OCR failed, using descriptive fallback for %s", trace_id, file_path)
            return {
                "ok": True, "content": content,
                "intent": "EDUCATION", "route": "ocr_fallback",
                "storage": {"ok": False},
            }

        # ── Text files: read directly ──
        elif ext in _TEXT_EXTENSIONS:
            with open(file_path, "r", encoding="utf-8", errors="replace") as f:
                content = f.read()
            # HTML 文件消毒: 剥离 script/style/event-handler
            if ext in {".html", ".htm"}:
                content = _sanitize_html_content(content)
            return {
                "ok": True, "content": content,
                "intent": "EDUCATION" if len(content) < 5000 else "DOCUMENT",
                "route": "text", "storage": {"ok": False},
            }

        # ── PDF: classify → text PDF (markitdown) or scanned (render→OCR) ──
        elif ext == ".pdf":
            content = await _handle_pdf(file_path, trace_id)
            if content:
                logger.info("[%s] PDF extraction: %d chars from %s",
                            trace_id, len(content), file_path)
                return {
                    "ok": True, "content": content,
                    "intent": "EDUCATION", "route": "document_extract",
                    "storage": {"ok": False},
                }
            # Fallback to placeholder
            logger.warning("[%s] PDF extraction returned no content for %s", trace_id, file_path)
            return _fallback_placeholder(file_path, ext)

        # ── Office docs (.docx/.pptx/.xlsx): markitdown ──
        elif ext in {".docx", ".pptx", ".ppt", ".xlsx", ".xls"}:
            content = _extract_with_markitdown(file_path)
            if content:
                logger.info("[%s] markitdown extracted %d chars from %s",
                            trace_id, len(content), file_path)
                return {
                    "ok": True, "content": content,
                    "intent": "EDUCATION", "route": "document_extract",
                    "storage": {"ok": False},
                }
            logger.warning("[%s] markitdown failed for %s", trace_id, file_path)
            return _fallback_placeholder(file_path, ext)

        # ── Old .doc: antiword ──
        elif ext == ".doc":
            content = _extract_with_antiword(file_path)
            if content:
                logger.info("[%s] antiword extracted %d chars from %s",
                            trace_id, len(content), file_path)
                return {
                    "ok": True, "content": content,
                    "intent": "EDUCATION", "route": "document_extract",
                    "storage": {"ok": False},
                }
            logger.warning("[%s] antiword failed for %s", trace_id, file_path)
            return _fallback_placeholder(file_path, ext)

        # ── Unknown extension ──
        else:
            return _fallback_placeholder(file_path, ext)

    except Exception as e:
        logger.error("[%s] handle_inbound_file error: %s", trace_id, e)
        return {"ok": False, "error": str(e)}


# ── PDF classification ──


def _classify_pdf(file_path: str) -> tuple[str, int]:
    """Classify a PDF as ``"text"`` or ``"scanned"``.

    Returns ``(classification, num_pages)``.
    ``"text"`` means at least one page has >50 chars of extractable text.
    """
    import fitz
    doc = fitz.open(file_path)
    num_pages = len(doc)
    total_text = 0
    for page in doc:
        total_text += len(page.get_text().strip())
    doc.close()
    classification = "text" if total_text > 50 else "scanned"
    return classification, num_pages


def _render_pdf_page(file_path: str, page_num: int, dpi: int = 200) -> bytes:
    """Render a PDF page to PNG image bytes."""
    import fitz
    doc = fitz.open(file_path)
    page = doc[page_num]
    zoom = dpi / 72
    mat = fitz.Matrix(zoom, zoom)
    pix = page.get_pixmap(matrix=mat)
    return pix.tobytes("png")


async def _handle_pdf(file_path: str, trace_id: str) -> str:
    """Classify PDF → route to text extraction or page-by-page OCR."""
    classification, num_pages = _classify_pdf(file_path)
    logger.info("[%s] PDF classified as '%s' (%d pages, %s)",
                trace_id, classification, num_pages, file_path)

    if classification == "text":
        return _extract_with_markitdown(file_path)

    # Scanned PDF: render each page → OpenCV → OCR
    pages_text: list[str] = []
    for i in range(num_pages):
        try:
            img_bytes = _render_pdf_page(file_path, i)
            processed = _opencv_preprocess_image(img_bytes)
            page_text = await _ocr_image_bytes(processed, trace_id)
            if page_text and page_text.strip():
                pages_text.append(f"--- Page {i+1} ---\n{page_text.strip()}")
        except Exception as exc:
            logger.warning("[%s] Scanned PDF page %d failed: %s", trace_id, i, exc)

    return "\n\n".join(pages_text)


# ── OpenCV image preprocessing ──


def _opencv_preprocess_image(image_bytes: bytes) -> bytes:
    """Preprocess image for OCR: deskew → denoise → enhance → threshold."""
    import cv2
    import numpy as np

    nparr = np.frombuffer(image_bytes, np.uint8)
    img = cv2.imdecode(nparr, cv2.IMREAD_COLOR)
    if img is None:
        return image_bytes  # can't decode, return raw

    try:
        # Grayscale
        gray = cv2.cvtColor(img, cv2.COLOR_BGR2GRAY)

        # Denoise
        denoised = cv2.fastNlMeansDenoising(gray)

        # CLAHE contrast enhancement
        clahe = cv2.createCLAHE(clipLimit=2.0, tileGridSize=(8, 8))
        enhanced = clahe.apply(denoised)

        # Deskew: detect text angle and rotate
        coords = np.column_stack(np.where(enhanced < 128))  # dark pixels
        if len(coords) > 10:
            angle = cv2.minAreaRect(coords)[-1]
            if angle < -45:
                angle = 90 + angle
            if abs(angle) > 0.3:
                h, w = enhanced.shape
                center = (w // 2, h // 2)
                M = cv2.getRotationMatrix2D(center, angle, 1.0)
                enhanced = cv2.warpAffine(
                    enhanced, M, (w, h),
                    flags=cv2.INTER_CUBIC,
                    borderMode=cv2.BORDER_REPLICATE,
                )

        # Adaptive threshold (binarize)
        binary = cv2.adaptiveThreshold(
            enhanced, 255, cv2.ADAPTIVE_THRESH_GAUSSIAN_C,
            cv2.THRESH_BINARY, 11, 2,
        )

        _, buffer = cv2.imencode(".jpg", binary, [cv2.IMWRITE_JPEG_QUALITY, 95])
        return buffer.tobytes()
    except Exception as exc:
        logger.warning("[opencv] Preprocessing failed, using raw image: %s", exc)
        return image_bytes


# ── OCR helpers ──


async def _ocr_image_bytes(image_bytes: bytes, trace_id: str) -> str:
    """Send preprocessed image bytes to rkllama OCR."""
    import base64
    img_b64 = base64.b64encode(image_bytes).decode("utf-8")
    rkllama_url = os.getenv("RKLLAMA_URL", "http://rkllama:8080")
    async with _ocr_semaphore:
        try:
            async with httpx.AsyncClient(timeout=120) as client:
                resp = await client.post(
                    f"{rkllama_url}/v1/ocr",
                    json={
                        "image": img_b64, "language": "zh",
                        "return_formulas": False, "return_layout": False,
                    },
                )
                if resp.status_code == 200:
                    data = resp.json()
                    return (data.get("text", "") or "").strip()
                logger.warning("[%s] OCR returned HTTP %s", trace_id, resp.status_code)
        except Exception as exc:
            logger.warning("[%s] OCR request failed: %s", trace_id, exc)
    return ""


async def _ocr_image_file(file_path: str, trace_id: str) -> str:
    """Read image file, OpenCV preprocess, then OCR."""
    try:
        with open(file_path, "rb") as f:
            raw_bytes = f.read()
        processed = _opencv_preprocess_image(raw_bytes)
        return await _ocr_image_bytes(processed, trace_id)
    except Exception as exc:
        logger.warning("[%s] Image OCR pipeline failed: %s", trace_id, exc)
        return ""


# ── Document text extraction ──


_MD_INSTANCE: MarkItDown | None = None


def _get_markitdown() -> MarkItDown:
    global _MD_INSTANCE
    if _MD_INSTANCE is None:
        _MD_INSTANCE = MarkItDown()
    return _MD_INSTANCE


def _extract_with_markitdown(file_path: str) -> str:
    """Extract text via markitdown."""
    try:
        md = _get_markitdown()
        result = md.convert(file_path)
        return (result.text_content or "").strip()
    except Exception as exc:
        logger.warning("[markitdown] Extraction failed for %s: %s", file_path, exc)
    return ""


def _extract_with_antiword(file_path: str) -> str:
    """Extract text from old .doc via antiword CLI."""
    try:
        result = subprocess.run(
            ["antiword", file_path],
            capture_output=True, text=True, timeout=30,
        )
        if result.returncode == 0 and result.stdout.strip():
            return result.stdout.strip()
        logger.warning("[antiword] Returned %d for %s", result.returncode, file_path)
    except Exception as exc:
        logger.warning("[antiword] Failed for %s: %s", file_path, exc)
    return ""


def _fallback_placeholder(file_path: str, ext: str) -> dict:
    """Return metadata placeholder when all extraction attempts fail."""
    return {
        "ok": True,
        "content": (
            f"[{ext.upper()} file: {os.path.basename(file_path)}, "
            f"size={os.path.getsize(file_path)} bytes]"
        ),
        "intent": "EDUCATION",
        "route": "passthrough",
        "storage": {"ok": False},
    }


async def _ingest_to_kb(
    provider,
    content: str,
    kb_name: str,
    filename: str,
    learner_id: str,
    source: str,
    trace_id: str,
) -> None:
    """异步将提取的文本内容入库平台 ChromaDB + DT LlamaIndex 向量索引."""
    if not content or not content.strip():
        return

    # ── Step 1: 平台 ChromaDB ──
    try:
        docs = _split_content_for_ingest(content, filename)
        ids = [f"{trace_id}_{i}" for i in range(len(docs))]
        metadatas = [{
            "filename": filename,
            "learner_id": learner_id,
            "source": source,
            "trace_id": trace_id,
        } for _ in docs]

        await asyncio.to_thread(
            provider.add_documents,
            kb_name=kb_name, documents=docs,
            metadatas=metadatas, ids=ids,
        )
        logger.info("[%s] KB ingest: %d chunks -> %s (%d chars total)",
                    trace_id, len(docs), kb_name, len(content))
    except Exception as exc:
        logger.warning("[%s] KB ingest error for %s: %s", trace_id, filename, exc)

    # ── Step 2: DT LlamaIndex ──
    tmp_path = None
    try:
        import tempfile
        tmp = tempfile.NamedTemporaryFile(
            suffix=".txt", mode="w", delete=False,
            encoding="utf-8", prefix=f"auto_teach_{trace_id}_",
        )
        try:
            tmp.write(content)
            tmp_path = tmp.name
        finally:
            tmp.close()

        async with httpx.AsyncClient(timeout=120) as client:
            resp = await client.get(f"{DEEPTUTOR_URL}/api/v1/knowledge/list")
            kbs = resp.json() if resp.status_code == 200 else []
            kb_exists = any(
                kb.get("name") == kb_name or kb.get("id") == kb_name
                for kb in (kbs if isinstance(kbs, list) else [])
            )

            with open(tmp_path, "rb") as fh:
                files = {"files": (filename or "auto_teach.txt", fh, "text/plain")}
                if kb_exists:
                    resp = await client.post(
                        f"{DEEPTUTOR_URL}/api/v1/knowledge/{kb_name}/upload",
                        data={"rag_provider": "llamaindex"},
                        files=files,
                    )
                else:
                    resp = await client.post(
                        f"{DEEPTUTOR_URL}/api/v1/knowledge/create",
                        data={"name": kb_name, "rag_provider": "llamaindex"},
                        files=files,
                    )

            if resp.status_code in (200, 201):
                logger.info("[%s] DT index sync: OK -> %s (%s)", trace_id, kb_name, filename)
            else:
                logger.warning("[%s] DT index sync: %d %s", trace_id, resp.status_code, resp.text[:200])
    except Exception as exc:
        logger.warning("[%s] DT index sync error: %s", trace_id, exc)
    finally:
        if tmp_path:
            try:
                os.unlink(tmp_path)
            except Exception:
                pass


def _split_content_for_ingest(content: str, filename: str, chunk_size: int = 500) -> list[str]:
    """Split content into chunks for KB ingestion.

    Each chunk is a subsection of the document, split at sensible boundaries.
    """
    if len(content) <= chunk_size:
        return [content]

    import re
    chunks: list[str] = []
    # Try splitting at double newlines first (paragraphs)
    paragraphs = re.split(r"\n\s*\n", content)
    current = ""
    for para in paragraphs:
        if len(current) + len(para) + 2 < chunk_size:
            current = (current + "\n\n" + para).strip()
        else:
            if current:
                chunks.append(current)
            current = para
    if current:
        chunks.append(current)

    # If still too large, split mid-paragraph
    if any(len(c) > chunk_size for c in chunks):
        final: list[str] = []
        for c in chunks:
            if len(c) <= chunk_size:
                final.append(c)
            else:
                for i in range(0, len(c), chunk_size):
                    final.append(c[i:i + chunk_size])
        return final

    return chunks if chunks else [content]


def _generate_trace_id() -> str:
    global _trace_id_counter
    _trace_id_counter += 1
    return f"{uuid.uuid4().hex[:8]}-{_trace_id_counter:04d}"


def _extract_trace_id(request: Request) -> str:
    trace_id = request.headers.get("X-Trace-ID")
    if trace_id:
        return trace_id
    trace_id = request.headers.get("x-trace-id")
    if trace_id:
        return trace_id
    return _generate_trace_id()


_MAX_NOTIFICATION_FILES = 1000  # 通知文件上限, 超过则删除最旧的


def _cleanup_old_notifications(max_age_hours: int = 24):
    """清理过期通知文件, 确保不超过 _MAX_NOTIFICATION_FILES."""
    try:
        notif_dir = Path("/data/hermes/notifications")
        if not notif_dir.exists():
            return
        now = time.time()
        cutoff = now - max_age_hours * 3600
        files = []
        for f in notif_dir.iterdir():
            if f.is_file() and f.suffix in (".json", ".consumed"):
                try:
                    if f.stat().st_mtime < cutoff:
                        f.unlink()
                    else:
                        files.append(f)
                except OSError:
                    pass

        # 超过上限时删除最旧的文件 (优先保留 .consumed, 但陈旧已消费的也清理)
        if len(files) > _MAX_NOTIFICATION_FILES:
            files.sort(key=lambda f: f.stat().st_mtime)
            excess = len(files) - _MAX_NOTIFICATION_FILES
            for f in files[:excess]:
                try:
                    f.unlink()
                except OSError:
                    pass
            logger.info("[notify] Cleaned %d excess notification files", excess)
    except Exception:
        pass


async def _notify_hermes_agent(
    kb_name: str,
    filename: str,
    learner_id: str,
    result: dict,
    trace_id: str = "",
    source_url: str = "",
) -> None:
    notification = {
        "type": "file_processed",
        "kb_name": kb_name,
        "filename": filename,
        "learner_id": learner_id,
        "intent": result.get("intent", "?"),
        "route": result.get("route", "?"),
        "content_length": len(result.get("content", "")),
        "storage_ok": result.get("storage", {}).get("ok", False),
        "content_preview": result.get("content", "")[:300],
        "trace_id": trace_id or "",
        "source_url": source_url,
    }
    _cleanup_old_notifications()
    try:
        notif_dir = Path("/data/hermes/notifications")
        notif_dir.mkdir(parents=True, exist_ok=True)

        # 快速检查: 如果文件数远超上限, 跳过本次通知 (降级保护)
        try:
            existing = list(notif_dir.iterdir())
            if len(existing) > _MAX_NOTIFICATION_FILES * 1.5:
                logger.warning("[notify_ha] skipping — %d files exceeds limit", len(existing))
                return
        except OSError:
            pass
        notif_path = notif_dir / f"{trace_id or int(time.time())}.json"
        tmp_path = notif_path.with_suffix(".tmp")
        with open(tmp_path, "w", encoding="utf-8") as f:
            json.dump(notification, f, ensure_ascii=False)
        os.replace(tmp_path, notif_path)
        logger.info("[notify_ha] notification written: %s", notif_path)
    except Exception as e:
        logger.debug("[notify_ha] notification write failed: %s", e)


class QuizSyncRequest(BaseModel):
    session_id: str = ""
    learner_id: str = "default"
    results: list[dict] = []


async def _sync_quiz_with_retry(
    learner_id: str,
    results: list[dict],
    trace_id: str = "",
    max_retries: int = 3,
) -> dict:
    synced = 0
    failed = 0
    for item in results:
        topic = item.get("topic", "")
        correct = item.get("correct", False)
        domain = item.get("domain", "math")
        for attempt in range(max_retries):
            try:
                update_mastery(learner_id, f"{domain}/{topic}", correct)
                synced += 1
                break
            except Exception as e:
                logger.warning(
                    "[sync_quiz] retry %d/%d: %s/%s: %s",
                    attempt + 1, max_retries, learner_id, topic, e,
                )
                if attempt < max_retries - 1:
                    await asyncio.sleep(0.5)
                else:
                    failed += 1
    logger.info(
        "[sync_quiz] trace=%s learner=%s synced=%d failed=%d total=%d",
        trace_id, learner_id, synced, failed, len(results),
    )
    return {"ok": failed == 0, "synced": synced, "failed": failed, "total": len(results)}


BEIJING_TZ = timezone(timedelta(hours=8))


async def _session_cleanup_loop():
    """Daily 4-6 AM Beijing time: clear teacher bot session to prevent OOM.

    DeepTutor's AgentLoop accumulates session.messages indefinitely (the
    list is never trimmed — even after memory consolidation only the
    last_consolidated pointer advances).  Sending /new is the only way to
    free that memory without restarting the container.

    The 4-6 AM window is chosen because children in China are asleep, so
    no active conversations are disrupted.
    """
    while True:
        try:
            # Calculate seconds until next 4:00 AM Beijing time + jitter
            now_utc = datetime.now(timezone.utc)
            now_bj = now_utc.astimezone(BEIJING_TZ)
            target_bj = now_bj.replace(hour=4, minute=0, second=0, microsecond=0)
            if now_bj >= target_bj:
                target_bj += timedelta(days=1)

            wait = (target_bj - now_bj).total_seconds()
            wait += random.uniform(0, 1800)  # 0-30 min jitter, keep within 4-6 AM window

            logger.info(
                "[cleanup] Next session cleanup at +%.0f seconds (%.1f hours)",
                wait, wait / 3600,
            )
            await asyncio.sleep(wait)

            # Check if enough messages have accumulated to warrant cleanup
            global _session_msg_since_cleanup
            if _session_msg_since_cleanup < SESSION_CLEANUP_THRESHOLD:
                logger.info(
                    "[cleanup] Skipping — %d tutor_chat calls since last cleanup < threshold %d",
                    _session_msg_since_cleanup, SESSION_CLEANUP_THRESHOLD,
                )
                continue

            import websockets
            ws_url = f"ws://deeptutor:8001/api/v1/tutorbot/teacher/ws"
            ws = await asyncio.wait_for(
                websockets.connect(ws_url, close_timeout=5),
                timeout=15,
            )
            async with ws:
                await asyncio.wait_for(
                    ws.send(json.dumps({"content": "/new", "chat_id": "__cleanup__"})),
                    timeout=10,
                )
                # Drain response until done
                while True:
                    raw = await asyncio.wait_for(ws.recv(), timeout=10)
                    data = json.loads(raw)
                    if data.get("type") == "done":
                        break
            logger.info("[cleanup] Teacher bot session cleared")
            _session_msg_since_cleanup = 0
        except asyncio.CancelledError:
            break
        except Exception:
            logger.exception("[cleanup] Error during session cleanup")
            await asyncio.sleep(3600)


async def lifespan(app: FastAPI):
    # Legacy: UPLOADS_DIR no longer used by process_file/ingest endpoints (direct SOURCES_DIR write).
    # Keep _ensure_uploads_dir for backward compat with any external code writing there.
    _ensure_uploads_dir()
    try:
        await _init_provider()
    except Exception:
        logger.warning("Provider init failed, will retry on first request")

    # Phase B: enable MCP direct mode + mDNS
    try:
        _set_direct_mode(app)
        logger.info("MCP direct mode enabled — platform tools use ASGI transport")
    except Exception as e:
        logger.warning("MCP direct mode setup failed: %s", e)
    try:
        mcp_port = int(os.getenv("MERGED_MCP_PORT", "8100"))
        _start_mdns(mcp_port)
        logger.info("mDNS started on merged port %d", mcp_port)
    except Exception as e:
        logger.warning("mDNS start failed: %s", e)

    cleanup_task = asyncio.create_task(_session_cleanup_loop())
    dt_session_cleanup_task = asyncio.create_task(_dt_session_cleanup_loop())

    # Phase B: initialize FastMCP session manager (task group)
    _mcp_lifespan_task: asyncio.Task | None = None
    _mcp_lifespan_receive: asyncio.Queue | None = None
    try:
        _mcp_lifespan_task, _mcp_lifespan_receive = await _start_mcp_lifespan()
    except Exception as e:
        logger.warning("FastMCP lifespan init failed: %s", e)

    try:
        from tutor_platform.ingest_status import IngestStatusTracker
        orphans = IngestStatusTracker.get_orphaned()
        if orphans:
            logger.warning(
                "[lifespan] Found %d orphaned ingest task(s) — marking as failed",
                len(orphans),
            )
            for o in orphans[:5]:
                logger.warning(
                    "  orphan: %s stage=%s age=%.0fs",
                    o.get("trace_id"), o.get("stage"),
                    time.time() - o.get("ts", 0),
                )
                IngestStatusTracker.mark(
                    o["trace_id"], "orphaned_on_restart",
                    {"prev_stage": o.get("stage")},
                )
    except Exception:
        logger.debug("Orphan scan skipped", exc_info=True)

    yield
    cleanup_task.cancel()
    dt_session_cleanup_task.cancel()
    try:
        await cleanup_task
    except asyncio.CancelledError:
        pass
    try:
        await dt_session_cleanup_task
    except asyncio.CancelledError:
        pass
    # Phase B: shutdown FastMCP lifespan
    if _mcp_lifespan_receive is not None:
        try:
            await _stop_mcp_lifespan(_mcp_lifespan_receive)
        except Exception as e:
            logger.warning("FastMCP lifespan shutdown error: %s", e)
    try:
        reset_provider_instance()
        logger.info("Provider reset")
    except Exception as e:
        logger.warning("Provider reset error: %s", e)


app = FastAPI(title="Platform API", version="7.0.0", lifespan=lifespan)

# ── Phase B: MCP Server merge ──
# FastMCP ASGI app needs lifespan events to initialize its session
# manager (task group). Since Starlette Mount lifespan forwarding may
# not reach it reliably in all configurations, we initialize the
# lifespan manually in the parent FastAPI lifespan handler.
_mcp_asgi = mcp_fastmcp.streamable_http_app()
_mcp_asgi_ready = False


async def _start_mcp_lifespan():
    """Initialize FastMCP session manager by running lifespan.startup."""
    global _mcp_asgi_ready
    receive_queue = asyncio.Queue()
    send_queue = asyncio.Queue()

    async def _receive():
        return await receive_queue.get()

    async def _send(msg):
        await send_queue.put(msg)

    task = asyncio.create_task(_mcp_asgi({"type": "lifespan"}, _receive, _send))
    await receive_queue.put({"type": "lifespan.startup"})
    result = await send_queue.get()
    if result["type"] != "lifespan.startup.complete":
        raise RuntimeError(f"MCP lifespan startup failed: {result}")
    _mcp_asgi_ready = True
    logger.info("FastMCP session manager initialized")
    return task, receive_queue


async def _stop_mcp_lifespan(receive_queue):
    """Shutdown FastMCP session manager."""
    await receive_queue.put({"type": "lifespan.shutdown"})


# Lightweight middleware: catch /mcp and /mcp/* before Starlette routing
# and forward directly to the initialized FastMCP ASGI app.
class _MCPASGIMiddleware:
    """Route /mcp* requests directly to FastMCP ASGI app."""
    def __init__(self, app):
        self.app = app

    async def __call__(self, scope, receive, send):
        if scope["type"] == "http":
            path = scope.get("path", "")
            if path == "/mcp" or path.startswith("/mcp/"):
                if scope.get("method") == "POST":
                    await _handle_mcp_post(scope, receive, send, _mcp_asgi)
                    return
                await _mcp_asgi(scope, receive, send)
                return
        await self.app(scope, receive, send)


app.add_middleware(_MCPASGIMiddleware)


# Static source file serving (from mcp_server, for view_source tool)
app.mount("/sources", _serve_source_file)


# Minimal bind-qr bootstrap page (Phase B: no self-calls, client-side JS fetches QR)
_BIND_QR_HTML = """<!DOCTYPE html>
<html lang="zh-CN">
<head><meta charset="UTF-8"><meta name="viewport" content="width=device-width,initial-scale=1.0">
<title>绑定 AI 教学助手</title>
<style>
*{margin:0;padding:0;box-sizing:border-box}
body{font-family:-apple-system,BlinkMacSystemFont,sans-serif;background:#f5f5f5;display:flex;justify-content:center;padding:20px}
.container{max-width:400px;width:100%}
h1{font-size:20px;text-align:center;margin:20px 0 12px;color:#333}
.status{background:#fff;border-radius:12px;padding:16px;margin:12px 0;box-shadow:0 1px 4px rgba(0,0,0,.08)}
.status-row{display:flex;justify-content:space-between;padding:6px 0;font-size:14px}
.label{color:#888}.value{font-weight:600}
.ok{color:#2e7d32}.warn{color:#e65100}.err{color:#c62828}
.qr-wrapper{text-align:center;margin:16px 0}
.qr{max-width:280px;border-radius:8px}
.footer{text-align:center;font-size:12px;color:#999;margin:20px 0}
#qr-img{display:none}.loading{text-align:center;color:#888;font-size:14px;padding:40px 0}
</style>
</head>
<body>
<div class="container">
<h1>📚 绑定 AI 教学助手</h1>
<div class="status" id="status"><div class="loading">正在加载状态...</div></div>
<div id="qr-section" style="display:none">
<div class="qr-wrapper"><img id="qr-img" class="qr" alt="QR Code"></div>
<p class="footer" id="qr-hint"></p>
</div>
<p class="footer">首次绑定后即可通过微信与 AI 家庭教师对话</p>
</div>
<script>
async function refresh(){try{
let r=await fetch('/api/bot/qrcode?refresh=1');
if(r.ok&&r.headers.get('content-type').includes('image/png')){
let blob=await r.blob();
document.getElementById('qr-img').src=URL.createObjectURL(blob);
document.getElementById('qr-img').style.display='block';
document.getElementById('qr-section').style.display='block';
document.getElementById('status').innerHTML=
  '<div class="status-row"><span class="label">系统状态</span><span class="value ok">✓ 运行中</span></div>'+
  '<div class="status-row"><span class="label">微信服务</span><span class="value ok">✓ 可使用</span></div>';
document.getElementById('qr-hint').textContent='请使用微信「扫一扫」扫描上方二维码';
}else{
document.getElementById('status').innerHTML=
  '<div class="status-row"><span class="label">系统状态</span><span class="value ok">✓ 运行中</span></div>'+
  '<div class="status-row"><span class="label">微信服务</span><span class="value warn">⟳ 准备中</span></div>';
setTimeout(refresh,5000);
}}catch(e){
document.getElementById('status').innerHTML=
  '<div class="status-row"><span class="label">系统状态</span><span class="value err">✗ 启动中</span></div>'+
  '<div class="status-row"><span class="label">微信服务</span><span class="value err">—</span></div>';
setTimeout(refresh,10000);
}}
refresh();
</script>
</body>
</html>"""


@app.get("/")
@app.get("/bind-qr")
async def bind_qr_page():
    from fastapi.responses import HTMLResponse
    return HTMLResponse(_BIND_QR_HTML)


@app.middleware("http")
async def trace_id_middleware(request: Request, call_next):
    trace_id = _extract_trace_id(request)
    request.state.trace_id = trace_id
    response = await call_next(request)
    response.headers["X-Trace-ID"] = trace_id
    return response


@app.get("/api/ingest/status/{trace_id}")
def api_ingest_status(trace_id: str):
    from tutor_platform.ingest_status import IngestStatusTracker
    entry = IngestStatusTracker.get(trace_id)
    if not entry:
        return {"ok": False, "error": "trace_id not found"}
    return {"ok": True, "trace_id": trace_id, "status": entry}


@app.get("/api/source/{trace_id}")
def api_source_by_trace_id(trace_id: str):
    """根据 trace_id 查询归档的原始文件 (场景三: 原始资料检索).

    docs/business_scenarios.md 第99行: source_url 在 MCP 工具层面可通过 /api/source/{trace_id} 查询.
    """
    sources_dir = SOURCES_DIR
    if not os.path.isdir(sources_dir):
        return {"ok": False, "error": "归档目录不存在"}

    prefix = f"{trace_id}_"
    try:
        matches = sorted(f for f in os.listdir(sources_dir) if f.startswith(prefix))
    except OSError as e:
        return {"ok": False, "error": str(e)}

    if not matches:
        return {"ok": False, "error": f"未找到 trace_id={trace_id} 的归档文件"}

    files = []
    for filename in matches:
        filepath = os.path.join(sources_dir, filename)
        try:
            st = os.stat(filepath)
            files.append({
                "filename": filename,
                "size": st.st_size,
                "source_url": f"/sources/{filename}",
            })
        except OSError:
            pass

    return {"ok": True, "trace_id": trace_id, "files": files}


@app.get("/api/kb/search")
def api_kb_search(query: str = "", kb_name: str = "tutoring", top_k: int = 5):
    """ChromaDB 知识库搜索 (v7.0: PersistentClient 内嵌)."""
    if not query.strip():
        return {"ok": False, "error": "query is required"}
    try:
        import chromadb
        client = chromadb.PersistentClient(path="/data/chroma")
        try:
            from tutor_platform.tools.embeddings import RkllamaEmbeddingFunction
            coll = client.get_collection(
                kb_name,
                embedding_function=RkllamaEmbeddingFunction(),
            )
        except Exception:
            coll = client.get_collection(kb_name)
        results = coll.query(query_texts=[query], n_results=top_k)
        docs = results.get("documents", [[]])[0]
        metas = results.get("metadatas", [[]])[0]
        distances = results.get("distances", [[]])[0]
        items = []
        for i, doc in enumerate(docs):
            items.append({
                "text": doc[:500],
                "score": round(1.0 - min(float(distances[i]) if i < len(distances) else 0, 1.0), 3),
                "source": metas[i].get("source", "") if i < len(metas) else "",
                "learner_id": metas[i].get("learner_id", "") if i < len(metas) else "",
                "trace_id": metas[i].get("trace_id", "") if i < len(metas) else "",
            })
        return {"ok": True, "results": items, "source": "chromadb", "total": len(items)}
    except ImportError:
        return {"ok": False, "error": "chromadb not available"}
    except Exception as e:
        return {"ok": False, "error": str(e), "source": "chromadb"}


@app.post("/api/ingest/proxy/{kb_name}")
async def api_ingest_proxy(
    kb_name: str,
    file: UploadFile = File(...),
    learner_id: str = Form("default"),
    request: Request = None,
):
    content = await file.read()
    error = _validate_file(file.filename, len(content))
    if error:
        return {"ok": False, "error": error}
    tool_name = request.headers.get("X-Tool-Name", "") if request else ""
    trace_id = _extract_trace_id(request) if request else _generate_trace_id()
    sources_dir = SOURCES_DIR
    os.makedirs(sources_dir, exist_ok=True)
    safe = os.path.basename(file.filename or "unknown")
    archive_name = f"{trace_id}_{int(time.time())}_{safe}"
    dest = os.path.join(sources_dir, archive_name)
    with open(dest, "wb") as f:
        f.write(content)
    source_url = f"/sources/{archive_name}"

    if learner_id == "default":
        logger.warning("[%s] ingest_proxy called with default learner_id", trace_id)
    from tutor_platform.ingest_status import IngestStatusTracker
    IngestStatusTracker.mark(trace_id, "processing", {
        "filename": file.filename or "", "kb_name": kb_name, "source": "web",
    })
    provider = await _get_provider()
    result = await _handle_inbound_file(
        file_path=dest,
        metadata={
            "source": "web", "kb_name": kb_name, "learner_id": learner_id,
            "trace_id": trace_id, "tool_name": tool_name,
        },
    )
    IngestStatusTracker.mark(trace_id, "completed", {
        "intent": result.get("intent"), "route": result.get("route"),
        "storage_ok": result.get("storage", {}).get("ok", False),
    })
    asyncio.create_task(_notify_hermes_agent(
        kb_name=kb_name, filename=os.path.basename(file.filename) if file.filename else "unknown",
        learner_id=learner_id, result=result,
        trace_id=trace_id, source_url=source_url,
    ))
    return {
        "ok": True, "trace_id": trace_id, "status": "completed",
        "filename": file.filename, "kb_name": kb_name,
        "intent": result.get("intent"), "route": result.get("route"),
        "content_len": len(result.get("content", "")),
    }


@app.post("/api/ingest/proxy")
async def api_create_kb_and_ingest(
    kb_name: str = Form(...),
    file: UploadFile = File(...),
    learner_id: str = Form("default"),
    request: Request = None,
):
    content = await file.read()
    error = _validate_file(file.filename, len(content))
    if error:
        return {"ok": False, "error": error}
    tool_name = request.headers.get("X-Tool-Name", "") if request else ""
    trace_id = _extract_trace_id(request) if request else _generate_trace_id()
    sources_dir = SOURCES_DIR
    os.makedirs(sources_dir, exist_ok=True)
    safe = os.path.basename(file.filename or "unknown")
    archive_name = f"{trace_id}_{int(time.time())}_{safe}"
    dest = os.path.join(sources_dir, archive_name)
    with open(dest, "wb") as f:
        f.write(content)
    source_url = f"/sources/{archive_name}"

    if learner_id == "default":
        logger.warning("[%s] create_kb_and_ingest called with default learner_id", trace_id)
    from tutor_platform.ingest_status import IngestStatusTracker
    IngestStatusTracker.mark(trace_id, "processing", {
        "filename": file.filename or "", "kb_name": kb_name, "source": "web",
    })
    provider = await _get_provider()
    result = await _handle_inbound_file(
        file_path=dest,
        metadata={
            "source": "web", "kb_name": kb_name, "learner_id": learner_id,
            "trace_id": trace_id, "tool_name": tool_name,
        },
    )
    IngestStatusTracker.mark(trace_id, "completed", {
        "intent": result.get("intent"), "route": result.get("route"),
        "storage_ok": result.get("storage", {}).get("ok", False),
    })
    asyncio.create_task(_notify_hermes_agent(
        kb_name=kb_name, filename=os.path.basename(file.filename) if file.filename else "unknown",
        learner_id=learner_id, result=result,
        trace_id=trace_id, source_url=source_url,
    ))
    return {
        "ok": True, "trace_id": trace_id, "status": "completed",
        "filename": file.filename, "kb_name": kb_name,
        "intent": result.get("intent"), "route": result.get("route"),
        "content_len": len(result.get("content", "")),
    }


@app.post("/api/process/file")
async def api_process_file(request: Request):
    trace_id = getattr(request.state, 'trace_id', None) or _generate_trace_id()
    tool_name = request.headers.get("X-Tool-Name", "")
    content_type = request.headers.get("content-type", "")

    if "application/json" in content_type:
        body = await request.json()
        file_path = body.get("file_path", "")
        kb_name = body.get("kb_name", "tutoring")
        learner_id = body.get("learner_id", "default")
        auto_teach = body.get("auto_teach", False)
        file = None
    elif "multipart/form-data" in content_type:
        form = await request.form()
        file = form.get("file")
        file_path = form.get("file_path", "")
        kb_name = form.get("kb_name", "tutoring")
        learner_id = form.get("learner_id", "default")
        auto_teach = form.get("auto_teach", "false") in ("true", "1", "yes")
    else:
        return {"ok": False, "error": "Unsupported content-type; use JSON or multipart/form-data"}

    if learner_id == "default":
        logger.warning("[%s] process_file called with default learner_id", trace_id)

    provider = await _get_provider()
    if file and file.filename:
        content = await file.read()
        error = _validate_file(file.filename, len(content))
        if error:
            return {"ok": False, "error": error}
        # 直接写入 SOURCES_DIR 归档目录，trace_id 前缀避免重名
        sources_dir = SOURCES_DIR
        os.makedirs(sources_dir, exist_ok=True)
        safe = os.path.basename(file.filename)
        archive_name = f"{trace_id}_{int(time.time())}_{safe}"
        dest = os.path.join(sources_dir, archive_name)
        with open(dest, "wb") as f:
            f.write(content)
        file_path_ref = dest
        source_url = f"/sources/{archive_name}"
    elif file_path:
        file_path_ref = file_path
        source_url = ""
    else:
        return {"ok": False, "error": "No file uploaded and no file_path provided"}

    _file_ext = os.path.splitext(file_path_ref)[1].lower() if file_path_ref else ""
    if _file_ext in {".jpg", ".jpeg", ".png", ".webp", ".bmp"}:
        global _ocr_warmed
        if not _ocr_warmed:
            _ocr_warmed = _load_ocr_warmed()  # 先检查持久化标记
        if not _ocr_warmed:
            await _warm_ocr_model(trace_id)
            _ocr_warmed = True
            _save_ocr_warmed()

    from tutor_platform.ingest_status import IngestStatusTracker
    IngestStatusTracker.mark(trace_id, "processing", {
        "source": "mcp", "kb_name": kb_name, "learner_id": learner_id,
    })

    result = await _handle_inbound_file(
        file_path=file_path_ref,
        metadata={"source": "mcp", "kb_name": kb_name, "learner_id": learner_id,
                  "trace_id": trace_id, "tool_name": tool_name},
    )

    IngestStatusTracker.mark(trace_id, "completed", {
        "intent": result.get("intent"), "route": result.get("route"),
        "storage_ok": result.get("storage", {}).get("ok", False),
    })

    asyncio.create_task(_notify_hermes_agent(
        kb_name=kb_name,
        filename=file.filename if file and file.filename else os.path.basename(file_path_ref),
        learner_id=learner_id, result=result,
        trace_id=trace_id, source_url=source_url,
    ))

    # ── Update context cache + persist to disk for ALL files with OCR content ──
    # 后续 auto-trigger 或 tutor_chat 会从缓存取 context 更新 SOUL.md
    # 跳过 ocr_fallback 路线, 防止虚假上下文污染后续交互
    _ocr_content = result.get("content", "").strip()
    if _ocr_content and result.get("ok") and result.get("route") != "ocr_fallback":
        _last_tutor_context[learner_id] = _ocr_content
        if len(_last_tutor_context) > _MAX_CACHED_CONTEXTS:
            _last_tutor_context.clear()
        _save_context_to_disk(learner_id)
        logger.info("[%s] Cached context for %s (%d chars)", trace_id, learner_id, len(_ocr_content))

        # ── 入库: 异步将提取文本写入知识库 ChromaDB ──
        asyncio.create_task(_ingest_to_kb(
            provider=provider,
            content=_ocr_content,
            kb_name=kb_name,
            filename=file.filename if file and file.filename else os.path.basename(file_path_ref),
            learner_id=learner_id,
            source="weixin",
            trace_id=trace_id,
        ))

    # 🟢 v7.5: auto_teach 参数或 EDUCATION 意图 → 自动触发引导式教学
    # auto_teach=true 由 weixin.py 自动处理传入, 确保不依赖 LLM 自主调用
    # 跳过 ocr_fallback 路线 (OCR 失败, content 仅为"请学生重新输入"提示语)
    # 内容安全检查: 即使 auto_teach=true, 非教育内容不触发教学
    _auto_teach_effective = (
        auto_teach
        and result.get("ok")
        and result.get("route") != "ocr_fallback"
        and _is_educational_content(_ocr_content)
    ) or (result.get("intent") == "EDUCATION" and result.get("ok") and result.get("route") != "ocr_fallback")

    if _auto_teach_effective:
        content = _ocr_content
        if content:
            try:
                tutor_result = await _tutor_chat_core(
                    message="",
                    learner_id=learner_id,
                    context=content,
                    mode="guide",
                    trace_id=trace_id,
                )
                if tutor_result.get("ok") and tutor_result.get("content"):
                    result["tutor_content"] = tutor_result["content"]
                    logger.info("[%s] Auto tutor_chat success", trace_id)
                else:
                    logger.warning(
                        "[%s] Auto tutor_chat returned no content: %s",
                        trace_id, tutor_result.get("error", "empty response"),
                    )
            except Exception as e:
                logger.warning("[%s] Auto tutor_chat failed: %s", trace_id, e)

    return result


@app.post("/api/ingest/file")
async def api_ingest_file(
    file: UploadFile = File(...),
    kb_name: str = Form("tutoring"),
    learner_id: str = Form("default"),
    request: Request = None,
):
    trace_id = request.state.trace_id if request else _generate_trace_id()
    if learner_id == "default":
        logger.warning("[%s] ingest_file called with default learner_id", trace_id)
    tool_name = request.headers.get("X-Tool-Name", "") if request else ""
    filename = file.filename or "unknown.bin"
    content = await file.read()
    error = _validate_file(filename, len(content))
    if error:
        return {"ok": False, "error": error}
    sources_dir = SOURCES_DIR
    os.makedirs(sources_dir, exist_ok=True)
    archive_name = f"{trace_id}_{int(time.time())}_{filename}"
    dest = os.path.join(sources_dir, archive_name)
    with open(dest, "wb") as f:
        f.write(content)

    from tutor_platform.ingest_status import IngestStatusTracker
    IngestStatusTracker.mark(trace_id, "processing", {
        "source": "mcp", "kb_name": kb_name, "learner_id": learner_id,
    })

    provider = await _get_provider()
    result = await _handle_inbound_file(
        file_path=dest,
        metadata={"source": "mcp", "kb_name": kb_name, "learner_id": learner_id,
                  "trace_id": trace_id, "tool_name": tool_name},
    )

    IngestStatusTracker.mark(trace_id, "completed", {
        "intent": result.get("intent"), "route": result.get("route"),
        "storage_ok": result.get("storage", {}).get("ok", False),
    })

    asyncio.create_task(_notify_hermes_agent(
        kb_name=kb_name, filename=filename,
        learner_id=learner_id, result=result,
        trace_id=trace_id, source_url=f"/sources/{archive_name}",
    ))
    return result


_TEACHER_SOUL = """# Soul

你是一位耐心的 AI 家庭教师，通过微信为学生提供教育服务。

## 教学方式

你使用苏格拉底式教学法：只展示题目，用一个与题目知识点紧密相关的问题引导学生深入思考。
**绝不包含答案、分析、解析或解题步骤。**
**禁止使用"这个对吗？""对吗？""对不对？""你确定吗？"等无意义反问——这些问题没有引导学生思考，必须替换为针对具体知识点的提问。**

## 消息类型处理规则

根据收到的消息内容，选择以下对应策略：

### 学生提交整份试卷
如果消息包含"一、选择题"、"一、填空题"、"得分评卷人"等试卷特征：
- 这是一份试卷，包含多道题
- **每次只出一道题**（含完整题目和选项），不得省略
- 禁止列出所有题的标题或编号让学生选
- 学生答完当前题后**自动出下一道题**（不用等"下一题"）
- 第一轮只出题目和引导问题，不要提前给出答案或详细解析
- 学生给出答案后，判断对错并详细解释知识点和解题思路，然后自动出下一题

### 学生提交单个题目
如果消息不是整份试卷，而是一道具体题目：
- 只输出题目 + 一个引导性问题
- 第一轮不要给出答案或详细解析
- 学生给出答案后，判断对错并详细解释知识点和解题思路

### 学生回复答案（如 A/B/C/D 或简短回答）
- 判断对错，简要解释知识点和解题思路
- 如果当前有未完成的试卷，**直接输出下一道题**（含完整题目和选项），两步合一
- 不要等学生说"下一题"
- 用引导性问题结束

## 回复格式（严格遵循）

每次回复必须是以下格式之一：

### 格式 A：首次展示题目
```
第X题：题目内容
选项内容（如果有）

引导性问题？
```

### 格式 B：学生选择后继续引导
```
引导性问题？
```
（不重复题目，直接针对学生回答追问）

### 格式 C：学生答完后（判断 + 解释 + 下一题）
```
第X题 答对了/答错了。
【知识点：XXX】
简要解释...

第X+1题：题目内容
选项内容（如果有）

引导性问题？
```

## 示例

正确回复（首次展示）：
```
第6题：在下列考察中，是抽样调查的是（ ）
A. 了解全校学生人数
B. 调查某厂生产的鱼罐头质量
C. 调查杭州市出租车数量
D. 了解全班同学的家庭经济状况

想想看，全面调查需要逐一统计每个个体。哪个选项涉及的数量最大、逐一调查最不现实？
```

正确回复（学生选A后）：
```
全校学生人数确实可以逐个统计。但你再想想，哪种调查方式会破坏被调查的产品？
```

## 知识点教学规则

每道题都涉及一个或多个知识点。你的任务是让学生不仅做对题，更要理解背后的知识体系。

### 知识标注
- 每道题首次展示时，在题目后标注知识点：【知识点：XXX】
- 如果题目涉及多个知识点，标注核心知识点即可
- 示例：
  ```
  第3题：下列运算正确的是（ ）
  A. a²·a³=a⁶  B. (a²)³=a⁵  C. a⁶÷a²=a⁴  D. a²+a³=a⁵
  【知识点：幂的运算】
  幂的乘方和同底数幂的乘法有什么区别？
  ```

### 关联知识点串联
- 讲解时把当前知识点和之前做过的题目知识点做关联
- 示例："这道题考的是分式方程，和你刚才做错的因式分解有关，因为解分式方程的关键步骤就是因式分解"
- 帮助学生建立知识网络，而不是孤立地记每道题

### 错题归因分析
学生答错后，首先判断错因类型，针对性地解释：

- **概念不清**（选错定义类题目）：回归基本概念讲解，用生活类比
- **计算失误**（运算题步骤出错）：指出哪一步计算有误，不用重讲概念
- **审题问题**（漏看条件/误解题意）：引导重新读题，标注关键条件
- **知识混淆**（混淆两个相似概念）：对比区分两个概念的异同

如果无法判断错因，默认为概念不清处理。

## 分层引导策略

不要一步到位给答案。根据学生答题情况，逐步增加提示深度：

### 第一层：方向引导（首次展示或第一次答错）
- 指出题目涉及的知识点和章节
- 用一个开放性问题引导学生自己思考
- 示例："这道题是关于分式方程的。想想解分式方程的第一步应该做什么？"

### 第二层：方法提示（第一层引导后仍答错或犹豫）
- 给出解题方向或关键步骤提示
- 示例："注意，解分式方程的关键是去分母。回忆一下，去分母时两边要乘以什么？"

### 第三层：完整解析（第二层后仍答错）
- 给出完整解题过程
- 明确指出错误原因和正确方法
- 出一道同类巩固题确认是否真正掌握

## 巩固与进阶
- 学生答对后，出一道变式题确认真正掌握（改编数据/条件/问法）
- 学生连续答对同类题3道以上，标记该知识点已掌握，继续下一知识点
- 学生答错，出同类题加强巩固，不急于推进

## 禁止内容

- ❌ 答案、正确选项、选X（学生回答后的解析除外）
- ❌ 命题的定义、解析、分析、解答、考点
- ❌ 表格（| 选项 | 分析 | 结论 |）
- ❌ --- 分隔线
- ❌ ✅ / ❌ 标记
- ❌ 无意义引导问题（如"这个对吗？""对吗？""对不对？""你确定吗？"）
- ❌ 比较概念（如"全面调查 vs 抽样调查"）

## 答案评估标记（必须执行）

每次学生回答题目后，你在回复中判断对错时，**必须在回复末尾添加评估标记**：

```
[ANSWER:correct:知识点ID]
```
或
```
[ANSWER:wrong:知识点ID]
```

知识点ID 从题目中提取（如 `数学/代数/幂的运算`），使用 `学科/章/节` 三级格式。

### 示例
学生答错幂的运算题目，你的回复结尾应为：
```
...
第4题 答错了。
【知识点：幂的运算】
同底数幂相乘，底数不变指数相加。a²·a³ = a^(2+3) = a⁵，不是 a⁶。

[ANSWER:wrong:数学/代数/幂的运算]
```

这个标记对学生不可见（平台会处理后端移除），仅用于学习追踪和错题记录。

## 微信格式

- 微信不支持 LaTeX，公式用 Unicode 数学符号 (α β γ ∫ √ ∞ Δ π ² ³)
- 单条消息不超过 2048 字符
"""

_TEACHER_EXPLAIN_SOUL = """# Soul

你是一位资深学科讲解专家，为家长提供清晰完整的知识讲解。

## 教学方式

家长希望先学会知识点再去教孩子。你的讲解要求：
- ✅ 直接给出答案
- ✅ 分步骤详细解析
- ✅ 说明考察的知识点和适用年级
- ✅ 教孩子时可能的难点和常见错误提示

## 禁止内容

- 不要反问家长（如"你觉得呢？"、"你怎么想？"）
- 不要苏格拉底式引导
- 不要留悬疑

## 微信格式

- 微信不支持 LaTeX，公式用 Unicode 数学符号 (α β γ ∫ √ ∞ Δ π ² ³)
- 单条消息不超过 2048 字符
"""


class TutorChatResponse(BaseModel):
    content: str
    trace_id: str = ""


# Teaching context template — injected into LLM system prompt via context_prompt
# when process_file detects EDUCATION intent.  Guides the LLM to use Socratic
# teaching methodology instead of giving direct answers.
_TEACHING_CONTEXT_TEMPLATE = """## 教学引导上下文（硬性规则）

以下内容是学生提交的作业/试题内容。你在本次对话中**必须**遵守以下规则：

### 🔴 绝对禁止
- **不得直接给出答案或正确选项** — 即使学生追问也不能直接说
- **不得一次性展示所有选项的分析** — 最多点出 1 个易错点，然后把问题抛回给学生
- **不得在第一条回复中公布答案** — 必须先让学生思考
- **不得代替学生完成推理** — 你的角色是提问引导，不是解题演示

### 🟢 必须做
1. 选**一道题**（不要一次性涉及多题）
2. 把题目或选项发给学生
3. 用一个**提问**结束你的回复，例如：
   - "想想看，全面调查需要逐一统计每个个体。哪个选项涉及的数量最大、逐一调查最不现实？"
   - "分式的定义还记得吗？"
   - "先说说你的思路？"
4. **等学生回答后**，再针对他的回答给予反馈，继续引导
5. 只有学生尝试回答后，才能指出他对或错，并引导下一步

### 回复模板
每条回复的结构必须是：
```
[题目/概念] → [提问] → [等待学生回答]
```
不要出现 `答案：X`、`正确选项是X`、`选X` 这样的句式。

### 微信格式
- 微信不支持 LaTeX，公式用 Unicode 数学符号 (α β γ ∫ √ ∞ Δ π ² ³) 替代
- 选项用"回复 数字 说明"格式：正确: "回复 1 分式判断 / 2 概率"
- 单条消息不超过 2048 字符

### 学生提交内容
{content}

### 学生提问
{student_message}"""


@app.post("/api/tutor/context")
async def api_tutor_context(request: Request):
    """Return a teaching context prompt for LLM injection.

    Called by Hermes Agent gateway when process_file detects EDUCATION intent.
    Instead of generating a final answer (which TutorBot does), this endpoint
    returns a teaching context that gets injected into the LLM's system prompt
    via context_prompt, so the LLM itself generates the teaching response
    guided by the Socratic method rules.
    """
    trace_id = _extract_trace_id(request)
    body = await request.json()
    content = body.get("content", "")
    student_message = body.get("student_message", "")
    learner_id = body.get("learner_id", "default")

    if not content.strip() and not student_message.strip():
        return {"ok": False, "error": "content or student_message required"}

    teaching_context = _TEACHING_CONTEXT_TEMPLATE.format(
        content=content[:2000],
        student_message=student_message[:500] if student_message else "(无)",
    )

    return {"ok": True, "teaching_context": teaching_context, "trace_id": trace_id}


async def _update_soul_with_context(
    learner_id: str,
    context: str,
    mode: str = "guide",
) -> None:
    """Update DT's SOUL.md with current exam context so it's in the system prompt.

    DT's session key is fixed as "bot:{bot_id}" (ignores chat_id), so ALL
    messages share one monolithic session.  By injecting the current exam
    context into SOUL.md (loaded every turn via _load_bootstrap_files), the
    LLM always sees which exam is active, regardless of session history.

    使用 per-learner 锁防止并发写入竞态。
    """
    _persona = _TEACHER_EXPLAIN_SOUL if mode == "explain" else _TEACHER_SOUL
    _exam = context.strip()[:3000]
    if _exam:
        _persona += (
            "\n\n### 当前教学内容（优先级最高）\n"
            "学生当前正在做以下试卷中的题目，之前的试卷已全部结束、全部作废。\n"
            f"请完全专注于以下内容：\n\n{_exam}\n"
        )

    # Inject relevant knowledge from KB to supplement teaching.
    # Query ChromaDB for content related to the exam topic, excluding the
    # exam itself (skip results that overlap heavily with the current context).
    try:
        provider = get_provider_instance()
        _kb_results = provider.query("tutoring", [_exam], n_results=5)
        _kb_docs = _kb_results.get("documents", [[]])[0] if _kb_results else []
        _kb_found = []
        if _exam:
            _exam_normalized = _exam.replace(" ", "").replace("\n", "")[:200]
            for _doc in _kb_docs:
                _dc = _doc.strip()[:200]
                if not _dc:
                    continue
                _doc_normalized = _dc.replace(" ", "").replace("\n", "")
                # Skip if KB result is essentially the same as current exam
                _overlap = len(set(_exam_normalized) & set(_doc_normalized))
                _min_len = min(len(_exam_normalized), len(_doc_normalized)) or 1
                if _overlap / _min_len > 0.6:
                    continue
                _kb_found.append(_dc)
                if len(_kb_found) >= 3:
                    break
        if _kb_found:
            _persona += "\n### 相关知识库参考\n以下是与当前教学内容相关的背景知识点，可用于辅助讲解：\n"
            for _dc in _kb_found:
                _persona += f"- {_dc}\n"
    except Exception:
        logger.debug("KB query failed for %s, continuing", learner_id)

    # Inject due reviews (Ebbinghaus spaced repetition)
    try:
        _due = await asyncio.to_thread(get_due_reviews, learner_id)
        if _due:
            _lines = ["\n### 到期复习知识点（优先复习）\n以下知识点今天到期需要复习，请优先安排复习："]
            for r in _due[:5]:
                _name = r["kp_id"].split("/")[-1]
                _pct = int(r["level"] * 100)
                _lines.append(f"- {_name}（掌握度 {_pct}%，上次复习 {r['due_date']}）")
            _persona += "\n" + "\n".join(_lines) + "\n"
    except Exception:
        logger.debug("Due reviews lookup failed for %s, continuing", learner_id)

    # Inject learner weak points into SOUL.md for adaptive teaching.
    # DT reads this and adjusts its teaching strategy accordingly.
    try:
        _weak = await asyncio.to_thread(weak_points, learner_id)
        if _weak:
            _lines = ["\n### 该学生薄弱知识点（教学重点）\n以下知识点该学生掌握不足，请重点加强引导："]
            for w in _weak[:5]:
                _name = w["kp_id"].split("/")[-1]
                _pct = int(w["level"] * 100)
                _lines.append(f"- {_name}（正确率 {_pct}%，已答 {w['total']} 题）")
            _persona += "\n" + "\n".join(_lines) + "\n"

            # Add recent wrong answers for specific context
            _wrongs = await asyncio.to_thread(get_wrong_answers, learner_id, limit=3)
            if _wrongs:
                _persona += "\n### 近期错题记录\n以下题目学生最近答错，教学时注意关联：\n"
                for w in _wrongs:
                    _q = w.get("question", "")[:80]
                    _kp = w.get("kp_id", "").split("/")[-1]
                    _sa = w.get("student_answer", "")
                    _ca = w.get("correct_answer", "")
                    if _q:
                        _persona += f"- {_q}（知识点：{_kp}，学生回答：{_sa}，正确答案：{_ca}）\n"
    except Exception:
        logger.debug("Weak points lookup failed for %s, continuing", learner_id)

    lock = await _get_soul_lock()
    async with lock:
        global _soul_version
        try:
            async with httpx.AsyncClient(timeout=10) as _c:
                _r = await _c.get(f"{DEEPTUTOR_URL}/api/v1/tutorbot/teacher")
                if _r.status_code == 404:
                    await _c.post(f"{DEEPTUTOR_URL}/api/v1/tutorbot", json={
                        "bot_id": "teacher", "name": "AI 家庭教师", "persona": _persona,
                    })
                elif _r.status_code == 200:
                    await _c.patch(f"{DEEPTUTOR_URL}/api/v1/tutorbot/teacher", json={"persona": _persona})
                    _soul_version += 1
                    _ver = _soul_version
                    await _c.put(f"{DEEPTUTOR_URL}/api/v1/tutorbot/teacher/files/SOUL.md",
                                 json={"content": f"<!-- SOUL.md v{_ver} for learner:{learner_id} -->\n{_persona}"})
        except Exception:
            logger.warning("SOUL.md update failed for %s, continuing", learner_id)


# Answer evaluation marker pattern: [ANSWER:correct|wrong:kp_id]
_ANSWER_RE = re.compile(r'\n?\[ANSWER:(correct|wrong):([^\]]+)\]')


def _parse_answer_evaluation(content: str) -> tuple[str, str, str]:
    """Parse [ANSWER:result:kp_id] marker from DT response.

    Returns:
        (cleaned_content, is_correct, kp_id) — or (content, "", "") if no marker.
    """
    m = _ANSWER_RE.search(content)
    if not m:
        return content, "", ""
    result = m.group(1)  # "correct" or "wrong"
    kp_id = m.group(2).strip()
    cleaned = _ANSWER_RE.sub("", content).strip()
    return cleaned, result, kp_id


async def _trigger_practice_if_needed(learner_id: str, kp_id: str, trace_id: str) -> list[dict]:
    """Check wrong-answer threshold and generate practice questions.

    If the learner has >= 2 consecutive wrong answers for this KPI,
    generate practice questions and return them.
    """
    if not kp_id or not learner_id:
        return []

    # Check recent wrong answers for this KPI
    wrongs = get_wrong_answers(learner_id, kp_id=kp_id, limit=3)
    if len(wrongs) < 2:
        return []

    # Generate practice questions
    api_key = os.getenv("DEEPSEEK_API_KEY", "")
    llm_model = os.getenv("PRACTICE_LLM_MODEL") or os.getenv("LLM_MODEL", "deepseek-v4-flash")
    llm_url = os.getenv("PRACTICE_LLM_URL", "https://api.deepseek.com/v1/chat/completions")

    wrong_context_lines = []
    for wa in wrongs:
        q = wa.get("question", "")
        a = wa.get("correct_answer", "")
        sa = wa.get("user_answer", "")
        wrong_context_lines.append(f"- 题目: {q}")
        if a:
            wrong_context_lines.append(f"  正确答案: {a}")
        if sa:
            wrong_context_lines.append(f"  学生回答: {sa}")
    wrong_context = "\n".join(wrong_context_lines)

    topic = kp_id.split("/")[-1]
    domain = kp_id.split("/")[0] if "/" in kp_id else "general"

    system_prompt = (
        "你是一位资深学科出题专家。根据以下错题信息生成针对性练习题。\n\n"
        "出题规则：\n"
        "1. 题目必须与错题相关的知识点一致\n"
        "2. 难度适中，略低于或等于原题难度\n"
        "3. 题型以选择题为主\n"
        "4. 每道题必须包含正确答案和简要解析"
    )

    user_prompt = (
        f"## 学生错题记录\n{wrong_context}\n\n"
        f"## 要求\n请生成3道针对\"{topic}\"({domain})的练习题。\n\n"
        "只返回JSON，格式：{\"questions\":[{\"question\":\"...\",\"options\":{\"A\":\"...\",\"B\":\"...\",\"C\":\"...\",\"D\":\"...\"},\"correct_answer\":\"...\",\"explanation\":\"...\"}]}"
    )

    try:
        async with httpx.AsyncClient(timeout=60) as client:
            resp = await client.post(
                llm_url,
                json={
                    "model": llm_model,
                    "messages": [
                        {"role": "system", "content": system_prompt},
                        {"role": "user", "content": user_prompt},
                    ],
                    "temperature": 0.7,
                    "max_tokens": 2000,
                },
                headers={"Authorization": f"Bearer {api_key}"},
            )
            if resp.status_code != 200:
                logger.warning("[%s] auto-practice LLM error: HTTP %s", trace_id, resp.status_code)
                return []

            result = resp.json()
            content = result.get("choices", [{}])[0].get("message", {}).get("content", "")
            if not content:
                return []

            import json as json_lib
            import re
            try:
                questions = json_lib.loads(content)
            except json_lib.JSONDecodeError:
                m = re.search(r'```(?:json)?\s*([\s\S]*?)```', content)
                if m:
                    questions = json_lib.loads(m.group(1))
                else:
                    return []

            qs = questions.get("questions", [])
            logger.info("[%s] auto-practice: %d questions generated for %s/%s",
                        trace_id, len(qs), learner_id, kp_id)
            return qs
    except Exception as e:
        logger.warning("[%s] auto-practice failed: %s", trace_id, e)
        return []


# Auto-exam generation throttle: per-learner last-exam timestamp
_last_exam_time: dict[str, float] = {}
_EXAM_COOLDOWN = 86400  # 24 hours


async def _auto_generate_exam(learner_id: str, trace_id: str):
    """Background task: auto-generate exam paper and inject into teaching context.

    Throttled to once per 24h per learner.
    """
    now = time.time()
    last = _last_exam_time.get(learner_id, 0)
    if now - last < _EXAM_COOLDOWN:
        return

    _last_exam_time[learner_id] = now
    logger.info("[%s] Auto-generating exam for %s (weak points >= 3)", trace_id, learner_id)

    result = await _generate_exam_paper(learner_id, trace_id)
    if not result.get("ok"):
        logger.info("[%s] Auto-exam skipped for %s: %s", trace_id, learner_id, result.get("error"))
        return

    exam_text = result["exam_text"]
    # Save as pending exam context — next teaching interaction will pick it up
    _pending_exam_context[learner_id] = exam_text
    logger.info("[%s] Auto-exam generated for %s: %d questions, %d KPIs",
                trace_id, learner_id, result["total"], len(result.get("kp_covered", [])))


_pending_exam_context: dict[str, str] = {}


def _polish_guide_response(content: str) -> str:
    """Post-process guide-mode reply: strip forbidden emojis,
    ensure ends with a guiding question."""
    # Strip ✅/❌ (explicitly forbidden by SOUL.md)
    content = content.replace("✅", "").replace("❌", "")
    content = re.sub(r'\n{3,}', '\n\n', content).strip()
    # If DT Bot forgot the guiding question, append one
    if "？" not in content and "?" not in content:
        content += "\n\n你知道这道题的答案吗？"
    return content


async def _tutor_chat_core(
    message: str,
    learner_id: str,
    context: str,
    mode: str,
    trace_id: str,
) -> dict:
    """tutor_chat 核心逻辑 — 供 HTTP endpoint 和内部直接调用共用.

    Args:
        message: 学生消息 (follow-up turn)
        learner_id: 学习者标识
        context: 首轮上下文 (OCR 全文等), follow-up 留空
        mode: "guide" 或 "explain"
        trace_id: 追踪 ID
    Returns:
        {"ok": True, "content": "..."} 或 {"ok": False, "error": "..."}
    """
    if mode not in ("guide", "explain"):
        mode = "guide"

    if not message.strip() and not context.strip():
        return {"ok": False, "error": "message or context is required"}

    if learner_id == "default":
        logger.warning("[%s] tutor_chat called with default learner_id", trace_id)

    global _session_msg_since_cleanup
    _session_msg_since_cleanup += 1
    _t_start = time.time()

    # 1. Context cache: remember last teaching context per learner.
    #    Persisted to disk for container restart recovery.
    if context.strip():
        _last_tutor_context[learner_id] = context
        _last_question_num[learner_id] = 0
        if len(_last_tutor_context) > _MAX_CACHED_CONTEXTS:
            _last_tutor_context.clear()
        _save_context_to_disk(learner_id)

    # 2. Update SOUL.md with current exam context.
    #    DT re-reads SOUL.md on every system prompt build, so this is the
    #    PRIMARY mechanism for exam context (not DT session history).
    _t_bot = time.time()
    _soul_context = context.strip()
    if not _soul_context:
        _soul_context = _last_tutor_context.get(learner_id, "")
        if not _soul_context:
            _disk_ctx, _disk_qnum = _load_context_from_disk(learner_id)
            if not _disk_ctx and learner_id != "default":
                # 向后兼容: 重启前 context 存在 "default" key 下
                _disk_ctx, _disk_qnum = _load_context_from_disk("default")
                if _disk_ctx:
                    # 迁移到当前 learner_id, 删旧 key
                    _last_tutor_context[learner_id] = _disk_ctx
                    _last_question_num[learner_id] = _disk_qnum
                    _save_context_to_disk(learner_id)
                    try:
                        os.remove(_persist_path("default"))
                    except OSError:
                        pass
                    _soul_context = _disk_ctx
                    logger.info("[%s] Migrated context from 'default' to %s", trace_id, learner_id)
            if _disk_ctx and not _soul_context:
                _last_tutor_context[learner_id] = _disk_ctx
                _last_question_num[learner_id] = _disk_qnum
                _soul_context = _disk_ctx
                logger.info("[%s] Context restored from disk for %s", trace_id, learner_id)
    # Inject auto-generated exam context if available
    pending_exam = _pending_exam_context.pop(learner_id, "")
    if pending_exam:
        if _soul_context:
            _soul_context += "\n\n" + pending_exam
        else:
            _soul_context = pending_exam
        logger.info("[%s] Pending exam context injected for %s", trace_id, learner_id)

    if _soul_context != _last_soul_content.get(learner_id):
        await _update_soul_with_context(learner_id, _soul_context, mode)
        _last_soul_content[learner_id] = _soul_context

    # 3. Build payload — relay message as-is.
    #    NO instruction templates: all teaching strategy lives in SOUL.md.
    #    DT AgentLoop + SOUL.md handle everything (question selection,
    #    answer evaluation, auto-advance, mode).
    if context.strip():
        payload = context
        if message.strip():
            payload += "\n\n" + message
    else:
        payload = message

    # 4. Try local LLM first, cloud fallback.
    # When RKLLM_STUB_MODE=true (dev without real NPU), skip local LLM entirely.
    _llm_local = False
    if os.getenv("RKLLM_STUB_MODE", "").lower() != "true":
        try:
            await asyncio.wait_for(_llm_lock.acquire(), timeout=5)
            _llm_local = True
        except asyncio.TimeoutError:
            pass

    # ── Profile switching with cache: skip if DT already on the right profile ──
    global _last_llm_profile
    _profile_switched = False
    if _llm_local:
        if _last_llm_profile != ("rkllama", "r1-distill-1.5b"):
            if await _switch_dt_profile("rkllama", "r1-distill-1.5b", trace_id):
                _profile_switched = True
                _last_llm_profile = ("rkllama", "r1-distill-1.5b")
            else:
                _llm_lock.release()
                _llm_local = False
    if not _llm_local:
        if _last_llm_profile != ("deepseek", "deepseek-v4-flash"):
            _profile_switched = await _switch_dt_profile("deepseek", "deepseek-v4-flash", trace_id)
            if _profile_switched:
                _last_llm_profile = ("deepseek", "deepseek-v4-flash")

    # DT AgentLoop captures LLM config at bot start time and never refreshes.
    # After switching the catalog profile, stop the teacher bot so the next
    # WS connection triggers a fresh start with the new config.
    if _profile_switched:
        await _DTTutorSession.close_all()  # cached WS sessions reconnect with new config
        try:
            async with httpx.AsyncClient(timeout=10) as _sc:
                await _sc.delete(f"{DEEPTUTOR_URL}/api/v1/tutorbot/teacher")
        except Exception:
            pass

    # 5. Send via persistent WS session pool (reuses connection per learner).
    try:
        _session = await _DTTutorSession.get(learner_id)
        result = await _session.send_and_recv(payload, trace_id)

        # 5b. Fallback chain: Local weak → DeepSeek; Cloud empty → retry once.
        #     Both paths close the WS connection so the bot restarts with fresh config.
        _MIN_CHARS = 20
        _should_retry = False

        # --- Local LLM weak → fallback to DeepSeek ---
        if (_llm_local and (not result.get("ok")
                or len(result.get("content", "").strip()) < _MIN_CHARS)):
            logger.warning(
                "[%s] Local model weak (len=%d), falling back to DeepSeek",
                trace_id, len(result.get("content", "")),
            )
            _llm_lock.release()
            _llm_local = False
            _should_retry = True

        # --- Cloud returned empty → transient retry ---
        # Even without local LLM, a cloud call can return empty when DT's
        # provider config is stale (e.g. after binding change).  Closing the
        # WS forces a bot restart that re-reads the catalog.
        elif not _llm_local and not result.get("ok"):
            logger.warning(
                "[%s] Cloud returned empty (ok=%s), retrying once",
                trace_id, result.get("ok"),
            )
            _should_retry = True

        if _should_retry:
            # Ensure DT profile is on DeepSeek before retry
            if _last_llm_profile != ("deepseek", "deepseek-v4-flash"):
                _profile_switched = await _switch_dt_profile("deepseek", "deepseek-v4-flash", trace_id)
                if _profile_switched:
                    _last_llm_profile = ("deepseek", "deepseek-v4-flash")
            # Close WS so the next get() triggers a fresh bot start
            await _DTTutorSession.close_all()
            try:
                async with httpx.AsyncClient(timeout=10) as _sc:
                    await _sc.delete(f"{DEEPTUTOR_URL}/api/v1/tutorbot/teacher")
            except Exception:
                pass
            _soul_ctx = _soul_context or _last_tutor_context.get(learner_id, "")
            if _soul_ctx:
                await _update_soul_with_context(learner_id, _soul_ctx, mode)
            _session = await _DTTutorSession.get(learner_id)
            result = await _session.send_and_recv(payload, trace_id)

        # 6. Post-process: parse answer evaluation, polish guide mode.
        pending_practice = []
        if result.get("ok"):
            content = result.get("content", "")

            # Parse answer evaluation marker [ANSWER:correct|wrong:kp_id]
            cleaned, eval_result, eval_kp = _parse_answer_evaluation(content)
            if eval_result and eval_kp:
                content = cleaned
                is_correct = eval_result == "correct"
                await asyncio.to_thread(
                    update_mastery, learner_id, eval_kp, is_correct,
                    question="", user_answer="", correct_answer=""
                )
                # Schedule Ebbinghaus review based on updated level
                kp_data = await asyncio.to_thread(get_mastery, learner_id, eval_kp)
                await asyncio.to_thread(schedule_review, learner_id, eval_kp, kp_data.get("level", 0))
                logger.info("[%s] mastery update: %s %s=%s, review scheduled",
                            trace_id, learner_id, eval_kp, eval_result)

                # Trigger practice if wrong answers exceed threshold
                if not is_correct:
                    pending_practice = await _trigger_practice_if_needed(
                        learner_id, eval_kp, trace_id
                    )

                # Auto-trigger exam generation when weak points accumulate
                try:
                    weaks = await asyncio.to_thread(weak_points, learner_id)
                    if len(weaks) >= 3:
                        # Fire-and-forget: generate exam in background
                        asyncio.create_task(
                            _auto_generate_exam(learner_id, trace_id)
                        )
                except Exception:
                    pass

            if mode == "guide":
                content = _polish_guide_response(content)
            result["content"] = content

        # 7. Persist context + question number: 每次教学交互后保存,
        #    确保孩子隔天回来也能从中断处继续.
        if result.get("ok") and _last_tutor_context.get(learner_id):
            _save_context_to_disk(learner_id)

        logger.info(
            "[%s] tutor_chat timing: bot_setup=%.2fs total=%.2fs msg=%s profile_switched=%s",
            trace_id,
            _t_bot - _t_start,
            time.time() - _t_start,
            message[:30],
            _profile_switched,
        )
        result["pending_practice"] = pending_practice
        return result
    except Exception as e:
        logger.error("[%s] TutorBot failed: %s", trace_id, e)
        return {"ok": False, "error": f"教学引擎响应失败: {e}"}
    finally:
        if _llm_local:
            _llm_lock.release()


@app.post("/api/llm/acquire")
async def api_llm_acquire(request: Request):
    """Acquire the local LLM lock (blocks until available or timeout).

    Query params:
      - timeout: max wait in seconds (default 300, 0 = no wait)
    Returns 200 on success, 409 on timeout.
    """
    params = request.query_params
    timeout = float(params.get("timeout", "300"))
    try:
        if timeout <= 0:
            await _llm_lock.acquire()
        else:
            await asyncio.wait_for(_llm_lock.acquire(), timeout=min(timeout, 300.0))
        return {"ok": True, "acquired": True}
    except asyncio.TimeoutError:
        return Response(
            status_code=409,
            content=json.dumps({"ok": False, "error": "LLM resource busy", "acquired": False}),
            media_type="application/json",
        )


@app.post("/api/llm/release")
async def api_llm_release():
    """Release the local LLM lock."""
    try:
        _llm_lock.release()
        return {"ok": True, "released": True}
    except RuntimeError as e:
        return {"ok": False, "error": f"lock not held: {e}"}


@app.post("/api/tutor/chat")
async def api_tutor_chat(request: Request):
    """HTTP endpoint — 委托给 _tutor_chat_core."""
    trace_id = _extract_trace_id(request)
    body = await request.json()
    return await _tutor_chat_core(
        message=body.get("message", ""),
        learner_id=body.get("learner_id", "default"),
        context=body.get("context", ""),
        mode=body.get("mode", "guide"),
        trace_id=trace_id,
    )


@app.post("/api/solve")
async def api_solve(request: Request):
    """实时解题 — 将题目发给 DeepTutor solve 引擎, 返回逐步解答.

    内部通过 WebSocket 连接 DT 的 /api/v1/solve, 收集 Agent 推理结果后返回.
    """
    trace_id = _extract_trace_id(request)
    body = await request.json()
    question = body.get("question", "").strip()
    learner_id = body.get("learner_id", "default")
    detailed = body.get("detailed", False)

    if not question:
        return {"ok": False, "error": "question is required"}

    import websockets
    ws_url = f"ws://deeptutor:8001/api/v1/solve"
    try:
        ws = await asyncio.wait_for(
            websockets.connect(ws_url, close_timeout=10),
            timeout=30,
        )
        final_answer = ""
        error_msg = ""
        async with ws:
            # Send question
            await asyncio.wait_for(
                ws.send(json.dumps({
                    "question": question,
                    "detailed_answer": detailed,
                })),
                timeout=30,
            )
            # Collect all events until result or error
            while True:
                raw = await asyncio.wait_for(ws.recv(), timeout=300)
                data = json.loads(raw)
                msg_type = data.get("type", "")
                if msg_type == "result":
                    final_answer = data.get("final_answer", "")
                    break
                elif msg_type == "error":
                    error_msg = data.get("content", "解题引擎返回错误")
                    break

        if error_msg:
            logger.warning("[%s] solve error: %s", trace_id, error_msg)
            return {"ok": False, "error": error_msg}
        if not final_answer:
            return {"ok": False, "error": "解题引擎未返回有效解答"}

        logger.info("[%s] solve completed (%d chars)", trace_id, len(final_answer))
        return {"ok": True, "answer": final_answer, "trace_id": trace_id}

    except asyncio.TimeoutError:
        logger.error("[%s] solve WebSocket timeout", trace_id)
        return {"ok": False, "error": "解题引擎响应超时，请稍后再试"}
    except Exception as e:
        logger.error("[%s] solve failed: %s", trace_id, e)
        return {"ok": False, "error": f"解题引擎暂时不可用: {e}"}


@app.post("/api/vision/solve")
async def api_vision_solve(request: Request):
    """拍照解题 — 将题目图片发给 DeepTutor vision/solve 引擎.

    接收图片路径或 base64 数据 + 可选问题文本,
    内部通过 WebSocket 连接 DT 的 /api/v1/vision/solve, 返回图文解答.
    """
    trace_id = _extract_trace_id(request)
    body = await request.json()
    question = body.get("question", "").strip()
    image_data = body.get("image_data", "")  # base64 或文件路径
    learner_id = body.get("learner_id", "default")

    if not image_data:
        return {"ok": False, "error": "image_data is required"}

    # If image_data is a file path (not base64), read and encode it
    if not image_data.startswith("data:") and not image_data.startswith("/9j"):
        # Likely a file path — try to read from shared volumes
        actual_path = image_data
        for prefix, replacement in (
            ("/opt/data/child", "/data/hermes_child"),
            ("/opt/data", "/data/hermes"),
            ("/root/.hermes", "/data/hermes"),
        ):
            if image_data.startswith(prefix + "/") or image_data == prefix:
                actual_path = replacement + image_data[len(prefix):]
                break
        alt = os.path.join("/data/uploads", os.path.basename(image_data))
        if not os.path.exists(actual_path) and os.path.exists(alt):
            actual_path = alt

        if not os.path.exists(actual_path):
            return {"ok": False, "error": f"图片文件不存在: {image_data}"}

        import base64 as b64
        with open(actual_path, "rb") as f:
            raw = f.read()
        image_data = b64.b64encode(raw).decode()

    import websockets
    ws_url = f"ws://deeptutor:8001/api/v1/vision/solve"
    try:
        ws = await asyncio.wait_for(
            websockets.connect(ws_url, close_timeout=10),
            timeout=30,
        )
        text_parts: list[str] = []
        error_msg = ""
        async with ws:
            # Send image + question
            await asyncio.wait_for(
                ws.send(json.dumps({
                    "question": question,
                    "image_base64": image_data,
                })),
                timeout=30,
            )
            # Collect stream events
            while True:
                raw = await asyncio.wait_for(ws.recv(), timeout=300)
                data = json.loads(raw)
                msg_type = data.get("type", "")
                if msg_type == "text":
                    text_parts.append(data.get("content", ""))
                elif msg_type == "done":
                    break
                elif msg_type == "error":
                    error_msg = data.get("content", "视觉解题引擎返回错误")
                    break

        if error_msg:
            logger.warning("[%s] vision_solve error: %s", trace_id, error_msg)
            return {"ok": False, "error": error_msg}

        full_text = "\n".join(text_parts).strip()
        if not full_text:
            return {"ok": False, "error": "视觉解题引擎未返回有效解答"}

        logger.info("[%s] vision_solve completed (%d chars)", trace_id, len(full_text))
        return {"ok": True, "answer": full_text, "trace_id": trace_id}

    except asyncio.TimeoutError:
        logger.error("[%s] vision_solve WebSocket timeout", trace_id)
        return {"ok": False, "error": "视觉解题引擎响应超时，请稍后再试"}
    except Exception as e:
        logger.error("[%s] vision_solve failed: %s", trace_id, e)
        return {"ok": False, "error": f"视觉解题引擎暂时不可用: {e}"}


class IngestTextRequest(BaseModel):
    content: str
    kb_name: str = "tutoring"
    filename: str = ""
    source: str = "api"
    learner_id: str = "default"


@app.post("/api/ingest/text")
async def api_ingest_text(req: IngestTextRequest, request: Request = None):
    trace_id = _extract_trace_id(request) if request else _generate_trace_id()
    provider = await _get_provider()
    result = await provider.ingest_text(req.content, req.kb_name, req.filename, req.source, trace_id=trace_id)
    return result


class OCRRequest(BaseModel):
    image_data: str
    language: str = "zh"
    return_formulas: bool = True
    preprocess: bool = True


class VisionRequest(BaseModel):
    image_data: str
    question: str = ""


@app.post("/api/ocr")
async def api_ocr(req: OCRRequest, request: Request = None):
    import base64 as b64
    tool_name = request.headers.get("X-Tool-Name", "") if request else ""
    image_data = req.image_data
    if "," in image_data and image_data.startswith("data:"):
        image_data = image_data.split(",", 1)[1]
    if req.preprocess:
        try:
            from tutor_platform.tools.preprocess import preprocess_image_bytes
            raw = b64.b64decode(image_data)
            image_data = b64.b64encode(preprocess_image_bytes(raw)).decode()
        except ImportError:
            pass
        except Exception:
            pass
    provider = await _get_provider()
    return await provider.ocr(image_data, req.language, req.return_formulas, return_layout=True, tool_name=tool_name)


@app.post("/api/vision")
async def api_vision(req: VisionRequest, request: Request = None):
    import base64 as b64
    tool_name = request.headers.get("X-Tool-Name", "") if request else ""
    image_data = req.image_data
    if not image_data.startswith("data:"):
        image_data = f"data:image/png;base64,{image_data}"
    provider = await _get_provider()
    return await provider.vision(image_data, req.question, tool_name=tool_name)


@app.get("/api/mastery/")
def api_list_learners():
    """列出所有有掌握度数据的学习者."""
    mastery_root = "/data/mastery"
    if not os.path.isdir(mastery_root):
        return []
    try:
        learners = sorted(
            f.name.removesuffix(".json") for f in os.scandir(mastery_root)
            if f.is_file() and f.name.endswith(".json")
        )
        return learners
    except OSError as e:
        logger.warning("Failed to list learners: %s", e)
        return []


@app.get("/api/mastery/{learner_id}")
def api_get_mastery(learner_id: str, kp_id: str = ""):
    if kp_id:
        return get_mastery(learner_id, kp_id)
    from domains.tutoring.mastery import get_mastery_summary
    return get_mastery_summary(learner_id)


@app.get("/api/mastery/{learner_id}/wrong")
def api_get_wrong_answers(learner_id: str, kp_id: str = "", limit: int = 10):
    return get_wrong_answers(learner_id, kp_id=kp_id, limit=limit)


@app.get("/api/mastery/{learner_id}/weak")
def api_get_weak_points(learner_id: str):
    """返回薄弱知识点列表 (掌握度 < 0.6)."""
    return {"weak_points": weak_points(learner_id)}


@app.get("/api/mastery/{learner_id}/stats/weekly")
def api_get_weekly_stats(learner_id: str):
    """返回最近 7 天统计."""
    return get_weekly_stats(learner_id)


@app.get("/api/mastery/{learner_id}/stats/monthly")
def api_get_monthly_stats(learner_id: str):
    """返回最近 30 天统计."""
    return get_monthly_stats(learner_id)


@app.get("/api/mastery/{learner_id}/history")
def api_get_answer_history(learner_id: str, limit: int = 20, kp_id: str = ""):
    """返回答题历史."""
    return {"history": get_answer_history(learner_id, limit=limit, kp_id=kp_id)}


@app.post("/api/mastery/{learner_id}")
async def api_update_mastery(learner_id: str, req: dict):
    domain = req.get("domain", "math")
    topic = req.get("topic", "")
    correct = req.get("correct", False)
    kp_id = f"{domain}/{topic}" if topic else domain
    return await asyncio.to_thread(update_mastery, learner_id, kp_id, correct)


@app.get("/api/mastery/{learner_id}/report")
def api_get_report(learner_id: str):
    return generate_parent_report(learner_id)


@app.post("/api/report/generate")
async def api_generate_report(request: Request):
    """生成学习报告 (日报/周报/月报), 返回格式化文本.

    由 cron 触发的 MCP generate_report 工具调用后端.
    不推送, 只返回报告文本. 空结果 = 当日无学习记录.
    """
    from tutor_platform.report_push import (
        format_daily_report,
        format_parent_report_for_wechat,
        format_monthly_report_text,
    )

    body = await request.json()
    learner_id = body.get("learner_id", "default")
    report_type = body.get("type", "daily")

    if report_type == "daily":
        data = _load(learner_id)
        report = generate_daily_report(learner_id, data)
        if report["summary"]["total_questions"] == 0:
            return Response(content="", media_type="text/plain; charset=utf-8")
        text = format_daily_report(report)
    elif report_type == "weekly":
        report = generate_parent_report(learner_id)
        text = format_parent_report_for_wechat(learner_id, report)
    elif report_type == "monthly":
        report = generate_parent_report(learner_id)
        text = format_monthly_report_text(learner_id, report)
    else:
        return Response(content=f"未知报告类型: {report_type}", status_code=400)

    return Response(content=text, media_type="text/plain; charset=utf-8")


@app.post("/api/report/push")
async def api_report_push(request: Request):
    """生成并推送学习报告到微信.

    由 HA 定时任务触发, 为所有学习者生成报告后写入通知文件,
    Hermes Agent 消费后推送到家长的微信.
    """
    from tutor_platform.report_scheduler import (
        push_daily_reports,
        push_weekly_reports,
        push_monthly_reports,
    )

    body = await request.json()
    report_type = body.get("type", "daily")

    if report_type == "daily":
        results = await push_daily_reports()
    elif report_type == "weekly":
        results = await push_weekly_reports()
    elif report_type == "monthly":
        results = await push_monthly_reports()
    else:
        return Response(content=f"未知报告类型: {report_type}", status_code=400)

    pushed = sum(1 for r in results if r.get("ok"))
    return {
        "ok": True,
        "report_type": report_type,
        "total_learners": len(results),
        "pushed": pushed,
        "results": results,
    }


@app.post("/api/practice/generate")
async def api_generate_practice(request: Request):
    """根据错题生成针对性练习题.

    读取 learner 的错题记录, 通过 LLM 生成相似题目加强训练.
    在 quiz_review 返回错题后调用.
    """
    trace_id = _extract_trace_id(request)
    body = await request.json()
    learner_id = body.get("learner_id", "default")
    kp_id = body.get("kp_id", "")
    count = max(1, min(int(body.get("count", 3)), 10))

    # 1. Get wrong answers as context
    wrong_answers = get_wrong_answers(learner_id, kp_id=kp_id, limit=5)
    if wrong_answers:
        lines = []
        for wa in wrong_answers:
            q = wa.get("question", "")
            a = wa.get("correct_answer", "")
            sa = wa.get("student_answer", "")
            lines.append(f"- 题目: {q}")
            if a:
                lines.append(f"  正确答案: {a}")
            if sa:
                lines.append(f"  学生回答: {sa}")
        wrong_context = "\n".join(lines)
    elif kp_id:
        topic = kp_id.split("/")[-1]
        wrong_context = f"知识点: {topic}（暂无错题记录，建议巩固练习）"
    else:
        wrong_context = "全面巩固练习（无特定知识点）"

    domain = kp_id.split("/")[0] if "/" in kp_id else "general"
    topic = kp_id.split("/")[-1] if "/" in kp_id else kp_id or "综合"

    system_prompt = (
        "你是一位资深学科出题专家。根据以下错题信息生成针对性练习题，帮助学生巩固薄弱知识点。\n\n"
        "出题规则：\n"
        "1. 题目必须与错题相关的知识点一致\n"
        "2. 难度适中，略低于或等于原题难度\n"
        "3. 题型以选择题为主，可包含填空题\n"
        "4. 每道题必须包含正确答案和简要解析\n"
        "5. 题目用中文，适合中小学生\n"
        "6. 解析要简明扼要，指出考察点"
    )

    user_prompt = (
        f"## 学生错题记录\n{wrong_context}\n\n"
        f"## 要求\n请生成{count}道针对\"{topic}\"({domain})的练习题。\n\n"
        "请以JSON格式返回，格式如下：\n"
        "{\n"
        '  "questions": [\n'
        "    {\n"
        '      "question": "题目内容",\n'
        '      "question_type": "multiple_choice",\n'
        '      "options": {"A": "选项A", "B": "选项B", "C": "选项C", "D": "选项D"},\n'
        '      "correct_answer": "正确答案",\n'
        '      "explanation": "简要解析",\n'
        '      "difficulty": "easy"\n'
        "    }\n"
        "  ]\n"
        "}\n"
        "只返回JSON，不要其他文字。"
    )

    api_key = os.getenv("DEEPSEEK_API_KEY", "")
    llm_model = os.getenv("PRACTICE_LLM_MODEL") or os.getenv("LLM_MODEL", "deepseek-v4-flash")
    llm_url = os.getenv("PRACTICE_LLM_URL", "https://api.deepseek.com/v1/chat/completions")

    try:
        async with httpx.AsyncClient(timeout=120) as client:
            resp = await client.post(
                llm_url,
                json={
                    "model": llm_model,
                    "messages": [
                        {"role": "system", "content": system_prompt},
                        {"role": "user", "content": user_prompt},
                    ],
                    "temperature": 0.7,
                    "max_tokens": 3000,
                },
                headers={"Authorization": f"Bearer {api_key}"},
            )
            if resp.status_code != 200:
                logger.error("[%s] generate_practice LLM error: HTTP %s", trace_id, resp.status_code)
                return {"ok": False, "error": f"LLM API error: HTTP {resp.status_code}"}

            result = resp.json()
            content = result.get("choices", [{}])[0].get("message", {}).get("content", "")
            if not content:
                return {"ok": False, "error": "LLM returned empty response"}

            import json as json_lib
            try:
                questions = json_lib.loads(content)
            except json_lib.JSONDecodeError:
                import re
                m = re.search(r'```(?:json)?\s*([\s\S]*?)```', content)
                if m:
                    questions = json_lib.loads(m.group(1))
                else:
                    logger.error("[%s] generate_practice parse error: %s", trace_id, content[:500])
                    return {"ok": False, "error": "无法解析生成的题目"}

            qs = questions.get("questions", [])
            logger.info("[%s] generate_practice: %d questions for %s/%s",
                        trace_id, len(qs), learner_id, kp_id)
            return {
                "ok": True,
                "kp_id": kp_id,
                "questions": qs,
                "total": len(qs),
                "trace_id": trace_id,
            }
    except httpx.TimeoutException:
        logger.error("[%s] generate_practice LLM timeout", trace_id)
        return {"ok": False, "error": "生成超时，请稍后再试"}
    except Exception as e:
        logger.error("[%s] generate_practice failed: %s", trace_id, e)
        return {"ok": False, "error": str(e)}


async def _generate_exam_paper(
    learner_id: str,
    trace_id: str,
    kp_filter: str = "",
    question_count: int = 10,
) -> dict:
    """根据学情自动生成强化训练试卷。

    读取学习者的全部薄弱知识点和错题记录，通过 LLM 生成一份完整的
    强化训练试卷，格式为可注入 SOUL.md 的考试文本。

    Args:
        learner_id: 学习者标识
        trace_id: 追踪 ID
        kp_filter: 可选，限定知识点范围
        question_count: 题目总数

    Returns:
        {"ok": true, "exam_text": "...", "kp_cover": [...], "total": N}
        或 {"ok": false, "error": "..."}
    """
    # 1. Gather weak points and wrong answers
    weaks = weak_points(learner_id)
    if kp_filter:
        weaks = [w for w in weaks if kp_filter in w["kp_id"]]

    if not weaks:
        return {"ok": False, "error": "暂无薄弱知识点，无需生成强化训练"}

    wrongs = get_wrong_answers(learner_id, limit=15)
    if kp_filter:
        wrongs = [w for w in wrongs if kp_filter in w.get("kp_id", "")]

    # 2. Build LLM context
    weak_context_lines = ["## 学生薄弱知识点"]
    for w in weaks[:8]:
        kp_name = w["kp_id"].split("/")[-1]
        weak_context_lines.append(f"- {kp_name}（正确率 {int(w['level']*100)}%，已答 {w['total']} 题）")

    wrong_context_lines = ["## 近期错题记录"]
    for w in wrongs[:10]:
        q = w.get("question", "")[:100]
        ca = w.get("correct_answer", "")[:60]
        sa = w.get("user_answer", "")[:60]
        kp = w.get("kp_id", "").split("/")[-1]
        wrong_context_lines.append(f"- [{kp}] {q}")
        if ca:
            wrong_context_lines.append(f"  正确答案: {ca}  学生回答: {sa}")

    # 3. Determine grade/subject from KPIs
    subjects = set()
    for w in weaks:
        parts = w["kp_id"].split("/")
        if len(parts) >= 2:
            subjects.add(parts[0])
    subject_hint = "、".join(subjects) if subjects else "综合"

    # Count questions per weak KPI (distribute evenly)
    per_kpi = max(2, question_count // max(len(weaks), 1))

    system_prompt = (
        "你是一位资深中小学出题专家。根据学生的薄弱知识点和错题记录，"
        "生成一份完整的强化训练试卷帮助学生巩固提高。\n\n"
        "## 出题规则\n"
        "1. 试卷结构：包含选择题(约40%)、填空题(约30%)、解答题(约30%)\n"
        "2. 每道题都要标注对应的知识点\n"
        "3. 难度分布：基础题50%、中等题35%、拔高题15%\n"
        "4. 重点覆盖学生薄弱知识点，兼顾已错题目的同类变式\n"
        "5. 试卷要附有完整的参考答案和解析\n"
        "6. 题目用中文，适合中小学生\n\n"
        "## 输出格式\n"
        "必须以JSON格式返回，不要其他文字：\n"
        "{\n"
        '  "title": "试卷标题",\n'
        '  "sections": [\n'
        "    {\n"
        '      "type": "选择题",\n'
        '      "count": N,\n'
        '      "questions": [\n'
        "        {\n"
        '          "num": 1,\n'
        '          "question": "题目内容",\n'
        '          "options": {"A": "...", "B": "...", "C": "...", "D": "..."},\n'
        '          "kpi": "知识点ID",\n'
        '          "difficulty": "easy|medium|hard",\n'
        '          "correct_answer": "A",\n'
        '          "explanation": "解析"\n'
        "        }\n"
        "      ]\n"
        "    }\n"
        "  ]\n"
        "}"
    )

    user_prompt = (
        f"## 学科领域\n{subject_hint}\n\n"
        f"{weak_context_lines}\n\n"
        f"{wrong_context_lines}\n\n"
        f"## 要求\n请生成一份约{question_count}道题的强化训练试卷，"
        f"重点覆盖以上薄弱知识点。每个薄弱点至少出{per_kpi}道题。"
    )

    api_key = os.getenv("DEEPSEEK_API_KEY", "")
    llm_model = os.getenv("PRACTICE_LLM_MODEL") or os.getenv("LLM_MODEL", "deepseek-v4-flash")
    llm_url = os.getenv("PRACTICE_LLM_URL", "https://api.deepseek.com/v1/chat/completions")

    try:
        async with httpx.AsyncClient(timeout=180) as client:
            resp = await client.post(
                llm_url,
                json={
                    "model": llm_model,
                    "messages": [
                        {"role": "system", "content": system_prompt},
                        {"role": "user", "content": user_prompt},
                    ],
                    "temperature": 0.7,
                    "max_tokens": 6000,
                },
                headers={"Authorization": f"Bearer {api_key}"},
            )
            if resp.status_code != 200:
                logger.error("[%s] exam_paper LLM error: HTTP %s", trace_id, resp.status_code)
                return {"ok": False, "error": f"LLM API error: HTTP {resp.status_code}"}

            result = resp.json()
            content = result.get("choices", [{}])[0].get("message", {}).get("content", "")
            if not content:
                return {"ok": False, "error": "LLM returned empty response"}

            import json as json_lib
            import re
            try:
                paper = json_lib.loads(content)
            except json_lib.JSONDecodeError:
                m = re.search(r'```(?:json)?\s*([\s\S]*?)```', content)
                if m:
                    paper = json_lib.loads(m.group(1))
                else:
                    logger.error("[%s] exam_paper parse error: %s", trace_id, content[:500])
                    return {"ok": False, "error": "无法解析生成试卷"}

            # 4. Build exam text for SOUL.md injection
            title = paper.get("title", "强化训练")
            sections = paper.get("sections", [])
            exam_lines = [title, "=" * len(title), ""]
            all_questions = []
            kp_covered = set()

            for sec in sections:
                sec_type = sec.get("type", "题目")
                questions = sec.get("questions", [])
                if not questions:
                    continue
                exam_lines.append(f"## {sec_type}（共{len(questions)}题）")
                for q in questions:
                    num = q.get("num", len(all_questions) + 1)
                    text = q.get("question", "")
                    exam_lines.append(f"{num}. {text}")
                    opts = q.get("options", {})
                    for k, v in opts.items():
                        exam_lines.append(f"   {k}. {v}")
                    exam_lines.append("")
                    kpi = q.get("kpi", "")
                    if kpi:
                        kp_covered.add(kpi)
                    all_questions.append(q)
                exam_lines.append("")

            exam_lines.append("---")
            exam_lines.append("本试卷由AI根据学情自动生成，请在教师指导下使用。")

            exam_text = "\n".join(exam_lines)
            total = len(all_questions)
            logger.info(
                "[%s] exam_paper: %d questions covering %d KPIs for %s",
                trace_id, total, len(kp_covered), learner_id,
            )

            return {
                "ok": True,
                "exam_text": exam_text,
                "title": title,
                "kp_covered": sorted(kp_covered),
                "total": total,
                "sections": [
                    {"type": s["type"], "count": len(s.get("questions", []))}
                    for s in sections if s.get("questions")
                ],
            }
    except httpx.TimeoutException:
        logger.error("[%s] exam_paper LLM timeout", trace_id)
        return {"ok": False, "error": "生成超时，请稍后再试"}
    except Exception as e:
        logger.error("[%s] exam_paper failed: %s", trace_id, e)
        return {"ok": False, "error": str(e)}


@app.post("/api/practice/exam")
async def api_generate_exam(request: Request):
    """根据学情自动生成强化训练试卷。

    读取学习者的全部薄弱知识点和错题记录，生成一份完整的强化试卷。
    可选参数 kp_id 限定到特定知识点范围。
    生成的试卷可直接注入 SOUL.md 进入教学流程。
    """
    trace_id = _extract_trace_id(request)
    body = await request.json()
    learner_id = body.get("learner_id", "default")
    kp_id = body.get("kp_id", "")
    count = max(5, min(int(body.get("count", 10)), 30))

    result = await _generate_exam_paper(learner_id, trace_id, kp_id, count)
    result["trace_id"] = trace_id
    return result


@app.post("/api/practice/exam/push")
async def api_push_exam(request: Request):
    """生成强化训练试卷并推送到微信。

    生成试卷后写入通知文件，由 Hermes Agent 推送到家长/学生微信。
    """
    from tutor_platform.report_scheduler import _write_notification

    trace_id = _extract_trace_id(request)
    body = await request.json()
    learner_id = body.get("learner_id", "default")
    kp_id = body.get("kp_id", "")
    count = max(5, min(int(body.get("count", 10)), 30))

    result = await _generate_exam_paper(learner_id, trace_id, kp_id, count)
    if not result.get("ok"):
        return result

    # Write as notification for Hermes Agent to push to WeChat
    exam_text = result["exam_text"]
    title = result.get("title", "强化训练")
    push_content = (
        f"📝 {title}\n"
        f"─" * 20 + "\n"
        f"覆盖 {len(result.get('kp_covered', []))} 个薄弱知识点，"
        f"共 {result.get('total', 0)} 道题\n\n"
        f"{exam_text[:1800]}"
    )
    ok = _write_notification(learner_id, "exam", push_content)

    result["pushed"] = ok
    result["trace_id"] = trace_id
    return result


@app.post("/api/sync/quiz")
async def api_sync_quiz(req: QuizSyncRequest, request: Request = None):
    trace_id = request.state.trace_id if request else _generate_trace_id()
    learner_id = req.learner_id or "default"
    results = req.results or []
    if not results and req.session_id:
        logger.warning("[sync_quiz] trace=%s empty results for session=%s", trace_id, req.session_id)
        return {"ok": True, "synced": 0, "note": "no quiz results to sync"}
    return await _sync_quiz_with_retry(learner_id, results, trace_id)


@app.get("/admin/gc")
async def admin_gc():
    import gc
    before = {"collections": [gc.get_count()[i] for i in range(3)]}
    gc.collect()
    after = {"collections": [gc.get_count()[i] for i in range(3)]}
    mem_info = {"gc_triggered": True}
    try:
        import psutil
        proc = psutil.Process()
        mem = proc.memory_info()
        mem_info.update({
            "rss_mb": round(mem.rss / 1024 / 1024, 1),
            "vms_mb": round(mem.vms / 1024 / 1024, 1),
            "percent": round(proc.memory_percent(), 1),
        })
    except ImportError:
        mem_info["note"] = "psutil not installed"
    return {"ok": True, "gc_before": before, "gc_after": after, "memory": mem_info}


# iLink Bot 二维码缓存
_qr_code_cache: dict = {}

# iLink API 错误分类, 用于 /bind-qr 页面展示友好提示
QR_ERR_NOT_FOUND = "BOT_NOT_FOUND"   # Bot 未注册 / token 无效 (页面端判断用)
QR_ERR_NETWORK  = "NETWORK_ERROR"    # 网络不通或 iLink 服务端异常
QR_ERR_EMPTY    = "EMPTY_RESPONSE"   # 返回了空数据
QR_ERR_AUTH     = "BOT_NOT_FOUND"    # 页面端统一用 BOT_NOT_FOUND 判断未配置状态


def _categorize_ilink_error(exc: Exception) -> str:
    """将 iLink API 异常分类."""
    msg = str(exc).lower()
    if isinstance(exc, httpx.TimeoutException):
        return QR_ERR_NETWORK
    if isinstance(exc, httpx.HTTPStatusError):
        code = exc.response.status_code
        if code in (401, 403):
            return QR_ERR_AUTH
        if code >= 500:
            return QR_ERR_NETWORK
    if any(kw in msg for kw in ("connect", "resolve", "eof", "reset")):
        return QR_ERR_NETWORK
    return QR_ERR_NETWORK


@app.get("/api/bot/qrcode")
async def get_bot_qrcode(refresh: bool = False, text_only: bool = False,
                          bot_type: str = "parent"):
    """获取 iLink Bot 添加好友二维码。

    家长在微信中发送"加孩子学习"时, HA agent 通过 MCP tool 调用此接口,
    获取二维码图片后转发给家长, 家长可给孩子扫码加子网关。

    双网关架构:
      - bot_type="parent" → 使用 WEIXIN_TOKEN 生成家长机器人二维码
      - bot_type="child"  → 使用 CHILD_WEIXIN_TOKEN 生成孩子机器人二维码
      默认 parent 以保持向后兼容。

    refresh=true 时强制从 iLink 刷新二维码。
    text_only=true 时返回 liteapp URL 文本而非二维码图片
    (用于绕过 WeChat CDN 上传导致的网关卡死问题)。
    """
    import base64
    from io import BytesIO

    # 根据 bot_type 选择 token
    if bot_type == "child":
        token_key = "CHILD_WEIXIN_TOKEN"
        identity_path = "/data/hermes_child/.child_identity.json"
    else:
        token_key = "WEIXIN_TOKEN"
        identity_path = "/data/hermes/.parent_identity.json"
    weixin_token = os.getenv(token_key, "").strip()
    # 环境变量为空时降级读取持久身份文件 (Docker API 重启后 .env 不刷新)
    if not weixin_token and os.path.exists(identity_path):
        try:
            with open(identity_path, "r") as f:
                identity = json.load(f)
            weixin_token = (identity.get("token", "") or "").strip()
        except Exception:
            pass
    if not weixin_token:
        if bot_type == "child":
            return {
                "ok": False, "error": (
                    "孩子机器人尚未绑定，请先在设备终端运行: "
                    "bash scripts/setup_wechat_child.sh 完成绑定后再生成二维码。"
                ),
                "error_type": QR_ERR_AUTH,
            }
        # Parent token empty — device not yet bound.
        # 当 refresh=True 时禁止返回过期缓存（可能来自之前的子网关二维码或已过期的父网关二维码）
        cache = _qr_code_cache
        if cache.get("png_b64") and not refresh:
            import base64
            return Response(
                content=base64.b64decode(cache["png_b64"]),
                media_type="image/png",
                headers={"X-QR-Cached": "true", "X-QR-Error-Type": QR_ERR_AUTH},
            )
        return {
            "ok": False,
            "error": "家长机器人尚未绑定，请在浏览器中打开设备页面完成一键绑定",
            "error_type": QR_ERR_AUTH,
        }

    try:
        async with httpx.AsyncClient(timeout=15) as client:
            # iLink API 需要 App-Id 和 ClientVersion headers (与 hermes-agent weixin.py 一致)
            # 添加 Authorization: Bearer 确保返回"加好友"二维码而非"绑定"二维码
            qr_headers = {
                "iLink-App-Id": "bot",
                "iLink-App-ClientVersion": str((2 << 16) | (2 << 8) | 0),  # 131584
            }
            if weixin_token:
                qr_headers["Authorization"] = f"Bearer {weixin_token}"
            resp = await client.get(
                "https://ilinkai.weixin.qq.com/ilink/bot/get_bot_qrcode?bot_type=3",
                headers=qr_headers,
            )
            resp.raise_for_status()
            data = resp.json()
    except Exception as e:
        err_type = _categorize_ilink_error(e)
        # 无 token / token 无效时返回清晰提示
        if err_type == QR_ERR_AUTH:
            if bot_type == "child":
                err_msg = ("孩子机器人尚未绑定，无法生成子网关二维码。"
                           "请先让管理员在设备终端运行: bash scripts/setup_wechat_child.sh 完成绑定。")
            else:
                err_msg = ("家长您好，设备尚未绑定微信机器人，无法生成子网关二维码。"
                           "请先让管理员在设备终端运行: bash scripts/setup_wechat.sh 完成绑定。")
        elif err_type == QR_ERR_NETWORK:
            err_msg = "获取二维码失败: 网络连接异常，请检查设备网络后重试。"
        else:
            err_msg = f"获取二维码失败: {e}"
        cache = _qr_code_cache
        if cache.get("png_b64") and not refresh and not text_only:
            return Response(
                content=base64.b64decode(cache["png_b64"]),
                media_type="image/png",
                headers={
                    "X-QR-Cached": "true",
                    "X-QR-Error": str(e),
                    "X-QR-Error-Type": err_type,
                },
            )
        return {"ok": False, "error": err_msg, "error_type": err_type}

    qrcode_url = str(data.get("qrcode_img_content") or "")
    qrcode_value = str(data.get("qrcode") or "")
    qr_scan_data = qrcode_url if qrcode_url else qrcode_value
    logger.info("iLink QR response: url_len=%s qr_len=%s ret=%s",
                len(qrcode_url), len(qrcode_value), data.get("ret"))
    if not qr_scan_data:
        logger.warning("iLink returned empty QR data: %s", data)
        return {"ok": False, "error": "iLink 未返回二维码数据", "error_type": QR_ERR_EMPTY}

    # text_only bypasses image generation/CDN path (微信发图会导致网关卡死)
    if text_only:
        return {"ok": True, "url": qrcode_url, "code": qrcode_value}

    try:
        import qrcode as _qrlib
        _qr = _qrlib.QRCode(box_size=8, border=2)
        _qr.add_data(qr_scan_data)
        _qr.make(fit=True)
        img = _qr.make_image(fill_color="black", back_color="white")

        buf = BytesIO()
        img.save(buf, format="PNG")
        png_bytes = buf.getvalue()
        cache = _qr_code_cache
        cache["png_b64"] = base64.b64encode(png_bytes).decode()
        cache["qr_url"] = qrcode_url

        return Response(content=png_bytes, media_type="image/png")
    except ImportError:
        return {"ok": False, "error": "服务端缺少 qrcode 库 (qrcode[pil])"}
    except Exception as e:
        return {"ok": False, "error": f"生成二维码图片失败: {e}"}


# ── Docker exec helpers (for child bot binding) ──

_DOCKER_SOCKET = "/var/run/docker.sock"
_HERMES_CONTAINER = os.getenv("HERMES_CONTAINER", "deepseek-hermes_agent-1")


class _DockerStreamBuf:
    """Buffer for demuxing Docker multiplexed stream across arbitrary chunks.

    Docker raw-stream format (Tty=false):
      [1 byte stream_type] [3 bytes padding] [4 bytes big-endian length] [data]
    """
    def __init__(self):
        self._buf = bytearray()
        self._frames: list[tuple[int, bytes]] = []

    def feed(self, chunk: bytes) -> list[tuple[int, bytes]]:
        self._buf.extend(chunk)
        self._frames.clear()
        i = 0
        while i + 8 <= len(self._buf):
            stream_type = self._buf[i]
            frame_len = struct.unpack_from(">I", self._buf, i + 4)[0]
            start = i + 8
            end = start + frame_len
            if end > len(self._buf):
                break  # partial frame, wait for more data
            self._frames.append((stream_type, bytes(self._buf[start:end])))
            i = end
        # Keep any remaining partial data in buffer
        if i > 0:
            self._buf = self._buf[i:]
        return self._frames


_QR_LOGIN_SCRIPT = """
import asyncio, json, os, sys
from gateway.platforms.weixin import qr_login

old_out = sys.stdout
sys.stdout = sys.stderr  # QR text -> stderr (captured by API)
try:
    creds = asyncio.run(qr_login('/opt/data/child'))
finally:
    sys.stdout = old_out

if creds:
    # Atomic write for gateway_start.sh to read on restart
    tmp = '/opt/data/child/.child_identity.json.tmp'
    with open(tmp, 'w') as f:
        json.dump(creds, f, ensure_ascii=False)
    os.replace(tmp, '/opt/data/child/.child_identity.json')
    print(json.dumps(creds, ensure_ascii=False))
    sys.stdout.flush()
"""


async def _docker_exec_bind_child() -> dict:
    """Run qr_login in hermes_agent via Docker API, return QR text + credentials."""
    if not os.path.exists(_DOCKER_SOCKET):
        return {"ok": False, "error": "Docker socket not available, cannot bind child bot"}
    transport = httpx.AsyncHTTPTransport(uds=_DOCKER_SOCKET)
    async with httpx.AsyncClient(transport=transport, timeout=120) as client:
        # Create exec
        create_resp = await client.post(
            f"http://localhost/containers/{_HERMES_CONTAINER}/exec",
            json={
                "Cmd": ["python3", "-c", _QR_LOGIN_SCRIPT],
                "AttachStdout": True,
                "AttachStderr": True,
            },
        )
        if create_resp.status_code == 404:
            return {"ok": False, "error": f"容器 {_HERMES_CONTAINER} 未找到，请确认 hermes_agent 正在运行"}
        create_resp.raise_for_status()
        exec_id = create_resp.json()["Id"]

        # Start exec, read stream — QR comes within seconds on stderr
        stream_buf = _DockerStreamBuf()
        stderr_text = ""
        qr_text: str | None = None
        try:
            async with asyncio.timeout(25):
                async with client.stream(
                    "POST",
                    f"http://localhost/exec/{exec_id}/start",
                    json={"Detach": False, "Tty": False},
                ) as stream:
                    async for chunk in stream.aiter_bytes():
                        if qr_text:
                            break
                        frames = stream_buf.feed(chunk)
                        for stype, sdata in frames:
                            if stype == 2:  # stderr — QR content
                                stderr_text += sdata.decode("utf-8", errors="replace")
                                for line in stderr_text.split("\n"):
                                    line_s = line.strip()
                                    if line_s.startswith("https://") and "://" in line_s and len(line_s) > 10:
                                        qr_text = line_s
                                        break
                        if qr_text:
                            break
        except asyncio.TimeoutError:
            pass  # 25s timeout — enough for QR, exec continues waiting for scan

        if not qr_text and stderr_text:
            qr_text = stderr_text[:500]

        if not qr_text:
            return {"ok": False, "error": "无法从孩子机器人获取二维码，请检查 hermes_agent 日志"}

        return {"ok": True, "qr_text": qr_text}


def _clean_child_identity(data_dir: str):
    """清理已存在的孩子身份文件, 确保重新绑定时不受旧数据干扰."""
    targets = [
        os.path.join(data_dir, ".child_identity.json"),
        os.path.join(data_dir, ".child_identity.json.tmp"),
        os.path.join(data_dir, ".child_bound"),
    ]
    for path in targets:
        try:
            if os.path.exists(path):
                os.remove(path)
                logger.info("Cleaned old identity: %s", path)
        except OSError as e:
            logger.warning("Failed to clean %s: %s", path, e)


async def _docker_clean_child_identity():
    """通过 Docker API 在 hermes_agent 内清理旧身份数据."""
    if not os.path.exists(_DOCKER_SOCKET):
        return
    transport = httpx.AsyncHTTPTransport(uds=_DOCKER_SOCKET)
    async with httpx.AsyncClient(transport=transport, timeout=15) as client:
        # 清理 qr_login 可能残留的 identity 文件
        clean_cmd = "rm -f /opt/data/child/.child_identity.json /opt/data/child/.child_identity.json.tmp /opt/data/child/identity.json /opt/data/child/session.json 2>/dev/null; echo done"
        try:
            exec_resp = await client.post(
                f"http://localhost/containers/{_HERMES_CONTAINER}/exec",
                json={
                    "Cmd": ["sh", "-c", clean_cmd],
                    "AttachStdout": True,
                    "AttachStderr": False,
                },
            )
            if exec_resp.status_code == 200:
                eid = exec_resp.json()["Id"]
                await client.post(
                    f"http://localhost/exec/{eid}/start",
                    json={"Detach": True, "Tty": False},
                )
                logger.info("Docker clean: old child identity removed from container")
        except Exception as e:
            logger.warning("Docker clean error: %s", e)


@app.post("/api/bot/bind_child")
async def bind_child(force: bool = False):
    """创建孩子机器人身份并返回二维码。

    家长在微信中说"加孩子学习"时, HA agent 通过 MCP tool 调用此接口。
    通过 Docker socket 在 hermes_agent 容器中执行 qr_login() 创建新的孩子机器人身份,
    返回二维码图片供家长转发给孩子扫码绑定。

    绑定完成后, 凭据保存到共享卷 /data/hermes_child/.child_identity.json,
    由 cron 检测后持久化到 .env 并重启 hermes_agent 使子网关生效。

    force=true 时忽略已有绑定状态, 强制创建新身份 (用于重新绑定 / 换绑场景).
    """
    identity_file = "/data/hermes_child/.child_identity.json"
    child_token = os.getenv("CHILD_WEIXIN_TOKEN", "").strip()
    already_bound = bool(child_token or os.path.exists(identity_file))

    if already_bound and not force:
        return {"ok": False, "error": "孩子机器人已绑定", "hint": "请使用 get_bot_qrcode(bot_type='child') 获取子网关二维码; 如需重新绑定请设置 force=true"}

    if already_bound and force:
        logger.info("Force re-binding child bot — cleaning old identity...")
        # Clean old identity files so fresh qr_login doesn't conflict
        _clean_child_identity("/data/hermes_child")
        await _docker_clean_child_identity()
        # Reset restart sentinels so cron reprocesses new binding
        for s in ("/data/platform/.child_bind_restarted", "/data/platform/.child_bind_pending"):
            try:
                if os.path.exists(s):
                    os.remove(s)
                    logger.info("Reset %s for re-bind", s)
            except OSError as e:
                logger.warning("Failed to reset %s: %s", s, e)
        # Also clean any backup
        try:
            backup = "/data/platform/child_identity_backup.json"
            if os.path.exists(backup):
                os.remove(backup)
        except OSError:
            pass

    import base64
    from io import BytesIO

    # 清理过期缓存，确保返回的是新生成的子网关二维码
    _qr_code_cache.clear()

    result = await _docker_exec_bind_child()
    if not result.get("ok"):
        return result

    qr_scan_data = result["qr_text"]
    try:
        import qrcode as _qrlib

        _qr = _qrlib.QRCode(box_size=8, border=2)
        _qr.add_data(qr_scan_data)
        _qr.make(fit=True)
        img = _qr.make_image(fill_color="black", back_color="white")
        buf = BytesIO()
        img.save(buf, format="PNG")
        return Response(content=buf.getvalue(), media_type="image/png")
    except ImportError:
        return {"ok": True, "url": qr_scan_data, "text_only": True}
    except Exception as e:
        return {"ok": False, "error": f"生成二维码图片失败: {e}"}





# ═══════════════════════════════════════════════════════════
# 父网关重新绑定网关 (v7.4)
# ═══════════════════════════════════════════════════════════


async def _docker_clean_parent_identity() -> dict:
    """通过 Docker API 在 hermes_agent 内清理父网关身份文件."""
    if not os.path.exists(_DOCKER_SOCKET):
        return {"ok": False, "error": "Docker socket not available"}
    transport = httpx.AsyncHTTPTransport(uds=_DOCKER_SOCKET)
    async with httpx.AsyncClient(transport=transport, timeout=15) as client:
        clean_cmd = (
            "rm -f /opt/data/.parent_identity.json "
            "/opt/data/.parent_identity.json.tmp "
            "/opt/data/weixin/accounts/*.json "
            "2>/dev/null; echo done"
        )
        try:
            exec_resp = await client.post(
                f"http://localhost/containers/{_HERMES_CONTAINER}/exec",
                json={
                    "Cmd": ["sh", "-c", clean_cmd],
                    "AttachStdout": True,
                    "AttachStderr": False,
                },
            )
            if exec_resp.status_code == 404:
                return {"ok": False, "error": f"容器 {_HERMES_CONTAINER} 未找到"}
            if exec_resp.status_code != 200:
                return {"ok": False, "error": f"Docker exec 失败: {exec_resp.status_code}"}
            eid = exec_resp.json()["Id"]
            await client.post(
                f"http://localhost/exec/{eid}/start",
                json={"Detach": True, "Tty": False},
            )
            logger.info("Parent identity files cleaned via Docker exec")
            return {"ok": True}
        except Exception as e:
            return {"ok": False, "error": f"清理身份文件失败: {e}"}


@app.post("/api/bot/rebind_parent")
async def rebind_parent():
    """清除父网关凭据，触发下次启动时重新绑定.

    1. 清除 /config/.env 中的 WEIXIN_TOKEN / WEIXIN_ACCOUNT_ID
    2. Docker exec 清理 hermes_agent 中的身份文件
    3. 用户手动重启设备后, gateway_start.sh 检测到无凭据 → 进入引导模式

    顺序: 先清除 .env, 再清理文件. .env 写入失败时提前返回, 不破坏身份文件.
    """
    # Step 1: 清除 .env 中的凭据 (先于文件清理, 失败时提前返回)
    env_path = "/config/.env"
    try:
        if os.path.exists(env_path):
            with open(env_path, "r", encoding="utf-8") as f:
                lines = f.readlines()
            with open(env_path, "w", encoding="utf-8") as f:
                for line in lines:
                    stripped = line.strip()
                    if (stripped.startswith("WEIXIN_TOKEN=") or
                            stripped.startswith("WEIXIN_ACCOUNT_ID=")):
                        f.write(line.split("=")[0] + "=\n")
                    else:
                        f.write(line)
            logger.info("Cleared WEIXIN_TOKEN in %s", env_path)
    except OSError as e:
        return {"ok": False, "error": f"清除环境变量失败: {e}"}

    # Step 2: 清理身份文件 (Docker exec rm -f, 无流式解析, 1s 完成)
    clean_result = await _docker_clean_parent_identity()
    if not clean_result.get("ok"):
        return clean_result

    return {"ok": True, "message": "凭据已清除，请重启设备"}


@app.get("/api/bot/bootstrap_parent/status")
async def bootstrap_parent_status():
    """查询父网关引导绑定状态 (多层检测, 适应 Docker API 重启后 .env 不刷新的情况)."""
    # 1. 环境变量已设置 (正常 docker compose up -d 后)
    if os.getenv("WEIXIN_TOKEN", "").strip():
        return {"ok": True, "bound": True, "source": "env"}
    # 2. 持久身份文件存在 (Docker API 重启后 gateway_start.sh 降级读取)
    if os.path.exists("/data/hermes/.parent_identity.json"):
        return {"ok": True, "bound": True, "source": "identity_file"}
    # 3. 重启哨兵存在 (cron 已完成处理)
    if os.path.exists("/data/platform/.parent_bootstrap_restarted"):
        return {"ok": True, "bound": True, "source": "sentinel"}
    # 4. 结果文件存在且包含凭据 (正在扫码等待中或 cron 尚未处理)
    result_file = "/data/hermes/.parent_bootstrap_result.json"
    if os.path.exists(result_file):
        try:
            with open(result_file, "r") as f:
                creds = json.load(f)
            return {"ok": True, "bound": bool(creds.get("token", ""))}
        except (json.JSONDecodeError, OSError):
            return {"ok": True, "bound": False}
    return {"ok": True, "bound": False}


@app.get("/health")
async def health():
    provider_ok = _provider_error is None
    provider_uptime = (time.time() - _provider_init_time) if _provider_init_time > 0 else 0
    response = {
        "status": "ok" if provider_ok else "degraded",
        "service": "platform_api",
        "version": "7.0.0",
        "provider": {
            "ok": provider_ok,
            "uptime_s": round(provider_uptime, 0),
            "error": _provider_error,
        },
    }
    if provider_ok:
        try:
            provider = get_provider_instance()
            response["intent_stats"] = provider.intent_stats
        except Exception:
            pass
    return response


def run_provider_api(port: int = 8100):
    print(f"[provider_api] v7.0 starting on internal port {port}")
    print("[provider_api] Provider + trace_id + ChromaDB(PersistentClient) + Mastery")
    try:
        validation = validate_provider_config()
        if validation["errors"]:
            print(f"[provider_api] CONFIG ERRORS: {'; '.join(validation['errors'])}")
        if validation["warnings"]:
            print(f"[provider_api] CONFIG WARNINGS: {'; '.join(validation['warnings'])}")
        if not validation["errors"] and not validation["warnings"]:
            print("[provider_api] Config validation passed")
    except Exception as e:
        print(f"[provider_api] Config validation skipped: {e}")

    uvicorn.run(app, host="0.0.0.0", port=port, log_level="info")


if __name__ == "__main__":
    port = int(os.getenv("INGEST_PORT", "8100"))
    run_provider_api(port)
