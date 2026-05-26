"""
rkllama_server.py — 多模型 NPU 推理服务 (FastAPI)

基于 rknn-llm v1.2.3, 提供 OpenAI 兼容 API。
支持 3 类模型:
  - text:   DeepSeek-R1-Distill-Qwen-1.5B  (常驻)
  - ocr:    DeepSeekOCR-3B                  (懒加载)
  - vision: Qwen3-VL-2B                     (懒加载)

API 端点:
  GET  /health                      → 健康检查 + 模型状态
  GET  /v1/models                   → 可用模型列表
  POST /v1/chat/completions         → 文本 LLM (OpenAI 兼容)
  POST /v1/chat/completions         → 视觉问答 (含 base64 image)
  POST /v1/ocr                      → OCR + 公式识别
  POST /v1/audio/transcriptions     → 语音转文字 (计划)

PC 开发: rkllm 包不可用时自动降级为桩模式, 返回模拟响应。
"""
from __future__ import annotations


import os, sys, json, time, base64, threading, logging, asyncio
from pathlib import Path
from io import BytesIO
from typing import Optional

from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import JSONResponse, StreamingResponse
from pydantic import BaseModel
import uvicorn

from model_registry import MODELS, get_model, list_available, NPU_CORES

# ── 日志 ──────────────────────────────────────────────────
logging.basicConfig(level=logging.INFO, format="[rkllama] %(message)s")
logger = logging.getLogger("rkllama")

# ── RKLLM 运行时检测 ──────────────────────────────────────
try:
    from rkllm.api import RKLLM
    _HAS_RKLLM = True
    logger.info("rkllm Python API loaded")
except ImportError:
    _HAS_RKLLM = False
    logger.warning("rkllm not available — running in STUB mode")

# ── 显式 STUB 模式 (v6.1: 开发环境强制开启) ──
_STUB_MODE = os.getenv("RKLLM_STUB_MODE", "").lower() in ("true", "1", "yes")
if _STUB_MODE:
    _HAS_RKLLM = False
    logger.info("RKLLM_STUB_MODE=true — forced stub mode")

# ── 模型管理器 ────────────────────────────────────────────

class ModelManager:
    """多模型生命周期管理: 加载/卸载/懒加载/空闲回收 + 并发安全.

    并发安全策略:
    - 每个模型实例有独立推理锁 (prevent concurrent inference on same model)
    - 加载门控 (prevent double-load race when model not yet loaded)
    - 内存预算 (OOM prevention — evict idle models before loading)
    - 请求队列 (when model busy, wait with timeout instead of fail)
    """

    MAX_MEM_MB = int(os.getenv("RKLLM_MAX_MEM_MB", "3200"))  # v6.5: 3500→3200, KV cache留300MB
    INFERENCE_TIMEOUT_S = float(os.getenv("RKLLM_INFERENCE_TIMEOUT", "120"))

    def __init__(self):
        self._loaded: dict[str, object] = {}       # name → RKLLM instance
        self._inference_lock: dict[str, threading.Lock] = {}  # name → per-model lock
        self._loading: dict[str, threading.Event] = {}  # name → loading gate
        self._last_used: dict[str, float] = {}      # name → unix timestamp
        self._lock = threading.Lock()
        self._unload_cooldown: dict[str, float] = {}  # name → cooldown until

    # ── 公开 API ──

    @property
    def loaded_models(self) -> list[str]:
        with self._lock:
            return list(self._loaded.keys())

    @property
    def current_mem_mb(self) -> int:
        """当前已加载模型总内存 (MB)."""
        total = 0
        with self._lock:
            for name in self._loaded:
                cfg = get_model(name)
                if cfg:
                    total += cfg.get("benchmark_mem", 0)
        return total

    def acquire_inference(self, name: str, timeout_s: float = 120, priority: str = "normal") -> threading.Lock | None:
        """获取模型推理锁. 如果模型未加载则触发加载, 如果被占用则等待.

        Returns:
            推理锁 (已 acquire) 或 None (超时/加载失败)
        """
        deadline = time.time() + timeout_s
        model_cfg = get_model(name)
        if not model_cfg:
            return None

        # 等待模型加载就绪
        while time.time() < deadline:
            with self._lock:
                if name in self._loaded:
                    break
                # 检查是否正在加载
                loading_event = self._loading.get(name)

            if loading_event:
                # 等待加载完成
                remaining = deadline - time.time()
                if remaining <= 0:
                    logger.warning(f"Inference timeout waiting for {name} to load")
                    return None
                loading_event.wait(timeout=min(remaining, 5.0))
                continue

            # 触发加载
            instance = self._load_model(name)
            if not instance:
                return None
            with self._lock:
                self._last_used[name] = time.time()
            break

        # 获取推理锁
        with self._lock:
            if name not in self._inference_lock:
                self._inference_lock[name] = threading.Lock()
            infer_lock = self._inference_lock[name]

        # v6.3: 优先级策略 — high 用 spin-wait (100ms), normal 用 blocking wait
        if priority == "high":
            acquired = False
            _spin_deadline = time.time() + min(timeout_s, 30)
            while time.time() < _spin_deadline and not acquired:
                acquired = infer_lock.acquire(timeout=0.1)
        else:
            acquired = infer_lock.acquire(timeout=timeout_s)
        if acquired:
            with self._lock:
                self._last_used[name] = time.time()
            return infer_lock
        else:
            logger.warning(f"Inference lock timeout for {name} ({timeout_s}s)")
            return None

    def release_inference(self, name: str, infer_lock: threading.Lock):
        """释放推理锁."""
        try:
            infer_lock.release()
        except RuntimeError:
            pass  # 锁可能已被释放

    def get_model(self, name: str):
        """获取已加载的模型实例 (不获取推理锁 — 仅用于状态检查)."""
        model_cfg = get_model(name)
        if not model_cfg:
            return None
        with self._lock:
            if name in self._loaded:
                return self._loaded[name]
        return None

    def _load_model(self, name: str):
        """内部加载模型 — 带内存预算和加载门控."""
        model_cfg = get_model(name)
        if not model_cfg:
            return None

        # 加载门控: 确保只有一个线程执行加载
        with self._lock:
            if name in self._loaded:
                return self._loaded[name]

            loading_event = self._loading.get(name)
            if loading_event is None:
                loading_event = threading.Event()
                self._loading[name] = loading_event
            else:
                # 已经在加载中, 退出让 acquire_inference 等待
                return None

        try:
            # 内存预算检查
            needed_mb = model_cfg.get("benchmark_mem", 0)
            current_mb = self.current_mem_mb
            if current_mb + needed_mb > self.MAX_MEM_MB:
                logger.warning(
                    f"Memory budget exceeded (current={current_mb}MB + need={needed_mb}MB "
                    f"> max={self.MAX_MEM_MB}MB), evicting idle models"
                )
                self._evict_for_memory(needed_mb)

            if not _HAS_RKLLM:
                instance = _StubModel(model_cfg["display"])
            elif not os.path.exists(model_cfg["path"]):
                logger.error(f"Model file not found: {model_cfg['path']}")
                instance = None
            else:
                logger.info(
                    f"Loading {model_cfg['display']} ({model_cfg.get('dtype','?')}, "
                    f"{needed_mb}MB, mem={current_mb}/{self.MAX_MEM_MB}MB)..."
                )
                try:
                    # v6.11: core_mask 从模型注册表读取 — 文本模型单核(0x1), VL模型双核(0x3)
                    cores_needed = model_cfg.get("npu_cores_generate", NPU_CORES)
                    core_mask = 0x3 if cores_needed >= 2 else 0x1
                    # v6.2: RKLLM 加载超时保护 — ThreadPoolExecutor 防硬件卡死
                    import concurrent.futures
                    _LOAD_TIMEOUT = 180
                    with concurrent.futures.ThreadPoolExecutor(max_workers=1) as _exec:
                        _future = _exec.submit(
                            RKLLM, model_path=model_cfg["path"], core_mask=core_mask
                        )
                        instance = _future.result(timeout=_LOAD_TIMEOUT)
                    logger.info("  core_mask=0x%x (%d core(s))", core_mask, cores_needed)
                except concurrent.futures.TimeoutError:
                    logger.error(
                        "Failed to load %s: timed out after %ds",
                        name, _LOAD_TIMEOUT,
                    )
                    instance = None
                except Exception as e:
                    logger.error(f"Failed to load {name}: {e}")
                    instance = None

            with self._lock:
                if instance is not None:
                    self._loaded[name] = instance
                    self._last_used[name] = time.time()
                # 通知等待者
                loading_event.set()

            if instance is not None:
                cfg = get_model(name)
                logger.info(
                    f"{cfg['display'] if cfg else name} loaded "
                    f"({len(self._loaded)} models, {self.current_mem_mb}MB)"
                )

            return instance

        finally:
            with self._lock:
                self._loading.pop(name, None)

    def _evict_for_memory(self, needed_mb: int):
        """为加载新模型腾出内存 — 按 LRU 顺序卸载懒加载模型.

        约束: 模型必须加载超过 EVICT_MIN_AGE_S 秒才能被驱逐 (防止刚加载就被踢).
        """
        EVICT_MIN_AGE_S = 30  # 加载后至少 30 秒才能被驱逐
        now = time.time()
        candidates: list[tuple[float, str, int]] = []  # (last_used, name, mem_mb)
        with self._lock:
            for name in list(self._loaded.keys()):
                cfg = get_model(name)
                if not cfg or not cfg.get("lazy"):
                    continue
                # 冷却期检查
                if now < self._unload_cooldown.get(name, 0):
                    continue
                # 最小年龄检查: 刚加载不久的模型不驱逐
                loaded_at = self._last_used.get(name, 0)
                if now - loaded_at < EVICT_MIN_AGE_S:
                    continue
                candidates.append((loaded_at, name, cfg.get("benchmark_mem", 0)))

        # 检查释放是否足够
        max_free = sum(mem for _, _, mem in candidates)
        if max_free < needed_mb:
            logger.warning(
                f"Eviction cannot free enough memory: need {needed_mb}MB, "
                f"max available {max_free}MB from {len(candidates)} candidates"
            )

        # LRU: 最久未用的先卸载
        candidates.sort(key=lambda x: x[0])
        freed = 0
        for _, name, mem in candidates:
            if freed >= needed_mb:
                break
            if self._can_unload(name):
                with self._lock:
                    if name in self._loaded:
                        del self._loaded[name]
                        self._unload_cooldown[name] = now + 60
                freed += mem
                logger.info(f"Evicted {name} ({mem}MB) — freed {freed}/{needed_mb}MB")

    def _can_unload(self, name: str) -> bool:
        """检查模型是否可以安全卸载 (未被推理占用)."""
        with self._lock:
            infer_lock = self._inference_lock.get(name)
        if infer_lock and infer_lock.locked():
            return False
        return True

    def unload_idle(self):
        """卸载超过空闲超时的懒加载模型."""
        now = time.time()
        to_unload = []
        with self._lock:
            for name in list(self._loaded.keys()):
                cfg = get_model(name)
                if not cfg or not cfg.get("lazy"):
                    continue
                timeout = cfg.get("idle_timeout_s", 300)
                if now - self._last_used.get(name, 0) > timeout:
                    if now < self._unload_cooldown.get(name, 0):
                        continue
                    to_unload.append(name)

        for name in to_unload:
            if self._can_unload(name):
                with self._lock:
                    if name in self._loaded:
                        del self._loaded[name]
                        self._unload_cooldown[name] = now + 60
                logger.info(f"Unloaded idle model: {name}")

    def model_status(self) -> dict:
        """返回所有模型的状态信息."""
        status = {}
        now = time.time()
        for name, cfg in MODELS.items():
            if not cfg.get("enabled", True):
                continue
            loaded = name in self._loaded
            status[name] = {
                "display": cfg["display"],
                "type": cfg["type"],
                "loaded": loaded,
                "lazy": cfg.get("lazy", False),
                "file_exists": os.path.exists(cfg["path"]),
            }
            if loaded:
                status[name]["idle_s"] = round(now - self._last_used.get(name, now), 1)
                infer_lock = self._inference_lock.get(name)
                status[name]["busy"] = infer_lock.locked() if infer_lock else False
            status[name]["mem_mb"] = cfg.get("benchmark_mem", 0)
        status["_total_mem_mb"] = self.current_mem_mb
        status["_max_mem_mb"] = self.MAX_MEM_MB
        return status


# ── 桩模型 (PC 开发) ──────────────────────────────────────

class _StubModel:
    def __init__(self, name: str):
        self.name = name

    def generate(self, prompt: str, max_tokens: int = 512) -> str:
        return f"[STUB:{self.name}] Received: {prompt[:200]}..."


# ── 全局实例 ──────────────────────────────────────────────

manager = ModelManager()


# ── 定频工具 (RK 官方 fix_freq_rk3576.sh 的 Python 等效) ─

def _try_fix_frequency():
    """尝试锁定 NPU/LPDDR4X/CPU 最高频率 (非特权环境静默失败).

    RK3576 官方 benchmark 频率:
      NPU: 1.0 GHz, LPDDR4X: 2.133 GHz, CPU 大核: 2.304 GHz
    DVFS 降频会导致 10-30% 吞吐抖动, 生产环境建议执行.
    """
    freq_targets = [
        ("/sys/class/devfreq/27700000.npu", 1_000_000_000),   # NPU 1.0 GHz
        ("/sys/class/devfreq/dmc",           2_133_000_000),   # LPDDR4X 2.133 GHz
    ]
    for sysfs_path, target_hz in freq_targets:
        try:
            gov_path = f"{sysfs_path}/governor"
            set_path = f"{sysfs_path}/userspace/set_freq"
            with open(gov_path, "w") as f:
                f.write("userspace")
            with open(set_path, "w") as f:
                f.write(str(target_hz))
            with open(f"{sysfs_path}/cur_freq", "r") as f:
                cur = int(f.readline().strip())
            logger.info("Fixed %s → %d MHz", sysfs_path.rsplit("/", 1)[-1], cur // 1_000_000)
        except Exception:
            pass  # 非特权容器或无 sysfs 静默跳过
app = FastAPI(title="rkllama", version="4.1.0", description="Multi-model NPU Inference Server")


# ── 生命周期 ──────────────────────────────────────────────

@app.on_event("startup")
async def startup():
    """启动时预加载常驻文本模型 + 定频 + 性能日志."""
    logger.info("rkllama v4.2.0 starting (NPU cores: %d)...", NPU_CORES)

    # 尝试定频 (RK 官方建议: benchmark 均在此频率采集)
    _try_fix_frequency()

    # 预加载文本 LLM (常驻, 不卸载)
    instance = manager._load_model("r1-distill-1.5b")
    if instance:
        logger.info("Text LLM preloaded: r1-distill-1.5b")
    else:
        logger.warning("Text LLM not available — running limited mode")

    # 启动空闲回收线程
    def _idle_loop():
        while True:
            time.sleep(60)
            manager.unload_idle()

    threading.Thread(target=_idle_loop, daemon=True).start()
    logger.info("Idle model reclaimer started (interval: 60s)")


# ── 模型预热 ──────────────────────────────────────────────

@app.post("/v1/warm/{model_name}")
async def warm_model(model_name: str):
    """触发模型预加载, 不执行推理.

    用于 MCP process_file 到达时提前加载 OCR 模型,
    利用 OpenCV 预处理的 CPU 空档完成 NPU 模型加载.
    """
    if model_name not in MODELS:
        raise HTTPException(404, f"Unknown model: {model_name}")

    cfg = get_model(model_name)
    start = time.time()
    instance = await asyncio.to_thread(manager._load_model, model_name)
    elapsed_ms = round((time.time() - start) * 1000, 1)

    return {
        "model": model_name,
        "display": cfg.get("display", model_name) if cfg else model_name,
        "loaded": instance is not None,
        "warm_ms": elapsed_ms,
        "mem_mb": manager.current_mem_mb,
    }


# ── 健康检查 ──────────────────────────────────────────────

@app.get("/health")
async def health():
    # 读取 NPU 负载 (如果 sysfs 可用)
    npu_load = {}
    try:
        with open("/sys/kernel/debug/rknpu/load", "r") as f:
            line = f.readline().strip()
            import re
            for match in re.finditer(r"Core(\d+)=(\d+)%", line):
                npu_load[f"core{match.group(1)}"] = int(match.group(2))
    except Exception:
        pass

    # 读取 NPU 频率
    npu_freq_mhz = 0
    try:
        with open("/sys/class/devfreq/27700000.npu/cur_freq", "r") as f:
            npu_freq_mhz = int(f.readline().strip()) // 1_000_000
    except Exception:
        pass

    return {
        "status": "ok",
        "rkllm_available": _HAS_RKLLM,
        "npu_cores": NPU_CORES,
        "npu_load": npu_load,
        "npu_freq_mhz": npu_freq_mhz or None,
        "mem_used_mb": manager.current_mem_mb,
        "mem_max_mb": manager.MAX_MEM_MB,
        "models": manager.model_status(),
    }


# ── 模型列表 (OpenAI 兼容) ────────────────────────────────

@app.get("/v1/models")
async def list_models():
    return {"object": "list", "data": list_available()}


# ── Chat Completions (OpenAI 兼容) ────────────────────────

class ChatMessage(BaseModel):
    role: str
    content: str | list[dict]

class ChatRequest(BaseModel):
    model: str = "r1-distill-1.5b"
    messages: list[ChatMessage]
    max_tokens: int = 512
    temperature: float = 0.7
    stream: bool = False


async def _stream_chat_result(model_name: str, content: str):
    """Yield OpenAI-compatible SSE chunks for a streaming chat completion."""
    chunk_id = f"chatcmpl-{int(time.time())}"
    # Role chunk
    yield f"data: {json.dumps({'id': chunk_id, 'object': 'chat.completion.chunk', 'model': model_name, 'choices': [{'index': 0, 'delta': {'role': 'assistant'}, 'finish_reason': None}]})}\n\n"
    # Content chunks
    for i in range(0, len(content), 10):
        segment = content[i:i+10]
        yield f"data: {json.dumps({'id': chunk_id, 'object': 'chat.completion.chunk', 'model': model_name, 'choices': [{'index': 0, 'delta': {'content': segment}, 'finish_reason': None}]})}\n\n"
    # Final chunk
    yield f"data: {json.dumps({'id': chunk_id, 'object': 'chat.completion.chunk', 'model': model_name, 'choices': [{'index': 0, 'delta': {}, 'finish_reason': 'stop'}]})}\n\n"
    yield "data: [DONE]\n\n"


@app.post("/v1/chat/completions")
async def chat_completions(req: ChatRequest):
    """OpenAI 兼容 chat completions. 并发安全: per-model inference lock.

    文本模型: model=r1-distill-1.5b
    视觉模型: model=qwen3-vl-2b, messages[].content 含 image_url
    """
    model_name = req.model
    model_cfg = get_model(model_name)
    if not model_cfg:
        raise HTTPException(404, f"Model '{model_name}' not found")

    # v6.3: chat 请求高优先级 — spin-wait 抢占, 不被后台任务阻塞
    infer_lock = await asyncio.to_thread(
        manager.acquire_inference, model_name, 90, "high"
    )
    if not infer_lock:
        raise HTTPException(503, f"Model '{model_name}' busy or loading, retry later")

    try:
        instance = manager.get_model(model_name)
        if not instance:
            raise HTTPException(503, f"Model '{model_name}' failed to load")

        # 提取文本和图片
        prompt = ""
        image_b64 = None

        if req.messages:
            last_msg = req.messages[-1]
            if isinstance(last_msg.content, str):
                prompt = last_msg.content
            elif isinstance(last_msg.content, list):
                parts = []
                for part in last_msg.content:
                    if part.get("type") == "text":
                        parts.append(part["text"])
                    elif part.get("type") == "image_url":
                        url = part.get("image_url", {}).get("url", "")
                        if url.startswith("data:"):
                            image_b64 = url.split(",", 1)[-1] if "," in url else url
                        else:
                            parts.append(f"[image: {url}]")
                prompt = "\n".join(parts)

        if not prompt:
            raise HTTPException(400, "No text content in messages")

        # VL 模型 (ocr / ocr_fast / vision): 视觉编码 + 文本生成
        if model_cfg["type"] in ("ocr", "vision") and image_b64:
            prompt = _build_vl_prompt(model_cfg, prompt, image_b64)

        # 生成
        max_tokens = min(req.max_tokens, model_cfg.get("max_tokens", 512))
        try:
            result = await asyncio.to_thread(
                instance.generate, prompt, max_tokens=max_tokens
            )
        except Exception as e:
            logger.error(f"Inference error ({model_name}): {e}")
            raise HTTPException(500, f"Inference failed: {e}")

        if req.stream:
            return StreamingResponse(
                _stream_chat_result(model_name, result),
                media_type="text/event-stream",
            )
        return {
            "id": f"chatcmpl-{int(time.time())}",
            "object": "chat.completion",
            "model": model_name,
            "choices": [{
                "index": 0,
                "message": {"role": "assistant", "content": result},
                "finish_reason": "stop",
            }],
            "usage": {"prompt_tokens": len(prompt)//4, "completion_tokens": len(result)//4},
        }
    finally:
        await asyncio.to_thread(manager.release_inference, model_name, infer_lock)


# ── OCR Endpoint ──────────────────────────────────────────

class OCRRequest(BaseModel):
    image: str          # base64 encoded image (data:image/...;base64,... or raw base64)
    language: str = "zh"  # 输出语言
    return_formulas: bool = True
    return_layout: bool = True
    ingest: bool = False     # True: 入库场景, 附加质量检查 (OCR 结果进入 ChromaDB 前验证)
    # fast_mode 已废弃 (v4.2): SmolVLM-256M 移除, OCR 统一使用 DeepSeekOCR-3B
    fast_mode: bool = False  # 保留字段兼容旧客户端, 不再生效

@app.post("/v1/ocr")
async def ocr(req: OCRRequest):
    """OCR + 公式识别.

    统一使用 DeepSeekOCR-3B (唯一 OCR 引擎).
    ingest=True 时附加入库质量检查.

    并发安全: 同一模型同时只有一个推理 (acquire_inference 锁).
    """
    model_name = "deepseekocr-3b"

    model_cfg = get_model(model_name)
    if not model_cfg:
        raise HTTPException(503, f"OCR model '{model_name}' not configured")

    infer_lock = await asyncio.to_thread(manager.acquire_inference, model_name, 60)
    if not infer_lock:
        raise HTTPException(503, f"OCR model '{model_name}' busy or loading, retry later")

    try:
        instance = manager.get_model(model_name)
        if not instance:
            raise HTTPException(503, "DeepSeekOCR failed to load")

        image_b64 = req.image
        if image_b64.startswith("data:"):
            image_b64 = image_b64.split(",", 1)[-1] if "," in image_b64 else image_b64

        tmp_path = f"/tmp/ocr_{int(time.time())}_{threading.get_ident()}.jpg"
        try:
            with open(tmp_path, "wb") as f:
                f.write(base64.b64decode(image_b64))
        except Exception as e:
            raise HTTPException(400, f"Invalid base64 image: {e}")

        prompt_parts = ["请识别图片中的所有文字和数学公式。"]
        if req.return_formulas:
            prompt_parts.append("数学公式请用 LaTeX 格式输出，包裹在 $...$ 或 $$...$$ 中。")
        if req.return_layout:
            prompt_parts.append("请标注每个区域的位置（如：标题、题目、选项、公式）。")
        prompt_parts.append(f"输出语言: {req.language}")

        full_prompt = _build_vl_prompt(model_cfg, "\n".join(prompt_parts), image_b64,
                                         image_path=tmp_path)

        try:
            raw = await asyncio.to_thread(
                instance.generate, full_prompt,
                max_tokens=model_cfg.get("max_tokens", 4096),
            )
        except Exception as e:
            raise HTTPException(500, f"OCR inference failed: {e}")
        finally:
            try:
                os.remove(tmp_path)
            except OSError:
                pass

        formulas = _extract_formulas(raw)
        result = {
            "text": raw,
            "formulas": formulas if req.return_formulas else [],
            "layout": _parse_layout(raw) if req.return_layout else {},
            "model": model_name,
        }

        # 入库场景: 附加质量检查
        if req.ingest:
            quality = _check_ocr_quality(raw, req.return_formulas)
            result["quality"] = quality
            if not quality.get("passed", True):
                logger.warning("OCR quality check failed for ingest: %s", quality.get("issues", []))

        return result
    finally:
        await asyncio.to_thread(manager.release_inference, model_name, infer_lock)


# ── 批量 OCR Endpoint (v4.1) ─────────────────────────────

class BatchOCRRequest(BaseModel):
    images: list[str]     # base64 图片列表
    language: str = "zh"
    return_formulas: bool = True
    return_layout: bool = False  # 批量模式默认关闭版面分析
    ingest: bool = False     # True: 入库场景, 附加质量检查

@app.post("/v1/ocr/batch")
async def ocr_batch(req: BatchOCRRequest):
    """批量 OCR — 一次加锁, 多图顺序推理 (PDF 多页/多图场景).

    优势: 只获取一次推理锁, 消除逐页 lock acquire/release 开销.
    适用: PDF 扫描件逐页 OCR, 多图批量识别.
    """
    model_name = "deepseekocr-3b"
    model_cfg = get_model(model_name)
    if not model_cfg:
        raise HTTPException(503, "DeepSeekOCR model not configured")

    if len(req.images) > 50:
        raise HTTPException(400, "Batch limit: 50 images")

    infer_lock = await asyncio.to_thread(manager.acquire_inference, model_name, 300)
    if not infer_lock:
        raise HTTPException(503, "DeepSeekOCR busy or loading, retry later")

    results = []
    try:
        instance = manager.get_model(model_name)
        if not instance:
            raise HTTPException(503, "DeepSeekOCR failed to load")

        for idx, image_b64 in enumerate(req.images):
            # 标准化
            if isinstance(image_b64, str) and image_b64.startswith("data:"):
                image_b64 = image_b64.split(",", 1)[-1] if "," in image_b64 else image_b64

            tmp_path = f"/tmp/ocr_batch_{idx}_{threading.get_ident()}.jpg"
            try:
                with open(tmp_path, "wb") as f:
                    f.write(base64.b64decode(image_b64))
            except Exception as e:
                results.append({"index": idx, "error": f"Invalid base64: {e}"})
                continue

            prompt = "\n".join([
                "请识别图片中的所有文字和数学公式。",
                "数学公式请用 LaTeX 格式输出，包裹在 $...$ 或 $$...$$ 中。" if req.return_formulas else "",
                f"输出语言: {req.language}",
            ])
            full_prompt = _build_vl_prompt(model_cfg, prompt, image_b64, image_path=tmp_path)

            try:
                raw = await asyncio.to_thread(
                    instance.generate, full_prompt,
                    max_tokens=model_cfg.get("max_tokens", 4096),
                )
                formulas = _extract_formulas(raw)
                entry = {
                    "index": idx,
                    "text": raw,
                    "formulas": formulas if req.return_formulas else [],
                    "layout": _parse_layout(raw) if req.return_layout else {},
                }
                if req.ingest:
                    entry["quality"] = _check_ocr_quality(raw, req.return_formulas)
                results.append(entry)
            except Exception as e:
                logger.error(f"Batch OCR [{idx}] failed: {e}")
                results.append({"index": idx, "error": str(e)})
            finally:
                try:
                    os.remove(tmp_path)
                except OSError:
                    pass

        return {
            "model": "deepseekocr-3b",
            "total": len(req.images),
            "success": sum(1 for r in results if "error" not in r),
            "results": results,
        }
    finally:
        await asyncio.to_thread(manager.release_inference, model_name, infer_lock)


# ── 音频转录 (计划) ──────────────────────────────────────

@app.post("/v1/audio/transcriptions")
async def transcribe():
    raise HTTPException(501, "Whisper STT not yet deployed (model pending)")


# ── Embedding 端点 (BGE-small-zh, NPU) ─────────────────

@app.post("/api/embed")
@app.post("/api/embeddings")
async def embed(request: Request):
    """Embedding 端点 (BGE-small-zh, 384 维).

    DeepTutor RAG 管道通过此端点获取文本向量.
    PC 开发模式返回零向量桩; 生产环境需 rknn-llm embedding API.

    v6.9: 接入 model_registry, 验证模型文件存在性.
    """
    EMBED_MODEL = "bge-small-zh"
    EMBED_DIM = 384

    try:
        body = await request.json()
        texts = body.get("input", [])
        if isinstance(texts, str):
            texts = [texts]
    except Exception:
        raise HTTPException(400, "Invalid request body")

    if not texts:
        raise HTTPException(400, "No input texts")

    if not _HAS_RKLLM:
        return {"object": "list", "data": [
            {"object": "embedding", "index": i, "embedding": [0.0] * EMBED_DIM}
            for i in range(len(texts))
        ]}

    # v6.9: 检查 registry — 模型文件是否存在
    try:
        from model_registry import get_model

        model_cfg = get_model(EMBED_MODEL)
        if model_cfg is None:
            logger.error("Embedding model '%s' not registered", EMBED_MODEL)
            raise HTTPException(503, f"Embedding model '{EMBED_MODEL}' not in registry")
        if not os.path.exists(model_cfg["path"]):
            logger.error("Embedding model file not found: %s", model_cfg["path"])
            raise HTTPException(503,
                f"Embedding model file missing: {os.path.basename(model_cfg['path'])}. "
                f"Place it in {os.path.dirname(model_cfg['path'])}/")
    except HTTPException:
        raise
    except Exception as e:
        logger.warning("Registry check skipped: %s", e)

    try:
        from rkllm.api import RKLLMEmbedding
        embedder = RKLLMEmbedding()
        vectors = embedder.embed(texts)
        return {"object": "list", "data": [
            {"object": "embedding", "index": i, "embedding": v}
            for i, v in enumerate(vectors)
        ]}
    except ImportError:
        logger.warning("RKLLMEmbedding not available, returning stub embeddings")
        return {"object": "list", "data": [
            {"object": "embedding", "index": i, "embedding": [0.0] * EMBED_DIM}
            for i in range(len(texts))
        ]}
    except Exception as e:
        logger.error("Embedding failed: %s", e)
        raise HTTPException(500, f"Embedding error: {e}")


# ── VL Prompt 构建 ────────────────────────────────────────

def _build_vl_prompt(cfg: dict, text: str, image_b64: str,
                     image_path: str = None) -> str:
    """构建 VL 模型的完整 prompt (含视觉 token 占位符).

    图片先保存为临时文件, 视觉编码器读取文件路径进行 RKNN 推理,
    生成的视觉 token 拼接到 prompt 中.

    模型类型差异:
    - DeepSeekOCR: img_start="", 用 IMAGE_PATH 方式传递图片路径给 rkllm
    - Qwen3-VL: img_start="<|vision_start|>", 用 vision token 占位符
    """
    # 保存临时图片 (如果未提供路径)
    if not image_path:
        image_path = f"/tmp/vl_{int(time.time())}.jpg"
        with open(image_path, "wb") as f:
            f.write(base64.b64decode(image_b64))

    img_start = cfg.get("img_start", "")
    img_end = cfg.get("img_end", "")
    img_content = cfg.get("img_content", "<image>")

    if img_start and img_content:
        # Qwen3-VL: vision token 占位符
        # 假设 256 个 visual tokens (实际数量由 vision encoder 决定)
        vision_tokens = img_content * 256
        return f"{img_start}{vision_tokens}{img_end}\n{text}"
    else:
        # DeepSeekOCR: IMAGE_PATH 方式 (img_start 为空)
        return f"{text}\n\n[IMAGE_PATH:{image_path}]"


# ── 公式提取 ──────────────────────────────────────────────

_LATEX_PATTERN = r"\${1,2}[^$]+\${1,2}"

def _extract_formulas(text: str) -> list[str]:
    import re
    return re.findall(_LATEX_PATTERN, text)


def _parse_layout(text: str) -> dict:
    """简单的版面信息提取 (基于关键词). DeepSeekOCR 输出中通常包含版面描述."""
    regions = {}
    for kw in ["标题", "题目", "选项", "公式", "图表", "说明"]:
        if kw in text:
            regions[kw] = True
    return regions


def _check_ocr_quality(text: str, expect_formulas: bool = False) -> dict:
    """入库 OCR 质量检查 — 启发式检测, 不阻塞入库, 仅标记.

    Returns:
        {"passed": bool, "score": float (0-1), "issues": list[str]}
    """
    issues = []
    score = 1.0

    # 1. 文本长度检查 — 空结果或过短
    if len(text.strip()) < 10:
        issues.append("text_too_short")
        score -= 0.5

    # 2. 中文汉字密度检查 (用于中文 OCR 场景)
    chinese_chars = sum(1 for c in text if '\u4e00' <= c <= '\u9fff')
    total_chars = max(len(text), 1)
    chinese_ratio = chinese_chars / total_chars
    if chinese_ratio < 0.05 and total_chars > 50:
        issues.append("low_chinese_density")
        score -= 0.2

    # 3. 公式检测 (如果预期有公式)
    if expect_formulas:
        formulas = _extract_formulas(text)
        if not formulas:
            issues.append("expected_formulas_not_found")
            score -= 0.3

    # 4. 乱码检测 — 连续不可打印字符过多
    garbage_count = sum(1 for c in text if c not in '\n\r\t ' and not c.isprintable())
    if garbage_count > len(text) * 0.1:
        issues.append("high_garbage_ratio")
        score -= 0.4

    passed = score >= 0.6 and len(issues) <= 1
    return {
        "passed": passed,
        "score": max(round(score, 2), 0.0),
        "issues": issues,
    }


# ── 临时文件清理 ──────────────────────────────────────────

def _tmp_cleanup_loop():
    """后台线程: 每 10 分钟清理超过 30 分钟的 /tmp/ocr_*.jpg 和 /tmp/vl_*.jpg.

    v6.4: 零拷贝原则下仅 3 处豁免写 /tmp, 此线程防进程崩溃后残留 (R5).
    """
    import glob as _glob
    while True:
        time.sleep(600)  # 10 分钟
        try:
            cutoff = time.time() - 1800  # 30 分钟
            for pattern in ("/tmp/ocr_*.jpg", "/tmp/vl_*.jpg"):
                for f in _glob.glob(pattern):
                    try:
                        if os.path.getmtime(f) < cutoff:
                            os.remove(f)
                            logger.debug("Cleaned stale temp: %s", f)
                    except OSError:
                        pass
        except Exception:
            pass


# ── 启动 ──────────────────────────────────────────────────

if __name__ == "__main__":
    # v6.4: 启动临时文件清理线程 (R5)
    cleanup_thread = threading.Thread(target=_tmp_cleanup_loop, daemon=True)
    cleanup_thread.start()

    port = int(os.getenv("RKLLM_PORT", "8080"))
    logger.info(f"rkllama v4.2.0 starting on port {port}")
    uvicorn.run(app, host="0.0.0.0", port=port, log_level="warning")