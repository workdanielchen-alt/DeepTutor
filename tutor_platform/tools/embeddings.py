"""
tutor_platform/tools/embeddings.py — Embedding 函数

RkllamaEmbeddingFunction: ChromaDB 兼容的 embedding function。
优先使用 RKLLM (RK3576 NPU) 生成 embedding，不可用时回退到 CPU 端 ollama / 确定性哈希。
"""

import hashlib
import logging
import os

logger = logging.getLogger("tutor_platform.tools.embeddings")

RKLLM_SERVER_URL = os.environ.get("RKLLM_SERVER_URL", "http://rkllama:8080")
OLLAMA_URL = os.environ.get("OLLAMA_URL", "http://ollama:11434")
_EMBED_DIM = 768


def _hash_embed(text: str, dim: int = _EMBED_DIM) -> list[float]:
    """Generate a deterministic embedding vector from text content via MD5.

    This is NOT a semantically meaningful embedding — it only guarantees
    that identical texts produce identical vectors, preventing ChromaDB
    from inserting duplicate chunks on reprocess.  Used as last resort
    when no embedding service is reachable.
    """
    h = hashlib.sha256(text.encode("utf-8")).digest()
    seed = int.from_bytes(h[:8], "big")
    rng = __import__("random").Random(seed)
    return [rng.random() * 2 - 1 for _ in range(dim)]


class RkllamaEmbeddingFunction:
    """ChromaDB embedding function 兼容接口。

    封装 RKLLM (rkllama) 的 embedding API，调用 /api/embed 获取向量。
    fallback 链: RKLLM → ollama → 确定性哈希。
    """

    def __init__(self, model: str = "", batch_size: int = 8):
        self.model = model or os.environ.get("EMBEDDING_MODEL", "bge-small-zh-v1.5")
        self.batch_size = batch_size
        self._http_client = None

    async def _get_client(self):
        if self._http_client is None:
            import httpx
            self._http_client = httpx.AsyncClient(timeout=30)
        return self._http_client

    def __call__(self, texts: list[str]) -> list[list[float]]:
        """ChromaDB 兼容接口：同步返回 embedding 向量列表。"""

        # ── Tier 1: RKLLM (NPU) ──
        try:
            import httpx
            client = httpx.Client(timeout=30)
            resp = client.post(
                f"{RKLLM_SERVER_URL}/api/embed",
                json={"texts": texts, "model": self.model},
            )
            if resp.status_code == 200:
                data = resp.json()
                embeds = data.get("embeddings")
                if embeds and len(embeds) == len(texts):
                    return embeds
                logger.warning("RKLLM embed returned %d vectors (expected %d), fallback",
                               len(embeds or []), len(texts))
            else:
                logger.warning("RKLLM embed returned %s, trying fallback", resp.status_code)
        except Exception as e:
            logger.warning("RKLLM embed failed: %s, trying fallback", e)

        # ── Tier 2: ollama (CPU) ──
        try:
            import httpx
            client = httpx.Client(timeout=30)
            resp = client.post(
                f"{OLLAMA_URL}/api/embed",
                json={"model": self.model, "input": texts},
            )
            if resp.status_code == 200:
                data = resp.json()
                embeds = data.get("embeddings")
                if embeds and len(embeds) == len(texts):
                    logger.info("Ollama embed fallback OK for %d texts", len(texts))
                    return embeds
        except Exception:
            logger.debug("Ollama embed fallback unavailable")

        # ── Tier 3: Deterministic hash (last resort) ──
        logger.warning("No embedding service available, using deterministic hash fallback")
        return [_hash_embed(t) for t in texts]

    def close(self):
        if self._http_client is not None:
            import asyncio
            try:
                asyncio.create_task(self._http_client.aclose())
            except Exception:
                pass
            self._http_client = None
