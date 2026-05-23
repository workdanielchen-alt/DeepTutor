"""
tutor_platform/unified_provider.py — UnifiedLocalProvider + ChromaDB 管理 (v7.0)

管理 ChromaDB PersistentClient 单例，提供 collection CRUD、
文件入库、知识库搜索和 Provider 生命周期管理。
"""

import os
import json
import logging
import threading
from pathlib import Path

logger = logging.getLogger("tutor_platform.unified_provider")

CHROMA_PERSIST_DIR = os.environ.get("CHROMA_PERSIST_DIR", "/data/chromadb")
UPLOADS_DIR = os.environ.get("UPLOADS_DIR", "/data/uploads")
SOURCES_DIR = os.environ.get("SOURCES_DIR", "/data/sources")


class UnifiedLocalProvider:
    """ChromaDB PersistentClient 封装。

    提供:
      - collection 按 kb_name 自动创建/获取
      - add / query / delete / count
      - 文件入库 (ingest) — PDF/Office/文本 → chunk → 向量化
      - Provider 生命周期管理
    """

    def __init__(self, persist_dir: str = CHROMA_PERSIST_DIR):
        self._persist_dir = persist_dir
        self._client = None
        self._collections: dict[str, any] = {}
        self._initialized = False
        self._lock = threading.Lock()

    def initialize(self):
        """初始化 ChromaDB 客户端。"""
        if self._initialized:
            return
        with self._lock:
            if self._initialized:
                return
            try:
                import chromadb
                from chromadb.config import Settings
                os.makedirs(self._persist_dir, exist_ok=True)
                self._client = chromadb.PersistentClient(
                    path=self._persist_dir,
                    settings=Settings(anonymized_telemetry=False),
                )
                self._initialized = True
                logger.info("ChromaDB initialized at %s", self._persist_dir)
            except Exception as e:
                logger.error("ChromaDB init failed: %s", e)
                raise

    def get_or_create_collection(self, kb_name: str, embedding_function=None):
        """按 kb_name 获取或创建 collection。"""
        self.initialize()
        with self._lock:
            if kb_name not in self._collections:
                try:
                    col = self._client.get_collection(kb_name)
                except Exception:
                    col = self._client.create_collection(
                        name=kb_name,
                        embedding_function=embedding_function,
                    )
                self._collections[kb_name] = col
            return self._collections[kb_name]

    def add_documents(self, kb_name: str, documents: list[str],
                      metadatas: list[dict] = None, ids: list[str] = None,
                      embedding_function=None):
        """向知识库添加文档。"""
        col = self.get_or_create_collection(kb_name, embedding_function)
        col.add(
            documents=documents,
            metadatas=metadatas or [{}] * len(documents),
            ids=ids or [f"doc_{i}" for i in range(len(documents))],
        )

    def query(self, kb_name: str, query_texts: list[str], n_results: int = 5,
              embedding_function=None) -> dict:
        """搜索知识库。"""
        try:
            col = self.get_or_create_collection(kb_name, embedding_function)
            return col.query(query_texts=query_texts, n_results=n_results)
        except Exception as e:
            logger.warning("ChromaDB query failed: %s", e)
            return {"ids": [[]], "documents": [[]], "metadatas": [[]], "distances": [[]]}

    def delete_collection(self, kb_name: str):
        """删除知识库。"""
        self.initialize()
        with self._lock:
            try:
                self._client.delete_collection(kb_name)
                self._collections.pop(kb_name, None)
            except Exception as e:
                logger.warning("Delete collection %s failed: %s", kb_name, e)

    def count(self, kb_name: str) -> int:
        """返回集合中的文档数。"""
        try:
            col = self.get_or_create_collection(kb_name)
            return col.count()
        except Exception:
            return 0

    def close(self):
        """关闭客户端。"""
        with self._lock:
            self._collections.clear()
            self._client = None
            self._initialized = False

    @property
    def client(self):
        self.initialize()
        return self._client


# ── 全局单例 ──

_instance: UnifiedLocalProvider | None = None
_instance_lock = threading.Lock()


def get_provider_instance() -> UnifiedLocalProvider:
    """获取 UnifiedLocalProvider 全局单例。"""
    global _instance
    if _instance is None:
        with _instance_lock:
            if _instance is None:
                _instance = UnifiedLocalProvider()
                _instance.initialize()
    return _instance


def reset_provider_instance():
    """重置全局单例（用于配置变更后重建）。"""
    global _instance
    with _instance_lock:
        if _instance is not None:
            try:
                _instance.close()
            except Exception:
                pass
            _instance = None
    logger.info("UnifiedLocalProvider instance reset")
