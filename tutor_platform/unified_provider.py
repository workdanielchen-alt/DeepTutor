"""Unified provider: LLM, OCR, vision, and vector store abstraction.

Provides a singleton provider instance that wraps Ollama/DeepSeek APIs,
ChromaDB vector store, and OCR capabilities.
"""

from __future__ import annotations

import asyncio
import base64
import json
import logging
import os
from typing import Any

import httpx

logger = logging.getLogger(__name__)

_provider_instance: UnifiedLocalProvider | None = None


class UnifiedLocalProvider:
    """Unified provider for LLM, OCR, vision, and vector store operations."""

    def __init__(self):
        self._ollama_url = os.getenv("OLLAMA_URL", "http://ollama:11434")
        self._deepseek_key = os.getenv("DEEPSEEK_API_KEY", "").strip()
        self._chroma_dir = os.getenv("CHROMA_PERSIST_DIR", "/data/chromadb")
        self._client = httpx.AsyncClient(timeout=120)
        self._ocr_model = os.getenv("OLLAMA_OCR_MODEL", "openbmb/minicpm-v4.6:q4_K_M")
        logger.info(
            "UnifiedLocalProvider: ollama=%s chroma=%s",
            self._ollama_url,
            self._chroma_dir,
        )

    async def add_documents(
        self,
        kb_name: str,
        documents: list[str],
        metadatas: list[dict] | None = None,
        ids: list[str] | None = None,
    ) -> dict:
        """Batch-add chunked documents to the knowledge base."""
        if not documents:
            return {"ok": True, "count": 0}
        try:
            import chromadb
            from chromadb.config import Settings

            os.makedirs(self._chroma_dir, exist_ok=True)
            client = chromadb.PersistentClient(
                path=self._chroma_dir,
                settings=Settings(anonymized_telemetry=False),
            )
            collection = client.get_or_create_collection(name=kb_name)

            embeddings: list[list[float]] = []
            for doc in documents:
                emb_resp = await self._client.post(
                    f"{self._ollama_url}/api/embeddings",
                    json={"model": "nomic-embed-text", "prompt": doc[:512]},
                )
                if emb_resp.status_code == 200:
                    emb_data = emb_resp.json()
                    embeddings.append(emb_data.get("embedding", []))
                else:
                    embeddings.append([])

            collection.add(
                embeddings=embeddings,
                documents=documents,
                ids=ids or [f"chunk_{i}" for i in range(len(documents))],
                metadatas=metadatas,
            )
            return {"ok": True, "count": len(documents)}
        except Exception as e:
            logger.warning("add_documents failed (non-fatal): %s", e)
            return {"ok": False, "error": str(e)}

    async def ingest_text(
        self,
        content: str,
        kb_name: str,
        filename: str = "",
        source: str = "",
        trace_id: str = "",
    ) -> dict:
        """Ingest text content into the knowledge base."""
        try:
            import chromadb
            from chromadb.config import Settings

            os.makedirs(self._chroma_dir, exist_ok=True)
            client = chromadb.PersistentClient(
                path=self._chroma_dir,
                settings=Settings(anonymized_telemetry=False),
            )
            collection = client.get_or_create_collection(name=kb_name)

            doc_id = f"{filename or 'text'}_{trace_id or id(content)}"
            # Get embedding from Ollama
            emb_resp = await self._client.post(
                f"{self._ollama_url}/api/embeddings",
                json={"model": "nomic-embed-text", "prompt": content[:512]},
            )
            if emb_resp.status_code == 200:
                emb_data = emb_resp.json()
                embedding = emb_data.get("embedding", [])
                collection.add(
                    embeddings=[embedding],
                    documents=[content],
                    ids=[doc_id],
                    metadatas=[{"filename": filename, "source": source}],
                )
            return {"ok": True, "doc_id": doc_id}
        except Exception as e:
            logger.warning("Ingest text failed (non-fatal): %s", e)
            return {"ok": False, "error": str(e)}

    async def query(
        self,
        collection_name: str,
        query_texts: list[str],
        n_results: int = 5,
    ) -> list[dict]:
        """Query the vector store for relevant documents."""
        try:
            import chromadb
            from chromadb.config import Settings

            os.makedirs(self._chroma_dir, exist_ok=True)
            client = chromadb.PersistentClient(
                path=self._chroma_dir,
                settings=Settings(anonymized_telemetry=False),
            )
            collection = client.get_or_create_collection(name=collection_name)

            emb_resp = await self._client.post(
                f"{self._ollama_url}/api/embeddings",
                json={"model": "nomic-embed-text", "prompt": query_texts[0][:512]},
            )
            if emb_resp.status_code != 200:
                return []
            emb_data = emb_resp.json()
            embedding = emb_data.get("embedding", [])
            if not embedding:
                return []

            results = collection.query(
                query_embeddings=[embedding],
                n_results=n_results,
            )
            docs = []
            distances_list = results.get("distances", [[]])[0] or []
            for i, doc in enumerate(results.get("documents", [[]])[0]):
                meta = (results.get("metadatas", [[]])[0] or {}) if results.get("metadatas") else {}
                dist = float(distances_list[i]) if i < len(distances_list) else 1.0
                docs.append({"content": doc, "metadata": meta, "distance": dist})
            return docs
        except Exception as e:
            logger.warning("Vector query failed (non-fatal): %s", e)
            return []

    async def ocr(
        self,
        image_data: str,
        language: str = "zh",
        return_formulas: bool = True,
        return_layout: bool = True,
        tool_name: str = "",
    ) -> str:
        """OCR an image using the configured multimodal LLM."""
        try:
            img_bytes = base64.b64decode(image_data)
            img_b64 = base64.b64encode(img_bytes).decode("ascii")
            prompt = (
                "Transcribe all visible text from this image exactly as written, "
                "preserving the original language, paragraphs, and line breaks. "
                "Return only the transcribed text."
            )
            payload = {
                "model": self._ocr_model,
                "messages": [
                    {
                        "role": "user",
                        "content": [
                            {"type": "text", "text": prompt},
                            {
                                "type": "image_url",
                                "image_url": {"url": f"data:image/png;base64,{img_b64}"},
                            },
                        ],
                    }
                ],
                "stream": False,
            }
            resp = await self._client.post(
                f"{self._ollama_url}/v1/chat/completions",
                json=payload,
            )
            if resp.status_code != 200:
                logger.warning("OCR failed: HTTP %d", resp.status_code)
                return ""
            data = resp.json()
            return (data.get("choices", [{}])[0].get("message", {}).get("content") or "").strip()
        except Exception as e:
            logger.warning("OCR failed: %s", e)
            return ""

    async def vision(
        self,
        image_data: str,
        question: str = "",
        tool_name: str = "",
    ) -> str:
        """Vision QA using multimodal LLM."""
        try:
            prompt = question or "Describe what you see in this image."
            payload = {
                "model": self._ocr_model,
                "messages": [
                    {
                        "role": "user",
                        "content": [
                            {"type": "text", "text": prompt},
                            {
                                "type": "image_url",
                                "image_url": {"url": image_data},
                            },
                        ],
                    }
                ],
                "stream": False,
            }
            resp = await self._client.post(
                f"{self._ollama_url}/v1/chat/completions",
                json=payload,
            )
            if resp.status_code != 200:
                logger.warning("Vision failed: HTTP %d", resp.status_code)
                return ""
            data = resp.json()
            return (data.get("choices", [{}])[0].get("message", {}).get("content") or "").strip()
        except Exception as e:
            logger.warning("Vision failed: %s", e)
            return ""


def get_provider_instance() -> UnifiedLocalProvider:
    """Get or create the singleton provider instance."""
    global _provider_instance
    if _provider_instance is None:
        _provider_instance = UnifiedLocalProvider()
    return _provider_instance


def reset_provider_instance() -> None:
    """Reset the provider singleton (forces re-initialization on next access)."""
    global _provider_instance
    _provider_instance = None
