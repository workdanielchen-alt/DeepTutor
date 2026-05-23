"""
tutor_platform/storage.py — Provider 配置校验
"""

import logging

logger = logging.getLogger("tutor_platform.storage")


def validate_provider_config(config: dict | None = None) -> dict:
    """校验 Provider 配置是否有效。

    Args:
        config: 配置字典, 含 persist_dir, uploads_dir 等

    Returns:
        字典: {"ok": bool, "errors": list[str], "warnings": list[str]}
    """
    errors: list[str] = []
    warnings: list[str] = []
    if config is None:
        return {"ok": True, "errors": errors, "warnings": warnings}
    import os

    persist = config.get("persist_dir") or os.environ.get("CHROMA_PERSIST_DIR")
    if persist:
        try:
            os.makedirs(persist, exist_ok=True)
        except Exception as e:
            logger.warning("Cannot create persist_dir %s: %s", persist, e)
            errors.append(f"无法创建持久化目录 {persist}: {e}")
    return {"ok": len(errors) == 0, "errors": errors, "warnings": warnings}
