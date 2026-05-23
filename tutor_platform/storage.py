"""
tutor_platform/storage.py — Provider 配置校验
"""

import logging

logger = logging.getLogger("tutor_platform.storage")


def validate_provider_config(config: dict | None = None) -> bool:
    """校验 Provider 配置是否有效。

    Args:
        config: 配置字典, 含 persist_dir, uploads_dir 等

    Returns:
        True 如果配置有效
    """
    if config is None:
        return True
    import os
    persist = config.get("persist_dir") or os.environ.get("CHROMA_PERSIST_DIR")
    if persist:
        try:
            os.makedirs(persist, exist_ok=True)
        except Exception as e:
            logger.warning("Cannot create persist_dir %s: %s", persist, e)
            return False
    return True
