"""
tutor_platform/tools/preprocess.py — 图像预处理

提供 OCR 前的图像增强预处理。
"""

import logging

logger = logging.getLogger("tutor_platform.tools.preprocess")


def preprocess_image_bytes(image_bytes: bytes) -> bytes:
    """对图像字节进行 OCR 前预处理 (增强对比度、去噪、二值化).

    使用 OpenCV 进行:
      1. 灰度化
      2. 自适应直方图均衡 (CLAHE)
      3. 双边滤波去噪
      4. 自适应二值化

    Args:
        image_bytes: 原始图像字节 (JPEG/PNG)

    Returns:
        预处理后的 PNG 图像字节
    """
    try:
        import cv2
        import numpy as np

        nparr = np.frombuffer(image_bytes, np.uint8)
        img = cv2.imdecode(nparr, cv2.IMREAD_COLOR)
        if img is None:
            logger.warning("preprocess: decode failed, returning original")
            return image_bytes

        gray = cv2.cvtColor(img, cv2.COLOR_BGR2GRAY)

        clahe = cv2.createCLAHE(clipLimit=2.0, tileGridSize=(8, 8))
        enhanced = clahe.apply(gray)

        denoised = cv2.bilateralFilter(enhanced, 9, 75, 75)

        binary = cv2.adaptiveThreshold(
            denoised, 255, cv2.ADAPTIVE_THRESH_GAUSSIAN_C,
            cv2.THRESH_BINARY, 11, 2,
        )

        _, buffer = cv2.imencode(".png", binary)
        return buffer.tobytes()

    except ImportError:
        logger.warning("OpenCV not available, returning original image bytes")
        return image_bytes
    except Exception as e:
        logger.warning("Image preprocessing failed: %s, returning original", e)
        return image_bytes
