# ============================================
# Platform Service Dockerfile
# Provider API + MCP + Cron + Device Manager
# ============================================

FROM python:3.11-slim

# Install avahi-utils for mDNS LAN discovery + antiword (old .doc) + catdoc (catppt for old .ppt)
# libreoffice-writer converts old .doc → PDF for image/equation OCR via MiniCPM-V
RUN sed -i 's/deb.debian.org/mirrors.aliyun.com/g' /etc/apt/sources.list.d/debian.sources 2>/dev/null || true \
    && sed -i 's/deb.debian.org/mirrors.aliyun.com/g' /etc/apt/sources.list 2>/dev/null || true \
    && apt-get update && apt-get install -y --no-install-recommends \
    avahi-utils \
    antiword \
    catdoc \
    libreoffice-writer \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Install Python dependencies (shared project deps + platform extras)
COPY requirements/ ./requirements/
COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt \
    && pip install --no-cache-dir chromadb mcp markitdown "qrcode[pil]" opencv-python-headless

# Copy local packages (paths match dev sys.path inserts)
COPY tutor_platform/ /tutor_platform/
COPY domains/ /domains/
COPY vendor/hermes-agent/ /vendor/hermes-agent/

# Copy platform source code
COPY docker/platform/provider_api.py ./
COPY docker/platform/mcp_server.py ./
COPY docker/platform/device_manager_api.py ./

# Runtime environment
ENV PROVIDER_API_PORT=8100
ENV DEVICE_MANAGER_PORT=8101
ENV PYTHONPATH=/vendor/hermes-agent:/tutor_platform

EXPOSE 8100 8101

CMD ["python", "provider_api.py"]
