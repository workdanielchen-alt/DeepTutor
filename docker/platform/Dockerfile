# ============================================
# Platform Service Dockerfile
# Provider API + MCP + Cron + Device Manager
# ============================================

FROM python:3.11-slim

# Install avahi-utils for mDNS LAN discovery (avahi-publish-service → host avahi-daemon via D-Bus)
RUN apt-get update && apt-get install -y avahi-utils && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Install Python dependencies (shared project deps + platform extras)
COPY requirements/ ./requirements/
COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt \
    && pip install --no-cache-dir chromadb mcp markitdown qrcode[pil]

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
ENV PYTHONPATH=/vendor/hermes-agent

EXPOSE 8100 8101

CMD ["python", "provider_api.py"]
