FROM ollama/ollama:latest AS base

# Download and extract ollama v0.30.0-rc23
RUN apt-get update -qq && \
    apt-get install -y -qq --no-install-recommends zstd ca-certificates curl && \
    rm -rf /var/lib/apt/lists/* && \
    curl -sL "https://github.com/ollama/ollama/releases/download/v0.30.0-rc23/ollama-linux-amd64.tar.zst" -o /tmp/ollama.tar.zst && \
    mkdir -p /extract && \
    tar -I zstd -xf /tmp/ollama.tar.zst -C /extract/ && \
    rm /tmp/ollama.tar.zst

# Copy binary + libs into the existing ollama installation
RUN cp /extract/ollama/bin/ollama /usr/bin/ollama && \
    chmod +x /usr/bin/ollama && \
    cp -r /extract/ollama/lib/* /usr/lib/ && \
    rm -rf /extract && \
    ldconfig && \
    ollama --version
