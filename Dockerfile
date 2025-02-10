ARG RUST_VERSION=1.83.0

FROM rust:${RUST_VERSION}-bookworm AS builder
WORKDIR /app
COPY . .
RUN \
  --mount=type=cache,target=/app/target/ \
  --mount=type=cache,target=/usr/local/cargo/registry/ \
  cargo build --release && \
  cp ./target/release/kiyoshi /

FROM debian:bookworm-slim AS final
# Add SSL runtime libraries
RUN apt-get update && apt-get install -y \
    ca-certificates \
    openssl \
    && rm -rf /var/lib/apt/lists/*
RUN adduser \
  --disabled-password \
  --gecos "" \
  --home "/nonexistent" \
  --shell "/sbin/nologin" \
  --no-create-home \
  --uid "10001" \
  appuser
COPY --from=builder /kiyoshi /usr/local/bin
RUN chown appuser /usr/local/bin/kiyoshi
# COPY --from=builder /app/config /opt/kiyoshi/config
# RUN chown -R appuser /opt/kiyoshi
USER appuser
ENV RUST_LOG="kiyoshi=info"
WORKDIR /opt/kiyoshi
# COPY config/config.yaml /opt/kiyoshi/config.yaml
ENTRYPOINT ["kiyoshi", "-c", "config.yaml", "-v"]
# EXPOSE 8080/tcp