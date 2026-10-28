FROM rust:1-bookworm AS build

RUN apt-get update \
    && apt-get install --no-install-recommends -y build-essential pkg-config ca-certificates \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /build
COPY Cargo.toml Cargo.lock ./
COPY src ./src
COPY LICENSE-APACHE LICENSE-MIT ./
RUN cargo build --release --locked --bin appview-rs

FROM debian:bookworm-slim AS runtime

LABEL org.opencontainers.image.licenses="MIT OR Apache-2.0"

RUN apt-get update \
    && apt-get install --no-install-recommends -y ca-certificates curl \
    && rm -rf /var/lib/apt/lists/* \
    && groupadd --gid 10001 appview \
    && useradd --uid 10001 --gid appview --no-create-home --shell /usr/sbin/nologin appview \
    && mkdir -p /data \
    && chown appview:appview /data

COPY --from=build /build/target/release/appview-rs /usr/local/bin/appview-rs
COPY LICENSE-APACHE LICENSE-MIT /usr/share/doc/appview-rs/

ENV APPVIEW_DATA_DIR=/data \
    APPVIEW_LISTEN=0.0.0.0:3000

VOLUME ["/data"]
EXPOSE 3000
USER 10001:10001

HEALTHCHECK --interval=30s --timeout=3s --start-period=10s --retries=3 \
    CMD ["/usr/bin/curl", "--fail", "--silent", "http://127.0.0.1:3000/healthz"]

ENTRYPOINT ["/usr/local/bin/appview-rs"]
