FROM rust:1-bookworm AS builder

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        ca-certificates \
        libsqlite3-dev \
        libssl-dev \
        pkg-config \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY Cargo.toml Cargo.lock ./
COPY src ./src
COPY migrations ./migrations

RUN cargo build --release --locked

FROM debian:bookworm-slim

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        ca-certificates \
        libsqlite3-0 \
        libssl3 \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY --from=builder /app/target/release/audiobooks_downloader /usr/local/bin/audiobooks_downloader
RUN useradd --system --uid 10001 --home-dir /data --shell /usr/sbin/nologin app \
    && mkdir -p /data/storage \
    && chown -R app:app /data

ENV APP_BIND_ADDR=0.0.0.0:3000 \
    APP_STORAGE_ROOT=/data/storage \
    APP_DATABASE_URL=sqlite:///data/audiobooks_downloader.db

VOLUME /data

USER app

EXPOSE 3000

CMD ["audiobooks_downloader"]
