FROM rust:1.97-alpine AS builder
LABEL authors="joe"
WORKDIR /app
COPY . .
RUN cargo build --release
FROM debian:bookworm-slim
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        ca-certificates \
    && rm -rf /var/lib/apt/lists/*
COPY --from=builder /app/target/release/imagers /usr/bin/
ENTRYPOINT ["imagers"]

