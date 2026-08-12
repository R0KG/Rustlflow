FROM rust:1.88-bookworm AS builder

WORKDIR /app

COPY Cargo.toml Cargo.lock ./
COPY src ./src
COPY docs ./docs

# Both binaries use the same crate graph. Build them together so Docker does
# not compile DataFusion and its dependencies separately for each service.
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/usr/local/cargo/git \
    cargo build --release --bin rustflow --bin worker

FROM debian:bookworm-slim AS runtime

RUN apt-get update \
    && apt-get install --no-install-recommends -y ca-certificates wget \
    && rm -rf /var/lib/apt/lists/* \
    && useradd --system --uid 10001 --create-home rustflow \
    && mkdir -p /app/data/datasets \
    && chown -R rustflow:rustflow /app

WORKDIR /app
USER rustflow

FROM runtime AS api

COPY --from=builder /app/target/release/rustflow /usr/local/bin/rustflow

EXPOSE 3000
ENTRYPOINT ["/usr/local/bin/rustflow"]

FROM runtime AS worker

COPY --from=builder /app/target/release/worker /usr/local/bin/worker

ENTRYPOINT ["/usr/local/bin/worker"]
