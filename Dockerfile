FROM rust:1-bookworm AS builder

WORKDIR /build

COPY . .

RUN cargo build --release --locked

FROM debian:bookworm-slim AS runtime

RUN useradd --uid 1000 --create-home --shell /usr/sbin/nologin dash

COPY --from=builder /build/target/release/dash /usr/local/bin/dash

# The default bind address is loopback-only; containers must listen on all interfaces.
ENV DASH_ADDR=0.0.0.0:9090

USER dash

EXPOSE 9090

CMD ["dash"]
