# Stage 1: build
FROM debian:bookworm-slim AS builder

RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    make \
    libhiredis-dev \
    libjson-c-dev \
    libssl-dev \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /build
COPY . .
RUN make

# Stage 2: runtime (CLI only)
FROM debian:bookworm-slim

RUN apt-get update && apt-get install -y --no-install-recommends \
    libhiredis0.14 \
    libjson-c5 \
    libssl3 \
    && rm -rf /var/lib/apt/lists/*

COPY --from=builder /build/build/fastq /usr/local/bin/fastq

ENTRYPOINT ["fastq"]
CMD ["--help"]
