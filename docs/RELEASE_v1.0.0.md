## FastQ v1.0.0

First public release of FastQ — a high-performance Redis-backed job queue written in C.

FastQ is faster and leaner than BullMQ, Sidekiq, or Celery, with zero runtime dependencies beyond Redis.

### Core features (free)

- Priority queues (high / normal / low) via Redis lists with MULTI/EXEC pipelining
- Multi-threaded worker pool with Redis connection pooling
- Automatic retries with exponential backoff
- Dead Letter Queue for permanently failed jobs
- Crash recovery for orphaned jobs
- Daemon mode with systemd support
- Python and Node.js bindings
- CLI tool (`fastq push`, `pop`, `stats`, `worker`, `recover`)
- Colored logging (DEBUG / INFO / WARN / ERROR)

### Pro features (commercial license required)

- **Cron scheduler** — persistent cron jobs and one-shot delayed jobs, survive restarts
- **Rate limiting** — token-bucket limiter attached per worker
- **Batch processing** — pop up to N jobs in one shot with a deadline
- **Job chaining** — automatically push a child job when a parent completes
- **DAG workflows** — multi-step dependency graphs; dependents unlock atomically
- **Metrics** — Prometheus `/metrics` and JSON `/health` HTTP endpoint
- **HMAC license** — key verification with constant-time comparison

### Benchmarks (localhost, Redis 8.0, 10 000 jobs, 200 warmup)

Test machine: Debian 13 · Intel Core i7 (13th Gen) · 64 GB RAM · consumer laptop

| Operation | Throughput | p50 | p99 |
|-----------|-----------|-----|-----|
| push | ~27 000 jobs/s | 31 µs | 138 µs |
| pop | ~14 000 jobs/s | 49 µs | 164 µs |
| worker (1 thread) | ~3 500 jobs/s | — | — |
| worker (8 threads) | ~4 300 jobs/s | — | — |

Redis single-thread is the bottleneck at 8 threads.
Results on server-grade hardware will be proportionally higher for all frameworks.

### Installation

**Arch Linux (AUR):**

```bash
yay -S fastq-bin
```

**From source:**

```bash
git clone https://github.com/OxoGhost01/FastQ.git
cd FastQ
make
sudo make install
```

**Docker:**

```bash
docker pull oxoghost/fastq:1.0.0
docker run --rm oxoghost/fastq:1.0.0 push --help
```

**Python:**

```bash
pip install fastq
```

**Node.js:**

```bash
npm install fastq-js
```
