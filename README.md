# FastQ

A high-performance job queue written in C, backed by Redis.

FastQ aims to be **faster**, **leaner on RAM**, and **polyglot** compared to existing job queue solutions like BullMQ, Sidekiq, or Celery.

## Features

### Core (free)

- Fast push/pop via Redis Lists with MULTI/EXEC pipelining
- Priority queues (high / normal / low)
- Multi-threaded worker pool (pthreads) with Redis connection pooling
- Automatic retries with exponential backoff
- Dead Letter Queue for permanently failed jobs
- Crash recovery for orphaned jobs
- Daemon mode with systemd support
- Python bindings (CPython C extension)
- Node.js bindings (N-API)
- CLI tool (`fastq push`, `pop`, `stats`, `worker`, `recover`)
- Colored logging (DEBUG/INFO/WARN/ERROR)
- Zero external runtime dependencies beyond Redis

### Pro

- **Cron scheduler** — persistent cron jobs and one-shot delayed jobs, survive restarts
- **Rate limiting** — token-bucket limiter attached per worker
- **Batch processing** — pop up to N jobs in one shot with a deadline
- **Job chaining** — automatically push a child job when a parent completes
- **DAG workflows** — multi-step dependency graphs; dependents unlock atomically
- **Metrics** — Prometheus `/metrics` and JSON `/health` HTTP endpoint
- **HMAC license** — key verification with constant-time comparison (OpenSSL)

## Building

### Dependencies

- [hiredis](https://github.com/redis/hiredis) — Redis C client
- [json-c](https://github.com/json-c/json-c) — JSON parsing
- [OpenSSL](https://github.com/openssl/openssl) — HMAC-SHA256 (license + TLS)
- Redis server (runtime)

### Compile

```bash
make          # library + CLI
make test     # run all tests
make bench    # throughput benchmark
make python   # Python C extension
make node     # Node.js N-API addon
```

### Run tests

```bash
# Start a dev Redis instance (in a separate terminal)
./run_redis.sh

make test

# Run tests under Valgrind
make valgrind
```

### Benchmark

```bash
make bench
```

### Python bindings

```bash
make python
python3 -c "import fastq; q = fastq.Queue('myqueue'); q.push('{\"hello\":1}')"
```

### Node.js bindings

```bash
make node
node -e "const { Queue } = require('./bindings/node'); const q = new Queue('127.0.0.1', 6379, 'myqueue'); console.log(q.push('{\"hello\":1}'));"
```

## Project Structure

```
fastq/
├── src/                # Core library source
├── include/            # Public headers
├── tests/              # Unit and integration tests (38 tests, 7 suites)
├── benchmarks/         # Throughput benchmarks
├── bindings/
│   ├── python/         # Python CPython extension
│   └── node/           # Node.js N-API addon
├── docs/               # Architecture & API design docs
├── Makefile
├── fastq.service       # systemd unit file
└── run_redis.sh        # Dev Redis launcher
```

## Roadmap

**Done**

- Core engine, priority queues, multi-threaded workers, connection pooling
- Crash recovery, daemon mode, CLI
- Python and Node.js bindings
- Cron scheduler, rate limiter, batch processing
- Job chaining and DAG workflows
- Prometheus metrics, health endpoint
- HMAC license system
- 38 tests across 7 suites, all passing

**Next**

- Go / Rust bindings
- Web UI
- Kubernetes operator

## License

TBD
