# FastQ

A high-performance job queue written in C, backed by Redis.

FastQ aims to be **faster**, **leaner on RAM**, and **polyglot** compared to existing job queue solutions like BullMQ, Sidekiq, or Celery.

## Features

- Fast push/pop via Redis Lists with MULTI/EXEC pipelining
- Priority queues (high / normal / low)
- Multi-threaded worker pool (pthreads) with Redis connection pooling
- Automatic retries with exponential backoff
- Dead Letter Queue for permanently failed jobs
- Crash recovery for orphaned jobs
- Daemon mode with systemd support
- Python bindings (CPython C extension)
- CLI tool (`fastq push`, `pop`, `stats`, `worker`, `recover`)
- Colored logging (DEBUG/INFO/WARN/ERROR)
- Zero external runtime dependencies beyond Redis

## Building

### Dependencies

- [hiredis](https://github.com/redis/hiredis) - Redis C client
- [json-c](https://github.com/json-c/json-c) - JSON parsing
- Redis server (for runtime)

### Compile

```bash
make
```

### Run tests

```bash
# Start a dev Redis instance (in a separate terminal)
./run_redis.sh

# Run tests
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

## Project Structure

```
fastq/
├── src/                # Core library source
├── include/            # Public headers
├── tests/              # Unit and integration tests
├── benchmarks/         # Throughput benchmarks
├── bindings/python/    # Python CPython extension
├── docs/               # Architecture & API design docs
├── Makefile
├── fastq.service       # systemd unit file
└── run_redis.sh        # Dev Redis launcher
```

## Roadmap

**Done** — Core engine, multi-threaded workers, priority queues, connection pooling, daemon mode, crash recovery, Python bindings

**Next up**
- Node.js bindings
- Job scheduling (delayed + cron)
- Rate limiting & batch processing

**Later**
- Prometheus metrics & health endpoint
- Workflow DAGs (job chaining with dependencies)

**Future**
- Go / Rust bindings
- Web UI
- Kubernetes operator

## License

TBD
