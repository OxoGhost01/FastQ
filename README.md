# FastQ

A high-performance job queue written in C, backed by Redis.

FastQ aims to be **faster**, **leaner on RAM**, and **polyglot** compared to existing job queue solutions like BullMQ, Sidekiq, or Celery.

## Features (planned)

- Blazing fast push/pop via Redis Lists
- Priority queues (high / normal / low)
- Automatic retries with exponential backoff
- Dead Letter Queue
- Multi-threaded worker pool
- Language bindings: **C** (native), **Python**, **Node.js**, and more
- Prometheus metrics & health endpoint
- Daemon mode with systemd support

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

## Project Structure

```
fastq/
├── src/           # Core library source
├── include/       # Public headers
├── tests/         # Unit and integration tests
├── examples/      # Usage examples
├── docs/          # Architecture & API design docs
├── Makefile
└── run_redis.sh   # Dev Redis launcher
```

## Roadmap

### Phase 0 - Setup & Architecture
- [x] Research (hiredis, Redis data structures, existing solutions)
- [x] Project structure & build system
- [x] Architecture & API design documentation
- [x] Dev environment setup

### Phase 1 - Core MVP
- [ ] Redis adapter (connect, disconnect, ping)
- [ ] Queue operations (push / pop)
- [ ] Job serialization (JSON)
- [ ] Single-threaded worker with callback handler
- [ ] Retry with exponential backoff
- [ ] Dead Letter Queue
- [ ] CLI tool (`fastq push`, `pop`, `stats`, `worker`)
- [ ] Basic stats & logging
- [ ] Integration tests, zero memory leaks

### Phase 2 - Production Ready
- [ ] Multi-threaded worker pool (pthreads)
- [ ] Redis connection pooling
- [ ] Configuration file support
- [ ] Priority queues
- [ ] Memory & Redis optimizations
- [ ] Python bindings
- [ ] Daemon mode & systemd service
- [ ] Crash recovery for orphaned jobs

### Phase 3 - Pro Features
- [ ] Delayed & cron-like job scheduling
- [ ] Rate limiting (token bucket, per-queue & global)
- [ ] Batch processing
- [ ] Prometheus metrics & health endpoint
- [ ] Job chaining & workflow DAGs
- [ ] Node.js bindings

### Phase 4 - Polish & Launch
- [ ] Full test suite (unit, integration, load, stress)
- [ ] Security audit
- [ ] Packaging (deb, Docker)
- [ ] Documentation & website
- [ ] v1.0.0 release

## License

TBD
