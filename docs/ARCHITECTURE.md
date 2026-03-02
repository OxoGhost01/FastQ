# FastQ - Architecture

## Overview

FastQ is a high-performance job queue written in C, backed by Redis. It is designed to be faster and more memory-efficient than existing solutions (BullMQ, Sidekiq, Celery) while providing bindings for multiple languages.

## Data Flow

```
  Producer               Redis                 Worker(s)
                    ┌──────────────┐
  ── push() ───────►│  LIST/ZSET   │──── pop() ──────► handler()
                    │  HASH        │
                    └──────────────┘
```

### Lifecycle of a Job

```
  push()         pop()          handler()
 ┌──────┐     ┌──────────┐     ┌──────────┐
 │QUEUED│────►│PROCESSING│────►│   DONE   │
 └──────┘     └─────┬────┘     └──────────┘
                    │
                    │ fail()
                    ▼
              ┌──────────┐     retries < max
              │  FAILED  │─────────────────►  re-queue (with backoff)
              └─────┬────┘
                    │ retries >= max
                    ▼
              ┌──────────┐
              │   DEAD   │  (Dead Letter Queue)
              └──────────┘
```

## Redis Key Structure

All keys are prefixed with `fastq:` to avoid collisions.

| Key | Type | Description |
|-----|------|-------------|
| `fastq:queue:{name}:high` | LIST | High priority jobs |
| `fastq:queue:{name}:normal` | LIST | Normal priority jobs |
| `fastq:queue:{name}:low` | LIST | Low priority jobs |
| `fastq:job:{id}` | HASH | Job metadata (status, retries, payload, timestamps) |
| `fastq:queue:{name}:delayed` | ZSET | Delayed/retry jobs, scored by execution timestamp |
| `fastq:queue:{name}:dead` | LIST | Dead letter queue |
| `fastq:queue:{name}:done` | LIST | Completed job IDs |
| `fastq:chain:{id}` | STRING | Chain/workflow continuation for job `{id}` |
| `fastq:wf:{wf_id}` | HASH | Workflow total/remaining counters |
| `fastq:sched:{name}` | HASH | Persisted cron entries |

### Job Hash Fields (`fastq:job:{id}`)

| Field | Description |
|-------|-------------|
| `id` | Unique job identifier |
| `queue` | Queue name |
| `payload` | JSON-encoded job data |
| `status` | `queued`, `processing`, `done`, `failed`, `dead` |
| `priority` | `1` (high), `2` (normal), `3` (low) |
| `retries` | Current retry count |
| `max_retries` | Maximum retries before DLQ |
| `created_at` | Unix timestamp |
| `processed_at` | Unix timestamp (when picked up) |
| `completed_at` | Unix timestamp (when done/failed) |
| `error` | Last error message (if failed) |

## Core Components

### 1. Redis Adapter (`src/redis_adapter.c`)
Thin wrapper around hiredis. Handles connection and ping.

### 2. Connection Pool (`src/pool.c`)
Thread-safe pool of Redis connections. Workers acquire/release connections to avoid contention.

### 3. Job (`src/job.c`)
Job struct creation, serialization (JSON via json-c), and destruction.

### 4. Queue (`src/queue.c`)
Push/pop operations, priority routing, delayed promotion, stats, DLQ, and crash recovery.

### 5. Worker (`src/worker.c`)
Single or multi-threaded job processor with optional rate limiting and batch mode.

### 6. Scheduler (`src/scheduler.c`)
Cron-based and one-shot job scheduler with Redis persistence across restarts.

### 7. Workflow (`src/workflow.c`)
DAG workflow engine. Jobs are chained via dependency tracking in Redis; root jobs are pushed immediately, dependents unlock atomically when all predecessors complete.

### 8. Rate Limiter (`src/ratelimit.c`)
Token bucket limiter attached to a worker; controls maximum job throughput.

### 9. Metrics (`src/metrics.c`)
HTTP server exposing `/metrics` (Prometheus) and `/health` (JSON) on a configurable port.

### 10. License (`src/license.c`)
HMAC-SHA256 license verification with constant-time comparison.

### 11. CLI (`src/cli.c`)
Command-line interface for push, pop, stats, and worker management.

## Thread Safety

- Each worker thread gets its own Redis connection (connection pool).
- Job structs are not shared between threads.
- Queue-level operations use Redis atomicity (RPUSH, BLPOP are atomic).
- Stats counters use atomic operations or mutex where needed.

## Memory Management

- All `_create()` functions have a corresponding `_destroy()` function.
- Jobs are allocated on the heap and freed after processing.
- Object pooling for `fastq_job_t` in high-throughput mode to avoid malloc/free churn.
- Zero tolerance for memory leaks (verified with Valgrind).
