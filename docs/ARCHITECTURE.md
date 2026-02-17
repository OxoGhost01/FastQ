# FastQ - Architecture

## Overview

FastQ is a high-performance job queue written in C, backed by Redis. It is designed to be faster and more memory-efficient than existing solutions (BullMQ, Sidekiq, Celery) while providing bindings for multiple languages.

## Data Flow

```
                          Redis
                     ┌──────────────┐
  Producer           │              │           Worker(s)
  ────────►  push()  │  LIST/ZSET   │  pop()  ────────►  handler()
             ────►   │  HASH        │  ◄────             ────►
                     │              │
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
| `fastq:queue:{name}` | LIST | Main job queue (FIFO) |
| `fastq:queue:{name}:high` | LIST | High priority jobs |
| `fastq:queue:{name}:normal` | LIST | Normal priority jobs |
| `fastq:queue:{name}:low` | LIST | Low priority jobs |
| `fastq:job:{id}` | HASH | Job metadata (status, retries, payload, timestamps) |
| `fastq:queue:{name}:delayed` | ZSET | Delayed/retry jobs, scored by execution timestamp |
| `fastq:queue:{name}:dead` | LIST | Dead letter queue |
| `fastq:queue:{name}:done` | LIST | Completed job IDs (optional history) |

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
Thin wrapper around hiredis. Handles connection, reconnection, and connection pooling.

### 2. Job (`src/job.c`)
Job struct creation, serialization (JSON via json-c), and destruction.

### 3. Queue (`src/queue.c`)
Push/pop operations, stats, queue management.

### 4. Worker (`src/worker.c`)
Single or multi-threaded job processor. Calls user-defined handler callback for each job.

### 5. CLI (`src/cli.c`)
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
