"""Basic tests for FastQ Python bindings."""
import sys
import os
import threading
import time

# Add build dir to path so we can import the extension
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import fastq


def test_push_pop():
    q = fastq.Queue("py_test_push_pop")
    job_id = q.push('{"task": "test"}')
    assert isinstance(job_id, str)
    assert len(job_id) > 0

    job = q.pop(2)
    assert job is not None
    assert job["id"] == job_id
    assert job["payload"] == '{"task": "test"}'
    print("  test_push_pop              OK")


def test_stats():
    q = fastq.Queue("py_test_stats")
    q.push('{"n": 1}')
    q.push('{"n": 2}')

    s = q.stats()
    assert s["pending"] >= 2
    assert "done" in s
    assert "failed" in s

    # clean up
    q.pop(1)
    q.pop(1)
    print("  test_stats                 OK")


def test_pop_timeout():
    q = fastq.Queue("py_test_timeout")
    job = q.pop(1)
    assert job is None
    print("  test_pop_timeout           OK")


def test_priority():
    q = fastq.Queue("py_test_prio")
    q.push('{"p": "low"}', fastq.PRIORITY_LOW)
    q.push('{"p": "high"}', fastq.PRIORITY_HIGH)

    job = q.pop(1)
    assert job is not None
    assert job["payload"] == '{"p": "high"}'

    job = q.pop(1)
    assert job is not None
    assert job["payload"] == '{"p": "low"}'
    print("  test_priority              OK")


def test_worker():
    q = fastq.Queue("py_test_worker")
    results = []

    def handler(job):
        results.append(job["id"])

    w = fastq.Worker(q, handler)

    q.push('{"w": 1}')
    q.push('{"w": 2}')

    # run worker in a thread, stop after a short delay
    def stop_later():
        time.sleep(2)
        w.stop()

    t = threading.Thread(target=stop_later)
    t.start()
    w.start()
    t.join()

    assert len(results) == 2
    print("  test_worker                OK")


if __name__ == "__main__":
    fastq.set_log_level(2)  # WARN
    print("test_fastq_python:")
    test_push_pop()
    test_stats()
    test_pop_timeout()
    test_priority()
    test_worker()
    print("  All Python tests passed.")
