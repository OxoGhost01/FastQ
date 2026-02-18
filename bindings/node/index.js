'use strict';

const native = require('./build/Release/fastq_native');

/**
 * FastQ Queue wrapper.
 *
 * @example
 * const { Queue } = require('fastq-native');
 * const q = new Queue('127.0.0.1', 6379, 'emails');
 * const id = q.push('{"user":1}');            // normal priority
 * const id = q.push('{"user":2}', 1);         // high priority (1=high, 3=low)
 * const job = q.pop(1);                        // block up to 1s
 * if (job) q.done(job.id);
 * const stats = q.stats();                     // {pending, done, failed, delayed}
 */
class Queue {
    constructor(host, port, name, poolSize = 0) {
        return new native.Queue(host, port, name, poolSize);
    }
}

/**
 * FastQ Worker wrapper.
 *
 * @example
 * const { Queue, Worker } = require('fastq-native');
 * const q = new Queue('127.0.0.1', 6379, 'emails', 4);
 * const w = new Worker(q, (job) => {
 *     console.log('Processing', job.payload);
 *     return true;  // return truthy to mark done, falsy to fail
 * }, 4);
 * w.start().then(() => console.log('Worker stopped'));
 * // later: w.stop();
 */
class Worker {
    constructor(queue, handler, numThreads = 1) {
        return new native.Worker(queue, handler, numThreads);
    }
}

module.exports = { Queue, Worker };
