#define PY_SSIZE_T_CLEAN
#include <Python.h>
#include "fastq.h"

/* ── Queue Object ───────────────────────────────────────────────── */

typedef struct {
    PyObject_HEAD
    fastq_redis_t *redis;
    fastq_queue_t *queue;
} PyFastQQueue;

static void Queue_dealloc(PyFastQQueue *self)
{
    if (self->queue) fastq_queue_destroy(self->queue);
    if (self->redis) fastq_redis_disconnect(self->redis);
    Py_TYPE(self)->tp_free((PyObject *)self);
}

static int Queue_init(PyFastQQueue *self, PyObject *args, PyObject *kwds)
{
    const char *host = "127.0.0.1";
    int port = 6379;
    const char *name = NULL;

    static char *kwlist[] = {"name", "host", "port", NULL};
    if (!PyArg_ParseTupleAndKeywords(args, kwds, "s|si", kwlist,
                                     &name, &host, &port))
        return -1;

    self->redis = fastq_redis_connect(host, port);
    if (!self->redis) {
        PyErr_SetString(PyExc_ConnectionError, "Failed to connect to Redis");
        return -1;
    }

    self->queue = fastq_queue_create(self->redis, name);
    if (!self->queue) {
        fastq_redis_disconnect(self->redis);
        self->redis = NULL;
        PyErr_SetString(PyExc_RuntimeError, "Failed to create queue");
        return -1;
    }

    return 0;
}

static PyObject *Queue_push(PyFastQQueue *self, PyObject *args)
{
    const char *payload;
    int priority = FASTQ_PRIORITY_NORMAL;

    if (!PyArg_ParseTuple(args, "s|i", &payload, &priority))
        return NULL;

    fastq_job_t *job = fastq_job_create(payload, priority);
    if (!job) {
        PyErr_SetString(PyExc_RuntimeError, "Failed to create job");
        return NULL;
    }

    fastq_err_t err = fastq_push(self->queue, job);
    if (err != FASTQ_OK) {
        fastq_job_destroy(job);
        PyErr_Format(PyExc_RuntimeError, "Push failed (error %d)", err);
        return NULL;
    }

    PyObject *job_id = PyUnicode_FromString(job->id);
    fastq_job_destroy(job);
    return job_id;
}

static PyObject *Queue_pop(PyFastQQueue *self, PyObject *args)
{
    int timeout = 5;
    if (!PyArg_ParseTuple(args, "|i", &timeout))
        return NULL;

    fastq_job_t *job = fastq_pop(self->queue, timeout);
    if (!job)
        Py_RETURN_NONE;

    PyObject *dict = PyDict_New();
    PyDict_SetItemString(dict, "id", PyUnicode_FromString(job->id));
    PyDict_SetItemString(dict, "payload", PyUnicode_FromString(job->payload));
    PyDict_SetItemString(dict, "priority", PyLong_FromLong(job->priority));
    PyDict_SetItemString(dict, "retries", PyLong_FromLong(job->retries));

    fastq_job_done(self->queue, job);
    fastq_job_destroy(job);
    return dict;
}

static PyObject *Queue_stats(PyFastQQueue *self, PyObject *Py_UNUSED(args))
{
    fastq_stats_t s;
    if (fastq_stats(self->queue, &s) != FASTQ_OK) {
        PyErr_SetString(PyExc_RuntimeError, "Failed to get stats");
        return NULL;
    }

    return Py_BuildValue("{s:i, s:i, s:i, s:i, s:i}",
                         "pending",    s.pending,
                         "processing", s.processing,
                         "done",       s.done,
                         "failed",     s.failed,
                         "delayed",    s.delayed);
}

static PyMethodDef Queue_methods[] = {
    {"push",  (PyCFunction)Queue_push,  METH_VARARGS,
     "push(payload, priority=2) -> job_id\n"
     "Push a job onto the queue. Returns the job ID string."},
    {"pop",   (PyCFunction)Queue_pop,   METH_VARARGS,
     "pop(timeout=5) -> dict or None\n"
     "Pop a job from the queue. Returns a dict with id, payload, priority, retries."},
    {"stats", (PyCFunction)Queue_stats, METH_NOARGS,
     "stats() -> dict\n"
     "Return queue statistics."},
    {NULL}
};

static PyTypeObject PyFastQQueueType = {
    PyVarObject_HEAD_INIT(NULL, 0)
    .tp_name      = "fastq.Queue",
    .tp_basicsize = sizeof(PyFastQQueue),
    .tp_dealloc   = (destructor)Queue_dealloc,
    .tp_flags     = Py_TPFLAGS_DEFAULT,
    .tp_doc       = "FastQ Queue(name, host='127.0.0.1', port=6379)",
    .tp_methods   = Queue_methods,
    .tp_init      = (initproc)Queue_init,
    .tp_new       = PyType_GenericNew,
};

/* ── Worker Object ──────────────────────────────────────────────── */

typedef struct {
    PyObject_HEAD
    PyFastQQueue    *py_queue;
    PyObject        *callback;
    fastq_worker_t  *worker;
    int              num_threads;
} PyFastQWorker;

static int py_handler(fastq_job_t *job, void *user_data)
{
    PyObject *callback = (PyObject *)user_data;

    PyGILState_STATE gstate = PyGILState_Ensure();

    PyObject *dict = PyDict_New();
    PyDict_SetItemString(dict, "id", PyUnicode_FromString(job->id));
    PyDict_SetItemString(dict, "payload", PyUnicode_FromString(job->payload));
    PyDict_SetItemString(dict, "priority", PyLong_FromLong(job->priority));
    PyDict_SetItemString(dict, "retries", PyLong_FromLong(job->retries));

    PyObject *result = PyObject_CallOneArg(callback, dict);
    Py_DECREF(dict);

    int rc = 0;
    if (!result) {
        PyErr_Print();
        rc = -1;
    } else {
        Py_DECREF(result);
    }

    PyGILState_Release(gstate);
    return rc;
}

static void Worker_dealloc(PyFastQWorker *self)
{
    if (self->worker) {
        fastq_worker_stop(self->worker);
        fastq_worker_destroy(self->worker);
    }
    Py_XDECREF(self->callback);
    Py_XDECREF(self->py_queue);
    Py_TYPE(self)->tp_free((PyObject *)self);
}

static int Worker_init(PyFastQWorker *self, PyObject *args, PyObject *kwds)
{
    PyObject *queue_obj;
    PyObject *callback;
    int threads = 1;

    static char *kwlist[] = {"queue", "handler", "threads", NULL};
    if (!PyArg_ParseTupleAndKeywords(args, kwds, "O!O|i", kwlist,
                                     &PyFastQQueueType, &queue_obj,
                                     &callback, &threads))
        return -1;

    if (!PyCallable_Check(callback)) {
        PyErr_SetString(PyExc_TypeError, "handler must be callable");
        return -1;
    }

    self->py_queue   = (PyFastQQueue *)queue_obj;
    Py_INCREF(self->py_queue);
    self->callback   = callback;
    Py_INCREF(self->callback);
    self->num_threads = threads;

    self->worker = fastq_worker_create(self->py_queue->queue,
                                        py_handler, self->callback);
    if (!self->worker) {
        PyErr_SetString(PyExc_RuntimeError, "Failed to create worker");
        return -1;
    }

    fastq_worker_set_threads(self->worker, threads);
    return 0;
}

static PyObject *Worker_start(PyFastQWorker *self, PyObject *Py_UNUSED(args))
{
    Py_BEGIN_ALLOW_THREADS
    fastq_worker_start(self->worker);
    Py_END_ALLOW_THREADS
    Py_RETURN_NONE;
}

static PyObject *Worker_stop(PyFastQWorker *self, PyObject *Py_UNUSED(args))
{
    fastq_worker_stop(self->worker);
    Py_RETURN_NONE;
}

static PyMethodDef Worker_methods[] = {
    {"start", (PyCFunction)Worker_start, METH_NOARGS, "Start the worker (blocking)."},
    {"stop",  (PyCFunction)Worker_stop,  METH_NOARGS, "Signal the worker to stop."},
    {NULL}
};

static PyTypeObject PyFastQWorkerType = {
    PyVarObject_HEAD_INIT(NULL, 0)
    .tp_name      = "fastq.Worker",
    .tp_basicsize = sizeof(PyFastQWorker),
    .tp_dealloc   = (destructor)Worker_dealloc,
    .tp_flags     = Py_TPFLAGS_DEFAULT,
    .tp_doc       = "FastQ Worker(queue, handler, threads=1)",
    .tp_methods   = Worker_methods,
    .tp_init      = (initproc)Worker_init,
    .tp_new       = PyType_GenericNew,
};

/* ── Module ─────────────────────────────────────────────────────── */

static PyObject *py_set_log_level(PyObject *Py_UNUSED(self), PyObject *args)
{
    int level;
    if (!PyArg_ParseTuple(args, "i", &level)) return NULL;
    fastq_set_log_level(level);
    Py_RETURN_NONE;
}

static PyMethodDef module_methods[] = {
    {"set_log_level", py_set_log_level, METH_VARARGS,
     "set_log_level(level) - 0=DEBUG, 1=INFO, 2=WARN, 3=ERROR"},
    {NULL}
};

static struct PyModuleDef fastq_module = {
    PyModuleDef_HEAD_INIT,
    "fastq",
    "FastQ - High-performance job queue",
    -1,
    module_methods
};

PyMODINIT_FUNC PyInit_fastq(void)
{
    PyObject *m;

    if (PyType_Ready(&PyFastQQueueType) < 0) return NULL;
    if (PyType_Ready(&PyFastQWorkerType) < 0) return NULL;

    m = PyModule_Create(&fastq_module);
    if (!m) return NULL;

    Py_INCREF(&PyFastQQueueType);
    PyModule_AddObject(m, "Queue", (PyObject *)&PyFastQQueueType);

    Py_INCREF(&PyFastQWorkerType);
    PyModule_AddObject(m, "Worker", (PyObject *)&PyFastQWorkerType);

    /* Priority constants */
    PyModule_AddIntConstant(m, "PRIORITY_HIGH",   FASTQ_PRIORITY_HIGH);
    PyModule_AddIntConstant(m, "PRIORITY_NORMAL", FASTQ_PRIORITY_NORMAL);
    PyModule_AddIntConstant(m, "PRIORITY_LOW",    FASTQ_PRIORITY_LOW);

    return m;
}
