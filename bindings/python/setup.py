from setuptools import setup, Extension
import subprocess

# Get Python include path
py_includes = subprocess.check_output(
    ["python3-config", "--includes"]
).decode().split()

fastq_ext = Extension(
    "fastq",
    sources=["fastq_module.c"],
    include_dirs=["../../include"],
    library_dirs=["../../build"],
    libraries=["fastq", "hiredis", "json-c", "pthread"],
)

setup(
    name="fastq",
    version="0.1.0",
    description="Python bindings for FastQ job queue",
    ext_modules=[fastq_ext],
)
