{
  "targets": [{
    "target_name": "fastq_native",
    "sources": ["src/fastq_node.c"],
    "include_dirs": [
      "../../include"
    ],
    "libraries": [
      "-L<(module_root_dir)/../../build",
      "-lfastq_pic",
      "-lhiredis",
      "-ljson-c",
      "-lpthread",
      "-lssl",
      "-lcrypto"
    ],
    "cflags": [
      "-std=c11",
      "-Wall",
      "-Wextra",
      "-D_POSIX_C_SOURCE=200809L"
    ],
    "ldflags": [
      "-Wl,-rpath,<(module_root_dir)/../../build"
    ]
  }]
}
