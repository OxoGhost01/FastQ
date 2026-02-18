CC      = gcc
CFLAGS  = -Wall -Wextra -Werror -std=c11 -pedantic -D_POSIX_C_SOURCE=200809L -Iinclude
LDFLAGS = -lhiredis -ljson-c -lpthread

SRC_DIR   = src
INC_DIR   = include
TEST_DIR  = tests
BENCH_DIR = benchmarks
BUILD_DIR = build

# Library sources (everything except cli.c)
LIB_SRCS = $(filter-out $(SRC_DIR)/cli.c, $(wildcard $(SRC_DIR)/*.c))
LIB_OBJS = $(patsubst $(SRC_DIR)/%.c, $(BUILD_DIR)/%.o, $(LIB_SRCS))

TEST_SRCS = $(wildcard $(TEST_DIR)/*.c)
TEST_BINS = $(patsubst $(TEST_DIR)/%.c, $(BUILD_DIR)/%, $(TEST_SRCS))

LIB = $(BUILD_DIR)/libfastq.a
BIN = $(BUILD_DIR)/fastq
BENCH_BIN = $(BUILD_DIR)/bench_throughput

# Python bindings
PY_SRC    = bindings/python/fastq_module.c
PY_CFLAGS = $(shell python3-config --includes) -Iinclude -fPIC
PY_LDFLAGS = -L$(BUILD_DIR) -lfastq $(LDFLAGS) $(shell python3-config --ldflags --embed 2>/dev/null || python3-config --ldflags)

.PHONY: all clean test valgrind bench python

all: $(LIB) $(BIN)

$(BUILD_DIR):
	mkdir -p $(BUILD_DIR)

$(BUILD_DIR)/%.o: $(SRC_DIR)/%.c | $(BUILD_DIR)
	$(CC) $(CFLAGS) -c $< -o $@

$(LIB): $(LIB_OBJS)
	ar rcs $@ $^

# CLI binary
$(BIN): $(SRC_DIR)/cli.c $(LIB) | $(BUILD_DIR)
	$(CC) $(CFLAGS) $< -L$(BUILD_DIR) -lfastq $(LDFLAGS) -o $@

# Tests
test: $(TEST_BINS)
	@for t in $(TEST_BINS); do \
		echo "\n=== Running $$t ==="; \
		$$t || exit 1; \
	done
	@echo "\n All tests passed."

$(BUILD_DIR)/%: $(TEST_DIR)/%.c $(LIB) | $(BUILD_DIR)
	$(CC) $(CFLAGS) $< -L$(BUILD_DIR) -lfastq $(LDFLAGS) -o $@

# Valgrind
valgrind: $(TEST_BINS)
	@for t in $(TEST_BINS); do \
		echo "\n=== Valgrind $$t ==="; \
		valgrind --leak-check=full --error-exitcode=1 $$t || exit 1; \
	done
	@echo "\n No memory leaks detected."

# Benchmark
bench: $(BENCH_BIN)
	./$(BENCH_BIN)

$(BENCH_BIN): $(BENCH_DIR)/throughput.c $(LIB) | $(BUILD_DIR)
	$(CC) $(CFLAGS) -O2 $< -L$(BUILD_DIR) -lfastq $(LDFLAGS) -o $@

# Python bindings (direct gcc, no setuptools needed)
PY_SO = bindings/python/fastq$(shell python3-config --extension-suffix)
PIC_OBJS = $(patsubst $(SRC_DIR)/%.c, $(BUILD_DIR)/%_pic.o, $(LIB_SRCS))
PIC_LIB  = $(BUILD_DIR)/libfastq_pic.a

$(BUILD_DIR)/%_pic.o: $(SRC_DIR)/%.c | $(BUILD_DIR)
	$(CC) $(CFLAGS) -fPIC -c $< -o $@

$(PIC_LIB): $(PIC_OBJS)
	ar rcs $@ $^

python: $(PY_SO)

$(PY_SO): $(PY_SRC) $(PIC_LIB) | $(BUILD_DIR)
	$(CC) -shared -fPIC $(PY_CFLAGS) $< -L$(BUILD_DIR) -lfastq_pic $(LDFLAGS) -o $@

clean:
	rm -rf $(BUILD_DIR)
	rm -f bindings/python/fastq*.so
	rm -rf bindings/python/build
