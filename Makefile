CC      = gcc
CFLAGS  = -Wall -Wextra -Werror -std=c11 -pedantic -Iinclude
LDFLAGS = -lhiredis -ljson-c -lpthread

SRC_DIR   = src
INC_DIR   = include
TEST_DIR  = tests
BUILD_DIR = build

SRCS = $(wildcard $(SRC_DIR)/*.c)
OBJS = $(patsubst $(SRC_DIR)/%.c, $(BUILD_DIR)/%.o, $(SRCS))

TEST_SRCS = $(wildcard $(TEST_DIR)/*.c)
TEST_BINS = $(patsubst $(TEST_DIR)/%.c, $(BUILD_DIR)/%, $(TEST_SRCS))

# Library
LIB = $(BUILD_DIR)/libfastq.a

.PHONY: all clean test valgrind

all: $(LIB)

$(BUILD_DIR):
	mkdir -p $(BUILD_DIR)

$(BUILD_DIR)/%.o: $(SRC_DIR)/%.c | $(BUILD_DIR)
	$(CC) $(CFLAGS) -c $< -o $@

$(LIB): $(OBJS)
	ar rcs $@ $^

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

clean:
	rm -rf $(BUILD_DIR)
