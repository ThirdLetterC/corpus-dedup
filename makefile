CMAKE ?= cmake
CLANG_FORMAT ?= clang-format
CLANG_TIDY ?= clang-tidy

USE_ASM ?= ON
CMAKE_FLAGS ?=

BUILD_DIR ?= build
BUILD_DIR_DEBUG := $(BUILD_DIR)/debug
BUILD_DIR_RELEASE := $(BUILD_DIR)/release
BUILD_DIR_RELEASE_C := $(BUILD_DIR)/release_c

COMMON_CMAKE_FLAGS := -DCMAKE_EXPORT_COMPILE_COMMANDS=ON -DUSE_ASM=$(USE_ASM)
COMMON_CMAKE_FLAGS_NO_ASM := -DCMAKE_EXPORT_COMPILE_COMMANDS=ON -DUSE_ASM=OFF

REPO_ROOT := $(dir $(abspath $(lastword $(MAKEFILE_LIST))))
FORMAT_SOURCES := $(shell cd $(REPO_ROOT) && git ls-files '*.c' '*.h')

.PHONY: release release_c debug fmt lint clean

release:
	$(CMAKE) -S . -B $(BUILD_DIR_RELEASE) -DCMAKE_BUILD_TYPE=Release $(COMMON_CMAKE_FLAGS) $(CMAKE_FLAGS)
	$(CMAKE) --build $(BUILD_DIR_RELEASE) --config Release

release_c:
	$(CMAKE) -S . -B $(BUILD_DIR_RELEASE_C) -DCMAKE_BUILD_TYPE=Release $(COMMON_CMAKE_FLAGS_NO_ASM) $(CMAKE_FLAGS)
	$(CMAKE) --build $(BUILD_DIR_RELEASE_C) --config Release

debug:
	$(CMAKE) -S . -B $(BUILD_DIR_DEBUG) -DCMAKE_BUILD_TYPE=Debug $(COMMON_CMAKE_FLAGS) $(CMAKE_FLAGS)
	$(CMAKE) --build $(BUILD_DIR_DEBUG) --config Debug

fmt:
	cd $(REPO_ROOT) && $(CLANG_FORMAT) -i $(FORMAT_SOURCES)

lint: $(BUILD_DIR_DEBUG)/compile_commands.json
	$(CLANG_TIDY) -p $(BUILD_DIR_DEBUG) $(FORMAT_SOURCES)

$(BUILD_DIR_DEBUG)/compile_commands.json:
	$(CMAKE) -S . -B $(BUILD_DIR_DEBUG) -DCMAKE_BUILD_TYPE=Debug $(COMMON_CMAKE_FLAGS) $(CMAKE_FLAGS)

clean:
	$(CMAKE) --build $(BUILD_DIR_DEBUG) --config Debug --target clean || true
	$(CMAKE) --build $(BUILD_DIR_RELEASE) --config Release --target clean || true
	$(CMAKE) --build $(BUILD_DIR_RELEASE_C) --config Release --target clean || true
