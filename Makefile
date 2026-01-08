CMAKE ?= cmake
CLANG_FORMAT ?= clang-format
CLANG_TIDY ?= clang-tidy

USE_ASM ?= ON
CMAKE_FLAGS ?=

BUILD_DIR ?= build
BUILD_DIR_DEBUG := $(BUILD_DIR)/debug
BUILD_DIR_RELEASE := $(BUILD_DIR)/release

COMMON_CMAKE_FLAGS := -DCMAKE_EXPORT_COMPILE_COMMANDS=ON -DUSE_ASM=$(USE_ASM)

FORMAT_SOURCES := $(shell find . \( -path ./$(BUILD_DIR) -o -path ./out -o -path ./out2 -o -path ./data -o -path ./.git \) -prune -o \( -name '*.c' -o -name '*.h' \) -print)

.PHONY: release debug fmt lint clean

release:
	$(CMAKE) -S . -B $(BUILD_DIR_RELEASE) -DCMAKE_BUILD_TYPE=Release $(COMMON_CMAKE_FLAGS) $(CMAKE_FLAGS)
	$(CMAKE) --build $(BUILD_DIR_RELEASE) --config Release

debug:
	$(CMAKE) -S . -B $(BUILD_DIR_DEBUG) -DCMAKE_BUILD_TYPE=Debug $(COMMON_CMAKE_FLAGS) $(CMAKE_FLAGS)
	$(CMAKE) --build $(BUILD_DIR_DEBUG) --config Debug

fmt:
	$(CLANG_FORMAT) -i $(FORMAT_SOURCES)

lint: $(BUILD_DIR_DEBUG)/compile_commands.json
	$(CLANG_TIDY) -p $(BUILD_DIR_DEBUG) $(FORMAT_SOURCES)

$(BUILD_DIR_DEBUG)/compile_commands.json:
	$(CMAKE) -S . -B $(BUILD_DIR_DEBUG) -DCMAKE_BUILD_TYPE=Debug $(COMMON_CMAKE_FLAGS) $(CMAKE_FLAGS)

clean:
	$(CMAKE) --build $(BUILD_DIR_DEBUG) --config Debug --target clean || true
	$(CMAKE) --build $(BUILD_DIR_RELEASE) --config Release --target clean || true
