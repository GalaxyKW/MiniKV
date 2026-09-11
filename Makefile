BUILD_DIR ?= build
BUILD_TYPE ?= RelWithDebInfo
JOBS ?= 2

.PHONY: all engine go test unit-test integration-test sanitize-test clean

all: engine go

engine:
	cmake -S cpp_engine -B $(BUILD_DIR) -DCMAKE_BUILD_TYPE=$(BUILD_TYPE) -DBUILD_TESTING=ON
	cmake --build $(BUILD_DIR) -j$(JOBS)

go:
	mkdir -p bin
	go build -o bin/minikv-go ./go_server
	go build -o bin/minikv-bench ./benmark

unit-test: all
	ctest --test-dir $(BUILD_DIR) --output-on-failure
	go test -race -timeout 60s ./...

integration-test: all
	MINIKV_TEST_ENGINE=$(abspath $(BUILD_DIR))/engine python3 tests/integration_test.py -v

test: unit-test integration-test

sanitize-test: go
	cmake -S cpp_engine -B build-asan -DCMAKE_BUILD_TYPE=Debug -DBUILD_TESTING=ON -DMINIKV_SANITIZERS=ON
	cmake --build build-asan -j$(JOBS)
	ctest --test-dir build-asan --output-on-failure
	MINIKV_TEST_ENGINE=$(CURDIR)/build-asan/engine python3 tests/integration_test.py -v

clean:
	cmake --build $(BUILD_DIR) --target clean
