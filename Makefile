BUILD_DIR ?= build
BUILD_TYPE ?= RelWithDebInfo
JOBS ?= 2
BENCH_ARGS ?=

.PHONY: all engine go test docs-test history-test unit-test integration-test experiment-test benchmark sanitize-test clean

all: engine go

engine:
	cmake -S cpp_engine -B $(BUILD_DIR) -DCMAKE_BUILD_TYPE=$(BUILD_TYPE) -DBUILD_TESTING=ON
	cmake --build $(BUILD_DIR) -j$(JOBS)

go:
	mkdir -p bin
	go build -o bin/minikv-go ./go_server
	go build -o bin/minikv-bench ./benmark

unit-test: all
	cd "$(BUILD_DIR)" && ctest --output-on-failure
	go test -race -timeout 60s ./...

integration-test: all
	MINIKV_TEST_ENGINE=$(abspath $(BUILD_DIR))/engine python3 tests/integration_test.py -v

experiment-test:
	python3 -m unittest discover -s tests -p 'experiment*_test.py' -v

docs-test:
	python3 -m unittest discover -s tests -p 'docs_test.py' -v
	python3 tools/check_docs.py

history-test:
	python3 -m unittest discover -s tests -p 'history_checker_test.py' -v

benchmark: all
	python3 benmark/experiment.py --engine $(abspath $(BUILD_DIR))/engine $(BENCH_ARGS)

test: docs-test history-test unit-test integration-test experiment-test

sanitize-test: go
	cmake -S cpp_engine -B build-asan -DCMAKE_BUILD_TYPE=Debug -DBUILD_TESTING=ON -DMINIKV_SANITIZERS=ON
	cmake --build build-asan -j$(JOBS)
	cd build-asan && ctest --output-on-failure
	MINIKV_TEST_ENGINE=$(CURDIR)/build-asan/engine python3 tests/integration_test.py -v

clean:
	cmake --build $(BUILD_DIR) --target clean
