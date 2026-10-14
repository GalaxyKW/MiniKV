BUILD_DIR ?= build
BUILD_TYPE ?= RelWithDebInfo
JOBS ?= 2
BENCH_ARGS ?=
FUZZ_BUILD_DIR ?= build-fuzz
FUZZ_CXX ?= clang++
FUZZ_SECONDS ?= 10
FUZZ_PARALLEL ?= 2

.PHONY: all engine go test docs-test history-test unit-test check-test integration-test experiment-test benchmark sanitize-test fuzz-test cpp-fuzz-test go-fuzz-test fuzz-config clean

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

check-test: engine
	MINIKV_TEST_ENGINE=$(abspath $(BUILD_DIR))/engine python3 tests/check_test.py -v

experiment-test:
	python3 -m unittest discover -s tests -p 'experiment*_test.py' -v

docs-test:
	python3 -m unittest discover -s tests -p 'docs_test.py' -v
	python3 tools/check_docs.py

history-test:
	python3 -m unittest discover -s tests -p 'history_checker_test.py' -v

benchmark: all
	python3 benmark/experiment.py --engine $(abspath $(BUILD_DIR))/engine $(BENCH_ARGS)

test: docs-test history-test unit-test check-test integration-test experiment-test

sanitize-test: go
	cmake -S cpp_engine -B build-asan -DCMAKE_BUILD_TYPE=Debug -DBUILD_TESTING=ON -DMINIKV_SANITIZERS=ON
	cmake --build build-asan -j$(JOBS)
	cd build-asan && ctest --output-on-failure
	MINIKV_TEST_ENGINE=$(CURDIR)/build-asan/engine python3 tests/check_test.py -v
	MINIKV_TEST_ENGINE=$(CURDIR)/build-asan/engine python3 tests/integration_test.py -v

# libFuzzer parses signed 32-bit budgets and treats zero as unbounded. Validate
# before either campaign starts, including when only one language is requested.
fuzz-config:
	@python3 -c 'import sys; values = sys.argv[1:]; sys.exit(0 if all(v.isascii() and v.isdecimal() and 0 < int(v) <= 2147483647 for v in values) else "FUZZ_SECONDS and FUZZ_PARALLEL must be integers in 1..2147483647")' "$(FUZZ_SECONDS)" "$(FUZZ_PARALLEL)"

fuzz-test: cpp-fuzz-test go-fuzz-test

cpp-fuzz-test: fuzz-config
	cmake -S cpp_engine -B "$(FUZZ_BUILD_DIR)" -DCMAKE_CXX_COMPILER="$(FUZZ_CXX)" -DCMAKE_BUILD_TYPE=RelWithDebInfo -DBUILD_TESTING=OFF -DMINIKV_FUZZING=ON
	cmake --build "$(FUZZ_BUILD_DIR)" --target codec_fuzz -j$(JOBS)
	python3 tools/seed_codec_fuzz.py "$(FUZZ_BUILD_DIR)/corpus"
	mkdir -p "$(FUZZ_BUILD_DIR)/artifacts"
	"$(FUZZ_BUILD_DIR)/codec_fuzz" -max_total_time="$(FUZZ_SECONDS)" -timeout=5 -rss_limit_mb=1024 -max_len=1052706 -artifact_prefix="$(FUZZ_BUILD_DIR)/artifacts/" "$(FUZZ_BUILD_DIR)/corpus"

go-fuzz-test: fuzz-config
	go test ./go_server -run='^$$' -fuzz='^FuzzReadResponse$$' -fuzztime="$(FUZZ_SECONDS)s" -parallel="$(FUZZ_PARALLEL)"

clean:
	cmake --build $(BUILD_DIR) --target clean
