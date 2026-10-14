#include "engine.h"
#include "codec.h"

#include <atomic>
#include <condition_variable>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <mutex>
#include <new>
#include <stdexcept>

// Only this executable replaces allocation. The I/O hook arms one allocation
// failure on its own thread, after all setup and before diagnostic publication.
// Other threads and subsequent allocations continue normally, including ASan.
namespace allocation_fault {
thread_local bool armed = false;
thread_local unsigned failures = 0;
}

void* operator new(std::size_t size) {
    if (allocation_fault::armed) {
        allocation_fault::armed = false;
        ++allocation_fault::failures;
        throw std::bad_alloc();
    }
    if (void* memory = std::malloc(size ? size : 1)) return memory;
    throw std::bad_alloc();
}
void* operator new[](std::size_t size) { return ::operator new(size); }
void operator delete(void* memory) noexcept { std::free(memory); }
void operator delete[](void* memory) noexcept { std::free(memory); }
void operator delete(void* memory, std::size_t) noexcept { std::free(memory); }
void operator delete[](void* memory, std::size_t) noexcept { std::free(memory); }

namespace {
using namespace minikv;
using namespace std::chrono_literals;

void require(bool condition, const char* message) {
    if (!condition) throw std::runtime_error(message);
}

template <class Function>
void must_fail(Function function, const char* message) {
    bool failed = false;
    try { function(); } catch (const std::exception&) { failed = true; }
    require(failed, message);
}

struct TempDir {
    std::string path;
    TempDir() {
        std::string pattern = (std::filesystem::temp_directory_path() / "minikv-allocation-XXXXXX").string();
        const char* created = ::mkdtemp(pattern.data());
        if (!created) throw std::runtime_error("mkdtemp failed");
        path = created;
    }
    ~TempDir() { std::error_code ignored; std::filesystem::remove_all(path, ignored); }
};

std::string read_file(const std::string& path) {
    std::ifstream file(path, std::ios::binary);
    require(file.is_open(), "cannot open test WAL");
    return {std::istreambuf_iterator<char>(file), std::istreambuf_iterator<char>()};
}

struct WriteFailure : std::exception {
    const char* what() const noexcept override {
        // Static text avoids allocating while constructing the injected error.
        return "injected WAL write failure with a diagnostic that exceeds the small string buffer";
    }
};

struct Reply {
    std::mutex mutex;
    std::condition_variable changed;
    unsigned count = 0;
    Response response{Status::Ok, {}};

    void receive(Response value) {
        std::lock_guard<std::mutex> lock(mutex);
        response = std::move(value);
        ++count;
        changed.notify_all();
    }
    bool wait() {
        std::unique_lock<std::mutex> lock(mutex);
        return changed.wait_for(lock, 2s, [&] { return count != 0; });
    }
    void verify() {
        std::lock_guard<std::mutex> lock(mutex);
        require(count == 1 && response.status == Status::IOError && response.value.empty(),
                "reliable request did not receive exactly one error without a diagnostic");
    }
};

void diagnostic_allocation_failure(WalMode mode) {
    TempDir dir;
    // Keep callback captures alive through Engine destruction on assertion failure.
    Reply written, observed;
    std::atomic<unsigned> unexpected_callbacks{0};
    std::atomic<bool> inject{false};
    EngineConfig config;
    config.data_dir = dir.path;
    config.wal_mode = mode;
    config.snapshot_interval = 0ms;
    config.wal_flush_interval = 60000ms;
    config.io_hook = [&](const std::string& point) {
        if (point == "wal.write" && inject.exchange(false)) {
            allocation_fault::armed = true;
            throw WriteFailure{};
        }
    };
    Engine engine(config);
    if (mode == WalMode::Throughput) {
        require(engine.execute({Operation::Put, "key", "value"}).status == Status::Ok,
                "throughput PUT was rejected before the fault");
    } else {
        require(!engine.execute_async({Operation::Put, "key", "value"},
                                      [&](Response value) { written.receive(std::move(value)); }),
                "reliable PUT was acknowledged before WAL commit");
        require(!engine.execute_async({Operation::Get, "key", {}},
                                      [&](Response value) { observed.receive(std::move(value)); }),
                "reliable GET did not wait for the captured state");
        require(engine.stats().wal_durable_waiters == 2, "reliable waiters were not registered");
    }
    const auto original_wal = read_file(dir.path + "/wal.v1");
    require(original_wal == codec::wal_file_header(), "WAL flushed before the manual snapshot");
    allocation_fault::failures = 0;
    inject = true;
    bool original_error = false;
    try { engine.snapshot(); }
    catch (const WriteFailure&) { original_error = true; }
    catch (const std::exception&) { /* Assert the original type after checking the latch. */ }
    const bool consumed = !allocation_fault::armed && allocation_fault::failures == 1;
    allocation_fault::armed = false;
    require(consumed && !inject, "the one-shot diagnostic allocation failure was not exercised");
    const auto failed = engine.stats();
    require(failed.io_failed, "storage failure was lost when diagnostic allocation failed");
    require(original_error, "diagnostic allocation replaced the original WAL exception");
    require(failed.applied_sequence == 1 && failed.durable_sequence == 0 &&
            failed.wal_commit_failures_total == 1 && failed.wal_commits_total == 0 &&
            failed.wal_queued_records == 0 && failed.wal_inflight_bytes == 0 &&
            !failed.snapshot_in_progress && failed.snapshot_failures_total == 1,
            "failed detached WAL batch changed durability or failure accounting");

    if (mode == WalMode::Reliable) {
        const bool put_finished = written.wait();
        const bool get_finished = observed.wait();
        // Wake and join callbacks before reporting a failed liveness assertion.
        if (!put_finished || !get_finished) {
            try { engine.close(); } catch (const std::exception&) {}
        }
        require(put_finished && get_finished, "diagnostic failure did not wake reliable requests");
        engine.drain_async();
        written.verify();
        observed.verify();
        const auto drained = engine.stats();
        require(drained.wal_durable_waiters == 0 && drained.wal_durable_waits_total == 2 &&
                drained.async_requests_inflight == 0 && drained.async_callback_failures_total == 0,
                "diagnostic failure leaked reliable waiters or async reservations");
    }

    for (const Request& request : {Request{Operation::Get, "key", {}},
                                   Request{Operation::Put, "later", "value"},
                                   Request{Operation::Delete, "key", {}}}) {
        const auto response = engine.execute(request);
        require(response.status == Status::IOError && response.value.empty(),
                "synchronous request bypassed terminal state without a diagnostic");
        const auto immediate = engine.execute_async(request, [&](Response) { ++unexpected_callbacks; });
        require(immediate && immediate->status == Status::IOError && immediate->value.empty(),
                "async request bypassed terminal state without a diagnostic");
    }
    require(unexpected_callbacks == 0 && engine.stats().applied_sequence == 1,
            "rejected request was applied or transferred callback ownership");
    must_fail([&] { engine.snapshot(); }, "another snapshot hid the terminal storage failure");
    must_fail([&] { engine.close(); }, "close hid the terminal storage failure");
    require(read_file(dir.path + "/wal.v1") == original_wal,
            "close appended a later sequence after the failed detached batch");

    config.io_hook = {};
    config.wal_flush_interval = 1ms;
    config.wal_batch_size = 1;
    Engine recovered(config);
    require(!recovered.stats().io_failed && recovered.durable_sequence() == 0 &&
            recovered.execute({Operation::Get, "key", {}}).status == Status::NotFound &&
            recovered.execute({Operation::Get, "later", {}}).status == Status::NotFound,
            "recovery retained failure state or exposed failed/rejected writes");
    require(recovered.execute({Operation::Put, "recovered", "value"}).status == Status::Ok,
            "recovered engine could not accept a new write");
    recovered.close();
    const auto checked = check_storage(dir.path);
    require(checked.applied_sequence == 1 && checked.keys == 1 && checked.wal_trailing_bytes == 0,
            "writing after recovery left an invalid WAL sequence");
}
} // namespace

int main() {
    try {
        diagnostic_allocation_failure(WalMode::Throughput);
        diagnostic_allocation_failure(WalMode::Reliable);
        std::cout << "allocation failure tests passed\n";
    } catch (const std::exception& error) {
        allocation_fault::armed = false;
        std::cerr << error.what() << '\n';
        return 1;
    }
}
