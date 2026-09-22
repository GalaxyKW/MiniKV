#include "engine.h"
#include "codec.h"
#include "threadpool.h"

#include <atomic>
#include <filesystem>
#include <future>
#include <iostream>
#include <stdexcept>
#include <string>
#include <utility>
#include <unistd.h>

using namespace minikv;
using namespace std::chrono_literals;

namespace {

void require(bool condition, const std::string& message) {
    if (!condition) throw std::runtime_error(message);
}

template <class Fn> void must_fail(Fn function, const std::string& message) {
    bool failed = false;
    try { function(); } catch (const std::exception&) { failed = true; }
    require(failed, message);
}

struct TempDir {
    std::string path;
    TempDir() {
        std::string pattern = (std::filesystem::temp_directory_path() / "minikv-stats-XXXXXX").string();
        const auto* created = ::mkdtemp(pattern.data());
        if (!created) throw std::runtime_error("mkdtemp failed");
        path = created;
    }
    ~TempDir() { std::error_code error; std::filesystem::remove_all(path, error); }
};

EngineConfig config_for(const TempDir& dir) {
    EngineConfig config;
    config.data_dir = dir.path;
    config.wal_mode = WalMode::Reliable;
    config.wal_batch_size = 1;
    config.wal_flush_interval = 1ms;
    config.snapshot_interval = 0ms;
    return config;
}

struct Gate {
    std::string target = "wal.sync";
    std::atomic<bool> armed{false};
    std::atomic<bool> used{false};
    bool fail = false;
    std::promise<void> entered, resume;
    std::future<void> waiting = entered.get_future();
    std::shared_future<void> resumed = resume.get_future().share();

    void operator()(const std::string& point) {
        if (point != target || !armed.load() || used.exchange(true)) return;
        entered.set_value();
        if (resumed.wait_for(5s) != std::future_status::ready) throw std::runtime_error("stats test gate timeout");
        if (fail) throw std::runtime_error("injected stats test I/O failure");
    }
    bool wait() { return waiting.wait_for(2s) == std::future_status::ready; }
    void release() { resume.set_value(); }
};

void stats_frame_is_read_only() {
    std::string frame = "MKV1";
    frame.push_back(static_cast<char>(Operation::Stats));
    frame.append(11, '\0');
    require(codec::request_size(frame) == 16, "empty stats frame was rejected");
    const auto request = codec::decode_request(frame);
    require(request.operation == Operation::Stats && request.key.empty() && request.value.empty(), "stats frame changed");
    require(!codec::valid_request(request), "stats was accepted as a storage operation");
    for (const size_t offset : {size_t(11), size_t(15)}) {
        auto bad = frame;
        bad[offset] = 1;
        must_fail([&] { codec::request_size(bad); }, "stats payload length was accepted");
    }
    must_fail([&] { codec::record(1, Operation::Stats, "key", ""); }, "stats was encoded into the WAL");
    auto record = codec::record(1, Operation::Put, "key", "");
    record[4] = static_cast<char>(Operation::Stats);
    record.resize(record.size() - 4);
    codec::append_u32(record, codec::crc32(record));
    must_fail([&] { codec::decode_record(record); }, "stats was accepted in the WAL");
    TempDir dir;
    Engine engine(config_for(dir));
    require(engine.execute(request).status == Status::Invalid, "stats fell into Engine::execute mutation logic");
    require(engine.stats().applied_sequence == 0, "stats mutated the sequence");
}

void wal_progress_and_failure_remain_observable() {
    for (const bool fail : {false, true}) {
        TempDir dir;
        auto config = config_for(dir);
        Gate gate;
        gate.fail = fail;
        config.io_hook = [&](const std::string& point) { gate(point); };
        Engine engine(config);
        gate.armed = true;
        auto writer = std::async(std::launch::async, [&] { return engine.execute({Operation::Put, "first", "one"}); });
        const bool entered = gate.wait();
        auto reader = std::async(std::launch::async, [&] { return engine.stats(); });
        const bool available = reader.wait_for(300ms) == std::future_status::ready;
        const bool write_waited = writer.wait_for(20ms) == std::future_status::timeout;
        gate.release();
        const auto in_flight = reader.get();
        const auto response = writer.get();
        const auto finished = engine.stats();
        require(entered && available && write_waited, "stats waited for reliable WAL synchronization");
        const uint64_t bytes = codec::kWalRecordHeader + 5 + 3 + 4;
        require(in_flight.applied_sequence == 1 && in_flight.durable_sequence == 0 && in_flight.keys == 1,
                "in-flight visibility and durability differ from their stats");
        require(in_flight.wal_pending_bytes == bytes && in_flight.wal_inflight_bytes == bytes &&
                in_flight.wal_queued_records == 0 && in_flight.wal_commits_total == 0,
                "in-flight batch was counted as queued or already committed");
        require(finished.wal_inflight_bytes == 0 && finished.wal_commit_last_duration_ns > 0 &&
                finished.wal_commit_duration_ns_total == finished.wal_commit_last_duration_ns,
                "WAL attempt duration or in-flight cleanup is incorrect");
        if (fail) {
            require(response.status == Status::IOError && finished.io_failed && finished.wal_commit_failures_total == 1 &&
                    finished.wal_commits_total == 0 && finished.durable_sequence == 0 && finished.wal_pending_bytes == bytes,
                    "failed WAL statistics hid uncommitted bytes or advanced durability");
            must_fail([&] { engine.close(); }, "failed close lost the storage failure");
        } else {
            require(response.status == Status::Ok && !finished.io_failed && finished.wal_commit_failures_total == 0 &&
                    finished.wal_commits_total == 1 && finished.durable_sequence == 1 && finished.wal_pending_bytes == 0,
                    "committed WAL statistics did not release pending bytes");
            engine.close();
        }
        require(engine.stats().stopping, "stats unavailable or incorrect after close");
    }
}

void queued_and_inflight_bytes_are_distinct() {
    TempDir dir;
    auto config = config_for(dir);
    config.wal_mode = WalMode::Throughput;
    Gate gate;
    config.io_hook = [&](const std::string& point) { gate(point); };
    Engine engine(config);
    gate.armed = true;
    require(engine.execute({Operation::Put, "a", "first"}).status == Status::Ok, "first write failed");
    const bool entered = gate.wait();
    const auto result = engine.execute({Operation::Put, "b", "second"});
    const auto stats = engine.stats();
    gate.release();
    engine.close();
    require(entered && result.status == Status::Ok, "second throughput write could not queue during I/O");
    const auto first = codec::kWalRecordHeader + 1 + 5 + 4;
    const auto second = codec::kWalRecordHeader + 1 + 6 + 4;
    require(stats.wal_pending_bytes == first + second && stats.wal_inflight_bytes == first &&
            stats.wal_queued_records == 1 && stats.applied_sequence == 2 && stats.durable_sequence == 0,
            "queued and detached WAL batches were conflated");
    const auto closed = engine.stats();
    require(closed.wal_commits_total == 2 && closed.wal_pending_bytes == 0 && closed.durable_sequence == 2,
            "shutdown did not account for both WAL batches exactly once");
}

void final_flush_is_accounted_without_relocking() {
    TempDir dir;
    auto config = config_for(dir);
    config.wal_mode = WalMode::Throughput;
    config.wal_batch_size = 512;
    config.wal_flush_interval = 60s;
    Engine engine(config);
    require(engine.execute({Operation::Put, "key", "value"}).status == Status::Ok, "write failed");
    require(engine.stats().wal_commits_total == 0, "test unexpectedly committed before closing");
    engine.close();
    const auto stats = engine.stats();
    require(stats.wal_commits_total == 1 && stats.wal_commit_duration_ns_total > 0 && stats.wal_pending_bytes == 0,
            "final flush statistics were absent or counted twice");
}

void snapshot_capture_lock_excludes_wal_sync() {
    TempDir dir;
    auto config = config_for(dir);
    config.wal_mode = WalMode::Throughput;
    config.wal_batch_size = 512;
    config.wal_flush_interval = 60s;
    Gate gate;
    config.io_hook = [&](const std::string& point) { gate(point); };
    Engine engine(config);
    const auto initial = engine.stats();
    require(engine.execute({Operation::Put, "key", "value"}).status == Status::Ok, "write failed");
    gate.armed = true;
    auto checkpoint = std::async(std::launch::async, [&] { engine.snapshot(); });
    const bool entered = gate.wait();
    auto read = std::async(std::launch::async, [&] {
        const auto first = engine.stats();
        // The gate is outside the state lock. Observe the same capture while
        // its WAL synchronization remains blocked, without a timing threshold.
        (void)checkpoint.wait_for(20ms);
        return std::make_pair(first, engine.stats());
    });
    const bool available = read.wait_for(300ms) == std::future_status::ready;
    gate.release();
    const auto samples = read.get();
    checkpoint.get();
    const auto after = engine.stats();
    require(entered && available, "capture WAL synchronization blocked statistics");
    for (const auto* during : {&samples.first, &samples.second}) {
        require(during->snapshot_in_progress &&
                during->snapshot_capture_state_lock_acquisitions_total ==
                    initial.snapshot_capture_state_lock_acquisitions_total + 1 &&
                during->snapshot_capture_state_lock_acquisitions_total == after.snapshot_capture_state_lock_acquisitions_total &&
                during->snapshot_capture_state_lock_duration_ns_total == after.snapshot_capture_state_lock_duration_ns_total &&
                during->snapshot_capture_state_lock_duration_ns_max == after.snapshot_capture_state_lock_duration_ns_max,
                "capture lock accounting remained active during unlocked WAL synchronization");
        require(during->snapshot_capture_duration_ns_total == initial.snapshot_capture_duration_ns_total &&
                during->snapshot_file_write_calls_total == initial.snapshot_file_write_calls_total &&
                during->snapshot_file_written_bytes_total == initial.snapshot_file_written_bytes_total &&
                during->snapshot_file_installed_bytes_total == initial.snapshot_file_installed_bytes_total,
                "unfinished capture published its phase duration or future snapshot writes");
    }
    require(after.snapshot_capture_duration_ns_total > initial.snapshot_capture_duration_ns_total &&
            after.snapshot_capture_state_lock_duration_ns_total >= initial.snapshot_capture_state_lock_duration_ns_total &&
            after.snapshot_capture_state_lock_duration_ns_max >= initial.snapshot_capture_state_lock_duration_ns_max &&
            after.snapshot_capture_state_lock_duration_ns_max <= after.snapshot_capture_state_lock_duration_ns_total &&
            after.snapshot_capture_state_lock_duration_ns_total - initial.snapshot_capture_state_lock_duration_ns_total <=
                after.snapshot_capture_duration_ns_total - initial.snapshot_capture_duration_ns_total,
            "capture state-lock duration is inconsistent with its containing phase");
    engine.close();
}

void snapshots_report_phases_failures_and_recovery() {
    for (const std::string point : {"snapshot.write", "wal.compact.write"}) {
        TempDir dir;
        auto config = config_for(dir);
        Gate gate;
        gate.target = point;
        config.io_hook = [&](const std::string& at) { gate(at); };
        {
            Engine engine(config);
            const auto initial = engine.stats();
            require(initial.snapshot_successes_total == 1 && initial.snapshot_sequence == 0,
                    "initial database snapshot was not counted");
            require(initial.snapshot_capture_state_lock_acquisitions_total == 1 &&
                    initial.snapshot_capture_state_lock_duration_ns_max == initial.snapshot_capture_state_lock_duration_ns_total &&
                    initial.snapshot_file_write_calls_total >= 1 && initial.snapshot_file_written_bytes_total == 28 &&
                    initial.snapshot_file_installed_bytes_total == 28 && initial.snapshot_compact_written_bytes_total == codec::kWalFileHeader,
                    "new database did not account for its complete empty snapshot");
            require(engine.execute({Operation::Put, "key", "value"}).status == Status::Ok, "write failed");
            gate.armed = true;
            auto checkpoint = std::async(std::launch::async, [&] { engine.snapshot(); });
            const bool entered = gate.wait();
            auto read = std::async(std::launch::async, [&] { return engine.stats(); });
            const bool available = read.wait_for(300ms) == std::future_status::ready;
            gate.release();
            const auto during = read.get();
            checkpoint.get();
            const auto after = engine.stats();
            require(entered && available && during.snapshot_in_progress && during.snapshot_sequence == 0,
                    "snapshot I/O blocked statistics or prematurely completed the checkpoint");
            require(after.snapshot_successes_total == 2 && after.snapshot_sequence == 1 && !after.snapshot_in_progress &&
                    after.snapshot_capture_duration_ns_total > initial.snapshot_capture_duration_ns_total &&
                    after.snapshot_write_duration_ns_total > initial.snapshot_write_duration_ns_total &&
                    after.snapshot_compact_duration_ns_total > initial.snapshot_compact_duration_ns_total,
                    "completed snapshot phase statistics were not recorded");
            engine.close();
        }
        config.io_hook = {};
        Engine recovered(config);
        const auto recovered_stats = recovered.stats();
        require(recovered_stats.snapshot_sequence == 1 && recovered_stats.durable_sequence == 1 &&
                recovered_stats.applied_sequence == 1 && recovered_stats.keys == 1 &&
                recovered_stats.snapshot_successes_total == 0 && recovered_stats.wal_commits_total == 0,
                "restart did not preserve sequences and reset process counters");
        require(recovered_stats.snapshot_capture_state_lock_acquisitions_total == 0 &&
                recovered_stats.snapshot_capture_state_lock_duration_ns_total == 0 &&
                recovered_stats.snapshot_capture_state_lock_duration_ns_max == 0 &&
                recovered_stats.snapshot_file_write_calls_total == 0 && recovered_stats.snapshot_file_written_bytes_total == 0 &&
                recovered_stats.snapshot_file_installed_bytes_total == 0 && recovered_stats.snapshot_compact_written_bytes_total == 0,
                "recovery counted existing files as snapshot work by the new process");
    }
    for (const std::string point : {"wal.sync", "snapshot.write", "snapshot.sync", "snapshot.after_install", "wal.compact.write"}) {
        TempDir dir;
        auto config = config_for(dir);
        config.wal_mode = WalMode::Throughput;
        config.wal_batch_size = 512;
        config.wal_flush_interval = 60s;
        std::atomic<bool> armed{false};
        config.io_hook = [&](const std::string& at) {
            if (armed && at == point) throw std::runtime_error("injected snapshot stats failure");
        };
        Engine engine(config);
        const auto initial = engine.stats();
        require(engine.execute({Operation::Put, "key", "value"}).status == Status::Ok, "write failed");
        armed = true;
        must_fail([&] { engine.snapshot(); }, "snapshot failure not injected");
        const auto stats = engine.stats();
        require(stats.snapshot_successes_total == 1 && stats.snapshot_failures_total == 1 && !stats.snapshot_in_progress &&
                stats.io_failed == (point == "wal.sync" || point == "wal.compact.write"),
                "snapshot failure or storage health was misreported");
        require(stats.snapshot_capture_state_lock_acquisitions_total == initial.snapshot_capture_state_lock_acquisitions_total + 1 &&
                stats.snapshot_capture_state_lock_duration_ns_total >= initial.snapshot_capture_state_lock_duration_ns_total &&
                stats.snapshot_capture_state_lock_duration_ns_max >= initial.snapshot_capture_state_lock_duration_ns_max,
                "failed snapshot lost its completed capture critical section");
        const bool wrote_file = point == "snapshot.sync" || point == "snapshot.after_install" || point == "wal.compact.write";
        const bool installed_file = point == "snapshot.after_install" || point == "wal.compact.write";
        const uint64_t file_bytes = 28 + codec::kRecordHeader + 3 + 5 + 4;
        require(stats.snapshot_file_written_bytes_total - initial.snapshot_file_written_bytes_total == (wrote_file ? file_bytes : 0) &&
                stats.snapshot_file_installed_bytes_total - initial.snapshot_file_installed_bytes_total == (installed_file ? file_bytes : 0) &&
                stats.snapshot_compact_written_bytes_total == initial.snapshot_compact_written_bytes_total,
                "failed checkpoint conflated file writes, durable installation and WAL suffix copying: " + point);
        require(wrote_file ? stats.snapshot_file_write_calls_total > initial.snapshot_file_write_calls_total
                           : stats.snapshot_file_write_calls_total == initial.snapshot_file_write_calls_total,
                "snapshot write calls included unreached writes or omitted a completed write: " + point);
        require(stats.snapshot_capture_duration_ns_total > initial.snapshot_capture_duration_ns_total,
                "failed snapshot capture duration was not recorded");
        if (point == "wal.sync") {
            require(stats.wal_commit_failures_total == 1 && stats.wal_inflight_bytes == 0 && stats.wal_pending_bytes > 0 &&
                    stats.snapshot_write_duration_ns_total == initial.snapshot_write_duration_ns_total &&
                    stats.snapshot_compact_duration_ns_total == initial.snapshot_compact_duration_ns_total,
                    "failed capture leaked in-flight state or recorded phases that never started");
        } else {
            require(stats.snapshot_write_duration_ns_total > initial.snapshot_write_duration_ns_total,
                    "failed snapshot installation duration was not recorded");
            if (point == "wal.compact.write") {
                require(stats.snapshot_compact_duration_ns_total > initial.snapshot_compact_duration_ns_total,
                        "failed WAL compaction duration was not recorded");
            }
        }
        if (stats.io_failed) must_fail([&] { engine.close(); }, "WAL replacement failure was lost");
    }
}

void pool_reports_queued_and_active_work() {
    ThreadPool pool(1, 1);
    const auto initial = pool.stats();
    require(initial.started_total == 0 && initial.queue_wait_duration_ns_total == 0,
            "new pool retained request timing counters");
    std::promise<void> started, release;
    auto entered = started.get_future();
    auto resume = release.get_future().share();
    std::atomic<size_t> completed{0};
    require(pool.enqueue([&] { started.set_value(); resume.wait_for(5s); ++completed; }), "first task was rejected");
    const bool active = entered.wait_for(2s) == std::future_status::ready;
    const auto first = pool.stats();
    const bool queued = pool.enqueue([&] { ++completed; });
    const auto queued_before = std::chrono::steady_clock::now();
    const bool excess = pool.enqueue([&] { ++completed; });
    const auto stats = pool.stats();
    auto shutdown = std::async(std::launch::async, [&] { pool.shutdown(); });
    const bool draining = shutdown.wait_for(20ms) == std::future_status::timeout;
    const auto waiting = pool.stats();
    const auto minimum_queued_ns = std::chrono::duration_cast<std::chrono::nanoseconds>(
        std::chrono::steady_clock::now() - queued_before).count();
    release.set_value();
    shutdown.get();
    require(active && queued && !excess && stats.active == 1 && stats.queued == 1 && stats.capacity == 1 && stats.workers == 1,
            "pool statistics conflated admitted work, queue capacity and workers");
    require(draining && first.started_total == 1 && stats.started_total == 1 && waiting.started_total == 1 &&
            stats.queue_wait_duration_ns_total == first.queue_wait_duration_ns_total &&
            waiting.queue_wait_duration_ns_total == first.queue_wait_duration_ns_total,
            "queue timing included rejected, still-queued, or executing task time");
    const auto stopped = pool.stats();
    require(stopped.active == 0 && stopped.queued == 0 && stopped.started_total == 2 && completed == 2,
            "shutdown did not drain and count accepted tasks exactly once");
    require(stopped.queue_wait_duration_ns_total - first.queue_wait_duration_ns_total >=
                static_cast<uint64_t>(minimum_queued_ns),
            "dequeued task timing omitted its wait for a worker");
    require(!pool.enqueue([&] { ++completed; }), "shutdown accepted another task");
    const auto rejected = pool.stats();
    require(rejected.started_total == stopped.started_total &&
            rejected.queue_wait_duration_ns_total == stopped.queue_wait_duration_ns_total && completed == 2,
            "shutdown rejection changed task timing counters");
}

} // namespace

int main(int argc, char** argv) {
    try {
        const std::pair<const char*, void (*)()> tests[] = {
            {"stats frame read-only validation", stats_frame_is_read_only},
            {"WAL progress and failure statistics", wal_progress_and_failure_remain_observable},
            {"queued versus in-flight statistics", queued_and_inflight_bytes_are_distinct},
            {"final flush statistics", final_flush_is_accounted_without_relocking},
            {"snapshot capture lock excludes WAL synchronization", snapshot_capture_lock_excludes_wal_sync},
            {"snapshot phases, failures and recovery statistics", snapshots_report_phases_failures_and_recovery},
            {"worker pool statistics", pool_reports_queued_and_active_work},
        };
        size_t executed = 0;
        for (const auto& test : tests) {
            if (argc > 1 && std::string(argv[1]) != test.first) continue;
            test.second();
            ++executed;
            std::cout << "PASS " << test.first << std::endl;
        }
        require(executed != 0, "unknown test name");
    } catch (const std::exception& error) {
        std::cerr << "FAIL " << error.what() << '\n';
        return 1;
    }
    return 0;
}
