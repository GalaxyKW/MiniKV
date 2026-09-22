#include "engine.h"
#include "codec.h"

#include <atomic>
#include <chrono>
#include <csignal>
#include <filesystem>
#include <fstream>
#include <future>
#include <iostream>
#include <stdexcept>
#include <sys/resource.h>
#include <sys/wait.h>
#include <thread>
#include <unistd.h>
#include <vector>

using namespace minikv;
namespace fs = std::filesystem;

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
        std::string pattern = (fs::temp_directory_path() / "minikv-test-XXXXXX").string();
        auto* created = ::mkdtemp(pattern.data());
        if (!created) throw std::runtime_error("mkdtemp failed");
        path = created;
    }
    ~TempDir() { std::error_code error; fs::remove_all(path, error); }
};

EngineConfig config_for(const TempDir& dir) {
    EngineConfig config;
    config.data_dir = dir.path;
    config.wal_mode = WalMode::Reliable;
    config.wal_batch_size = 8;
    config.wal_flush_interval = std::chrono::milliseconds(1);
    config.snapshot_interval = std::chrono::milliseconds(0);
    return config;
}

Response get(Engine& engine, const std::string& key) { return engine.execute({Operation::Get, key, {}}); }
void put(Engine& engine, const std::string& key, const std::string& value) {
    require(engine.execute({Operation::Put, key, value}).status == Status::Ok, "PUT was not acknowledged");
}

std::string read_file(const std::string& path) {
    std::ifstream file(path, std::ios::binary);
    return {std::istreambuf_iterator<char>(file), std::istreambuf_iterator<char>()};
}

void write_file(const std::string& path, const std::string& bytes) {
    std::ofstream file(path, std::ios::binary | std::ios::trunc);
    file.write(bytes.data(), static_cast<std::streamsize>(bytes.size()));
    require(file.good(), "test file write failed");
}

void binary_round_trip() {
    TempDir dir;
    const auto config = config_for(dir);
    const std::string key = std::string("tenant:1 \n") + '\0' + "suffix";
    const std::string value = std::string(" \nhello") + '\0' + "world\t\r \xff";
    {
        Engine engine(config);
        put(engine, key, value);
        put(engine, "empty", "");
        put(engine, "deleted", "old");
        require(engine.execute({Operation::Delete, "deleted", {}}).status == Status::Ok, "delete failed");
        engine.snapshot();
        put(engine, "after", "checkpoint");
        engine.close();
    }
    Engine recovered(config);
    require(get(recovered, key).value == value, "binary key/value changed during recovery");
    require(get(recovered, "empty").status == Status::Value && get(recovered, "empty").value.empty(), "empty value was lost");
    require(get(recovered, "deleted").status == Status::NotFound, "deleted key resurrected");
    require(get(recovered, "after").value == "checkpoint", "post-snapshot WAL was not replayed");
    require(recovered.execute({Operation::Put, "too-big", std::string(kMaxValueSize + 1, 'x')}).status == Status::Invalid, "oversized value accepted");
}

void concurrent_writes_and_snapshots() {
    TempDir dir;
    auto config = config_for(dir);
    config.wal_mode = WalMode::Throughput;
    std::vector<std::string> expected;
    {
        Engine engine(config);
        std::vector<std::future<void>> tasks;
        for (int worker = 0; worker < 6; ++worker) {
            tasks.push_back(std::async(std::launch::async, [&, worker] {
                for (int i = 0; i < 150; ++i) {
                    const std::string value = std::to_string(worker) + ":" + std::to_string(i);
                    put(engine, "shared", value);
                    put(engine, "worker" + std::to_string(worker), value);
                    if (i % 3 == 0) engine.execute({Operation::Delete, "shared", {}});
                }
            }));
        }
        tasks.push_back(std::async(std::launch::async, [&] { for (int i = 0; i < 40; ++i) engine.snapshot(); }));
        for (auto& task : tasks) task.get();
        const auto shared = get(engine, "shared");
        expected.push_back(std::to_string(static_cast<int>(shared.status)) + shared.value);
        for (int worker = 0; worker < 6; ++worker) expected.push_back(get(engine, "worker" + std::to_string(worker)).value);
        engine.close();
    }
    Engine recovered(config);
    const auto shared = get(recovered, "shared");
    require(std::to_string(static_cast<int>(shared.status)) + shared.value == expected[0], "online and replayed write order differ");
    for (int worker = 0; worker < 6; ++worker) {
        require(get(recovered, "worker" + std::to_string(worker)).value == expected[worker + 1], "snapshot lost an acknowledged write");
    }
}

void group_commit() {
    TempDir dir;
    auto config = config_for(dir);
    config.wal_flush_interval = std::chrono::seconds(5);
    std::atomic<int> syncs{0};
    config.io_hook = [&](const std::string& point) { if (point == "wal.sync") ++syncs; };
    Engine engine(config);
    std::promise<void> start;
    auto ready = start.get_future().share();
    std::vector<std::future<void>> writers;
    for (int i = 0; i < 8; ++i) writers.push_back(std::async(std::launch::async, [&, i] { ready.wait(); put(engine, std::to_string(i), "value"); }));
    start.set_value();
    for (auto& writer : writers) writer.get();
    require(syncs == 1 && engine.durable_sequence() == 8, "writes did not share one durable commit");
}

// Pause the first WAL sync at a real I/O boundary. The timeout bounds cleanup
// if a test exits early; assertions normally run only after release().
struct WalSyncGate {
    std::promise<void> entered, resume;
    std::future<void> waiting = entered.get_future();
    std::shared_future<void> resumed = resume.get_future().share();
    std::atomic<bool> used{false};
    bool fail = false;

    void operator()(const std::string& point) {
        if (point != "wal.sync" || used.exchange(true)) return;
        entered.set_value();
        if (resumed.wait_for(std::chrono::seconds(5)) != std::future_status::ready) {
            throw std::runtime_error("test timed out releasing WAL sync");
        }
        if (fail) throw std::runtime_error("injected in-flight sync failure");
    }

    bool wait() { return waiting.wait_for(std::chrono::seconds(2)) == std::future_status::ready; }
    void release() { resume.set_value(); }
};

template <class Predicate> bool wait_for_stats(Engine& engine, Predicate ready) {
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(2);
    do {
        if (ready(engine.stats())) return true;
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    } while (std::chrono::steady_clock::now() < deadline);
    return ready(engine.stats());
}

void require_no_waits(const EngineStats& stats) {
    require(stats.wal_capacity_waiters == 0 && stats.wal_capacity_waits_total == 0 &&
            stats.wal_capacity_wait_duration_ns_total == 0 && stats.wal_durable_waiters == 0 &&
            stats.wal_durable_waits_total == 0 && stats.wal_durable_wait_duration_ns_total == 0,
            "ready requests or recovery were counted as WAL waits");
}

void wal_io_allows_throughput_progress() {
    TempDir dir;
    auto config = config_for(dir);
    config.wal_mode = WalMode::Throughput;
    config.wal_batch_size = 1;
    WalSyncGate gate;
    config.io_hook = [&](const std::string& point) { gate(point); };
    {
        Engine engine(config);
        put(engine, "first", "one");
        const bool syncing = gate.wait();
        // A snapshot waiting for the WAL writer must not hold the state lock.
        auto checkpoint = std::async(std::launch::async, [&] { engine.snapshot(); });
        auto writer = std::async(std::launch::async, [&] { put(engine, "second", "two"); });
        auto reader = std::async(std::launch::async, [&] { return get(engine, "first"); });
        const bool wrote = writer.wait_for(std::chrono::milliseconds(300)) == std::future_status::ready;
        const bool read = reader.wait_for(std::chrono::milliseconds(300)) == std::future_status::ready;
        const bool snapshot_waited = checkpoint.wait_for(std::chrono::milliseconds(20)) == std::future_status::timeout;
        gate.release();
        writer.get();
        const auto value = reader.get();
        checkpoint.get();
        require(syncing && snapshot_waited, "snapshot did not serialize with in-flight WAL");
        require(wrote && read, "WAL I/O blocked throughput requests");
        require(value.status == Status::Value && value.value == "one", "concurrent read changed data");
        require_no_waits(engine.stats());
        put(engine, "after", "checkpoint");
        engine.close();
    }
    config.io_hook = {};
    Engine recovered(config);
    require(get(recovered, "first").value == "one" && get(recovered, "second").value == "two" &&
            get(recovered, "after").value == "checkpoint", "overlapping flush and snapshot lost writes");
}

void in_flight_wal_counts_toward_queue_limit() {
    for (bool fail : {false, true}) {
        TempDir dir;
        auto config = config_for(dir);
        config.wal_mode = WalMode::Throughput;
        config.wal_batch_size = 1;
        config.wal_queue_bytes = codec::kWalRecordHeader + kMaxKeySize + kMaxValueSize + 4;
        WalSyncGate gate;
        gate.fail = fail;
        config.io_hook = [&](const std::string& point) { gate(point); };
        Engine engine(config);
        const std::string key(kMaxKeySize, 'k');
        put(engine, key, std::string(kMaxValueSize, 'v'));
        const bool syncing = gate.wait();
        auto writer = std::async(std::launch::async, [&] { return engine.execute({Operation::Put, "next", "value"}); });
        auto reader = std::async(std::launch::async, [&] { return get(engine, key); });
        const bool counted_waiter = wait_for_stats(engine, [](const EngineStats& stats) {
            return stats.wal_capacity_waiters == 1;
        });
        const auto during = engine.stats();
        const bool bounded = writer.wait_for(std::chrono::milliseconds(50)) == std::future_status::timeout;
        const bool read = reader.wait_for(std::chrono::milliseconds(300)) == std::future_status::ready;
        gate.release();
        const auto written = writer.get();
        const auto value = reader.get();
        const auto after = engine.stats();
        require(syncing && bounded, "in-flight WAL stopped counting toward the queue byte limit");
        require(read && value.status == Status::Value && value.value.size() == kMaxValueSize,
                "WAL backpressure unnecessarily blocked reads");
        require(counted_waiter && during.wal_capacity_waits_total == 0 &&
                during.wal_capacity_wait_duration_ns_total == 0,
                "active capacity wait was omitted or counted before it finished");
        require(after.wal_capacity_waiters == 0 && after.wal_capacity_waits_total == 1 &&
                after.wal_capacity_wait_duration_ns_total > 0,
                "capacity wait completion or duration was lost");
        require(after.wal_durable_waiters == 0 && after.wal_durable_waits_total == 0 &&
                after.wal_durable_wait_duration_ns_total == 0,
                "throughput request was counted as a durable wait");
        if (fail) {
            require(written.status == Status::IOError, "capacity waiter missed WAL failure");
            require(engine.execute({Operation::Delete, "later", {}}).status == Status::IOError &&
                    engine.stats().wal_capacity_waits_total == 1, "rejected request added a capacity wait");
            must_fail([&] { engine.close(); }, "close hid capacity waiter WAL failure");
        } else {
            require(written.status == Status::Ok, "backpressured write failed after capacity became available");
            // Small writes fit immediately even if their previous batch is still in flight.
            require(engine.execute({Operation::Delete, "absent", {}}).status == Status::NotFound &&
                    engine.stats().wal_capacity_waits_total == 1, "ready write added a capacity wait");
            engine.close();
            config.io_hook = {};
            Engine recovered(config);
            require(get(recovered, "next").value == "value", "backpressured write was lost");
            require_no_waits(recovered.stats());
        }
    }
}

void in_flight_wal_preserves_reliable_acknowledgements() {
    for (bool fail : {false, true}) {
        TempDir dir;
        auto config = config_for(dir);
        config.wal_batch_size = 1;
        WalSyncGate gate, following_gate;
        gate.fail = fail;
        std::atomic<int> syncs{0};
        config.io_hook = [&](const std::string& point) {
            if (point != "wal.sync") return;
            const auto batch = syncs.fetch_add(1);
            if (batch == 0) gate(point);
            else if (batch == 1) following_gate(point);
        };
        Engine engine(config);
        require(get(engine, "absent").status == Status::NotFound, "empty reliable read failed");
        require_no_waits(engine.stats());
        auto first = std::async(std::launch::async, [&] { return engine.execute({Operation::Put, "first", "one"}); });
        const bool syncing = gate.wait();
        auto second = std::async(std::launch::async, [&] { return engine.execute({Operation::Put, "second", "two"}); });
        auto reader = std::async(std::launch::async, [&] { return get(engine, "first"); });
        auto sequence = std::async(std::launch::async, [&] { return engine.durable_sequence(); });
        const bool counted_waiters = wait_for_stats(engine, [](const EngineStats& stats) {
            return stats.wal_durable_waiters == 3;
        });
        const auto during = engine.stats();
        const bool first_waited = first.wait_for(std::chrono::milliseconds(50)) == std::future_status::timeout;
        const bool second_waited = second.wait_for(std::chrono::milliseconds(50)) == std::future_status::timeout;
        const bool read_waited = reader.wait_for(std::chrono::milliseconds(50)) == std::future_status::timeout;
        const bool state_available = sequence.wait_for(std::chrono::milliseconds(300)) == std::future_status::ready;
        gate.release();
        bool first_batch_only = true;
        bool counted_dependent_read = true;
        if (!fail) {
            // Keep batch 2 unsynced after batch 1 commits: its acknowledgement
            // must not be released by advancing to the latest applied sequence.
            const bool following_syncing = following_gate.wait();
            auto committed = std::async(std::launch::async, [&] { return engine.durable_sequence(); });
            auto dependent_read = std::async(std::launch::async, [&] { return get(engine, "second"); });
            counted_dependent_read = wait_for_stats(engine, [](const EngineStats& stats) {
                // The first reader may have observed sequence 1 or 2. Either
                // way, exactly four requests must have entered durable waits.
                return stats.wal_durable_waiters + stats.wal_durable_waits_total == 4;
            });
            const bool first_done = first.wait_for(std::chrono::milliseconds(300)) == std::future_status::ready;
            const bool second_pending = second.wait_for(std::chrono::milliseconds(50)) == std::future_status::timeout;
            const bool read_pending = dependent_read.wait_for(std::chrono::milliseconds(50)) == std::future_status::timeout;
            const bool can_observe = committed.wait_for(std::chrono::milliseconds(300)) == std::future_status::ready;
            following_gate.release();
            const auto committed_sequence = committed.get();
            const auto value = dependent_read.get();
            first_batch_only = following_syncing && first_done && second_pending && read_pending && can_observe &&
                               committed_sequence == 1 && value.status == Status::Value && value.value == "two";
        }
        const auto first_result = first.get(), second_result = second.get(), read_result = reader.get();
        const auto before_sync = sequence.get();
        const auto after = engine.stats();
        require(syncing && state_available && before_sync == 0, "in-flight I/O locked state or advanced the commit point");
        require(first_waited && second_waited && read_waited, "reliable request returned before sync");
        require(first_batch_only, "one WAL batch acknowledged a later, unsynced operation");
        require(counted_waiters && during.wal_durable_waits_total == 0 &&
                during.wal_durable_wait_duration_ns_total == 0,
                "active reliable waits were omitted or counted before completion");
        require(counted_dependent_read && after.wal_durable_waiters == 0 &&
                after.wal_durable_waits_total == (fail ? 3U : 4U) && after.wal_durable_wait_duration_ns_total > 0,
                "reliable wait completion or duration was lost");
        require(after.wal_capacity_waiters == 0 && after.wal_capacity_waits_total == 0 &&
                after.wal_capacity_wait_duration_ns_total == 0, "writes with free WAL capacity were counted as waiting");
        if (fail) {
            require(first_result.status == Status::IOError && second_result.status == Status::IOError &&
                    read_result.status == Status::IOError && engine.durable_sequence() == 0,
                    "failed in-flight batch was acknowledged");
            require(engine.execute({Operation::Put, "later", "value"}).status == Status::IOError,
                    "writes continued after in-flight WAL failure");
            require(engine.stats().wal_durable_waits_total == after.wal_durable_waits_total &&
                    engine.stats().wal_durable_wait_duration_ns_total == after.wal_durable_wait_duration_ns_total,
                    "rejected write added a durable wait");
            must_fail([&] { engine.snapshot(); }, "snapshot bypassed in-flight WAL failure");
            must_fail([&] { engine.close(); }, "close hid in-flight WAL failure");
        } else {
            require(first_result.status == Status::Ok && second_result.status == Status::Ok &&
                    read_result.status == Status::Value && read_result.value == "one" && engine.durable_sequence() == 2,
                    "successful batches did not acknowledge in sequence");
            require(get(engine, "first").value == "one" && get(engine, "absent").status == Status::NotFound,
                    "already durable reads failed");
            const auto ready_reads = engine.stats();
            require(ready_reads.wal_durable_waits_total == after.wal_durable_waits_total &&
                    ready_reads.wal_durable_wait_duration_ns_total == after.wal_durable_wait_duration_ns_total,
                    "already durable reads added waits");
            engine.close();
            config.io_hook = {};
            Engine recovered(config);
            require(get(recovered, "first").value == "one" && get(recovered, "second").value == "two",
                    "reliable in-flight writes did not recover");
            require_no_waits(recovered.stats());
        }
    }
}

void close_drains_in_flight_and_queued_wal() {
    TempDir dir;
    auto config = config_for(dir);
    config.wal_mode = WalMode::Throughput;
    config.wal_batch_size = 1;
    WalSyncGate gate;
    config.io_hook = [&](const std::string& point) { gate(point); };
    Engine engine(config);
    put(engine, "first", "one");
    const bool syncing = gate.wait();
    auto writer = std::async(std::launch::async, [&] { put(engine, "queued", "two"); });
    const bool queued = writer.wait_for(std::chrono::milliseconds(300)) == std::future_status::ready;
    auto closer = std::async(std::launch::async, [&] { engine.close(); });
    const bool close_waited = closer.wait_for(std::chrono::milliseconds(50)) == std::future_status::timeout;
    gate.release();
    writer.get();
    closer.get();
    require(syncing && queued && close_waited, "close did not drain an overlapping WAL flush");
    config.io_hook = {};
    Engine recovered(config);
    require(get(recovered, "first").value == "one" && get(recovered, "queued").value == "two",
            "close lost in-flight or queued WAL records");
}

void close_completes_request_waits() {
    TempDir dir;
    auto config = config_for(dir);
    config.wal_batch_size = 1;
    config.wal_queue_bytes = codec::kWalRecordHeader + kMaxKeySize + kMaxValueSize + 4;
    WalSyncGate gate;
    config.io_hook = [&](const std::string& point) { gate(point); };
    Engine engine(config);
    const std::string key(kMaxKeySize, 'k');
    auto writer = std::async(std::launch::async, [&] {
        return engine.execute({Operation::Put, key, std::string(kMaxValueSize, 'v')});
    });
    const bool syncing = gate.wait();
    auto deleter = std::async(std::launch::async, [&] { return engine.execute({Operation::Delete, key, {}}); });
    auto reader = std::async(std::launch::async, [&] { return get(engine, key); });
    const bool waiting = wait_for_stats(engine, [](const EngineStats& stats) {
        return stats.wal_capacity_waiters == 1 && stats.wal_durable_waiters == 2;
    });
    auto closer = std::async(std::launch::async, [&] { engine.close(); });
    // close() wakes requests before joining the deliberately stalled WAL writer.
    // Capture accounting before releasing sync, so success cannot hide a missing
    // shutdown wakeup or an acknowledgement of data that is not yet durable.
    const bool woken = wait_for_stats(engine, [](const EngineStats& stats) {
        return stats.stopping && stats.wal_capacity_waiters == 0 && stats.wal_durable_waiters == 0;
    });
    const auto stopped = engine.stats();
    const bool close_waited = closer.wait_for(std::chrono::milliseconds(50)) == std::future_status::timeout;
    gate.release();
    const auto written = writer.get(), deleted = deleter.get(), read = reader.get();
    closer.get();
    require(syncing && waiting && woken && close_waited, "close did not wake requests while draining WAL");
    require(stopped.durable_sequence == 0 && stopped.wal_capacity_waits_total == 1 &&
            stopped.wal_durable_waits_total == 2 && stopped.wal_capacity_wait_duration_ns_total > 0 &&
            stopped.wal_durable_wait_duration_ns_total > 0, "shutdown lost request wait accounting");
    require(written.status == Status::IOError && read.status == Status::IOError && deleted.status == Status::Busy,
            "shutdown changed durable or capacity wait responses");
    const auto drained = engine.stats();
    require(drained.applied_sequence == 1 && drained.durable_sequence == 1 &&
            drained.wal_capacity_waits_total == stopped.wal_capacity_waits_total &&
            drained.wal_durable_waits_total == stopped.wal_durable_waits_total &&
            drained.wal_capacity_wait_duration_ns_total == stopped.wal_capacity_wait_duration_ns_total &&
            drained.wal_durable_wait_duration_ns_total == stopped.wal_durable_wait_duration_ns_total,
            "shutdown flush recounted completed waits or admitted a blocked delete");
    config.io_hook = {};
    Engine recovered(config);
    require(get(recovered, key).value.size() == kMaxValueSize, "shutdown did not preserve the admitted write");
    require_no_waits(recovered.stats());
}

void wal_failures() {
    for (const std::string point : {"wal.write", "wal.sync"}) {
        TempDir dir;
        auto config = config_for(dir);
        std::atomic<bool> armed{false};
        config.io_hook = [&](const std::string& operation) {
            if (armed && operation == point) throw std::runtime_error("injected disk failure");
        };
        Engine engine(config);
        armed = true;
        require(engine.execute({Operation::Put, "key", "value"}).status == Status::IOError, "failed WAL write was acknowledged");
        require(engine.durable_sequence() == 0, "failed sync advanced durable sequence");
        require(engine.execute({Operation::Put, "next", "value"}).status == Status::IOError, "writes continued after storage failure");
        must_fail([&] { engine.snapshot(); }, "snapshot discarded a failed WAL");
        must_fail([&] { engine.close(); }, "shutdown hid the storage failure");
    }
}

void partial_system_write_failure() {
    TempDir dir;
    const auto config = config_for(dir);
    const pid_t child = ::fork();
    require(child >= 0, "fork failed");
    if (child == 0) {
        try {
            Engine engine(config);
            // Force a real short write followed by EFBIG, without filling disk.
            std::signal(SIGXFSZ, SIG_IGN);
            const rlimit limit{codec::kWalFileHeader + 10, codec::kWalFileHeader + 10};
            if (::setrlimit(RLIMIT_FSIZE, &limit) != 0) ::_exit(90);
            const auto result = engine.execute({Operation::Put, "key", "value"});
            if (result.status != Status::IOError || engine.durable_sequence() != 0) ::_exit(91);
            ::_exit(0);
        } catch (...) { ::_exit(92); }
    }
    int status = 0;
    require(::waitpid(child, &status, 0) == child && WIFEXITED(status) && WEXITSTATUS(status) == 0, "partial WAL write was not rejected");
    require(read_file(dir.path + "/wal.v1") == codec::wal_file_header() +
            codec::wal_record(1, Operation::Put, "key", "value").substr(0, 10),
            "WAL fault did not write a partial record before failing");
    Engine recovered(config);
    require(get(recovered, "key").status == Status::NotFound, "partial system write became a value");
    put(recovered, "after", "repaired");
}

void snapshot_failures_preserve_wal() {
    for (const std::string point : {"snapshot.write", "snapshot.sync", "snapshot.rename", "snapshot.dir_sync"}) {
        TempDir dir;
        auto config = config_for(dir);
        std::atomic<bool> armed{false};
        config.io_hook = [&](const std::string& operation) {
            if (armed && operation == point) throw std::runtime_error("injected snapshot failure");
        };
        {
            Engine engine(config);
            put(engine, "key", point);
            const auto original_wal = read_file(dir.path + "/wal.v1");
            armed = true;
            must_fail([&] { engine.snapshot(); }, "snapshot failure was ignored");
            require(read_file(dir.path + "/wal.v1") == original_wal, "failed checkpoint changed WAL");
            armed = false;
            put(engine, "after", "failure");
            engine.close();
        }
        config.io_hook = {};
        Engine recovered(config);
        require(get(recovered, "key").value == point && get(recovered, "after").value == "failure", "snapshot failure lost data");
    }
}

void crash_at_snapshot_boundaries() {
    for (const std::string point : {"snapshot.write", "snapshot.sync", "snapshot.rename", "snapshot.dir_sync",
                                    "snapshot.after_install", "wal.truncate", "wal.after_truncate"}) {
        TempDir dir;
        auto config = config_for(dir);
        { Engine engine(config); put(engine, "before", "stable"); engine.close(); }
        // Fork before constructing an engine: no background threads cross fork.
        const pid_t child = ::fork();
        require(child >= 0, "fork failed");
        if (child == 0) {
            config.io_hook = [&](const std::string& operation) { if (operation == point) ::_exit(86); };
            try {
                Engine engine(config);
                put(engine, "acknowledged", "new");
                engine.snapshot();
            } catch (...) { ::_exit(87); }
            ::_exit(88);
        }
        int status = 0;
        require(::waitpid(child, &status, 0) == child && WIFEXITED(status) && WEXITSTATUS(status) == 86, "crash point did not execute");
        Engine recovered(config);
        require(get(recovered, "before").value == "stable" && get(recovered, "acknowledged").value == "new", "crash lost a durable write at " + point);
        put(recovered, "continued", "yes");
        recovered.close();
    }
}

void truncated_wal_tail() {
    const size_t first_size = codec::kWalFileHeader + codec::wal_record(1, Operation::Put, "first", "one").size();
    const size_t second_size = codec::wal_record(2, Operation::Put, "second", "two").size();
    for (size_t cut : {size_t{1}, size_t{20}, size_t{21}, second_size - 1}) {
        TempDir dir;
        auto config = config_for(dir);
        { Engine engine(config); put(engine, "first", "one"); put(engine, "second", "two"); engine.close(); }
        fs::resize_file(dir.path + "/wal.v1", first_size + cut);
        {
            Engine recovered(config);
            require(get(recovered, "first").value == "one" && get(recovered, "second").status == Status::NotFound, "incomplete WAL record was replayed");
            require(fs::file_size(dir.path + "/wal.v1") == first_size, "incomplete tail was not repaired");
            put(recovered, "third", "three");
            recovered.close();
        }
        Engine again(config);
        require(get(again, "third").value == "three", "new writes were appended behind a corrupt tail");
    }
}

void corruption_and_missing_files() {
    TempDir dir;
    const auto config = config_for(dir);
    { Engine engine(config); put(engine, "key", "value"); engine.close(); }
    const std::string original = read_file(dir.path + "/wal.v1");
    std::string corrupt = original;
    corrupt[codec::kWalFileHeader + codec::kWalRecordHeader + 1] ^= 0x10;
    write_file(dir.path + "/wal.v1", corrupt);
    must_fail([&] { Engine engine(config); }, "checksum corruption was silently skipped");
    write_file(dir.path + "/wal.v1", original);
    { Engine engine(config); engine.snapshot(); engine.close(); }
    const auto snapshot = read_file(dir.path + "/snapshot.v1");
    fs::resize_file(dir.path + "/snapshot.v1", snapshot.size() - 1);
    must_fail([&] { Engine engine(config); }, "truncated snapshot was accepted");
    write_file(dir.path + "/snapshot.v1", snapshot);
    fs::remove(dir.path + "/wal.v1");
    must_fail([&] { Engine engine(config); }, "missing WAL was silently recreated");
}

void missing_snapshot_with_empty_wal() {
    TempDir dir;
    const auto config = config_for(dir);
    { Engine engine(config); put(engine, "durable", "value"); engine.snapshot(); engine.close(); }
    require(fs::file_size(dir.path + "/wal.v1") == codec::kWalFileHeader, "checkpoint did not empty WAL");
    fs::remove(dir.path + "/snapshot.v1");
    must_fail([&] { Engine engine(config); }, "missing snapshot silently created an empty database");
    require(!fs::exists(dir.path + "/snapshot.v1"), "failed recovery replaced the missing snapshot");
}

void checkpointed_wal_tail_can_accept_new_writes() {
    for (bool torn_tail : {false, true}) {
        TempDir dir;
        auto config = config_for(dir);
        std::atomic<bool> armed{false};
        config.io_hook = [&](const std::string& point) {
            if (armed && point == "snapshot.after_install") throw std::runtime_error("leave checkpointed WAL behind");
        };
        {
            Engine engine(config);
            put(engine, "first", "one");
            put(engine, "second", "two");
            armed = true;
            must_fail([&] { engine.snapshot(); }, "WAL retention boundary not reached");
            engine.close();
        }
        config.io_hook = {};
        // The checkpoint covers both records, even when the retained WAL has
        // only an older complete prefix and an optional incomplete next record.
        const size_t first = codec::kWalFileHeader + codec::wal_record(1, Operation::Put, "first", "one").size();
        fs::resize_file(dir.path + "/wal.v1", first + (torn_tail ? 1 : 0));
        {
            Engine recovered(config);
            require(get(recovered, "second").value == "two", "checkpointed value missing");
            put(recovered, "third", "three");
            recovered.close();
        }
        Engine again(config);
        require(get(again, "first").value == "one" && get(again, "second").value == "two" &&
                get(again, "third").value == "three", "recovery appended a discontinuous WAL");
    }
}

void concurrent_close() {
    TempDir dir;
    auto config = config_for(dir);
    config.wal_mode = WalMode::Throughput;
    Engine engine(config);
    put(engine, "pending", "value");
    std::promise<void> start;
    auto ready = start.get_future().share();
    std::vector<std::future<void>> closers;
    for (int i = 0; i < 8; ++i) closers.push_back(std::async(std::launch::async, [&] { ready.wait(); engine.close(); }));
    start.set_value();
    for (auto& closer : closers) closer.get();
    Engine recovered(config);
    require(get(recovered, "pending").value == "value", "concurrent close lost the final flush");
}

void exclusive_directory_and_legacy_import() {
    TempDir dir;
    auto config = config_for(dir);
    const std::string snapshot = "first:old\n";
    const std::string wal = "PUT first new\nPUT other hello world\nDEL absent\n";
    write_file(dir.path + "/data.db", snapshot);
    write_file(dir.path + "/wal.log", wal);
    must_fail([&] { Engine engine(config); }, "legacy data was silently overwritten");
    config.import_legacy = true;
    { Engine engine(config); engine.close(); }
    require(read_file(dir.path + "/data.db") == snapshot && read_file(dir.path + "/wal.log") == wal, "legacy originals changed");
    config.import_legacy = false;
    Engine engine(config);
    require(get(engine, "first").value == "new" && get(engine, "other").value == "hello world", "legacy import changed data");
    must_fail([&] { Engine second(config); }, "two engines opened the same data directory");
}

int main(int argc, char** argv) {
    try {
        const std::vector<std::pair<const char*, void(*)()>> tests = {
            {"binary round trip", binary_round_trip}, {"concurrent writes and snapshots", concurrent_writes_and_snapshots},
            {"group commit", group_commit}, {"WAL failures", wal_failures}, {"partial system write", partial_system_write_failure},
            {"snapshot failures", snapshot_failures_preserve_wal}, {"snapshot crash boundaries", crash_at_snapshot_boundaries},
            {"truncated WAL tail", truncated_wal_tail}, {"corruption and missing files", corruption_and_missing_files},
            {"directory lock and legacy import", exclusive_directory_and_legacy_import},
            {"missing snapshot", missing_snapshot_with_empty_wal},
            {"checkpointed WAL tail", checkpointed_wal_tail_can_accept_new_writes},
            {"concurrent close", concurrent_close},
            {"WAL I/O progress", wal_io_allows_throughput_progress},
            {"in-flight WAL queue limit", in_flight_wal_counts_toward_queue_limit},
            {"in-flight WAL acknowledgements", in_flight_wal_preserves_reliable_acknowledgements},
            {"in-flight WAL close", close_drains_in_flight_and_queued_wal},
            {"shutdown request waits", close_completes_request_waits},
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
