#pragma once

#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <deque>
#include <functional>
#include <mutex>
#include <string>
#include <thread>
#include <unordered_map>

namespace minikv {

constexpr uint32_t kMaxKeySize = 4096;
constexpr uint32_t kMaxValueSize = 1024 * 1024;

enum class Operation : uint8_t { Put = 1, Get = 2, Delete = 3, Stats = 4 };
enum class Status : uint8_t { Ok = 0, Value = 1, NotFound = 2, Invalid = 3, IOError = 4, Busy = 5 };
enum class WalMode { Throughput, Reliable };

struct Request {
    Operation operation;
    std::string key;
    std::string value;
};

struct Response {
    Status status;
    std::string value;
};

struct EngineConfig {
    std::string data_dir = "./data";
    WalMode wal_mode = WalMode::Throughput;
    size_t wal_batch_size = 512;
    size_t wal_queue_bytes = 16 * 1024 * 1024;
    std::chrono::milliseconds wal_flush_interval{100};
    std::chrono::milliseconds snapshot_interval{20 * 60 * 1000};
    // Tests can fail or terminate at an actual I/O boundary. Must be thread-safe:
    // WAL and snapshot callbacks may run concurrently. Unset in the server.
    std::function<void(const std::string&)> io_hook;
    bool import_legacy = false;
};

// One state-lock snapshot. Counters reset on restart; sequences are recovered.
// Duration counters include failed attempts and use monotonic nanoseconds.
struct EngineStats {
    WalMode wal_mode = WalMode::Throughput;
    uint64_t keys = 0;
    uint64_t applied_sequence = 0;
    uint64_t durable_sequence = 0;
    uint64_t wal_pending_bytes = 0;
    uint64_t wal_inflight_bytes = 0;
    uint64_t wal_queued_records = 0;
    uint64_t wal_queue_capacity_bytes = 0;
    // Wait totals count completed episodes, including failure/shutdown wakeups.
    // Durations include CV waiting and state-lock reacquisition; immediate-ready
    // requests do not count. Capacity applies to writes; durability includes GET.
    uint64_t wal_capacity_waiters = 0;
    uint64_t wal_capacity_waits_total = 0;
    uint64_t wal_capacity_wait_duration_ns_total = 0;
    uint64_t wal_durable_waiters = 0;
    uint64_t wal_durable_waits_total = 0;
    uint64_t wal_durable_wait_duration_ns_total = 0;
    uint64_t wal_commits_total = 0;
    uint64_t wal_commit_failures_total = 0;
    uint64_t wal_commit_duration_ns_total = 0;
    uint64_t wal_commit_last_duration_ns = 0;
    uint64_t snapshot_successes_total = 0;
    uint64_t snapshot_failures_total = 0;
    bool snapshot_in_progress = false;
    uint64_t snapshot_sequence = 0;
    uint64_t snapshot_capture_duration_ns_total = 0;
    uint64_t snapshot_write_duration_ns_total = 0;
    uint64_t snapshot_compact_duration_ns_total = 0;
    bool io_failed = false;
    bool stopping = false;
};

class Engine {
public:
    explicit Engine(EngineConfig config);
    ~Engine();
    Engine(const Engine&) = delete;
    Engine& operator=(const Engine&) = delete;

    Response execute(const Request& request);
    void snapshot();
    void close();
    uint64_t durable_sequence() const;
    EngineStats stats() const;

private:
    struct PendingRecord {
        uint64_t sequence;
        std::string bytes;
    };

    void recover();
    void load_snapshot();
    void load_wal();
    void import_legacy();
    void write_batch(const std::deque<PendingRecord>& records);
    void commit_batch(const std::deque<PendingRecord>& records, size_t bytes);
    void flush_pending();
    // Requires both I/O and state locks; used only for the final shutdown flush.
    void flush_locked();
    void install_snapshot(const std::unordered_map<std::string, std::string>& image, uint64_t sequence);
    void compact_wal(int64_t boundary);
    void background_work();
    void background_snapshots();
    void hook(const std::string& point);
    void fail_locked(const std::string& message);
    void release_files() noexcept;

    EngineConfig config_;
    std::mutex close_mutex_;
    std::mutex snapshot_mutex_;
    // Acquire I/O before state when both are needed; never wait for I/O while
    // holding mutex_. Background WAL writes release mutex_ during file I/O.
    std::mutex io_mutex_;
    mutable std::mutex mutex_;
    std::condition_variable wake_;
    std::condition_variable snapshot_wake_;
    std::condition_variable committed_;
    std::unordered_map<std::string, std::string> kv_;
    std::deque<PendingRecord> pending_;
    size_t pending_bytes_ = 0; // Queued plus in-flight, not-yet-synced WAL bytes.
    uint64_t applied_sequence_ = 0;
    uint64_t durable_sequence_ = 0;
    EngineStats stats_;
    int wal_fd_ = -1;
    int lock_fd_ = -1;
    bool stopping_ = false;
    bool closed_ = false;
    std::string failure_;
    std::thread worker_;
    std::thread snapshot_worker_;
};

} // namespace minikv
