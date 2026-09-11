#include "engine.h"
#include "codec.h"

#include <atomic>
#include <csignal>
#include <filesystem>
#include <fstream>
#include <future>
#include <iostream>
#include <stdexcept>
#include <string>
#include <sys/wait.h>
#include <unistd.h>
#include <unordered_map>
#include <vector>

using namespace minikv;
namespace fs = std::filesystem;
using namespace std::chrono_literals;

namespace {

void require(bool condition, const std::string& message) {
    if (!condition) throw std::runtime_error(message);
}

struct TempDir {
    std::string path;
    TempDir() {
        std::string pattern = (fs::temp_directory_path() / "minikv-snapshot-XXXXXX").string();
        const auto* created = ::mkdtemp(pattern.data());
        if (!created) throw std::runtime_error("mkdtemp failed");
        path = created;
    }
    ~TempDir() { std::error_code error; fs::remove_all(path, error); }
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

Response get(Engine& engine, const std::string& key) {
    return engine.execute({Operation::Get, key, {}});
}

void put(Engine& engine, const std::string& key, const std::string& value) {
    require(engine.execute({Operation::Put, key, value}).status == Status::Ok,
            "PUT was not acknowledged: " + key);
}

void seed(Engine& engine) {
    put(engine, "changed", "before");
    put(engine, "deleted", "before");
    put(engine, "stable", "unchanged");
}

void mutate(Engine& engine, const std::string& changed_value = "after") {
    put(engine, "changed", changed_value);
    require(engine.execute({Operation::Delete, "deleted", {}}).status == Status::Ok,
            "DELETE was not acknowledged");
    put(engine, "added", "after");
}

void verify_mutations(Engine& engine, const std::string& changed_value = "after") {
    require(get(engine, "changed").value == changed_value, "checkpoint overwrote a newer value");
    require(get(engine, "deleted").status == Status::NotFound, "checkpoint resurrected a deleted key");
    require(get(engine, "added").value == "after", "checkpoint lost an added key");
    require(get(engine, "stable").value == "unchanged", "checkpoint lost an unchanged key");
}

std::string read_file(const std::string& path) {
    std::ifstream file(path, std::ios::binary);
    require(file.good(), "cannot read " + path);
    return {std::istreambuf_iterator<char>(file), std::istreambuf_iterator<char>()};
}

// Gates cannot outlive a failed test indefinitely. Every normal path releases
// the gate before checking assertions or joining futures that might use it.
struct SnapshotGate {
    std::string target = "snapshot.write";
    std::atomic<bool> armed{false};
    std::atomic<int> visits{0};
    std::promise<void> entered, resume;
    std::future<void> waiting = entered.get_future();
    std::shared_future<void> resumed = resume.get_future().share();

    void operator()(const std::string& point) {
        if (point != target || !armed.load()) return;
        if (visits.fetch_add(1) != 0) return;
        entered.set_value();
        if (resumed.wait_for(5s) != std::future_status::ready) {
            throw std::runtime_error("test timed out releasing snapshot writer");
        }
    }
    bool wait() { return waiting.wait_for(2s) == std::future_status::ready; }
    void release() { resume.set_value(); }
};

void verify_captured_snapshot(const TempDir& dir) {
    const auto bytes = read_file(dir.path + "/snapshot.v1");
    require(bytes.size() >= 28 && bytes.substr(0, 8) == "MKVSNP01" &&
            codec::u64(bytes, 8) == 3 && codec::u64(bytes, 16) == 3,
            "snapshot does not describe the captured sequence");
    std::unordered_map<std::string, std::string> entries;
    size_t offset = 28;
    while (offset < bytes.size()) {
        const auto remaining = std::string_view(bytes).substr(offset);
        const size_t size = codec::record_size(remaining.substr(0, codec::kRecordHeader));
        const auto record = remaining.substr(0, size);
        const auto entry = codec::decode_record(record);
        require(codec::u64(record, 5) == 3 && entry.operation == Operation::Put,
                "snapshot mixed records from different versions");
        entries.emplace(entry.key, entry.value);
        offset += size;
    }
    require(entries == std::unordered_map<std::string, std::string>{
                {"changed", "before"}, {"deleted", "before"}, {"stable", "unchanged"}},
            "snapshot contents changed after its state lock was released");
}

void snapshot_io_allows_reliable_progress() {
    TempDir dir;
    auto config = config_for(dir);
    // Cross several 64 KiB compaction buffers, including embedded NUL bytes.
    std::string large_value(192 * 1024 + 17, '\0');
    for (size_t i = 0; i < large_value.size(); ++i) large_value[i] = static_cast<char>(i % 251);
    SnapshotGate gate;
    config.io_hook = [&](const std::string& point) { gate(point); };
    {
        Engine engine(config);
        seed(engine);
        gate.armed = true;
        auto snapshot = std::async(std::launch::async, [&] { engine.snapshot(); });
        const bool entered = gate.wait();
        auto writer = std::async(std::launch::async, [&] { mutate(engine, large_value); });
        auto reader = std::async(std::launch::async, [&] { return get(engine, "stable"); });
        const bool wrote = writer.wait_for(500ms) == std::future_status::ready;
        const bool read = reader.wait_for(500ms) == std::future_status::ready;
        gate.release();
        writer.get();
        const auto result = reader.get();
        snapshot.get();
        require(entered && wrote && read, "snapshot file I/O blocked reliable requests");
        require(result.status == Status::Value && result.value == "unchanged", "concurrent GET changed data");
        verify_mutations(engine, large_value);
        verify_captured_snapshot(dir);
        const auto expected_wal = codec::record(4, Operation::Put, "changed", large_value) +
                                  codec::record(5, Operation::Delete, "deleted", "") +
                                  codec::record(6, Operation::Put, "added", "after");
        require(read_file(dir.path + "/wal.v1") == expected_wal,
                "WAL compaction did not retain exactly the post-checkpoint suffix");
        // This append detects writers left attached to the unlinked old WAL.
        put(engine, "after-replacement", "persisted");
        engine.close();
    }
    config.io_hook = {};
    {
        Engine recovered(config);
        verify_mutations(recovered, large_value);
        require(get(recovered, "after-replacement").value == "persisted", "writer appended to the old WAL inode");
        put(recovered, "second-restart", "persisted");
        recovered.close();
    }
    Engine again(config);
    verify_mutations(again, large_value);
    require(get(again, "second-restart").value == "persisted", "recovered suffix cannot accept new writes");
}

void automatic_snapshot_allows_reliable_progress() {
    TempDir dir;
    auto config = config_for(dir);
    config.snapshot_interval = 30ms;
    SnapshotGate gate;
    config.io_hook = [&](const std::string& point) { gate(point); };
    {
        Engine engine(config);
        put(engine, "before", "persisted");
        gate.armed = true;
        const bool entered = gate.wait();
        auto writer = std::async(std::launch::async, [&] { put(engine, "during", "persisted"); });
        const bool wrote = writer.wait_for(500ms) == std::future_status::ready;
        gate.release();
        writer.get();
        require(entered && wrote, "automatic snapshot prevented the WAL worker from committing");
        engine.close();
    }
    config.io_hook = {};
    config.snapshot_interval = 0ms;
    Engine recovered(config);
    require(get(recovered, "before").value == "persisted" && get(recovered, "during").value == "persisted",
            "automatic snapshot lost an acknowledged write");
}

void concurrent_snapshots_serialize() {
    TempDir dir;
    auto config = config_for(dir);
    SnapshotGate gate;
    config.io_hook = [&](const std::string& point) { gate(point); };
    Engine engine(config);
    seed(engine);
    gate.armed = true;
    auto first = std::async(std::launch::async, [&] { engine.snapshot(); });
    const bool entered = gate.wait();
    std::promise<void> second_started;
    auto started = second_started.get_future();
    auto second = std::async(std::launch::async, [&] { second_started.set_value(); engine.snapshot(); });
    const bool launched = started.wait_for(500ms) == std::future_status::ready;
    const bool waited = second.wait_for(100ms) == std::future_status::timeout;
    const int visits_while_paused = gate.visits.load();
    gate.release();
    first.get();
    second.get();
    require(entered && launched && waited && visits_while_paused == 1 && gate.visits == 2,
            "concurrent snapshots shared temporary files or installed out of order");
    engine.close();
    config.io_hook = {};
    Engine recovered(config);
    require(get(recovered, "changed").value == "before" && get(recovered, "deleted").value == "before",
            "serialized snapshots changed data");
}

void close_waits_for_active_snapshot() {
    TempDir dir;
    auto config = config_for(dir);
    SnapshotGate gate;
    config.io_hook = [&](const std::string& point) { gate(point); };
    Engine engine(config);
    seed(engine);
    gate.armed = true;
    auto snapshot = std::async(std::launch::async, [&] { engine.snapshot(); });
    const bool entered = gate.wait();
    auto writer = std::async(std::launch::async, [&] { mutate(engine); });
    const bool wrote = writer.wait_for(500ms) == std::future_status::ready;
    auto closer = std::async(std::launch::async, [&] { engine.close(); });
    const bool waited = closer.wait_for(100ms) == std::future_status::timeout;
    gate.release();
    writer.get();
    snapshot.get();
    closer.get();
    require(entered && wrote && waited, "close released files before an active snapshot finished");
    config.io_hook = {};
    Engine recovered(config);
    verify_mutations(recovered);
}

void replacement_preserves_pending_writes() {
    TempDir dir;
    auto config = config_for(dir);
    config.wal_mode = WalMode::Throughput;
    SnapshotGate gate;
    gate.target = "wal.compact.write";
    config.io_hook = [&](const std::string& point) { gate(point); };
    {
        Engine engine(config);
        put(engine, "before", "persisted");
        gate.armed = true;
        auto snapshot = std::async(std::launch::async, [&] { engine.snapshot(); });
        const bool entered = gate.wait();
        // Compaction owns the WAL I/O lock. This accepted record must remain
        // queued until the replacement descriptor is ready for the writer.
        auto writer = std::async(std::launch::async, [&] { put(engine, "queued", "persisted"); });
        const bool wrote = writer.wait_for(500ms) == std::future_status::ready;
        auto reader = std::async(std::launch::async, [&] { return get(engine, "before"); });
        const bool read = reader.wait_for(500ms) == std::future_status::ready;
        gate.release();
        writer.get();
        const auto result = reader.get();
        snapshot.get();
        require(entered && wrote && read && result.value == "persisted",
                "WAL replacement unnecessarily blocked throughput requests");
        engine.close();
    }
    config.io_hook = {};
    Engine recovered(config);
    require(get(recovered, "before").value == "persisted" && get(recovered, "queued").value == "persisted",
            "WAL replacement discarded a queued write or appended it to the old inode");
}

const std::vector<std::string> snapshot_boundaries = {
    "snapshot.write", "snapshot.sync", "snapshot.rename", "snapshot.dir_sync", "snapshot.after_install",
    "wal.truncate", "wal.compact.write", "wal.compact.sync", "wal.compact.rename",
    "wal.compact.dir_sync", "wal.compact.after_replace", "wal.after_truncate",
};

void crash_preserves_acknowledged_suffix() {
    for (const auto& boundary : snapshot_boundaries) {
        TempDir dir;
        auto config = config_for(dir);
        { Engine engine(config); seed(engine); engine.close(); }
        // No engine threads exist across fork. Terminate inside a real file
        // boundary, bypassing destructors and the normal final WAL flush.
        const pid_t child = ::fork();
        require(child >= 0, "fork failed");
        if (child == 0) {
            ::alarm(8);
            Engine* active = nullptr;
            bool mutated = false;
            config.io_hook = [&](const std::string& point) {
                if (point == "snapshot.write" && !mutated) {
                    mutated = true;
                    mutate(*active);
                }
                if (point == boundary) ::_exit(mutated ? 86 : 89);
            };
            try {
                Engine engine(config);
                active = &engine;
                engine.snapshot();
            } catch (...) { ::_exit(87); }
            ::_exit(88);
        }
        int status = 0;
        require(::waitpid(child, &status, 0) == child && WIFEXITED(status) && WEXITSTATUS(status) == 86,
                "crash boundary did not execute after durable suffix writes: " + boundary);
        {
            Engine recovered(config);
            verify_mutations(recovered);
            put(recovered, "continued", boundary);
            recovered.close();
        }
        Engine again(config);
        verify_mutations(again);
        require(get(again, "continued").value == boundary, "post-crash append lost at " + boundary);
    }
}

void failures_preserve_acknowledged_suffix() {
    for (const auto& boundary : snapshot_boundaries) {
        TempDir dir;
        auto config = config_for(dir);
        SnapshotGate gate;
        config.io_hook = [&](const std::string& point) {
            if (!gate.armed.load()) return;
            gate(point);
            if (point == boundary) throw std::runtime_error("injected failure at " + boundary);
        };
        bool continued = false;
        {
            Engine engine(config);
            seed(engine);
            gate.armed = true;
            auto snapshot = std::async(std::launch::async, [&] {
                try { engine.snapshot(); } catch (const std::exception&) { return true; }
                return false;
            });
            const bool entered = gate.wait();
            auto writer = std::async(std::launch::async, [&] { mutate(engine); });
            const bool wrote = writer.wait_for(500ms) == std::future_status::ready;
            gate.release();
            writer.get();
            const bool failed = snapshot.get();
            gate.armed = false;
            require(entered && wrote && failed, "failure boundary did not execute after durable suffix writes: " + boundary);
            const auto response = engine.execute({Operation::Put, "continued", boundary});
            continued = response.status == Status::Ok;
            require(continued || response.status == Status::IOError, "unexpected state after snapshot failure");
            if (boundary.rfind("snapshot.", 0) == 0) {
                require(continued, "snapshot file failure unnecessarily disabled WAL writes: " + boundary);
            }
            if (boundary.rfind("wal.compact.", 0) == 0) {
                require(response.status == Status::IOError,
                        "WAL replacement failure did not reject later writes: " + boundary);
            }
            if (continued) engine.close();
            else {
                // Failure after a rename may require a terminal engine state;
                // never treat an acknowledged append to the old inode as safe.
                try { engine.close(); } catch (const std::exception&) {}
            }
        }
        config.io_hook = {};
        {
            Engine recovered(config);
            verify_mutations(recovered);
            if (continued) require(get(recovered, "continued").value == boundary,
                                   "acknowledged write used the old WAL inode after " + boundary);
            put(recovered, "second-restart", "persisted");
            recovered.close();
        }
        Engine again(config);
        verify_mutations(again);
        require(get(again, "second-restart").value == "persisted", "failure recovery cannot append at " + boundary);
    }
}

} // namespace

int main(int argc, char** argv) {
    try {
        const std::vector<std::pair<const char*, void(*)()>> tests = {
            {"snapshot I/O progress and captured version", snapshot_io_allows_reliable_progress},
            {"automatic snapshot I/O progress", automatic_snapshot_allows_reliable_progress},
            {"concurrent snapshots serialize", concurrent_snapshots_serialize},
            {"close waits for snapshot", close_waits_for_active_snapshot},
            {"WAL replacement preserves pending writes", replacement_preserves_pending_writes},
            {"snapshot suffix crash boundaries", crash_preserves_acknowledged_suffix},
            {"snapshot suffix failure boundaries", failures_preserve_acknowledged_suffix},
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
