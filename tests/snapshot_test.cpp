#include "engine.h"
#include "codec.h"

#include <atomic>
#include <cerrno>
#include <csignal>
#include <filesystem>
#include <fstream>
#include <future>
#include <iostream>
#include <stdexcept>
#include <string>
#include <system_error>
#include <sys/resource.h>
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

std::unordered_map<std::string, std::string> small_snapshot_image() {
    std::unordered_map<std::string, std::string> image;
    for (size_t index = 0; index < 256; ++index) {
        const std::string key = "key-" + std::to_string(10000 + index).substr(1);
        std::string value(1024, static_cast<char>(index % 251));
        value.front() = '\0';
        image.emplace(key, std::move(value));
    }
    return image;
}

void verify_image(Engine& engine, const std::unordered_map<std::string, std::string>& expected) {
    require(engine.stats().keys == expected.size(), "recovered image has missing or extra keys");
    for (const auto& entry : expected) {
        const auto response = get(engine, entry.first);
        require(response.status == Status::Value && response.value == entry.second,
                "snapshot image did not recover an exact key/value");
    }
}

void complete_image_spans_multiple_writes() {
    TempDir dir;
    const auto config = config_for(dir);
    auto expected = small_snapshot_image();
    std::string large(192 * 1024 + 17, '\0');
    for (size_t index = 0; index < large.size(); ++index) large[index] = static_cast<char>(index % 251);
    expected.emplace("large", std::move(large));
    expected.emplace("empty", "");
    expected.emplace(std::string("binary\0key", 10), std::string("\0a\nb\xff", 5));
    const uint64_t sequence = expected.size();
    {
        Engine engine(config);
        // Every entry belongs to the captured image, including the large value.
        // No post-capture WAL suffix can conceal missing snapshot records.
        for (const auto& entry : expected) put(engine, entry.first, entry.second);
        const auto before = engine.stats();
        engine.snapshot();
        const auto bytes = read_file(dir.path + "/snapshot.v1");
        require(bytes.size() >= 28 && bytes.substr(0, 8) == "MKVSNP01" &&
                codec::u64(bytes, 8) == sequence && codec::u64(bytes, 16) == expected.size() &&
                codec::u32(bytes, 24) == codec::crc32(std::string_view(bytes).substr(0, 24)),
                "large snapshot header, count or checksum changed");
        std::unordered_map<std::string, std::string> decoded;
        size_t offset = 28;
        while (offset < bytes.size()) {
            const auto remaining = std::string_view(bytes).substr(offset);
            const auto size = codec::record_size(remaining);
            require(size <= remaining.size(), "snapshot ended inside a record");
            const auto record = remaining.substr(0, size);
            // decode_record validates the complete record and its CRC.
            auto entry = codec::decode_record(record);
            require(entry.operation == Operation::Put && codec::u64(record, 5) == sequence,
                    "snapshot record has the wrong operation or sequence");
            require(decoded.emplace(std::move(entry.key), std::move(entry.value)).second,
                    "snapshot contains a duplicate key");
            offset += size;
        }
        require(decoded == expected, "large snapshot changed its complete captured image");
        require(read_file(dir.path + "/wal.v1") == codec::wal_file_header(), "snapshot verification still depends on WAL replay");
        const auto stats = engine.stats();
        require(stats.applied_sequence == sequence && stats.durable_sequence == sequence &&
                stats.wal_pending_bytes == 0 && stats.wal_inflight_bytes == 0,
                "snapshot did not finish checkpointing the captured image");
        require(stats.snapshot_file_written_bytes_total - before.snapshot_file_written_bytes_total == bytes.size() &&
                stats.snapshot_file_installed_bytes_total - before.snapshot_file_installed_bytes_total == bytes.size() &&
                stats.snapshot_file_write_calls_total > before.snapshot_file_write_calls_total &&
                stats.snapshot_compact_written_bytes_total == before.snapshot_compact_written_bytes_total + codec::kWalFileHeader,
                "complete snapshot byte accounting differs from its verified file or includes an empty WAL suffix");
        engine.close();
    }
    require(read_file(dir.path + "/wal.v1") == codec::wal_file_header(), "close unexpectedly populated the checkpointed WAL");
    Engine recovered(config);
    verify_image(recovered, expected);
    require(recovered.stats().durable_sequence == sequence, "large snapshot recovered the wrong sequence");
}

// Used only in a forked child. Restore before Engine cleanup, assertions or any
// continued write. An unrecoverable restoration error must not be hidden by a
// destructor or leave the remainder of the test running under the reduced limit.
class ScopedFileSizeLimit {
public:
    explicit ScopedFileSizeLimit(rlim_t limit) {
        if (::getrlimit(RLIMIT_FSIZE, &original_) != 0) {
            throw std::system_error(errno, std::generic_category(), "getrlimit");
        }
        require(original_.rlim_cur == RLIM_INFINITY || original_.rlim_cur > limit,
                "file limit fixture must lower the original soft limit");
        struct sigaction ignored {};
        ignored.sa_handler = SIG_IGN;
        ::sigemptyset(&ignored.sa_mask);
        if (::sigaction(SIGXFSZ, &ignored, &previous_) != 0) {
            throw std::system_error(errno, std::generic_category(), "ignore SIGXFSZ");
        }
        auto reduced = original_;
        reduced.rlim_cur = limit;
        if (::setrlimit(RLIMIT_FSIZE, &reduced) != 0) {
            const auto error = errno;
            if (::sigaction(SIGXFSZ, &previous_, nullptr) != 0) ::_exit(93);
            throw std::system_error(error, std::generic_category(), "reduce file size limit");
        }
    }
    ~ScopedFileSizeLimit() {
        if (::setrlimit(RLIMIT_FSIZE, &original_) != 0 || ::sigaction(SIGXFSZ, &previous_, nullptr) != 0) ::_exit(93);
    }
    ScopedFileSizeLimit(const ScopedFileSizeLimit&) = delete;
    ScopedFileSizeLimit& operator=(const ScopedFileSizeLimit&) = delete;

private:
    struct rlimit original_ {};
    struct sigaction previous_ {};
};

void partial_snapshot_write_preserves_old_files() {
    TempDir dir;
    const auto config = config_for(dir);
    auto expected = small_snapshot_image();
    constexpr rlim_t limit = 96 * 1024 + 13;
    // Construct Engine only after fork, with no engine threads in the parent.
    const pid_t child = ::fork();
    require(child >= 0, "fork failed");
    if (child == 0) {
        ::alarm(20);
        try {
            Engine engine(config);
            put(engine, "key-0000", "old");
            engine.snapshot();
            for (const auto& entry : expected) put(engine, entry.first, entry.second);
            auto before = engine.stats();
            require(before.applied_sequence == expected.size() + 1 &&
                    before.durable_sequence == before.applied_sequence && before.wal_pending_bytes == 0,
                    "file limit was applied before reliable preload had drained");
            const auto old_snapshot = read_file(dir.path + "/snapshot.v1");
            const auto old_wal = read_file(dir.path + "/wal.v1");
            // Zero bytes first proves that failed write syscalls are counted,
            // without assuming a fixed number of positive writes or retries.
            // The last attempt leaves a real partial file for the first restart.
            for (const rlim_t trial_limit : {rlim_t(0), limit}) {
                std::error_code failure;
                {
                    ScopedFileSizeLimit scoped_limit(trial_limit);
                    try { engine.snapshot(); }
                    catch (const std::system_error& error) { failure = error.code(); }
                }
                // Restore the limit and signal disposition before assertions,
                // even when snapshot throws a different exception.
                require(failure == std::errc::file_too_large, "snapshot did not report real EFBIG");
                require(fs::file_size(dir.path + "/snapshot.v1.tmp") == trial_limit,
                        "snapshot did not write exactly up to the file limit");
                require(read_file(dir.path + "/snapshot.v1") == old_snapshot && read_file(dir.path + "/wal.v1") == old_wal,
                        "failed snapshot replaced the old image or compacted WAL");
                const auto after = engine.stats();
                require(after.snapshot_failures_total == before.snapshot_failures_total + 1 &&
                        after.snapshot_successes_total == before.snapshot_successes_total &&
                        !after.snapshot_in_progress && !after.io_failed &&
                        after.applied_sequence == before.applied_sequence && after.durable_sequence == before.durable_sequence &&
                        after.wal_pending_bytes == 0 && after.wal_commit_failures_total == before.wal_commit_failures_total,
                        "snapshot write failure changed WAL confirmation or terminal failure state");
                require(after.snapshot_file_written_bytes_total - before.snapshot_file_written_bytes_total == trial_limit &&
                        after.snapshot_file_installed_bytes_total == before.snapshot_file_installed_bytes_total &&
                        after.snapshot_compact_written_bytes_total == before.snapshot_compact_written_bytes_total &&
                        after.snapshot_file_write_calls_total > before.snapshot_file_write_calls_total,
                        "snapshot accounting lost failed write calls or partial bytes, or claimed installation");
                before = after;
            }
            put(engine, "continued", "after short write");
            engine.close();
            ::_exit(0);
        } catch (const std::exception& error) {
            std::cerr << "snapshot file-limit child: " << error.what() << std::endl;
            ::_exit(92);
        } catch (...) { ::_exit(94); }
    }
    int status = 0;
    require(::waitpid(child, &status, 0) == child && WIFEXITED(status) && WEXITSTATUS(status) == 0,
            "snapshot partial-write child failed, timed out or could not restore its file limit");
    require(fs::file_size(dir.path + "/snapshot.v1.tmp") == limit, "partial snapshot fixture disappeared before recovery");
    // Do not retry snapshot before this first recovery: the incomplete .tmp
    // must be ignored while the old snapshot and retained WAL recover new data.
    expected.emplace("continued", "after short write");
    Engine recovered(config);
    verify_image(recovered, expected);
    require(recovered.stats().durable_sequence == expected.size() + 1,
            "recovery after partial snapshot lost an acknowledged write");
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
        const auto before = engine.stats();
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
        const auto expected_wal = codec::wal_file_header() + codec::wal_record(4, Operation::Put, "changed", large_value) +
                                  codec::wal_record(5, Operation::Delete, "deleted", "") +
                                  codec::wal_record(6, Operation::Put, "added", "after");
        require(read_file(dir.path + "/wal.v1") == expected_wal,
                "WAL compaction did not retain exactly the post-checkpoint suffix");
        const auto after = engine.stats();
        const auto snapshot_bytes = fs::file_size(dir.path + "/snapshot.v1");
        require(after.snapshot_compact_written_bytes_total - before.snapshot_compact_written_bytes_total == expected_wal.size() &&
                after.snapshot_file_written_bytes_total - before.snapshot_file_written_bytes_total == snapshot_bytes &&
                after.snapshot_file_installed_bytes_total - before.snapshot_file_installed_bytes_total == snapshot_bytes,
                "snapshot and concurrently retained WAL suffix bytes were conflated");
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
            {"snapshot complete large image", complete_image_spans_multiple_writes},
            {"snapshot partial system write", partial_snapshot_write_preserves_old_files},
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
