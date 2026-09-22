#include "engine.h"
#include "codec.h"

#include <atomic>
#include <chrono>
#include <filesystem>
#include <fstream>
#include <future>
#include <iostream>
#include <memory>
#include <stdexcept>
#include <string>
#include <thread>
#include <unistd.h>
#include <utility>
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
        std::string pattern = (fs::temp_directory_path() / "minikv-data-limit-XXXXXX").string();
        const auto* created = ::mkdtemp(pattern.data());
        if (!created) throw std::runtime_error("mkdtemp failed");
        path = created;
    }
    ~TempDir() { std::error_code error; fs::remove_all(path, error); }
};

EngineConfig config_for(const TempDir& dir, WalMode mode = WalMode::Reliable) {
    EngineConfig config;
    config.data_dir = dir.path;
    config.wal_mode = mode;
    config.wal_batch_size = 1;
    config.wal_flush_interval = 1ms;
    config.snapshot_interval = 0ms;
    return config;
}

std::string read_file(const std::string& path) {
    std::ifstream file(path, std::ios::binary);
    require(file.good(), "cannot read fixture: " + path);
    return {std::istreambuf_iterator<char>(file), std::istreambuf_iterator<char>()};
}

void write_file(const std::string& path, const std::string& bytes) {
    std::ofstream file(path, std::ios::binary | std::ios::trunc);
    file.write(bytes.data(), static_cast<std::streamsize>(bytes.size()));
    require(file.good(), "cannot write fixture: " + path);
}

void put(Engine& engine, const std::string& key, const std::string& value) {
    require(engine.execute({Operation::Put, key, value}).status == Status::Ok, "PUT rejected: " + key);
}

void value_is(Engine& engine, const std::string& key, const std::string& value) {
    const auto result = engine.execute({Operation::Get, key, {}});
    require(result.status == Status::Value && result.value == value, "value changed: " + key);
}

void absent(Engine& engine, const std::string& key) {
    require(engine.execute({Operation::Get, key, {}}).status == Status::NotFound, "unexpected key: " + key);
}

template <class Predicate>
bool wait_for_stats(Engine& engine, Predicate ready) {
    const auto deadline = std::chrono::steady_clock::now() + 2s;
    do {
        if (ready(engine.stats())) return true;
        std::this_thread::sleep_for(1ms);
    } while (std::chrono::steady_clock::now() < deadline);
    return ready(engine.stats());
}

struct CallbackReleaseProbe {
    Engine& engine;
    std::atomic<int>& released;
    CallbackReleaseProbe(Engine& current, std::atomic<int>& count) : engine(current), released(count) {}
    ~CallbackReleaseProbe() { (void)engine.stats(); ++released; }
};

void reject_without_mutation(Engine& engine, const TempDir& dir, const Request& request, bool asynchronous) {
    // A completed checkpoint removes unrelated background WAL activity from
    // this comparison; rejection must not create even an uncommitted record.
    engine.snapshot();
    const auto before = engine.stats();
    const auto wal = read_file(dir.path + "/wal.v1");
    std::atomic<int> calls{0}, released{0};
    if (asynchronous) {
        const auto response = engine.execute_async(request,
            [&, probe = std::make_shared<CallbackReleaseProbe>(engine, released)](Response) { (void)probe; ++calls; });
        require(response && response->status == Status::Busy, "over-cap async PUT was not immediate BUSY");
        engine.drain_async();
    } else {
        require(engine.execute(request).status == Status::Busy, "over-cap PUT was not BUSY");
    }
    const auto after = engine.stats();
    require(calls == 0 && released == (asynchronous ? 1 : 0) && after.async_requests_inflight == 0,
            "rejection invoked or retained an async callback");
    require(after.data_rejections_total == before.data_rejections_total + 1, "rejection was not counted exactly once");
    require(after.data_bytes == before.data_bytes && after.keys == before.keys &&
            after.applied_sequence == before.applied_sequence && after.durable_sequence == before.durable_sequence &&
            after.wal_pending_bytes == before.wal_pending_bytes && after.wal_queued_records == before.wal_queued_records &&
            after.wal_inflight_bytes == before.wal_inflight_bytes && after.wal_commits_total == before.wal_commits_total &&
            after.wal_commit_failures_total == before.wal_commit_failures_total && !after.io_failed &&
            read_file(dir.path + "/wal.v1") == wal, "rejected PUT changed storage state");
}

void logical_bytes_and_rejections() {
    for (const auto mode : {WalMode::Throughput, WalMode::Reliable}) {
        TempDir dir;
        auto config = config_for(dir, mode);
        config.max_data_bytes = 8;
        config.max_async_requests = 1;
        const std::string binary_key("a\0", 2), binary_value("v\0x", 3);
        {
            Engine engine(config);
            require(engine.stats().data_bytes == 0 && engine.stats().data_capacity_bytes == 8,
                    "initial data accounting differs from configured cap");
            put(engine, binary_key, binary_value);
            put(engine, "xyz", "");
            require(engine.stats().data_bytes == 8, "logical bytes include framing or omit binary/empty data");
            reject_without_mutation(engine, dir, {Operation::Put, "z", ""}, false);
            reject_without_mutation(engine, dir, {Operation::Put, binary_key, "four"}, false);
            reject_without_mutation(engine, dir, {Operation::Put, "n", ""}, true);
            value_is(engine, binary_key, binary_value);
            value_is(engine, "xyz", "");
            absent(engine, "z");
            absent(engine, "n");
            put(engine, binary_key, std::string("q\0r", 3));
            require(engine.stats().data_bytes == 8, "same-size overwrite charged the key twice");

            // Reuse the sole async slot after rejection. Either immediate or
            // deferred success is legal, depending on the WAL commit race.
            std::promise<Response> completed;
            auto result = completed.get_future();
            std::atomic<int> calls{0};
            const auto immediate = engine.execute_async({Operation::Put, binary_key, ""}, [&](Response response) {
                ++calls;
                completed.set_value(std::move(response));
            });
            if (immediate) require(immediate->status == Status::Ok, "async slot leaked after rejection");
            else {
                require(result.wait_for(2s) == std::future_status::ready, "accepted async shrink never completed");
                require(result.get().status == Status::Ok, "accepted async shrink failed");
            }
            engine.drain_async();
            require(calls == (immediate ? 0 : 1) && engine.stats().async_requests_inflight == 0 &&
                    engine.stats().data_bytes == 5, "async shrink accounting or callback ownership changed");
            put(engine, "q", "r");
            require(engine.execute({Operation::Delete, "xyz", {}}).status == Status::Ok, "DELETE rejected at cap");
            require(engine.stats().data_bytes == 4, "DELETE did not free key and value bytes");
            const auto before_miss = engine.stats();
            require(engine.execute({Operation::Delete, "missing", {}}).status == Status::NotFound, "DELETE miss changed result");
            require(engine.stats().data_bytes == 4 && engine.stats().applied_sequence == before_miss.applied_sequence + 1,
                    "DELETE miss changed bytes or omitted its WAL sequence");
            put(engine, "more", "");
            require(engine.stats().data_bytes == 8 && engine.stats().data_rejections_total == 3,
                    "freed capacity was not reusable or accepted writes counted as rejections");
            engine.snapshot();
            engine.close();
        }
        Engine recovered(config);
        require(recovered.stats().data_bytes == 8 && recovered.stats().data_rejections_total == 0,
                "recovery data bytes or process-local rejection count is wrong");
        value_is(recovered, binary_key, "");
        value_is(recovered, "q", "r");
        value_is(recovered, "more", "");
        absent(recovered, "xyz");
    }
}

void unlimited_default() {
    TempDir dir;
    const auto config = config_for(dir);
    require(config.max_data_bytes == 0, "default data cap is no longer unlimited");
    Engine engine(config);
    put(engine, "first", std::string(8192, 'x'));
    put(engine, "second", std::string(16384, 'y'));
    require(engine.stats().data_capacity_bytes == 0 && engine.stats().data_bytes == 24587 &&
            engine.stats().data_rejections_total == 0, "unlimited mode omitted accounting or enforced a cap");
}

void concurrent_last_byte() {
    for (const auto mode : {WalMode::Throughput, WalMode::Reliable}) {
        TempDir dir;
        auto config = config_for(dir, mode);
        config.max_data_bytes = 1;
        Engine engine(config);
        std::promise<void> start;
        auto ready = start.get_future().share();
        std::vector<std::future<Status>> writers;
        for (int index = 0; index < 12; ++index) {
            writers.push_back(std::async(std::launch::async, [&, index] {
                ready.wait();
                return engine.execute({Operation::Put, std::string(1, static_cast<char>('a' + index)), ""}).status;
            }));
        }
        start.set_value();
        int accepted = 0, rejected = 0;
        for (auto& writer : writers) {
            const auto status = writer.get();
            accepted += status == Status::Ok;
            rejected += status == Status::Busy;
        }
        engine.snapshot();
        const auto stats = engine.stats();
        require(accepted == 1 && rejected == 11 && stats.keys == 1 && stats.data_bytes == 1 &&
                stats.applied_sequence == 1 && stats.durable_sequence == 1 && stats.data_rejections_total == 11 &&
                !stats.io_failed, "concurrent PUTs oversubscribed the final byte or logged rejections");
    }
}

struct WalGate {
    std::promise<void> entered, resume;
    std::future<void> waiting = entered.get_future();
    std::shared_future<void> resumed = resume.get_future().share();
    std::atomic<bool> used{false};
    void operator()(const std::string& point) {
        if (point != "wal.sync" || used.exchange(true)) return;
        entered.set_value();
        if (resumed.wait_for(5s) != std::future_status::ready) throw std::runtime_error("WAL gate release timed out");
    }
};

void wal_wait_rechecks_data_capacity() {
    struct Case { bool asynchronous; std::string change; bool unlimited; };
    const std::vector<Case> cases{{false, "other", false}, {true, "other", false},
                                 {true, "shrink", false}, {true, "delete", false},
                                 {true, "shrink", true}, {true, "delete", true}};
    for (const auto mode : {WalMode::Throughput, WalMode::Reliable}) for (const auto& test : cases) {
        TempDir dir;
        auto config = config_for(dir, mode);
        config.wal_queue_bytes = kMaxKeySize + kMaxValueSize + codec::kWalRecordHeader + 4;
        const bool same_key = test.change != "other";
        config.max_data_bytes = test.unlimited ? 0 : kMaxValueSize + (same_key ? 2 : 1);
        config.max_async_requests = 1;
        WalGate gate;
        config.io_hook = [&](const std::string& point) { gate(point); };
        Engine engine(config);
        auto initial = std::async(std::launch::async, [&] {
            return engine.execute({Operation::Put, "a", std::string(kMaxValueSize - 1, 'x')});
        });
        const bool entered = gate.waiting.wait_for(2s) == std::future_status::ready;
        std::atomic<int> calls{0}, released{0};
        std::atomic<bool> transferred{false};
        std::promise<Response> completion;
        auto completed = completion.get_future();
        auto growth = std::async(std::launch::async, [&] {
            const Request request{Operation::Put, "a", std::string(kMaxValueSize, 'y')};
            if (!test.asynchronous) return engine.execute(request);
            const auto response = engine.execute_async(request,
                [&, probe = std::make_shared<CallbackReleaseProbe>(engine, released)](Response result) {
                    (void)probe;
                    ++calls;
                    completion.set_value(std::move(result));
                });
            if (response) return *response;
            transferred = true;
            require(completed.wait_for(2s) == std::future_status::ready, "post-wait async completion stalled");
            return completed.get();
        });
        const bool blocked = wait_for_stats(engine, [&](const EngineStats& stats) {
            return stats.wal_capacity_waiters == 1 && stats.async_requests_inflight ==
                (test.asynchronous && mode == WalMode::Reliable ? 1U : 0U);
        });
        std::future<Response> small_change;
        bool changed = true;
        if (same_key) {
            small_change = std::async(std::launch::async, [&] {
                return engine.execute({test.change == "delete" ? Operation::Delete : Operation::Put, "a", ""});
            });
            changed = wait_for_stats(engine, [&](const EngineStats& stats) {
                return stats.applied_sequence == 2 && stats.data_bytes == (test.change == "delete" ? 0U : 1U);
            });
        }
        // These small records fit the WAL slack while the large overwrite
        // waits. Changing a alone cancels out of the projected total; adding b
        // also makes a cached projection observably wrong after shrink/delete.
        auto filler = std::async(std::launch::async, [&] { return engine.execute({Operation::Put, "b", ""}); });
        const bool filled = wait_for_stats(engine, [&](const EngineStats& stats) {
            const uint64_t expected = same_key ? (test.change == "delete" ? 1 : 2) : kMaxValueSize + 1;
            return stats.data_bytes == expected && stats.applied_sequence == (same_key ? 3U : 2U);
        });
        gate.resume.set_value();
        const auto initial_result = initial.get(), growth_result = growth.get(), filler_result = filler.get();
        if (small_change.valid()) require(small_change.get().status == Status::Ok, "concurrent same-key mutation failed");
        require(entered && blocked && changed && filled, "WAL wait/data-cap interleaving was not exercised: " + test.change);
        require(initial_result.status == Status::Ok && filler_result.status == Status::Ok &&
                growth_result.status == (same_key ? Status::Ok : Status::Busy),
                "PUT did not recheck logical capacity after WAL wait");
        engine.drain_async();
        engine.snapshot();
        const auto stats = engine.stats();
        const uint64_t sequence = same_key ? 4 : 2;
        require(stats.data_bytes == kMaxValueSize + (same_key ? 2U : 1U) && stats.applied_sequence == sequence &&
                stats.durable_sequence == sequence && stats.data_rejections_total == (same_key ? 0U : 1U) &&
                stats.wal_capacity_waiters == 0 && !stats.io_failed && calls == (transferred ? 1 : 0) &&
                released == (test.asynchronous ? 1 : 0) && stats.async_requests_inflight == 0 && (same_key || !transferred),
                "post-WAL-wait accounting or callback ownership is wrong: " + test.change);
        value_is(engine, "a", std::string(same_key ? kMaxValueSize : kMaxValueSize - 1, same_key ? 'y' : 'x'));
        value_is(engine, "b", "");
    }
}

void snapshot_fixture(const TempDir& dir, uint64_t sequence,
                      const std::vector<std::pair<std::string, std::string>>& image = {}) {
    std::string bytes = "MKVSNP01";
    codec::append_u64(bytes, sequence);
    codec::append_u64(bytes, image.size());
    codec::append_u32(bytes, codec::crc32(bytes));
    for (const auto& entry : image) bytes += codec::record(sequence, Operation::Put, entry.first, entry.second);
    write_file(dir.path + "/snapshot.v1", bytes);
}

void final_recovery_dataset() {
    for (const bool checkpoint_large : {false, true}) {
        TempDir dir;
        auto config = config_for(dir);
        {
            Engine original(config);
            put(original, "large", "123456");
            if (checkpoint_large) original.snapshot();
            require(original.execute({Operation::Delete, "large", {}}).status == Status::Ok, "fixture DELETE failed");
            put(original, "z", "");
            original.close();
        }
        config.max_data_bytes = 1;
        Engine recovered(config);
        require(recovered.stats().data_bytes == 1 && recovered.durable_sequence() == 3,
                "recovery enforced the cap on an intermediate dataset");
        value_is(recovered, "z", "");
        absent(recovered, "large");
    }
    TempDir legacy;
    snapshot_fixture(legacy, 0);
    write_file(legacy.path + "/wal.v1", codec::record(1, Operation::Put, "large", "123456") +
               codec::record(2, Operation::Delete, "large", "") + codec::record(3, Operation::Put, "z", ""));
    auto config = config_for(legacy);
    config.max_data_bytes = 1;
    Engine upgraded(config);
    require(upgraded.stats().data_bytes == 1 && upgraded.durable_sequence() == 3 &&
            upgraded.stats().snapshot_successes_total == 1 && read_file(legacy.path + "/wal.v1") == codec::wal_file_header(),
            "legacy replay was limited before its final dataset or failed to upgrade");
    value_is(upgraded, "z", "");
    absent(upgraded, "large");
}

void require_capacity_refusal(const EngineConfig& config) {
    bool refused = false;
    try { Engine rejected(config); }
    catch (const std::exception& error) {
        refused = std::string(error.what()).find("recovered data exceeds") != std::string::npos;
    }
    require(refused, "over-cap recovery did not report a data-capacity refusal");
}

void refusal_preserves_recovery_inputs() {
    for (const std::string variant : {"torn", "covered", "legacy"}) {
        TempDir dir;
        const bool covered = variant == "covered";
        snapshot_fixture(dir, covered ? 1 : 0, covered ?
                         std::vector<std::pair<std::string, std::string>>{{"aa", "bb"}} :
                         std::vector<std::pair<std::string, std::string>>{});
        const auto record = variant == "legacy" ? codec::record(1, Operation::Put, "aa", "bb") :
                                                 codec::wal_record(1, Operation::Put, "aa", "bb");
        std::string wal = (variant == "legacy" ? std::string{} : codec::wal_file_header()) + record;
        const auto repaired = covered ? codec::wal_file_header() : wal;
        if (variant == "torn") wal += codec::wal_record(2, Operation::Put, "incomplete", "ignored").substr(0, 10);
        write_file(dir.path + "/wal.v1", wal);
        const auto snapshot = read_file(dir.path + "/snapshot.v1");
        auto config = config_for(dir);
        config.max_data_bytes = 3;
        require_capacity_refusal(config);
        require(read_file(dir.path + "/snapshot.v1") == snapshot && read_file(dir.path + "/wal.v1") == wal &&
                !fs::exists(dir.path + "/snapshot.v1.tmp") && !fs::exists(dir.path + "/wal.v1.tmp"),
                "capacity refusal repaired, upgraded or replaced original files: " + variant);
        config.max_data_bytes = 4;
        Engine accepted(config);
        value_is(accepted, "aa", "bb");
        require(accepted.stats().data_bytes == 4 && accepted.durable_sequence() == 1 &&
                read_file(dir.path + "/wal.v1") == (variant == "legacy" ? codec::wal_file_header() : repaired),
                "valid recovery could not repair or upgrade after increasing the cap");
    }
}

void legacy_text_final_size_and_refusal() {
    for (const bool shrink : {false, true}) {
        TempDir dir;
        const std::string old_snapshot = "large:123456\n";
        const std::string old_wal = shrink ? "DEL large\nPUT z \n" : "PUT zz \n";
        write_file(dir.path + "/data.db", old_snapshot);
        write_file(dir.path + "/wal.log", old_wal);
        auto config = config_for(dir);
        config.import_legacy = true;
        config.max_data_bytes = 1;
        if (shrink) {
            Engine imported(config);
            require(imported.stats().data_bytes == 1 && imported.durable_sequence() == 2,
                    "legacy text import did not use its final dataset");
            value_is(imported, "z", "");
            absent(imported, "large");
        } else {
            require_capacity_refusal(config);
            require(!fs::exists(dir.path + "/snapshot.v1") && !fs::exists(dir.path + "/wal.v1") &&
                    !fs::exists(dir.path + "/snapshot.v1.tmp") && !fs::exists(dir.path + "/wal.v1.tmp"),
                    "over-cap legacy import installed binary files before refusal");
        }
        require(read_file(dir.path + "/data.db") == old_snapshot && read_file(dir.path + "/wal.log") == old_wal,
                "legacy import changed original text files");
    }
}

} // namespace

int main(int argc, char** argv) {
    // A regression in a blocking path must fail rather than strand the test job.
    ::alarm(40);
    const std::vector<std::pair<std::string, void(*)()>> tests{
        {"logical bytes and rejection", logical_bytes_and_rejections},
        {"unlimited default", unlimited_default},
        {"last byte contention", concurrent_last_byte},
        {"data recheck after WAL wait", wal_wait_rechecks_data_capacity},
        {"final recovery dataset", final_recovery_dataset},
        {"refusal preserves recovery inputs", refusal_preserves_recovery_inputs},
        {"legacy text capacity", legacy_text_final_size_and_refusal},
    };
    try {
        size_t executed = 0;
        for (const auto& test : tests) {
            if (argc > 1 && test.first != argv[1]) continue;
            test.second();
            ++executed;
            std::cout << "PASS " << test.first << std::endl;
        }
        require(executed != 0, "unknown test name");
        return 0;
    } catch (const std::exception& error) {
        std::cerr << "FAIL " << error.what() << '\n';
        return 1;
    }
}
