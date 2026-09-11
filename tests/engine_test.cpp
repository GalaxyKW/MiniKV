#include "engine.h"
#include "codec.h"

#include <atomic>
#include <csignal>
#include <filesystem>
#include <fstream>
#include <future>
#include <iostream>
#include <stdexcept>
#include <sys/resource.h>
#include <sys/wait.h>
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
            const rlimit limit{10, 10};
            if (::setrlimit(RLIMIT_FSIZE, &limit) != 0) ::_exit(90);
            const auto result = engine.execute({Operation::Put, "key", "value"});
            if (result.status != Status::IOError || engine.durable_sequence() != 0) ::_exit(91);
            ::_exit(0);
        } catch (...) { ::_exit(92); }
    }
    int status = 0;
    require(::waitpid(child, &status, 0) == child && WIFEXITED(status) && WEXITSTATUS(status) == 0, "partial WAL write was not rejected");
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
            armed = true;
            must_fail([&] { engine.snapshot(); }, "snapshot failure was ignored");
            require(fs::file_size(dir.path + "/wal.v1") > 0, "failed checkpoint discarded WAL");
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
    const size_t first_size = codec::record(1, Operation::Put, "first", "one").size();
    const size_t second_size = codec::record(2, Operation::Put, "second", "two").size();
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
    corrupt[codec::kRecordHeader + 1] ^= 0x10;
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
    require(fs::file_size(dir.path + "/wal.v1") == 0, "checkpoint did not empty WAL");
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
        const size_t first = codec::record(1, Operation::Put, "first", "one").size();
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
