#include "engine.h"
#include "codec.h"

#include <chrono>
#include <filesystem>
#include <fstream>
#include <functional>
#include <iostream>
#include <stdexcept>
#include <string>
#include <sys/wait.h>
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
        std::string pattern = (fs::temp_directory_path() / "minikv-wal-format-XXXXXX").string();
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
    config.wal_flush_interval = 1ms;
    config.snapshot_interval = 0ms;
    return config;
}

std::string read_file(const std::string& path) {
    std::ifstream file(path, std::ios::binary);
    require(file.good(), "cannot read fixture");
    return {std::istreambuf_iterator<char>(file), std::istreambuf_iterator<char>()};
}

void write_file(const std::string& path, const std::string& bytes) {
    std::ofstream file(path, std::ios::binary | std::ios::trunc);
    file.write(bytes.data(), static_cast<std::streamsize>(bytes.size()));
    require(file.good(), "cannot write fixture");
}

void put(Engine& engine, const std::string& key, const std::string& value) {
    require(engine.execute({Operation::Put, key, value}).status == Status::Ok, "PUT failed");
}

Response get(Engine& engine, const std::string& key) {
    return engine.execute({Operation::Get, key, {}});
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

void rejected_unchanged(const TempDir& dir, const std::string& wal, const std::string& label) {
    write_file(dir.path + "/wal.v1", wal);
    const auto snapshot = read_file(dir.path + "/snapshot.v1");
    bool rejected = false;
    try { Engine engine(config_for(dir)); }
    catch (const std::exception&) { rejected = true; }
    require(rejected, "accepted " + label);
    require(read_file(dir.path + "/wal.v1") == wal && read_file(dir.path + "/snapshot.v1") == snapshot,
            "rejected recovery changed original files: " + label);
}

void header_corruption_never_repairs_records() {
    TempDir dir;
    { Engine engine(config_for(dir)); put(engine, "first", "one"); put(engine, "second", "two"); engine.close(); }
    const auto wal = read_file(dir.path + "/wal.v1");
    const size_t second = codec::kWalFileHeader + codec::wal_record(1, Operation::Put, "first", "one").size();
    // Exercise every bit in both an interior and final record header. In
    // particular, length corruption must fail before EOF can trigger repair.
    for (const auto start : {codec::kWalFileHeader, second}) {
        for (size_t offset = 0; offset < codec::kWalRecordHeader; ++offset) {
            for (unsigned bit = 0; bit < 8; ++bit) {
                auto corrupt = wal;
                corrupt[start + offset] ^= static_cast<char>(1U << bit);
                rejected_unchanged(dir, corrupt, "record header bit corruption");
            }
        }
    }
    for (size_t offset = 0; offset < codec::kWalFileHeader; ++offset) {
        for (unsigned bit = 0; bit < 8; ++bit) {
            auto corrupt = wal;
            corrupt[offset] ^= static_cast<char>(1U << bit);
            rejected_unchanged(dir, corrupt, "file header bit corruption");
        }
    }
    for (size_t cut = 1; cut < codec::kWalFileHeader; ++cut) {
        rejected_unchanged(dir, wal.substr(0, cut), "truncated file header");
    }
    // A valid CRC cannot silently opt into undefined flags/reserved semantics.
    for (size_t offset : {size_t{8}, size_t{12}}) {
        auto unsupported = wal;
        unsupported[offset] = 1;
        std::string header = unsupported.substr(0, 20);
        codec::append_u32(header, codec::crc32(header));
        unsupported.replace(0, codec::kWalFileHeader, header);
        rejected_unchanged(dir, unsupported, "unsupported WAL header fields");
    }
}

void every_v2_record_tail_boundary_recovers() {
    const auto first = codec::wal_record(1, Operation::Put, "first", "one");
    const auto last = codec::wal_record(2, Operation::Put, "second", "two");
    for (const bool has_prefix : {false, true}) {
        const auto& torn = has_prefix ? last : first;
        const auto prefix = codec::wal_file_header() + (has_prefix ? first : std::string{});
        for (size_t cut = 0; cut < torn.size(); ++cut) {
            TempDir dir;
            snapshot_fixture(dir, 0);
            write_file(dir.path + "/wal.v1", prefix + torn.substr(0, cut));
            auto config = config_for(dir);
            {
                Engine engine(config);
                require(engine.durable_sequence() == (has_prefix ? 1U : 0U), "torn record advanced sequence");
                require(read_file(dir.path + "/wal.v1") == prefix, "tail repair discarded header or valid prefix");
                require(get(engine, has_prefix ? "second" : "first").status == Status::NotFound,
                        "torn record became visible");
                put(engine, "after", "repair");
                engine.close();
            }
            Engine again(config);
            require(get(again, "after").value == "repair", "append after repair failed to recover");
            if (has_prefix) require(get(again, "first").value == "one", "repair lost acknowledged prefix");
        }
    }
}

void legacy_ambiguous_tails_preserve_originals() {
    TempDir dir;
    snapshot_fixture(dir, 0);
    const auto first = codec::record(1, Operation::Put, "first", "one");
    const auto second = codec::record(2, Operation::Put, "second", "two");
    auto corrupt = first + second;
    corrupt[20] ^= 0x40; // 3 -> 67: old recovery silently erased both complete records.
    rejected_unchanged(dir, corrupt, "legacy corrupt length masquerading as torn body");
    for (size_t cut = 1; cut < first.size(); ++cut) {
        rejected_unchanged(dir, first.substr(0, cut), "legacy incomplete first record");
    }
    for (size_t cut = 1; cut < second.size(); ++cut) {
        rejected_unchanged(dir, first + second.substr(0, cut), "legacy incomplete suffix");
    }
}

void legacy_fixture(const TempDir& dir) {
    snapshot_fixture(dir, 2, {{"changed", "before"}, {"deleted", "before"}});
    write_file(dir.path + "/wal.v1",
               codec::record(1, Operation::Put, "covered", "old") +
               codec::record(2, Operation::Delete, "covered", "") +
               codec::record(3, Operation::Put, "added", std::string("a\0b", 3)) +
               codec::record(4, Operation::Delete, "deleted", "") +
               codec::record(5, Operation::Put, "changed", "after"));
}

void verify_migrated(Engine& engine) {
    require(get(engine, "changed").value == "after" && get(engine, "added").value == std::string("a\0b", 3) &&
            get(engine, "deleted").status == Status::NotFound && get(engine, "covered").status == Status::NotFound &&
            engine.durable_sequence() == 5, "legacy migration lost or replayed checkpointed data");
}

void legacy_upgrade_is_checkpointed_before_use() {
    TempDir dir;
    legacy_fixture(dir);
    auto config = config_for(dir);
    {
        Engine engine(config);
        verify_migrated(engine);
        require(read_file(dir.path + "/wal.v1") == codec::wal_file_header(), "legacy WAL was not fully upgraded");
        const auto snapshot = read_file(dir.path + "/snapshot.v1");
        require(snapshot.substr(0, 8) == "MKVSNP01" && codec::u64(snapshot, 8) == 5,
                "upgrade changed snapshot format or failed to checkpoint replay");
        put(engine, "new", "v2");
        engine.close();
    }
    Engine again(config);
    require(get(again, "new").value == "v2" && again.stats().snapshot_successes_total == 0,
            "v2 reopening repeated migration or lost new writes");

    for (const bool covered : {false, true}) {
        TempDir empty;
        snapshot_fixture(empty, 4, {{"stable", "yes"}});
        write_file(empty.path + "/wal.v1", covered ? codec::record(1, Operation::Put, "old", "ignored") : "");
        Engine migrated(config_for(empty));
        require(get(migrated, "stable").value == "yes" && migrated.durable_sequence() == 4 &&
                read_file(empty.path + "/wal.v1") == codec::wal_file_header(),
                "empty or fully checkpointed legacy WAL failed to upgrade");
    }
}

void interrupted_upgrades_remain_recoverable() {
    for (const std::string point : {"snapshot.write", "snapshot.sync", "snapshot.rename", "snapshot.dir_sync",
                                   "snapshot.after_install", "wal.truncate", "wal.compact.write", "wal.compact.sync",
                                   "wal.compact.rename", "wal.compact.after_replace", "wal.compact.dir_sync"}) {
        for (const bool crash : {false, true}) {
            TempDir dir;
            legacy_fixture(dir);
            auto config = config_for(dir);
            config.io_hook = [point, crash](const std::string& current) {
                if (current != point) return;
                if (crash) ::_exit(42);
                throw std::runtime_error("injected migration failure");
            };
            if (crash) {
                const auto child = ::fork();
                require(child >= 0, "fork failed");
                if (child == 0) {
                    try { Engine engine(config); } catch (...) { ::_exit(43); }
                    ::_exit(44);
                }
                int status = 0;
                require(::waitpid(child, &status, 0) == child && WIFEXITED(status) && WEXITSTATUS(status) == 42,
                        "upgrade did not hit crash boundary: " + point);
            } else {
                bool failed = false;
                try { Engine engine(config); } catch (const std::exception&) { failed = true; }
                require(failed, "upgrade ignored injected failure: " + point);
            }
            config.io_hook = {};
            {
                Engine recovered(config);
                verify_migrated(recovered);
                require(read_file(dir.path + "/wal.v1") == codec::wal_file_header(), "restart did not finish upgrade");
                put(recovered, "continued", point);
                recovered.close();
            }
            Engine again(config);
            require(get(again, "continued").value == point, "post-upgrade write did not recover");
        }
    }
}

void header_only_wal_cannot_be_a_legacy_tail() {
    TempDir dir;
    { Engine engine(config_for(dir)); engine.close(); }
    const auto wal = read_file(dir.path + "/wal.v1");
    require(wal == codec::wal_file_header() && wal.size() >= codec::kRecordHeader,
            "empty v2 WAL is too short for legacy rejection");
    // This is the old reader's first full-header operation, before its tail
    // repair branch. A real old executable is also exercised in interop checks.
    bool rejected = false;
    try { codec::record_size(std::string_view(wal).substr(0, codec::kRecordHeader)); }
    catch (const std::exception&) { rejected = true; }
    require(rejected, "legacy decoder would accept or truncate a v2 header-only WAL");
}

} // namespace

int main(int argc, char** argv) {
    const std::vector<std::pair<std::string, std::function<void()>>> tests{
        {"protected headers", header_corruption_never_repairs_records},
        {"v2 torn tails", every_v2_record_tail_boundary_recovers},
        {"legacy ambiguous tails", legacy_ambiguous_tails_preserve_originals},
        {"legacy upgrade", legacy_upgrade_is_checkpointed_before_use},
        {"upgrade crash boundaries", interrupted_upgrades_remain_recoverable},
        {"legacy downgrade rejection", header_only_wal_cannot_be_a_legacy_tail},
    };
    try {
        for (const auto& test : tests) {
            if (argc > 1 && test.first.find(argv[1]) == std::string::npos) continue;
            test.second();
            std::cout << "PASS " << test.first << '\n';
        }
        return 0;
    } catch (const std::exception& error) {
        std::cerr << "FAIL " << error.what() << '\n';
        return 1;
    }
}
