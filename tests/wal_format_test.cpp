#include "engine.h"
#include "codec.h"

#include <chrono>
#include <filesystem>
#include <fstream>
#include <functional>
#include <iostream>
#include <limits>
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

std::string protected_header(uint64_t sequence, Operation operation, uint32_t key_size, uint32_t value_size) {
    std::string header = "MKL2";
    header.push_back(static_cast<char>(operation));
    codec::append_u64(header, sequence);
    codec::append_u32(header, key_size);
    codec::append_u32(header, value_size);
    codec::append_u32(header, codec::crc32(header));
    return header;
}

void impossible_short_headers_preserve_originals() {
    TempDir dir;
    snapshot_fixture(dir, 0);
    const auto prefix = codec::wal_file_header() + codec::wal_record(1, Operation::Put, "first", "one");
    const auto valid = protected_header(2, Operation::Put, 1, 1);
    std::vector<std::pair<std::string, std::string>> tails;
    for (size_t cut = 1; cut <= 4; ++cut) {
        auto bytes = valid.substr(0, cut);
        bytes.back() ^= 1;
        tails.emplace_back("partial magic byte " + std::to_string(cut), bytes);
    }
    for (const auto op : {0, 2, 4, 255}) {
        auto bytes = valid.substr(0, 5);
        bytes[4] = static_cast<char>(op);
        tails.emplace_back("partial operation " + std::to_string(op), bytes);
    }
    for (const auto& test : std::vector<std::pair<uint32_t, size_t>>{
             {1U << 24, 14}, {1U << 16, 15}, {4352, 16}, {0, 17}, {4097, 17}}) {
        tails.emplace_back("partial key length at " + std::to_string(test.second) + ":" + std::to_string(test.first),
                           protected_header(2, Operation::Put, test.first, 1).substr(0, test.second));
    }
    for (const auto& test : std::vector<std::pair<uint32_t, size_t>>{
             {1U << 24, 18}, {17U << 16, 19}, {(1U << 20) + 256, 20}, {(1U << 20) + 1, 21}}) {
        tails.emplace_back("partial value length at " + std::to_string(test.second),
                           protected_header(2, Operation::Put, 1, test.first).substr(0, test.second));
    }
    for (size_t cut = 18; cut <= 21; ++cut) {
        const uint32_t nonzero = uint32_t{1} << ((21 - cut) * 8);
        tails.emplace_back("partial DELETE value length at " + std::to_string(cut),
                           protected_header(2, Operation::Delete, 1, nonzero).substr(0, cut));
    }
    for (size_t cut = 6; cut <= 13; ++cut) {
        const uint64_t wrong = uint64_t{2} ^ (uint64_t{1} << ((13 - cut) * 8));
        tails.emplace_back("partial sequence at " + std::to_string(cut),
                           protected_header(wrong, Operation::Put, 1, 1).substr(0, cut));
    }
    tails.emplace_back("zero sequence", protected_header(0, Operation::Put, 1, 1).substr(0, 13));
    for (size_t cut = 22; cut < codec::kWalRecordHeader; ++cut) {
        auto bytes = valid.substr(0, cut);
        bytes.back() ^= 1;
        tails.emplace_back("partial header checksum at " + std::to_string(cut), bytes);
    }
    for (const auto& tail : tails) rejected_unchanged(dir, prefix + tail.second, tail.first);

    // Before the first WAL record, snapshot+1 bounds the possible sequence.
    rejected_unchanged(dir, codec::wal_file_header() +
        protected_header(uint64_t{1} << 56, Operation::Put, 1, 1).substr(0, 6), "first sequence high byte");
    rejected_unchanged(dir, codec::wal_file_header() +
        protected_header(0, Operation::Put, 1, 1).substr(0, 13), "first zero sequence");

    const auto maximum = std::numeric_limits<uint64_t>::max();
    snapshot_fixture(dir, maximum);
    const auto exhausted = codec::wal_file_header() + codec::wal_record(maximum, Operation::Put, "old", "covered");
    for (size_t cut : {size_t{1}, size_t{5}, size_t{13}, codec::kWalRecordHeader}) {
        rejected_unchanged(dir, exhausted + valid.substr(0, cut), "tail after exhausted sequence");
    }
}

void protected_sequences_are_checked_before_incomplete_bodies() {
    TempDir dir;
    const auto first = codec::wal_record(1, Operation::Put, "first", "one");
    for (const auto checkpoint : {uint64_t{0}, uint64_t{2}}) {
        snapshot_fixture(dir, checkpoint);
        for (const bool has_prefix : {false, true}) {
            const auto prefix = codec::wal_file_header() + (has_prefix ? first : std::string{});
            const std::vector<uint64_t> sequences = has_prefix ? std::vector<uint64_t>{0, 1, 3}
                : std::vector<uint64_t>{0, checkpoint + 2};
            for (const auto sequence : sequences) {
                const auto tail = codec::wal_record(sequence, Operation::Put, "missing", "body");
                for (const size_t cut : {codec::kWalRecordHeader, codec::kWalRecordHeader + 1, tail.size() - 1}) {
                    rejected_unchanged(dir, prefix + tail.substr(0, cut),
                        "protected invalid sequence " + std::to_string(sequence) + " with incomplete body");
                }
            }
        }
    }
}

void partial_record_checksums_preserve_originals() {
    TempDir dir;
    snapshot_fixture(dir, 0);
    const auto prefix = codec::wal_file_header() + codec::wal_record(1, Operation::Put, "first", "one");
    for (const auto operation : {Operation::Put, Operation::Delete}) {
        const auto record = codec::wal_record(2, operation, "first", operation == Operation::Put ? "two" : "");
        for (size_t available = 1; available < 4; ++available) {
            auto tail = record.substr(0, record.size() - 4 + available);
            tail.back() ^= 1;
            rejected_unchanged(dir, prefix + tail, "partial record checksum byte " + std::to_string(available));
        }
    }
}

void possible_header_prefixes_remain_recoverable() {
    const auto maximum = std::numeric_limits<uint64_t>::max();
    for (const auto& sequences : std::vector<std::pair<uint64_t, uint64_t>>{
             {2, 1}, {2, 2}, {maximum - 1, maximum}, {maximum, maximum}}) {
        const auto checkpoint = sequences.first;
        const auto sequence = sequences.second;
        const auto tail = codec::wal_record(sequence, Operation::Put, "new", "value");
        for (size_t cut = 1; cut < tail.size(); ++cut) {
            TempDir dir;
            // An incomplete first record may be covered by the snapshot, the
            // next sequence after it, or the largest representable sequence.
            snapshot_fixture(dir, checkpoint, {{"stable", "yes"}});
            write_file(dir.path + "/wal.v1", codec::wal_file_header() + tail.substr(0, cut));
            Engine recovered(config_for(dir));
            require(get(recovered, "stable").value == "yes" && recovered.durable_sequence() == checkpoint,
                    "possible incomplete header changed the snapshot");
            require(read_file(dir.path + "/wal.v1") == codec::wal_file_header(),
                    "possible incomplete header was not repaired");
            recovered.close();
        }
    }
    {
        TempDir dir;
        snapshot_fixture(dir, maximum, {{"stable", "yes"}});
        write_file(dir.path + "/wal.v1", codec::wal_file_header() +
                   codec::wal_record(maximum, Operation::Put, "covered", "ignored"));
        Engine recovered(config_for(dir));
        require(recovered.durable_sequence() == maximum && get(recovered, "stable").value == "yes" &&
                get(recovered, "covered").status == Status::NotFound,
                "maximum snapshot rejected or reapplied its covered maximum record");
        recovered.close();
    }
    for (const auto& header : std::vector<std::string>{
             protected_header(1, Operation::Put, kMaxKeySize, kMaxValueSize),
             protected_header(1, Operation::Delete, kMaxKeySize, 0)}) {
        for (size_t cut = 1; cut <= header.size(); ++cut) {
            TempDir dir;
            snapshot_fixture(dir, 0);
            write_file(dir.path + "/wal.v1", codec::wal_file_header() + header.substr(0, cut));
            Engine recovered(config_for(dir));
            require(recovered.durable_sequence() == 0 &&
                    read_file(dir.path + "/wal.v1") == codec::wal_file_header(),
                    "valid maximum-length or DELETE header prefix was rejected");
            recovered.close();
        }
    }
    for (const auto operation : {Operation::Put, Operation::Delete}) {
        const auto first = codec::wal_record(1, Operation::Put, "first", "one");
        const auto record = codec::wal_record(2, operation, "first", operation == Operation::Put ? "two" : "");
        const size_t first_cut = operation == Operation::Delete ? 1 : record.size() - 3;
        for (size_t cut = first_cut; cut < record.size(); ++cut) {
            TempDir dir;
            snapshot_fixture(dir, 0);
            const auto prefix = codec::wal_file_header() + first;
            write_file(dir.path + "/wal.v1", prefix + record.substr(0, cut));
            Engine recovered(config_for(dir));
            require(get(recovered, "first").value == "one" && recovered.durable_sequence() == 1 &&
                    read_file(dir.path + "/wal.v1") == prefix, "possible partial PUT/DELETE record was not repaired");
            recovered.close();
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

void frozen_legacy_files_remain_compatible() {
    // Frozen with the pre-v2 codec at 6407aa7 and independently checked using
    // big-endian packing + IEEE CRC32. Do not regenerate with the current codec.
    // Checkpoint 2: changed=before, deleted=before. WAL 1..2 is covered;
    // 3 adds a NUL-containing value, 4 deletes, and 5 overwrites changed.
    static constexpr char snapshot[] =
        "\x4d\x4b\x56\x53\x4e\x50\x30\x31\x00\x00\x00\x00\x00\x00\x00\x02"
        "\x00\x00\x00\x00\x00\x00\x00\x02\x66\xf5\x8b\x95\x4d\x4b\x4c\x31"
        "\x01\x00\x00\x00\x00\x00\x00\x00\x02\x00\x00\x00\x07\x00\x00\x00"
        "\x06\x63\x68\x61\x6e\x67\x65\x64\x62\x65\x66\x6f\x72\x65\x06\x55"
        "\x22\x0b\x4d\x4b\x4c\x31\x01\x00\x00\x00\x00\x00\x00\x00\x02\x00"
        "\x00\x00\x07\x00\x00\x00\x06\x64\x65\x6c\x65\x74\x65\x64\x62\x65"
        "\x66\x6f\x72\x65\x43\xb0\xed\xf9";
    static constexpr char wal[] =
        "\x4d\x4b\x4c\x31\x01\x00\x00\x00\x00\x00\x00\x00\x01\x00\x00\x00"
        "\x07\x00\x00\x00\x03\x63\x6f\x76\x65\x72\x65\x64\x6f\x6c\x64\x31"
        "\x40\x83\xf3\x4d\x4b\x4c\x31\x03\x00\x00\x00\x00\x00\x00\x00\x02"
        "\x00\x00\x00\x07\x00\x00\x00\x00\x63\x6f\x76\x65\x72\x65\x64\x55"
        "\x32\xbe\x79\x4d\x4b\x4c\x31\x01\x00\x00\x00\x00\x00\x00\x00\x03"
        "\x00\x00\x00\x05\x00\x00\x00\x03\x61\x64\x64\x65\x64\x61\x00\x62"
        "\x6b\xf5\x8e\x64\x4d\x4b\x4c\x31\x03\x00\x00\x00\x00\x00\x00\x00"
        "\x04\x00\x00\x00\x07\x00\x00\x00\x00\x64\x65\x6c\x65\x74\x65\x64"
        "\x4c\x42\x39\xe4\x4d\x4b\x4c\x31\x01\x00\x00\x00\x00\x00\x00\x00"
        "\x05\x00\x00\x00\x07\x00\x00\x00\x05\x63\x68\x61\x6e\x67\x65\x64"
        "\x61\x66\x74\x65\x72\x88\x88\xc4\x3a";
    // The first 35-byte record alone is fully checkpointed: replaying it would
    // resurrect covered, unlike replaying the complete PUT/DELETE prefix.
    for (const size_t wal_size : {size_t{0}, size_t{35}, sizeof(wal) - 1}) {
        const bool replay = wal_size == sizeof(wal) - 1;
        TempDir dir;
        write_file(dir.path + "/snapshot.v1", std::string(snapshot, sizeof(snapshot) - 1));
        write_file(dir.path + "/wal.v1", std::string(wal, wal_size));
        const auto config = config_for(dir);
        const uint64_t sequence = replay ? 5 : 2;
        const auto verify = [replay](Engine& engine) {
            require(get(engine, "changed").value == (replay ? "after" : "before") &&
                    get(engine, "covered").status == Status::NotFound, "frozen checkpoint replay changed values");
            if (replay) {
                require(get(engine, "deleted").status == Status::NotFound &&
                        get(engine, "added").value == std::string("a\0b", 3), "frozen DELETE or binary PUT lost");
            } else {
                require(get(engine, "deleted").value == "before" &&
                        get(engine, "added").status == Status::NotFound, "checkpoint-only legacy WAL changed data");
            }
        };
        {
            Engine upgraded(config);
            verify(upgraded);
            require(upgraded.durable_sequence() == sequence && upgraded.stats().snapshot_successes_total == 1 &&
                    read_file(dir.path + "/wal.v1") == codec::wal_file_header(), "frozen legacy upgrade incomplete");
            put(upgraded, "next", std::string("v\0two", 5));
            upgraded.close();
        }
        Engine reopened(config);
        verify(reopened);
        require(get(reopened, "next").value == std::string("v\0two", 5) &&
                reopened.durable_sequence() == sequence + 1 && reopened.stats().snapshot_successes_total == 0,
                "frozen legacy upgrade did not preserve subsequent writes across restart");
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
        {"impossible short headers", impossible_short_headers_preserve_originals},
        {"protected incomplete sequences", protected_sequences_are_checked_before_incomplete_bodies},
        {"partial record checksums", partial_record_checksums_preserve_originals},
        {"possible header prefixes", possible_header_prefixes_remain_recoverable},
        {"legacy ambiguous tails", legacy_ambiguous_tails_preserve_originals},
        {"legacy upgrade", legacy_upgrade_is_checkpointed_before_use},
        {"frozen legacy compatibility", frozen_legacy_files_remain_compatible},
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
