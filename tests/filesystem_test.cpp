#include "engine.h"
#include "codec.h"

#include <algorithm>
#include <array>
#include <atomic>
#include <cerrno>
#include <chrono>
#include <csignal>
#include <filesystem>
#include <fstream>
#include <functional>
#include <iostream>
#include <locale>
#include <stdexcept>
#include <sys/stat.h>
#include <sys/wait.h>
#include <thread>
#include <unistd.h>
#include <vector>

using namespace minikv;
namespace fs = std::filesystem;
using namespace std::chrono_literals;

namespace {

std::atomic<bool> legacy_read_probe_enabled{false};
std::atomic<dev_t> legacy_read_probe_device{0};
std::atomic<ino_t> legacy_read_probe_inode{0};
std::atomic<size_t> legacy_read_probe_bytes{0};

} // namespace

// Linked only into this test binary. Count actual reads of one legacy source
// inode so a size-limit test can distinguish early rejection from reading and
// allocating the entire file first, without imposing an ASan-incompatible VM cap.
extern "C" ssize_t __real_read(int fd, void* buffer, size_t count);
extern "C" ssize_t __wrap_read(int fd, void* buffer, size_t count) {
    const auto result = __real_read(fd, buffer, count);
    if (result > 0 && legacy_read_probe_enabled.load()) {
        const int saved_errno = errno;
        struct stat identity{};
        if (::fstat(fd, &identity) == 0 && identity.st_dev == legacy_read_probe_device.load() &&
            identity.st_ino == legacy_read_probe_inode.load()) {
            legacy_read_probe_bytes.fetch_add(static_cast<size_t>(result));
        }
        errno = saved_errno;
    }
    return result;
}

namespace {

void require(bool condition, const std::string& message) {
    if (!condition) throw std::runtime_error(message);
}

struct TempDir {
    fs::path path;
    TempDir() {
        std::string pattern = (fs::temp_directory_path() / "minikv-filesystem-XXXXXX").string();
        const auto* created = ::mkdtemp(pattern.data());
        if (!created) throw std::runtime_error("mkdtemp failed");
        path = created;
    }
    ~TempDir() { std::error_code error; fs::remove_all(path, error); }
};

EngineConfig config_for(const fs::path& path) {
    EngineConfig config;
    config.data_dir = path.string();
    config.wal_mode = WalMode::Reliable;
    config.wal_flush_interval = 1ms;
    config.snapshot_interval = 0ms;
    return config;
}

std::string read_file(const fs::path& path) {
    std::ifstream file(path, std::ios::binary);
    require(file.good(), "cannot read test file");
    return {std::istreambuf_iterator<char>(file), std::istreambuf_iterator<char>()};
}

void write_file(const fs::path& path, const std::string& bytes) {
    std::ofstream file(path, std::ios::binary | std::ios::trunc);
    file.write(bytes.data(), static_cast<std::streamsize>(bytes.size()));
    require(file.good(), "cannot write test file");
}

void put(Engine& engine, const std::string& key, const std::string& value) {
    require(engine.execute({Operation::Put, key, value}).status == Status::Ok, "PUT was not acknowledged");
}

void value_is(Engine& engine, const std::string& key, const std::string& value) {
    const auto response = engine.execute({Operation::Get, key, {}});
    require(response.status == Status::Value && response.value == value, "recovered value differs: " + key);
}

template <class Function> void must_fail(Function function, const std::string& label) {
    bool failed = false;
    try { function(); } catch (const std::exception&) { failed = true; }
    require(failed, label);
}

// A wrong file type must fail before blocking in open/read. Isolate the check so
// a regression involving a FIFO cannot hang the whole test binary.
void open_must_fail_promptly(const EngineConfig& config, const std::string& label) {
    const auto child = ::fork();
    require(child >= 0, "fork failed");
    if (child == 0) {
        try { Engine engine(config); engine.close(); } catch (const std::exception&) { ::_exit(42); }
        ::_exit(0);
    }
    int status = 0;
    const auto deadline = std::chrono::steady_clock::now() + 2s;
    while (std::chrono::steady_clock::now() < deadline) {
        const auto result = ::waitpid(child, &status, WNOHANG);
        if (result == child) {
            require(WIFEXITED(status) && WEXITSTATUS(status) == 42, "accepted unexpected entry: " + label);
            return;
        }
        std::this_thread::sleep_for(2ms);
    }
    ::kill(child, SIGKILL);
    ::waitpid(child, &status, 0);
    throw std::runtime_error("opening filesystem entry blocked: " + label);
}

void installed_entries_must_be_regular_files() {
    for (const std::string name : {"LOCK", "snapshot.v1", "wal.v1", "data.db", "wal.log"}) {
        for (const std::string kind : {"symlink", "dangling", "fifo", "directory"}) {
            TempDir temporary;
            const auto data = temporary.path / "data";
            auto config = config_for(data);
            const bool legacy = name == "data.db" || name == "wal.log";
            if (legacy) {
                fs::create_directory(data);
                config.import_legacy = true;
            } else {
                Engine initialized(config);
                put(initialized, "preserved", "value");
                initialized.close();
            }
            const auto entry = data / name;
            const auto target = temporary.path / "target";
            if (fs::exists(entry)) fs::rename(entry, target);
            else write_file(target, name == "data.db" ? "legacy:value\n" : "PUT legacy value\n");
            const auto original = read_file(target);
            if (kind == "symlink") fs::create_symlink(target, entry);
            else if (kind == "dangling") fs::create_symlink(temporary.path / "absent", entry);
            else if (kind == "fifo") require(::mkfifo(entry.c_str(), 0600) == 0, "mkfifo failed");
            else fs::create_directory(entry);
            open_must_fail_promptly(config, name + "/" + kind);
            require(read_file(target) == original, "bad entry changed its target");
            require(!fs::exists(temporary.path / "absent"), "dangling link target was created");
            if (kind == "symlink" || kind == "dangling") require(fs::is_symlink(entry), "bad link was replaced");
        }
    }
}

void mutable_files_must_not_share_inodes() {
    for (const std::string name : {"LOCK", "wal.v1"}) {
        TempDir temporary;
        const auto data = temporary.path / "data";
        const auto config = config_for(data);
        { Engine initialized(config); put(initialized, "preserved", "value"); initialized.close(); }
        const auto original = read_file(data / name);
        fs::create_hard_link(data / name, temporary.path / "alias");
        open_must_fail_promptly(config, name + "/hardlink");
        require(read_file(data / name) == original && read_file(temporary.path / "alias") == original,
                "shared writable inode was modified");
    }
}

void read_only_hard_links_remain_unchanged() {
    TempDir temporary;
    const auto data = temporary.path / "data";
    auto config = config_for(data);
    fs::create_directory(data);
    const std::string legacy_snapshot = "original:before\n";
    const std::string legacy_wal = "PUT imported value\n";
    write_file(temporary.path / "data.backup", legacy_snapshot);
    write_file(temporary.path / "wal.backup", legacy_wal);
    fs::create_hard_link(temporary.path / "data.backup", data / "data.db");
    fs::create_hard_link(temporary.path / "wal.backup", data / "wal.log");
    config.import_legacy = true;
    { Engine imported(config); value_is(imported, "imported", "value"); imported.close(); }
    config.import_legacy = false;
    fs::create_hard_link(data / "snapshot.v1", temporary.path / "snapshot.backup");
    const auto snapshot_before = read_file(temporary.path / "snapshot.backup");
    {
        Engine opened(config);
        value_is(opened, "original", "before");
        put(opened, "original", "after");
        opened.snapshot();
    }
    require(read_file(temporary.path / "snapshot.backup") == snapshot_before,
            "snapshot replacement changed a read-only hard link");
    require(read_file(data / "data.db") == legacy_snapshot && read_file(data / "wal.log") == legacy_wal,
            "import changed a read-only legacy file");
    Engine recovered(config);
    value_is(recovered, "original", "after");
    value_is(recovered, "imported", "value");
}

void legacy_lines_preserve_bytes_and_require_terminators() {
    for (const size_t size : {8187u, 8188u, 8192u, 16384u}) {
        TempDir temporary;
        std::string value(size, 'x');
        value[size / 2] = '\0';
        value.back() = '\r';
        write_file(temporary.path / "data.db", "key:" + value + "\nempty:\n");
        write_file(temporary.path / "wal.log", "PUT log " + value + "\nDEL empty\n");
        auto config = config_for(temporary.path);
        config.import_legacy = true;
        { Engine imported(config); value_is(imported, "key", value); value_is(imported, "log", value); }
        config.import_legacy = false;
        Engine recovered(config);
        value_is(recovered, "key", value);
        value_is(recovered, "log", value);
        require(recovered.execute({Operation::Get, "empty", {}}).status == Status::NotFound,
                "legacy record following buffer boundary was lost");
    }
    for (const std::string name : {"data.db", "wal.log"}) {
        for (const bool blank_line : {false, true}) {
            TempDir temporary;
            const std::string record = name == "data.db" ? "key:value" : "PUT key value";
            const auto bytes = blank_line ? record + "\n\n" : record;
            write_file(temporary.path / name, bytes);
            auto config = config_for(temporary.path);
            config.import_legacy = true;
            must_fail([&] { Engine imported(config); }, "invalid legacy line was accepted");
            require(read_file(temporary.path / name) == bytes, "invalid import modified legacy input");
            require(!fs::exists(temporary.path / "wal.v1") && !fs::exists(temporary.path / "snapshot.v1"),
                    "invalid import installed new storage files");
        }
    }
}

void check_legacy_import(const std::string& snapshot, const std::string& wal,
                         const std::vector<std::pair<std::string, std::string>>& values,
                         const std::vector<std::string>& missing = {}) {
    TempDir temporary;
    write_file(temporary.path / "data.db", snapshot);
    write_file(temporary.path / "wal.log", wal);
    auto config = config_for(temporary.path);
    config.import_legacy = true;
    for (const bool importing : {true, false}) {
        config.import_legacy = importing;
        Engine engine(config);
        for (const auto& entry : values) value_is(engine, entry.first, entry.second);
        for (const auto& key : missing) {
            require(engine.execute({Operation::Get, key, {}}).status == Status::NotFound,
                    "legacy deletion did not survive import/restart: " + key);
        }
        engine.close();
        require(read_file(temporary.path / "data.db") == snapshot &&
                    read_file(temporary.path / "wal.log") == wal,
                "legacy import/restart changed source files");
    }
}

class LegacyReadProbe {
    bool active_;
public:
    LegacyReadProbe(const fs::path& source, bool active) : active_(active) {
        if (!active_) return;
        require(!legacy_read_probe_enabled.load(), "nested legacy read probe");
        struct stat identity{};
        require(::stat(source.c_str(), &identity) == 0, "cannot identify legacy read probe source");
        legacy_read_probe_device.store(identity.st_dev);
        legacy_read_probe_inode.store(identity.st_ino);
        legacy_read_probe_bytes.store(0);
        legacy_read_probe_enabled.store(true);
    }
    ~LegacyReadProbe() { if (active_) legacy_read_probe_enabled.store(false); }
    size_t finish() {
        if (!active_) return 0;
        legacy_read_probe_enabled.store(false);
        active_ = false;
        return legacy_read_probe_bytes.load();
    }
};

void check_legacy_rejection(const std::string& name, const std::string& record,
                            const std::string& oversized_field = {}, size_t max_read_bytes = 0) {
    TempDir temporary;
    const auto snapshot = std::string("before:snapshot\n") + (name == "data.db" ? record : "");
    const auto wal = std::string("PUT before wal\n") + (name == "wal.log" ? record : "");
    write_file(temporary.path / "data.db", snapshot);
    write_file(temporary.path / "wal.log", wal);
    auto config = config_for(temporary.path);
    config.import_legacy = true;
    std::string failure;
    // Import errors occur before background workers start. Stop observing
    // before the source-integrity reads below, and restore the probe on throws.
    LegacyReadProbe probe(temporary.path / name, max_read_bytes != 0);
    try { Engine imported(config); } catch (const std::exception& error) { failure = error.what(); }
    const auto read_bytes = probe.finish();
    require(!failure.empty(), "invalid legacy record was accepted: " + name);
    if (max_read_bytes != 0) {
        require(read_bytes > 0, "legacy read probe did not observe the source");
        require(read_bytes <= max_read_bytes, "legacy size limit consumed too much input: " + std::to_string(read_bytes));
    }
    if (!oversized_field.empty()) {
        // In particular, an oversized unterminated field must fail on its size;
        // reaching EOF and reporting only a missing LF permits unbounded reads.
        require(failure.find("legacy " + oversized_field + " exceeds limit") != std::string::npos,
                "oversized legacy field did not report its limit: " + failure);
    }
    require(read_file(temporary.path / "data.db") == snapshot &&
                read_file(temporary.path / "wal.log") == wal,
            "failed legacy import changed source files");
    for (const auto* installed : {"snapshot.v1", "wal.v1", "snapshot.v1.tmp", "wal.v1.tmp"}) {
        require(!fs::exists(temporary.path / installed), "failed legacy import created binary storage");
    }
}

void legacy_fields_have_exact_limits() {
    std::string snapshot_key(kMaxKeySize, 's');
    snapshot_key[snapshot_key.size() / 2] = '\0';
    snapshot_key.back() = '\r';
    std::string wal_key(kMaxKeySize, 'w');
    wal_key[wal_key.size() / 2] = '\0';
    std::string value(kMaxValueSize, 'v');
    value[8191] = '\0';
    value.back() = '\r';
    check_legacy_import(snapshot_key + ":" + value + "\ns:\n",
                        "PUT " + wal_key + " " + value + "\nPUT w\n",
                        {{snapshot_key, value}, {wal_key, value}, {"s", ""}, {"w", ""}});

    for (const std::string terminator : {"", "\n"}) {
        check_legacy_rejection("data.db", std::string(kMaxKeySize + 1, 'k') + ":v" + terminator, "key");
        check_legacy_rejection("data.db", std::string(kMaxKeySize + 1, 'k') + terminator, "key");
        check_legacy_rejection("data.db", "key:" + std::string(kMaxValueSize + 1, 'v') + terminator, "value");
        check_legacy_rejection("wal.log", "PUTX key value" + terminator, "operation");
        check_legacy_rejection("wal.log", "PUTX" + terminator, "operation");
        check_legacy_rejection("wal.log", "PUT " + std::string(kMaxKeySize + 1, 'k') + " v" + terminator, "key");
        check_legacy_rejection("wal.log", "PUT " + std::string(kMaxKeySize + 1, 'k') + terminator, "key");
        check_legacy_rejection("wal.log", "PUT key " + std::string(kMaxValueSize + 1, 'v') + terminator, "value");
    }
}

void legacy_token_padding_is_not_a_record_limit() {
    // These discarded runs each exceed the largest valid ordinary record. A
    // whole-line length cap would reject an import accepted by the old format.
    const std::string padding(2 * kMaxValueSize, ' ');
    check_legacy_import("existing:before\n",
                        padding + "\t\v\f\rPUT" + padding + "\t\v\f\rkey value\nDEL existing \n",
                        {{"key", "value"}}, {"existing"});
    // The reader must track consumed bytes even when no token bytes are kept.
    check_legacy_rejection("wal.log", padding + "\t\v\f\r");
    check_legacy_rejection("wal.log", padding + "\t\v\f\r\n");
}

void legacy_oversized_values_stop_reading_early() {
    const std::string oversized(4 * kMaxValueSize, 'v');
    for (const std::string name : {"data.db", "wal.log"}) {
        for (const std::string terminator : {"", "\n"}) {
            const std::string prefix = name == "data.db" ? "key:" : "PUT key ";
            check_legacy_rejection(name, prefix + oversized + terminator, "value",
                                   kMaxKeySize + kMaxValueSize + 8192);
        }
    }
}

void legacy_delimiters_preserve_the_original_format() {
    const std::string snapshot_key = std::string(" \ts") + '\0' + '\r';
    const std::string snapshot_value = std::string(" :\t") + '\0' + '\r';
    const std::string wal_key = std::string("wal:") + '\0' + "key";
    const std::string wal_value = std::string("\t :") + '\0' + '\r';
    check_legacy_import(snapshot_key + ":" + snapshot_value + "\ndelete:before\ndelete-space:before\n",
                        " \t\v\f\rPUT\t\v\f\r" + wal_key + wal_value +
                            "\nPUT no-value\nPUT one-space \nPUT two-spaces  \nPUT cr\r\n"
                            "DEL delete\nDEL delete-space \n",
                        {{snapshot_key, snapshot_value}, {wal_key, wal_value}, {"no-value", ""},
                         {"one-space", ""}, {"two-spaces", " "}, {"cr", "\r"}},
                        {"delete", "delete-space"});
    for (const std::string record : {":value\n", "no-colon\n", "\n", "key:value", " \t\r"}) {
        check_legacy_rejection("data.db", record);
    }
    for (const std::string record : {"GET key\n", "PUT\n", "PUT \t\r\n", "DEL key  \n",
                                     "DEL key\t\n", "DEL key\r\n", "PUT key value", "\n"}) {
        check_legacy_rejection("wal.log", record);
    }
    check_legacy_rejection("wal.log", std::string("DEL key ") + '\0' + '\n');
}

class LegacyWhitespace : public std::ctype<char> {
    static const std::array<mask, table_size>& table() {
        static const auto masks = [] {
            std::array<mask, table_size> result{};
            std::copy_n(classic_table(), table_size, result.begin());
            result[static_cast<unsigned char>('~')] |= space;
            return result;
        }();
        return masks;
    }
public:
    LegacyWhitespace() : std::ctype<char>(table().data()) {}
};

struct RestoreLocale {
    std::locale previous = std::locale();
    ~RestoreLocale() { std::locale::global(previous); }
};

void legacy_wal_uses_cpp_locale_for_token_whitespace() {
    RestoreLocale restore;
    std::locale::global(std::locale(restore.previous, new LegacyWhitespace));
    check_legacy_import("~snapshot:~value\n", "~~PUT~~~key~value\n",
                        {{"~snapshot", "~value"}, {"key", "~value"}});
}

class LegacyWidenedNewline : public std::ctype<char> {
    char delimiter_;
protected:
    char do_widen(char value) const override { return value == '\n' ? delimiter_ : value; }
    const char* do_widen(const char* first, const char* last, char* output) const override {
        for (auto current = first; current != last; ++current) *output++ = do_widen(*current);
        return last;
    }
public:
    explicit LegacyWidenedNewline(char delimiter) : delimiter_(delimiter) {}
};

void legacy_wal_preserves_widened_getline_delimiters() {
    // The original getline used input.widen('\n') after the physical LF was
    // removed. Bytes following that delimiter were ignored within that record.
    // A large ignored suffix is legal and must not become part of the value or
    // impose a whole-line length limit on the streaming reader.
    const std::string ignored(2 * kMaxValueSize, 'x');
    const std::string maximum(kMaxValueSize, 'v');
    for (const char delimiter : {'|', '\r', '\0'}) {
        RestoreLocale restore;
        std::locale::global(std::locale(restore.previous, new LegacyWidenedNewline(delimiter)));
        const std::string separator(1, delimiter);
        check_legacy_import("deleted:before\nraw:" + separator + "snapshot\n",
                            "PUT kept value" + separator + ignored + "\n"
                            "PUT maximum " + maximum + separator + "ignored\n"
                            "PUT empty " + separator + "ignored\n"
                            "PUT one-space  " + separator + "ignored\n"
                            "PUT no-delimiter complete\nPUT no-value\n"
                            "DEL deleted " + separator + "ignored\nPUT after next\n",
                            {{"raw", separator + "snapshot"}, {"kept", "value"}, {"maximum", maximum},
                             {"empty", ""}, {"one-space", " "}, {"no-delimiter", "complete"},
                             {"no-value", ""}, {"after", "next"}}, {"deleted"});
        check_legacy_rejection("wal.log", "PUT key value" + separator + ignored);
        check_legacy_rejection("wal.log", "PUT key " + maximum + "v" + separator + "ignored\n", "value");
        check_legacy_rejection("wal.log", "DEL key  " + separator + "ignored\n", "value");
    }
    {
        RestoreLocale restore;
        std::locale::global(std::locale(restore.previous, new LegacyWidenedNewline(' ')));
        // When space is the delimiter, it ends getline before the old code
        // strips a leading space. Removing it first would retain "discarded".
        check_legacy_import("deleted:before\n",
                            "PUT empty " + ignored + "\nPUT tab\tkept discarded\n"
                            "PUT no-value\nDEL deleted discarded\nPUT after ignored\n",
                            {{"empty", ""}, {"tab", "\tkept"}, {"no-value", ""}, {"after", ""}},
                            {"deleted"});
        check_legacy_rejection("wal.log", "PUT key " + ignored);
        check_legacy_rejection("wal.log", "DEL key\tnot-empty ignored\n", "value");
    }
}

void temporary_aliases_never_truncate_targets() {
    for (const std::string name : {"snapshot.v1.tmp", "wal.v1.tmp"}) {
        for (const std::string kind : {"symlink", "hardlink", "regular"}) {
            for (const bool fail_write : {false, true}) {
                TempDir temporary;
                const auto data = temporary.path / "data";
                auto config = config_for(data);
                bool armed = false;
                const std::string point = name == "snapshot.v1.tmp" ? "snapshot.write" : "wal.compact.write";
                config.io_hook = [&](const std::string& current) {
                    if (armed && current == point) throw std::runtime_error("injected temporary write failure");
                };
                const auto target = temporary.path / "target";
                write_file(target, "unrelated bytes must survive");
                {
                    Engine engine(config);
                    put(engine, "preserved", "value");
                    const auto entry = data / name;
                    if (kind == "symlink") fs::create_symlink(target, entry);
                    else if (kind == "hardlink") fs::create_hard_link(target, entry);
                    else write_file(entry, "stale unfinished file");
                    armed = fail_write;
                    if (fail_write) must_fail([&] { engine.snapshot(); }, "snapshot ignored injected failure");
                    else engine.snapshot();
                    armed = false;
                    require(read_file(target) == "unrelated bytes must survive", "temporary alias truncated its target");
                    if (fail_write && name == "wal.v1.tmp") {
                        must_fail([&] { engine.close(); }, "WAL replacement failure was not reported");
                    } else engine.close();
                }
                config.io_hook = {};
                Engine recovered(config);
                value_is(recovered, "preserved", "value");
                put(recovered, "after", "retry");
                recovered.snapshot();
                require(read_file(target) == "unrelated bytes must survive", "snapshot retry changed alias target");
                require(!fs::is_symlink(data / "snapshot.v1") && !fs::is_symlink(data / "wal.v1"),
                        "temporary link became an installed data file");
            }
        }
    }
}

void temporary_directories_are_not_removed() {
    for (const std::string name : {"snapshot.v1.tmp", "wal.v1.tmp"}) {
        TempDir temporary;
        const auto data = temporary.path / "data";
        const auto config = config_for(data);
        {
            Engine engine(config);
            put(engine, "preserved", "value");
            fs::create_directory(data / name);
            write_file(data / name / "child", "keep");
            must_fail([&] { engine.snapshot(); }, "directory at temporary path was accepted");
            require(read_file(data / name / "child") == "keep", "temporary directory was removed");
            if (name == "wal.v1.tmp") must_fail([&] { engine.close(); }, "WAL failure was not reported");
            else engine.close();
        }
        Engine recovered(config);
        value_is(recovered, "preserved", "value");
    }
}

void directory_changes_do_not_redirect_io() {
    for (const bool repoint_symlink : {true, false}) {
        TempDir temporary;
        const auto original = temporary.path / "original";
        const auto moved = temporary.path / "moved";
        const auto other = repoint_symlink ? temporary.path / "other" : original;
        const auto alias = temporary.path / "alias";
        fs::create_directory(original);
        if (repoint_symlink) fs::create_directory_symlink(original, alias);
        {
            Engine first(config_for(repoint_symlink ? alias : original));
            put(first, "first", "before");
            if (!repoint_symlink) fs::rename(original, moved);
            Engine second(config_for(other));
            put(second, "second", "before");
            if (repoint_symlink) {
                fs::remove(alias);
                fs::create_directory_symlink(other, alias);
            }
            const auto snapshot_before = read_file(other / "snapshot.v1");
            const auto wal_before = read_file(other / "wal.v1");
            put(first, "first-after", "anchored");
            first.snapshot();
            require(read_file(other / "snapshot.v1") == snapshot_before && read_file(other / "wal.v1") == wal_before,
                    "directory change redirected storage into another locked database");
            must_fail([&] { Engine duplicate(config_for(repoint_symlink ? original : moved)); },
                      "original directory lock was lost");
            put(first, "first-later", "kept");
            put(second, "second-after", "kept");
            first.close();
            second.close();
        }
        Engine first(config_for(repoint_symlink ? original : moved));
        Engine second(config_for(other));
        value_is(first, "first", "before");
        value_is(first, "first-after", "anchored");
        value_is(first, "first-later", "kept");
        value_is(second, "second", "before");
        value_is(second, "second-after", "kept");
        require(second.execute({Operation::Get, "first-after", {}}).status == Status::NotFound,
                "data crossed database directories");
    }
}

void symlink_parent_components_follow_filesystem_semantics() {
    TempDir temporary;
    const auto home = temporary.path / "home";
    const auto target = temporary.path / "target";
    fs::create_directories(target / "child");
    fs::create_directory(home);
    fs::create_directory_symlink(target / "child", home / "link");
    { Engine initialized(config_for(target / "data")); put(initialized, "expected", "value"); initialized.close(); }
    Engine opened(config_for(home / "link" / ".." / "data"));
    value_is(opened, "expected", "value");
    require(!fs::exists(home / "data"), "lexical normalization opened the wrong directory");
}

} // namespace

int main(int argc, char** argv) {
    const std::vector<std::pair<std::string, std::function<void()>>> tests{
        {"installed file types", installed_entries_must_be_regular_files},
        {"mutable hard links", mutable_files_must_not_share_inodes},
        {"read-only hard links", read_only_hard_links_remain_unchanged},
        {"legacy line boundaries", legacy_lines_preserve_bytes_and_require_terminators},
        {"legacy field limits", legacy_fields_have_exact_limits},
        {"legacy token padding", legacy_token_padding_is_not_a_record_limit},
        {"legacy bounded reads", legacy_oversized_values_stop_reading_early},
        {"legacy delimiters", legacy_delimiters_preserve_the_original_format},
        {"legacy token locale", legacy_wal_uses_cpp_locale_for_token_whitespace},
        {"legacy getline locale", legacy_wal_preserves_widened_getline_delimiters},
        {"temporary aliases", temporary_aliases_never_truncate_targets},
        {"temporary directories", temporary_directories_are_not_removed},
        {"directory identity", directory_changes_do_not_redirect_io},
        {"symlink parent components", symlink_parent_components_follow_filesystem_semantics},
    };
    try {
        for (const auto& test : tests) {
            if (argc > 1 && test.first != argv[1]) continue;
            test.second();
            std::cout << "PASS " << test.first << '\n';
        }
    } catch (const std::exception& error) {
        std::cerr << "FAIL " << error.what() << '\n';
        return 1;
    }
}
