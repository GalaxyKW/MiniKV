#include "engine.h"
#include "codec.h"

#include <chrono>
#include <csignal>
#include <filesystem>
#include <fstream>
#include <functional>
#include <iostream>
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
