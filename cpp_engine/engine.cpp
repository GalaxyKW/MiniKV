#include "engine.h"
#include "codec.h"

#include <cerrno>
#include <cstring>
#include <filesystem>
#include <fcntl.h>
#include <fstream>
#include <iostream>
#include <limits>
#include <sstream>
#include <stdexcept>
#include <sys/file.h>
#include <sys/stat.h>
#include <system_error>
#include <unistd.h>

namespace minikv {
namespace {

struct File {
    int fd;
    explicit File(int value) : fd(value) {}
    ~File() { if (fd >= 0) ::close(fd); }
    File(const File&) = delete;
    File& operator=(const File&) = delete;
};

[[noreturn]] void io_error(const std::string& operation) {
    throw std::system_error(errno, std::generic_category(), operation);
}

void write_all(int fd, std::string_view bytes) {
    while (!bytes.empty()) {
        const ssize_t count = ::write(fd, bytes.data(), bytes.size());
        if (count < 0 && errno == EINTR) continue;
        if (count < 0) io_error("write");
        if (count == 0) throw std::runtime_error("write made no progress");
        bytes.remove_prefix(static_cast<size_t>(count));
    }
}

std::string read_bytes(int fd, size_t size) {
    std::string bytes(size, '\0');
    size_t offset = 0;
    while (offset < size) {
        const ssize_t count = ::read(fd, bytes.data() + offset, size - offset);
        if (count < 0 && errno == EINTR) continue;
        if (count < 0) io_error("read");
        if (count == 0) break;
        offset += static_cast<size_t>(count);
    }
    bytes.resize(offset);
    return bytes;
}

void sync_file(int fd) {
    while (::fdatasync(fd) != 0) {
        if (errno != EINTR) io_error("fdatasync");
    }
}

void sync_directory(const std::filesystem::path& path) {
    File dir(::open(path.c_str(), O_RDONLY | O_DIRECTORY | O_CLOEXEC));
    if (dir.fd < 0) io_error("open directory " + path.string());
    while (::fsync(dir.fd) != 0) {
        if (errno != EINTR) io_error("fsync directory " + path.string());
    }
}

void ensure_directory(const std::filesystem::path& path) {
    if (std::filesystem::is_directory(path)) return;
    const auto parent = path.parent_path();
    if (parent != path) ensure_directory(parent);
    if (std::filesystem::create_directory(path)) sync_directory(parent);
}

std::string snapshot_header(uint64_t sequence, uint64_t size) {
    std::string bytes = "MKVSNP01";
    codec::append_u64(bytes, sequence);
    codec::append_u64(bytes, size);
    codec::append_u32(bytes, codec::crc32(bytes));
    return bytes;
}

} // namespace

Engine::Engine(EngineConfig config) : config_(std::move(config)) {
    if (config_.data_dir.empty() || config_.wal_batch_size == 0 || config_.wal_flush_interval.count() <= 0 ||
        config_.snapshot_interval.count() < 0 ||
        config_.wal_queue_bytes < kMaxKeySize + kMaxValueSize + codec::kRecordHeader + 4) {
        throw std::invalid_argument("invalid engine configuration");
    }
    config_.data_dir = std::filesystem::absolute(config_.data_dir).lexically_normal().string();
    try {
        ensure_directory(config_.data_dir);
        lock_fd_ = ::open((config_.data_dir + "/LOCK").c_str(), O_RDWR | O_CREAT | O_CLOEXEC, 0600);
        if (lock_fd_ < 0) io_error("open data directory lock");
        if (::flock(lock_fd_, LOCK_EX | LOCK_NB) != 0) io_error("data directory is already in use");
        recover();
        worker_ = std::thread(&Engine::background_work, this);
    } catch (...) {
        release_files();
        throw;
    }
}

Engine::~Engine() {
    try { close(); }
    catch (const std::exception& error) { std::cerr << "engine shutdown: " << error.what() << '\n'; }
}

void Engine::hook(const std::string& point) {
    if (config_.io_hook) config_.io_hook(point);
}

void Engine::release_files() noexcept {
    if (wal_fd_ >= 0) { ::close(wal_fd_); wal_fd_ = -1; }
    if (lock_fd_ >= 0) { ::close(lock_fd_); lock_fd_ = -1; }
}

void Engine::recover() {
    namespace fs = std::filesystem;
    const auto snapshot_path = config_.data_dir + "/snapshot.v1";
    const auto wal_path = config_.data_dir + "/wal.v1";
    if (fs::exists(snapshot_path)) {
        if (config_.import_legacy) throw std::runtime_error("v1 data already exists; legacy import refused");
        load_snapshot();
        wal_fd_ = ::open(wal_path.c_str(), O_RDWR | O_APPEND | O_CLOEXEC);
        if (wal_fd_ < 0) io_error("open existing WAL (refusing to recreate missing data)");
        load_wal();
        sync_file(wal_fd_);
        durable_sequence_ = applied_sequence_;
        return;
    }
    // Even an empty WAL may belong to a checkpointed database whose snapshot
    // was lost. It is not evidence that this is a new, empty database.
    if (fs::exists(wal_path)) {
        throw std::runtime_error("snapshot missing beside v1 WAL; refusing to recreate missing data");
    }
    const auto old_snapshot = config_.data_dir + "/data.db";
    const auto old_wal = config_.data_dir + "/wal.log";
    const bool legacy = (fs::exists(old_snapshot) && fs::file_size(old_snapshot) != 0) ||
                        (fs::exists(old_wal) && fs::file_size(old_wal) != 0);
    if (legacy && !config_.import_legacy) {
        throw std::runtime_error("legacy data detected; stop the old engine, back up the directory, then run engine --import-legacy");
    }
    if (legacy) import_legacy();
    wal_fd_ = ::open(wal_path.c_str(), O_RDWR | O_CREAT | O_APPEND | O_CLOEXEC, 0600);
    if (wal_fd_ < 0) io_error("create WAL");
    sync_file(wal_fd_);
    sync_directory(config_.data_dir);
    snapshot_locked();
}

void Engine::load_snapshot() {
    File file(::open((config_.data_dir + "/snapshot.v1").c_str(), O_RDONLY | O_CLOEXEC));
    if (file.fd < 0) io_error("open snapshot");
    const std::string header = read_bytes(file.fd, 28);
    if (header.size() != 28 || header.substr(0, 8) != "MKVSNP01" ||
        codec::u32(header, 24) != codec::crc32(std::string_view(header).substr(0, 24))) {
        throw std::runtime_error("corrupt snapshot header");
    }
    applied_sequence_ = codec::u64(header, 8);
    const uint64_t count = codec::u64(header, 16);
    struct stat metadata{};
    if (::fstat(file.fd, &metadata) != 0) io_error("stat snapshot");
    if (count > static_cast<uint64_t>(metadata.st_size) / (codec::kRecordHeader + 5)) {
        throw std::runtime_error("invalid snapshot entry count");
    }
    for (uint64_t i = 0; i < count; ++i) {
        auto bytes = read_bytes(file.fd, codec::kRecordHeader);
        const size_t size = codec::record_size(bytes);
        bytes += read_bytes(file.fd, size - bytes.size());
        const auto entry = codec::decode_record(bytes);
        if (entry.operation != Operation::Put || codec::u64(bytes, 5) != applied_sequence_ ||
            !kv_.emplace(entry.key, entry.value).second) {
            throw std::runtime_error("invalid snapshot entry");
        }
    }
    if (!read_bytes(file.fd, 1).empty()) throw std::runtime_error("unexpected snapshot trailing bytes");
    // Recovery may observe a rename from an interrupted installation. Make
    // that checkpoint durable before reclaiming any WAL records it covers.
    sync_file(file.fd);
    sync_directory(config_.data_dir);
}

void Engine::load_wal() {
    if (::lseek(wal_fd_, 0, SEEK_SET) < 0) io_error("seek WAL");
    off_t valid_bytes = 0;
    const uint64_t checkpoint_sequence = applied_sequence_;
    uint64_t previous = 0;
    bool incomplete_tail = false;
    while (true) {
        std::string bytes = read_bytes(wal_fd_, codec::kRecordHeader);
        if (bytes.empty()) break;
        if (bytes.size() != codec::kRecordHeader) { incomplete_tail = true; break; }
        const size_t size = codec::record_size(bytes);
        bytes += read_bytes(wal_fd_, size - bytes.size());
        if (bytes.size() != size) { incomplete_tail = true; break; }
        const auto entry = codec::decode_record(bytes);
        const uint64_t sequence = codec::u64(bytes, 5);
        if (sequence == 0 || (previous != 0 && sequence != previous + 1)) {
            throw std::runtime_error("non-contiguous WAL sequence");
        }
        previous = sequence;
        // A crash after snapshot installation can leave the old WAL intact.
        if (sequence > applied_sequence_) {
            if (sequence != applied_sequence_ + 1) throw std::runtime_error("missing WAL sequence");
            if (entry.operation == Operation::Put) kv_[entry.key] = entry.value;
            else kv_.erase(entry.key);
            applied_sequence_ = sequence;
        }
        valid_bytes += static_cast<off_t>(size);
    }
    if (applied_sequence_ == checkpoint_sequence && (valid_bytes != 0 || incomplete_tail)) {
        // A retained WAL can end before the checkpoint sequence. Leaving that
        // prefix in place would create a sequence gap after the next append.
        if (::ftruncate(wal_fd_, 0) != 0) io_error("truncate checkpointed WAL during recovery");
        sync_file(wal_fd_);
    } else if (incomplete_tail) {
        std::cerr << "discarding incomplete WAL tail at byte " << valid_bytes << '\n';
        if (::ftruncate(wal_fd_, valid_bytes) != 0) io_error("truncate incomplete WAL tail");
        sync_file(wal_fd_);
    }
}

void Engine::import_legacy() {
    const auto read_lines = [](const std::string& path, const auto& consume) {
        if (!std::filesystem::exists(path)) return;
        std::ifstream file(path, std::ios::binary);
        if (!file) throw std::runtime_error("cannot read legacy file: " + path);
        std::string line;
        while (std::getline(file, line)) {
            if (file.eof()) throw std::runtime_error("incomplete legacy record in " + path);
            consume(line);
        }
        if (!file.eof()) throw std::runtime_error("error reading legacy file: " + path);
    };
    read_lines(config_.data_dir + "/data.db", [this](const std::string& line) {
        const auto separator = line.find(':');
        if (separator == std::string::npos) throw std::runtime_error("invalid legacy snapshot record");
        Request entry{Operation::Put, line.substr(0, separator), line.substr(separator + 1)};
        if (!codec::valid_request(entry)) throw std::runtime_error("invalid legacy key/value size");
        kv_[entry.key] = entry.value;
    });
    read_lines(config_.data_dir + "/wal.log", [this](const std::string& line) {
        std::istringstream input(line);
        std::string op, key, value;
        if (!(input >> op >> key)) throw std::runtime_error("invalid legacy WAL record");
        std::getline(input, value);
        if (!value.empty() && value.front() == ' ') value.erase(0, 1);
        const Operation operation = op == "PUT" ? Operation::Put : Operation::Delete;
        Request entry{operation, key, value};
        if ((op != "PUT" && op != "DEL") || !codec::valid_request(entry)) {
            throw std::runtime_error("invalid legacy WAL operation or data");
        }
        if (operation == Operation::Put) kv_[key] = value;
        else kv_.erase(key);
        ++applied_sequence_;
    });
}

void Engine::fail_locked(const std::string& message) {
    if (failure_.empty()) {
        failure_ = message;
        std::cerr << "storage I/O failure; requests rejected until restart: " << message << '\n';
    }
    committed_.notify_all();
}

Response Engine::execute(const Request& request) {
    if (!codec::valid_request(request)) return {Status::Invalid, "invalid operation or key/value length"};
    std::unique_lock<std::mutex> lock(mutex_);
    if (!failure_.empty()) return {Status::IOError, failure_};
    if (stopping_) return {Status::Busy, "engine is stopping"};
    if (request.operation == Operation::Get) {
        const auto it = kv_.find(request.key);
        Response result = it == kv_.end() ? Response{Status::NotFound, {}} : Response{Status::Value, it->second};
        const uint64_t observed = applied_sequence_;
        if (config_.wal_mode == WalMode::Reliable) {
            committed_.wait(lock, [&] { return stopping_ || !failure_.empty() || durable_sequence_ >= observed; });
            if (!failure_.empty()) return {Status::IOError, failure_};
            if (durable_sequence_ < observed) return {Status::IOError, "shutdown before observed state was durable"};
        }
        return result;
    }
    const size_t size = codec::kRecordHeader + request.key.size() + request.value.size() + 4;
    committed_.wait(lock, [&] { return stopping_ || !failure_.empty() || pending_bytes_ + size <= config_.wal_queue_bytes; });
    if (!failure_.empty()) return {Status::IOError, failure_};
    if (stopping_) return {Status::Busy, "engine is stopping"};
    if (applied_sequence_ == std::numeric_limits<uint64_t>::max()) return {Status::IOError, "sequence exhausted"};
    const uint64_t sequence = applied_sequence_ + 1;
    Status status = Status::Ok;
    try {
        // This mutex defines one order for the WAL, memory, and snapshot boundary.
        pending_.push_back({sequence, codec::record(sequence, request.operation, request.key, request.value)});
        pending_bytes_ += size;
        if (request.operation == Operation::Put) kv_[request.key] = request.value;
        else if (kv_.erase(request.key) == 0) status = Status::NotFound;
        applied_sequence_ = sequence;
    } catch (const std::exception& error) {
        fail_locked(error.what());
        return {Status::IOError, failure_};
    }
    wake_.notify_one();
    if (config_.wal_mode == WalMode::Reliable) {
        committed_.wait(lock, [&] { return stopping_ || !failure_.empty() || durable_sequence_ >= sequence; });
        if (!failure_.empty()) return {Status::IOError, failure_};
        if (durable_sequence_ < sequence) return {Status::IOError, "shutdown before durable acknowledgement"};
    }
    return {status, {}};
}

void Engine::flush_locked() {
    if (pending_.empty()) return;
    try {
        hook("wal.write");
        for (const auto& record : pending_) write_all(wal_fd_, record.bytes);
        hook("wal.sync");
        sync_file(wal_fd_);
        hook("wal.after_sync");
        durable_sequence_ = pending_.back().sequence;
        pending_.clear();
        pending_bytes_ = 0;
        committed_.notify_all();
    } catch (const std::exception& error) {
        fail_locked(error.what());
        throw;
    }
}

void Engine::snapshot_locked() {
    if (!failure_.empty()) throw std::runtime_error(failure_);
    flush_locked();
    const std::string temporary = config_.data_dir + "/snapshot.v1.tmp";
    const std::string installed = config_.data_dir + "/snapshot.v1";
    File file(::open(temporary.c_str(), O_WRONLY | O_CREAT | O_TRUNC | O_CLOEXEC, 0600));
    if (file.fd < 0) io_error("create snapshot");
    hook("snapshot.write");
    write_all(file.fd, snapshot_header(applied_sequence_, kv_.size()));
    for (const auto& entry : kv_) write_all(file.fd, codec::record(applied_sequence_, Operation::Put, entry.first, entry.second));
    hook("snapshot.sync");
    sync_file(file.fd);
    hook("snapshot.rename");
    if (::rename(temporary.c_str(), installed.c_str()) != 0) io_error("install snapshot");
    hook("snapshot.dir_sync");
    sync_directory(config_.data_dir);
    hook("snapshot.after_install");
    // Only a durably installed checkpoint permits deleting its WAL prefix.
    try {
        hook("wal.truncate");
        if (::ftruncate(wal_fd_, 0) != 0) io_error("truncate checkpointed WAL");
        hook("wal.after_truncate");
        sync_file(wal_fd_);
        durable_sequence_ = applied_sequence_;
        committed_.notify_all();
    } catch (const std::exception& error) {
        fail_locked(error.what());
        throw;
    }
}

void Engine::snapshot() {
    std::lock_guard<std::mutex> lock(mutex_);
    if (stopping_) throw std::runtime_error("engine is stopping");
    snapshot_locked();
}

void Engine::background_work() {
    using Clock = std::chrono::steady_clock;
    std::unique_lock<std::mutex> lock(mutex_);
    auto flush_at = Clock::now() + config_.wal_flush_interval;
    auto snapshot_at = config_.snapshot_interval.count() == 0 ? Clock::time_point::max() : Clock::now() + config_.snapshot_interval;
    while (!stopping_) {
        wake_.wait_until(lock, std::min(flush_at, snapshot_at), [this] {
            return stopping_ || pending_.size() >= config_.wal_batch_size;
        });
        if (stopping_) break;
        const auto now = Clock::now();
        if (now >= flush_at || pending_.size() >= config_.wal_batch_size) {
            try { flush_locked(); }
            catch (const std::exception&) { /* flush_locked records the terminal failure. */ }
            flush_at = Clock::now() + config_.wal_flush_interval;
        }
        if (failure_.empty() && now >= snapshot_at) {
            try { snapshot_locked(); }
            catch (const std::exception& error) { std::cerr << "snapshot failed: " << error.what() << '\n'; }
            snapshot_at = Clock::now() + config_.snapshot_interval;
        }
        if (!failure_.empty()) wake_.wait(lock, [this] { return stopping_; });
    }
}

uint64_t Engine::durable_sequence() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return durable_sequence_;
}

void Engine::close() {
    // Joining the background worker must also be serialized across callers.
    std::lock_guard<std::mutex> close_lock(close_mutex_);
    {
        std::lock_guard<std::mutex> lock(mutex_);
        if (closed_) return;
        stopping_ = true;
        wake_.notify_all();
        committed_.notify_all();
    }
    if (worker_.joinable()) worker_.join();
    std::lock_guard<std::mutex> lock(mutex_);
    if (failure_.empty()) {
        try { flush_locked(); }
        catch (const std::exception&) { /* Preserve the failure while still releasing descriptors. */ }
    }
    closed_ = true;
    release_files();
    if (!failure_.empty()) throw std::runtime_error(failure_);
}

} // namespace minikv
