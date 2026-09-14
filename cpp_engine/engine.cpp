#include "engine.h"
#include "codec.h"

#include <array>
#include <cerrno>
#include <cstring>
#include <filesystem>
#include <fcntl.h>
#include <fstream>
#include <iostream>
#include <limits>
#include <sstream>
#include <stdexcept>
#include <string_view>
#include <sys/file.h>
#include <sys/stat.h>
#include <system_error>
#include <unistd.h>

namespace minikv {
namespace {

using StatsClock = std::chrono::steady_clock;

thread_local const Engine* callback_engine = nullptr;

struct CallbackScope {
    const Engine* previous = callback_engine;
    explicit CallbackScope(const Engine* engine) { callback_engine = engine; }
    ~CallbackScope() { callback_engine = previous; }
};

uint64_t elapsed_ns(StatsClock::time_point start) {
    return static_cast<uint64_t>(std::chrono::duration_cast<std::chrono::nanoseconds>(StatsClock::now() - start).count());
}

// Declare outside all state-lock scopes. Accounting must not wait for I/O and
// must still run when a phase throws, without replacing the original failure.
struct TimedStage {
    std::mutex& mutex;
    uint64_t& counter;
    StatsClock::time_point start = StatsClock::now();
    ~TimedStage() {
        const auto elapsed = elapsed_ns(start);
        std::lock_guard<std::mutex> lock(mutex);
        counter += elapsed;
    }
};

// The caller holds the state lock both when this starts and after the condition
// variable reacquires it. Do not lock again here, including on failure/close.
struct TimedWait {
    uint64_t& waiters;
    uint64_t& completed;
    uint64_t& duration;
    StatsClock::time_point start = StatsClock::now();

    TimedWait(uint64_t& waiting, uint64_t& count, uint64_t& elapsed)
        : waiters(waiting), completed(count), duration(elapsed) { ++waiters; }
    TimedWait(const TimedWait&) = delete;
    TimedWait& operator=(const TimedWait&) = delete;
    ~TimedWait() {
        --waiters;
        ++completed;
        duration += elapsed_ns(start);
    }
};

template <class Predicate>
void wait_with_stats(std::condition_variable& condition, std::unique_lock<std::mutex>& lock,
                     Predicate ready, uint64_t& waiters, uint64_t& completed, uint64_t& duration) {
    // Preserve the original predicate and avoid clock reads on the ready path.
    if (ready()) return;
    TimedWait timer(waiters, completed, duration);
    condition.wait(lock, ready);
}

// Declare before the state lock. On a pre-transfer return/exception, destroy
// callback captures outside the lock before releasing their slot; close/drain
// must not finish while those resources are still live.
template <class Resources> struct AsyncReservation {
    const Engine* engine;
    std::mutex& mutex;
    std::condition_variable& changed;
    uint64_t& inflight;
    Resources resources;
    bool owned = false;
    ~AsyncReservation() {
        CallbackScope callback_scope(engine);
        resources.clear();
        if (!owned) return;
        std::lock_guard<std::mutex> lock(mutex);
        --inflight;
        changed.notify_all();
    }
};

// The notifier must still deliver an error if copying diagnostic text fails.
void response_error(Response& response, std::string_view message) noexcept {
    response.status = Status::IOError;
    try { response.value.assign(message.data(), message.size()); }
    catch (...) { response.value.clear(); }
}

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
        config_.snapshot_interval.count() < 0 || config_.max_async_requests == 0 ||
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
        reply_worker_ = std::thread(&Engine::background_replies, this);
        if (config_.snapshot_interval.count() > 0) snapshot_worker_ = std::thread(&Engine::background_snapshots, this);
    } catch (...) {
        // A later thread can fail to start after another worker was created.
        {
            std::lock_guard<std::mutex> lock(mutex_);
            stopping_ = true;
            wake_.notify_all();
            snapshot_wake_.notify_all();
            committed_.notify_all();
        }
        if (worker_.joinable()) worker_.join();
        if (snapshot_worker_.joinable()) snapshot_worker_.join();
        if (reply_worker_.joinable()) reply_worker_.join();
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
    snapshot();
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
    stats_.snapshot_sequence = applied_sequence_;
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

Engine::AppliedRequest Engine::apply_locked(const Request& request, std::unique_lock<std::mutex>& lock) {
    if (!failure_.empty()) return {{Status::IOError, failure_}};
    if (stopping_) return {{Status::Busy, "engine is stopping"}};
    if (request.operation == Operation::Get) {
        const auto it = kv_.find(request.key);
        Response result = it == kv_.end() ? Response{Status::NotFound, {}} : Response{Status::Value, it->second};
        return {std::move(result), applied_sequence_, config_.wal_mode == WalMode::Reliable};
    }
    const size_t size = codec::kRecordHeader + request.key.size() + request.value.size() + 4;
    wait_with_stats(committed_, lock,
                    [&] { return stopping_ || !failure_.empty() || pending_bytes_ + size <= config_.wal_queue_bytes; },
                    stats_.wal_capacity_waiters, stats_.wal_capacity_waits_total,
                    stats_.wal_capacity_wait_duration_ns_total);
    if (!failure_.empty()) return {{Status::IOError, failure_}};
    if (stopping_) return {{Status::Busy, "engine is stopping"}};
    if (applied_sequence_ == std::numeric_limits<uint64_t>::max()) return {{Status::IOError, "sequence exhausted"}};
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
        return {{Status::IOError, failure_}};
    }
    wake_.notify_one();
    return {{status, {}}, sequence, config_.wal_mode == WalMode::Reliable};
}

Response Engine::execute(const Request& request) {
    if (!codec::valid_request(request)) return {Status::Invalid, "invalid operation or key/value length"};
    std::unique_lock<std::mutex> lock(mutex_);
    auto applied = apply_locked(request, lock);
    if (applied.confirm) {
        wait_with_stats(committed_, lock,
                        [&] { return stopping_ || !failure_.empty() || durable_sequence_ >= applied.target; },
                        stats_.wal_durable_waiters, stats_.wal_durable_waits_total,
                        stats_.wal_durable_wait_duration_ns_total);
        if (!failure_.empty()) return {Status::IOError, failure_};
        if (durable_sequence_ < applied.target) {
            return {Status::IOError, request.operation == Operation::Get
                ? "shutdown before observed state was durable" : "shutdown before durable acknowledgement"};
        }
    }
    return std::move(applied.response);
}

std::optional<Response> Engine::execute_async(const Request& request, AsyncCompletion completion) {
    if (!completion) throw std::invalid_argument("async completion must not be empty");
    if (!codec::valid_request(request)) return Response{Status::Invalid, "invalid operation or key/value length"};
    AsyncReservation<std::list<PendingReply>> reservation{this, mutex_, committed_, stats_.async_requests_inflight, {}};
    auto& prepared = reservation.resources;
    std::unique_lock<std::mutex> lock(mutex_);
    if (!failure_.empty()) return Response{Status::IOError, failure_};
    if (stopping_) return Response{Status::Busy, "engine is stopping"};

    const bool may_defer = config_.wal_mode == WalMode::Reliable &&
        (request.operation != Operation::Get || durable_sequence_ < applied_sequence_);
    if (may_defer) {
        if (stats_.async_requests_inflight >= config_.max_async_requests) {
            return Response{Status::Busy, "async request limit reached"};
        }
        // Both capacity and the list node are reserved before applying a write.
        // A WAL-capacity wait may release the lock; this slot stays charged.
        prepared.emplace_back();
        prepared.back().completion = std::move(completion);
        ++stats_.async_requests_inflight;
        reservation.owned = true;
    }

    auto applied = apply_locked(request, lock);
    if (!applied.confirm || durable_sequence_ >= applied.target) return std::move(applied.response);

    // Nothing after transfer can allocate or throw. Commit/failure/close cannot
    // race between capture, target comparison and registration under this lock.
    auto& reply = prepared.front();
    reply.response = std::move(applied.response);
    reply.target = applied.target;
    reply.wait_started = StatsClock::now();
    ++stats_.wal_durable_waiters;
    pending_replies_.splice(pending_replies_.end(), prepared);
    reservation.owned = false;
    committed_.notify_all();
    return std::nullopt;
}

void Engine::drain_async() {
    if (callback_engine == this) throw std::logic_error("cannot drain engine from its async callback");
    std::unique_lock<std::mutex> lock(mutex_);
    committed_.wait(lock, [this] { return stats_.async_requests_inflight == 0; });
}

void Engine::write_batch(const std::deque<PendingRecord>& records) {
    hook("wal.write");
    for (const auto& record : records) write_all(wal_fd_, record.bytes);
    hook("wal.sync");
    sync_file(wal_fd_);
    hook("wal.after_sync");
}

// Caller owns io_mutex_, so no other batch can advance the commit point.
void Engine::commit_batch(const std::deque<PendingRecord>& records, size_t bytes) {
    if (records.empty()) return;
    const auto started = StatsClock::now();
    try {
        write_batch(records);
        const auto elapsed = elapsed_ns(started);
        std::lock_guard<std::mutex> lock(mutex_);
        durable_sequence_ = records.back().sequence;
        pending_bytes_ -= bytes;
        stats_.wal_inflight_bytes = 0;
        ++stats_.wal_commits_total;
        stats_.wal_commit_duration_ns_total += elapsed;
        stats_.wal_commit_last_duration_ns = elapsed;
        committed_.notify_all();
    } catch (const std::exception& error) {
        const auto elapsed = elapsed_ns(started);
        std::lock_guard<std::mutex> lock(mutex_);
        stats_.wal_inflight_bytes = 0;
        ++stats_.wal_commit_failures_total;
        stats_.wal_commit_duration_ns_total += elapsed;
        stats_.wal_commit_last_duration_ns = elapsed;
        fail_locked(error.what());
        throw;
    }
}

void Engine::flush_pending() {
    std::lock_guard<std::mutex> io_lock(io_mutex_);
    try {
        std::deque<PendingRecord> batch;
        size_t batch_bytes;
        {
            std::lock_guard<std::mutex> lock(mutex_);
            if (!failure_.empty()) throw std::runtime_error(failure_);
            if (pending_.empty()) return;
            batch.swap(pending_);
            batch_bytes = pending_bytes_;
            stats_.wal_inflight_bytes = batch_bytes;
        }
        // New operations may enter pending_ while this batch is being written.
        // Its bytes stay charged until sync succeeds, maintaining backpressure.
        commit_batch(batch, batch_bytes);
    } catch (const std::exception& error) {
        std::lock_guard<std::mutex> lock(mutex_);
        fail_locked(error.what());
        throw;
    }
}

void Engine::flush_locked() {
    if (pending_.empty()) return;
    const auto started = StatsClock::now();
    stats_.wal_inflight_bytes = pending_bytes_;
    try {
        write_batch(pending_);
        const auto elapsed = elapsed_ns(started);
        durable_sequence_ = pending_.back().sequence;
        pending_.clear();
        pending_bytes_ = 0;
        stats_.wal_inflight_bytes = 0;
        ++stats_.wal_commits_total;
        stats_.wal_commit_duration_ns_total += elapsed;
        stats_.wal_commit_last_duration_ns = elapsed;
        committed_.notify_all();
    } catch (const std::exception& error) {
        const auto elapsed = elapsed_ns(started);
        stats_.wal_inflight_bytes = 0;
        ++stats_.wal_commit_failures_total;
        stats_.wal_commit_duration_ns_total += elapsed;
        stats_.wal_commit_last_duration_ns = elapsed;
        fail_locked(error.what());
        throw;
    }
}

void Engine::install_snapshot(const std::unordered_map<std::string, std::string>& image, uint64_t sequence) {
    const std::string temporary = config_.data_dir + "/snapshot.v1.tmp";
    const std::string installed = config_.data_dir + "/snapshot.v1";
    File file(::open(temporary.c_str(), O_WRONLY | O_CREAT | O_TRUNC | O_CLOEXEC, 0600));
    if (file.fd < 0) io_error("create snapshot");
    hook("snapshot.write");
    write_all(file.fd, snapshot_header(sequence, image.size()));
    for (const auto& entry : image) write_all(file.fd, codec::record(sequence, Operation::Put, entry.first, entry.second));
    hook("snapshot.sync");
    sync_file(file.fd);
    hook("snapshot.rename");
    if (::rename(temporary.c_str(), installed.c_str()) != 0) io_error("install snapshot");
    hook("snapshot.dir_sync");
    sync_directory(config_.data_dir);
    hook("snapshot.after_install");
}

// The snapshot at boundary is already durable. Retain every subsequent byte,
// including writes acknowledged while its file was being installed.
void Engine::compact_wal(int64_t boundary) {
    try {
        hook("wal.truncate");
        const off_t end = ::lseek(wal_fd_, 0, SEEK_END);
        if (end < 0) io_error("seek WAL for compaction");
        if (boundary < 0 || boundary > end) throw std::runtime_error("invalid WAL checkpoint boundary");
        const std::string temporary = config_.data_dir + "/wal.v1.tmp";
        const std::string installed = config_.data_dir + "/wal.v1";
        File replacement(::open(temporary.c_str(), O_RDWR | O_CREAT | O_TRUNC | O_APPEND | O_CLOEXEC, 0600));
        if (replacement.fd < 0) io_error("create WAL replacement");
        hook("wal.compact.write");
        std::array<char, 64 * 1024> buffer{};
        off_t offset = static_cast<off_t>(boundary);
        while (offset < end) {
            const size_t size = static_cast<size_t>(std::min<off_t>(buffer.size(), end - offset));
            const ssize_t count = ::pread(wal_fd_, buffer.data(), size, offset);
            if (count < 0 && errno == EINTR) continue;
            if (count < 0) io_error("read WAL suffix");
            if (count == 0) throw std::runtime_error("WAL suffix ended unexpectedly");
            write_all(replacement.fd, std::string_view(buffer.data(), static_cast<size_t>(count)));
            offset += count;
        }
        hook("wal.compact.sync");
        sync_file(replacement.fd);
        hook("wal.compact.rename");
        if (::rename(temporary.c_str(), installed.c_str()) != 0) io_error("install WAL replacement");
        // Once renamed, appends must use the new inode even if directory sync
        // fails. Swap descriptors before any throwing hook or syscall.
        const int previous = wal_fd_;
        wal_fd_ = replacement.fd;
        replacement.fd = -1;
        ::close(previous);
        hook("wal.compact.after_replace");
        hook("wal.after_truncate");
        hook("wal.compact.dir_sync");
        sync_directory(config_.data_dir);
    } catch (const std::exception& error) {
        std::lock_guard<std::mutex> lock(mutex_);
        fail_locked(error.what());
        throw;
    }
}

void Engine::snapshot() {
    std::lock_guard<std::mutex> snapshot_lock(snapshot_mutex_);
    {
        std::lock_guard<std::mutex> lock(mutex_);
        if (stopping_) throw std::runtime_error("engine is stopping");
        if (!failure_.empty()) throw std::runtime_error(failure_);
        stats_.snapshot_in_progress = true;
    }
    try {
        std::unique_lock<std::mutex> io_lock(io_mutex_, std::defer_lock);
        std::unordered_map<std::string, std::string> image;
        std::deque<PendingRecord> batch;
        size_t batch_bytes;
        uint64_t sequence;
        off_t boundary;
        {
            TimedStage timer{mutex_, stats_.snapshot_capture_duration_ns_total};
            io_lock.lock();
            {
                std::lock_guard<std::mutex> lock(mutex_);
                if (stopping_) throw std::runtime_error("engine is stopping");
                if (!failure_.empty()) throw std::runtime_error(failure_);
                // Copy before detaching: an allocation failure leaves the queue intact.
                image = kv_;
                sequence = applied_sequence_;
                batch.swap(pending_);
                batch_bytes = pending_bytes_;
                stats_.wal_inflight_bytes = batch_bytes;
            }
            commit_batch(batch, batch_bytes);
            boundary = ::lseek(wal_fd_, 0, SEEK_END);
            if (boundary < 0) io_error("seek WAL checkpoint boundary");
            io_lock.unlock();
        }
        batch.clear();
        {
            TimedStage timer{mutex_, stats_.snapshot_write_duration_ns_total};
            install_snapshot(image, sequence);
        }
        {
            TimedStage timer{mutex_, stats_.snapshot_compact_duration_ns_total};
            io_lock.lock();
            {
                std::lock_guard<std::mutex> lock(mutex_);
                if (!failure_.empty()) throw std::runtime_error(failure_);
            }
            compact_wal(boundary);
            {
                std::lock_guard<std::mutex> lock(mutex_);
                // A WAL commit during snapshot installation may be newer.
                durable_sequence_ = std::max(durable_sequence_, sequence);
                committed_.notify_all();
            }
            io_lock.unlock();
        }
        // Destroying a large image also happens outside the I/O and state locks.
        std::lock_guard<std::mutex> lock(mutex_);
        stats_.snapshot_sequence = sequence;
        stats_.snapshot_in_progress = false;
        ++stats_.snapshot_successes_total;
    } catch (...) {
        std::lock_guard<std::mutex> lock(mutex_);
        stats_.snapshot_in_progress = false;
        ++stats_.snapshot_failures_total;
        throw;
    }
}

void Engine::background_work() {
    using Clock = std::chrono::steady_clock;
    std::unique_lock<std::mutex> lock(mutex_);
    auto flush_at = Clock::now() + config_.wal_flush_interval;
    while (!stopping_) {
        wake_.wait_until(lock, flush_at, [this] {
            return stopping_ || pending_.size() >= config_.wal_batch_size;
        });
        if (stopping_) break;
        const auto now = Clock::now();
        if (now >= flush_at || pending_.size() >= config_.wal_batch_size) {
            lock.unlock();
            try { flush_pending(); }
            catch (const std::exception&) { /* flush_pending records the terminal failure. */ }
            lock.lock();
            flush_at = Clock::now() + config_.wal_flush_interval;
        }
        if (!failure_.empty()) wake_.wait(lock, [this] { return stopping_; });
    }
}

void Engine::background_snapshots() {
    std::unique_lock<std::mutex> lock(mutex_);
    while (!stopping_) {
        if (snapshot_wake_.wait_for(lock, config_.snapshot_interval, [this] { return stopping_; })) break;
        if (!failure_.empty()) continue;
        lock.unlock();
        try { snapshot(); }
        catch (const std::exception& error) { std::cerr << "snapshot failed: " << error.what() << '\n'; }
        lock.lock();
    }
}

void Engine::background_replies() {
    std::unique_lock<std::mutex> lock(mutex_);
    while (true) {
        committed_.wait(lock, [this] {
            return (stopping_ && stats_.async_requests_inflight == 0) ||
                (!pending_replies_.empty() &&
                 (stopping_ || !failure_.empty() || pending_replies_.front().target <= durable_sequence_));
        });
        if (stopping_ && stats_.async_requests_inflight == 0) return;

        std::list<PendingReply> ready;
        ready.splice(ready.end(), pending_replies_, pending_replies_.begin());
        auto& reply = ready.front();
        --stats_.wal_durable_waiters;
        ++stats_.wal_durable_waits_total;
        stats_.wal_durable_wait_duration_ns_total += elapsed_ns(reply.wait_started);
        // Match execute's error precedence at the moment this waiter reacquires
        // state. Later commits/failures cannot change the already chosen reply.
        if (!failure_.empty()) response_error(reply.response, failure_);
        else if (durable_sequence_ < reply.target) {
            response_error(reply.response, "shutdown before durable acknowledgement");
        }
        lock.unlock();
        bool failed = false;
        {
            CallbackScope callback_scope(this);
            try { reply.completion(std::move(reply.response)); }
            catch (...) { failed = true; }
            // Captured objects may consult stats in their destructors. Release
            // them with no engine locks, before publishing that the slot is free.
            ready.clear();
        }
        lock.lock();
        if (failed) ++stats_.async_callback_failures_total;
        --stats_.async_requests_inflight;
        committed_.notify_all();
    }
}

uint64_t Engine::durable_sequence() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return durable_sequence_;
}

EngineStats Engine::stats() const {
    std::lock_guard<std::mutex> lock(mutex_);
    auto result = stats_;
    result.wal_mode = config_.wal_mode;
    result.keys = kv_.size();
    result.applied_sequence = applied_sequence_;
    result.durable_sequence = durable_sequence_;
    result.wal_pending_bytes = pending_bytes_;
    result.wal_queued_records = pending_.size();
    result.wal_queue_capacity_bytes = config_.wal_queue_bytes;
    result.async_requests_capacity = config_.max_async_requests;
    result.io_failed = !failure_.empty();
    result.stopping = stopping_;
    return result;
}

void Engine::close() {
    // Check before close_mutex_: another thread may already hold it while
    // joining this notifier. Neither self-join nor that lock cycle is allowed.
    if (callback_engine == this) throw std::logic_error("cannot close engine from its async callback");
    // Joining the background worker must also be serialized across callers.
    std::lock_guard<std::mutex> close_lock(close_mutex_);
    {
        std::lock_guard<std::mutex> lock(mutex_);
        if (closed_) return;
        stopping_ = true;
        wake_.notify_all();
        snapshot_wake_.notify_all();
        committed_.notify_all();
    }
    if (worker_.joinable()) worker_.join();
    if (snapshot_worker_.joinable()) snapshot_worker_.join();
    {
        std::lock_guard<std::mutex> snapshot_lock(snapshot_mutex_);
        std::lock_guard<std::mutex> io_lock(io_mutex_);
        std::lock_guard<std::mutex> lock(mutex_);
        if (failure_.empty()) {
            try { flush_locked(); }
            catch (const std::exception&) { /* Preserve the failure while still releasing descriptors. */ }
        }
        closed_ = true;
        release_files();
    }
    // Callbacks may acquire state (e.g. stats), so never join under these locks.
    // The notifier also waits for reserved submitters to return on shutdown.
    if (reply_worker_.joinable()) reply_worker_.join();
    std::lock_guard<std::mutex> lock(mutex_);
    if (!failure_.empty()) throw std::runtime_error(failure_);
}

} // namespace minikv
