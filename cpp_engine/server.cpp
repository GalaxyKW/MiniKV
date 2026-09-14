#include "engine.h"
#include "codec.h"
#include "threadpool.h"

#include <arpa/inet.h>
#include <array>
#include <atomic>
#include <cerrno>
#include <csignal>
#include <cstdlib>
#include <cstring>
#include <iostream>
#include <mutex>
#include <sstream>
#include <stdexcept>
#include <sys/epoll.h>
#include <sys/eventfd.h>
#include <sys/socket.h>
#include <unistd.h>
#include <unordered_map>
#include <vector>

namespace {
using namespace minikv;
using Clock = std::chrono::steady_clock;
std::atomic<bool> stopping{false};
static_assert(std::atomic<bool>::is_always_lock_free, "signal handler requires lock-free atomics");

void stop_server(int) { stopping.store(true, std::memory_order_relaxed); }

int env_int(const char* name, int fallback, int minimum, int maximum) {
    const char* raw = std::getenv(name);
    if (raw == nullptr) return fallback;
    char* end = nullptr;
    errno = 0;
    const long result = std::strtol(raw, &end, 10);
    if (errno || end == raw || *end != '\0' || result < minimum || result > maximum) {
        throw std::invalid_argument(std::string("invalid ") + name);
    }
    return static_cast<int>(result);
}

std::string env_string(const char* name, const char* fallback) {
    const char* raw = std::getenv(name);
    return raw ? raw : fallback;
}

[[noreturn]] void network_error(const char* operation) {
    throw std::runtime_error(std::string(operation) + ": " + std::strerror(errno));
}

struct Client {
    int fd;
    std::string input;
    std::string output;
    size_t sent = 0;
    bool registered = false;
    bool busy = false;
    bool close_after_write = false;
    Clock::time_point active = Clock::now();
};

struct Completion {
    uint64_t id;
    Response response;
};

// The reactor owns every socket and buffer. Workers receive complete requests
// only; idle connections and partial frames never occupy a worker.
class Server {
public:
    explicit Server(Engine& engine)
        : engine_(engine),
          pool_(env_int("MINIKV_WORKERS", 20, 1, 1024),
                env_int("MINIKV_REQUEST_QUEUE_SIZE", 128, 1, 65536)),
          max_connections_(env_int("MINIKV_MAX_CONNECTIONS", 256, 1, 65536)),
          idle_timeout_(env_int("MINIKV_CLIENT_IDLE_MS", 30000, 1, 3600000)) {
        try {
            epoll_fd_ = ::epoll_create1(EPOLL_CLOEXEC);
            if (epoll_fd_ < 0) network_error("epoll_create1");
            completed_fd_ = ::eventfd(0, EFD_NONBLOCK | EFD_CLOEXEC);
            if (completed_fd_ < 0) network_error("eventfd");
            listen_fd_ = ::socket(AF_INET, SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0);
            if (listen_fd_ < 0) network_error("socket");
            int reuse = 1;
            if (::setsockopt(listen_fd_, SOL_SOCKET, SO_REUSEADDR, &reuse, sizeof(reuse)) != 0) network_error("SO_REUSEADDR");
            sockaddr_in address{};
            address.sin_family = AF_INET;
            const int port = env_int("MINIKV_ENGINE_PORT", 9090, 1, 65535);
            address.sin_port = htons(static_cast<uint16_t>(port));
            const auto host = env_string("MINIKV_ENGINE_HOST", "127.0.0.1");
            if (::inet_pton(AF_INET, host.c_str(), &address.sin_addr) != 1) throw std::invalid_argument("MINIKV_ENGINE_HOST must be an IPv4 address");
            if (::bind(listen_fd_, reinterpret_cast<sockaddr*>(&address), sizeof(address)) != 0) network_error("bind");
            if (::listen(listen_fd_, 256) != 0) network_error("listen");
            add_fd(listen_fd_, 1, EPOLLIN);
            add_fd(completed_fd_, 2, EPOLLIN);
            std::cout << "MiniKV engine listening on " << host << ':' << port << std::endl;
        } catch (...) {
            release();
            throw;
        }
    }

    ~Server() {
        // Finish accepted tasks before closing the completion eventfd or engine.
        pool_.shutdown();
        stats_pool_.shutdown();
        release();
    }

    void run() {
        std::array<epoll_event, 128> events{};
        while (true) {
            if (stopping.load(std::memory_order_relaxed) && !draining_) begin_shutdown();
            if (draining_ && (clients_.empty() || Clock::now() >= shutdown_deadline_)) break;
            const int count = ::epoll_wait(epoll_fd_, events.data(), static_cast<int>(events.size()), 100);
            if (count < 0) {
                if (errno == EINTR) continue;
                network_error("epoll_wait");
            }
            for (int i = 0; i < count; ++i) {
                const uint64_t id = events[i].data.u64;
                if (id == 1) {
                    if (!draining_) accept_clients();
                    continue;
                }
                if (id == 2) { finish_requests(); continue; }
                if (clients_.find(id) == clients_.end()) continue;
                const auto flags = events[i].events;
                if (flags & (EPOLLERR | EPOLLHUP)) { close_client(id); continue; }
                if (!draining_ && (flags & (EPOLLIN | EPOLLRDHUP))) read_client(id);
                if (clients_.find(id) != clients_.end() && !clients_.at(id).output.empty()) write_client(id);
            }
            const auto now = Clock::now();
            std::vector<uint64_t> expired;
            for (const auto& entry : clients_) {
                if (!draining_ && now - entry.second.active >= idle_timeout_) expired.push_back(entry.first);
            }
            for (auto id : expired) close_client(id);
        }
        // Slow peers have a bounded response grace period. Admitted storage
        // work must still finish before main() flushes and closes the engine.
        while (!clients_.empty()) close_client(clients_.begin()->first);
        pool_.shutdown();
        stats_pool_.shutdown();
    }

private:
    void begin_shutdown() {
        draining_ = true;
        shutdown_deadline_ = Clock::now() + std::chrono::seconds(5);
        ::epoll_ctl(epoll_fd_, EPOLL_CTL_DEL, listen_fd_, nullptr);
        ::close(listen_fd_);
        listen_fd_ = -1;
        std::vector<uint64_t> idle;
        for (const auto& entry : clients_) {
            if (!entry.second.busy && entry.second.output.empty()) idle.push_back(entry.first);
        }
        for (auto id : idle) close_client(id);
    }

    void add_fd(int fd, uint64_t id, uint32_t flags) {
        epoll_event event{};
        event.events = flags;
        event.data.u64 = id;
        if (::epoll_ctl(epoll_fd_, EPOLL_CTL_ADD, fd, &event) != 0) network_error("epoll add");
    }

    void arm(uint64_t id, uint32_t flags) {
        auto& client = clients_.at(id);
        epoll_event event{};
        event.events = flags | EPOLLRDHUP;
        event.data.u64 = id;
        if (::epoll_ctl(epoll_fd_, client.registered ? EPOLL_CTL_MOD : EPOLL_CTL_ADD, client.fd, &event) != 0) {
            close_client(id);
            return;
        }
        client.registered = true;
    }

    void close_client(uint64_t id) {
        auto it = clients_.find(id);
        if (it == clients_.end()) return;
        if (it->second.registered) ::epoll_ctl(epoll_fd_, EPOLL_CTL_DEL, it->second.fd, nullptr);
        ::close(it->second.fd);
        clients_.erase(it);
        connections_.fetch_sub(1, std::memory_order_relaxed);
    }

    void accept_clients() {
        // Bound each accept pass so established clients continue making progress.
        for (int i = 0; i < 128 && !stopping.load(std::memory_order_relaxed); ++i) {
            const int fd = ::accept4(listen_fd_, nullptr, nullptr, SOCK_NONBLOCK | SOCK_CLOEXEC);
            if (fd < 0) {
                if (errno == EINTR) continue;
                if (errno != EAGAIN && errno != EWOULDBLOCK) std::cerr << "accept: " << std::strerror(errno) << '\n';
                return;
            }
            if (clients_.size() >= max_connections_) {
                connections_rejected_.fetch_add(1, std::memory_order_relaxed);
                ::close(fd);
                continue;
            }
            const uint64_t id = next_id_++;
            clients_.emplace(id, Client{fd, {}, {}});
            connections_.fetch_add(1, std::memory_order_relaxed);
            arm(id, EPOLLIN);
        }
    }

    void respond(uint64_t id, Response response, bool close_after = false) {
        auto& client = clients_.at(id);
        client.output = codec::response(response);
        client.sent = 0;
        client.busy = false;
        client.close_after_write = close_after;
        arm(id, EPOLLOUT);
    }

    bool dispatch(uint64_t id) {
        if (draining_ || stopping.load(std::memory_order_relaxed)) return false;
        auto& client = clients_.at(id);
        if (client.input.size() < codec::kRequestHeader) return false;
        Request request;
        size_t size;
        try {
            size = codec::request_size(client.input);
            if (client.input.size() < size) return false;
            request = codec::decode_request(std::string_view(client.input).substr(0, size));
        } catch (const std::exception& error) {
            respond(id, {Status::Invalid, error.what()}, true);
            return true;
        }
        client.input.erase(0, size);
        const bool stats_request = request.operation == Operation::Stats;
        auto& target_pool = stats_request ? stats_pool_ : pool_;
        const bool admitted = target_pool.enqueue([this, id, request = std::move(request)] {
            Response response;
            try {
                response = request.operation == Operation::Stats
                    ? Response{Status::Value, stats_json()} : engine_.execute(request);
            }
            catch (const std::exception& error) { response = {Status::IOError, error.what()}; }
            {
                std::lock_guard<std::mutex> lock(completion_mutex_);
                completions_.push_back({id, std::move(response)});
            }
            const uint64_t one = 1;
            while (::write(completed_fd_, &one, sizeof(one)) < 0 && errno == EINTR) {}
        });
        if (!admitted) {
            if (!stats_request) requests_rejected_.fetch_add(1, std::memory_order_relaxed);
            respond(id, {Status::Busy, "request queue is full"});
            return true;
        }
        client.busy = true;
        // Only one request per connection executes at a time, preserving order.
        ::epoll_ctl(epoll_fd_, EPOLL_CTL_DEL, client.fd, nullptr);
        client.registered = false;
        return true;
    }

    void read_client(uint64_t id) {
        auto& client = clients_.at(id);
        if (client.busy || !client.output.empty()) return;
        if (dispatch(id)) return;
        std::array<char, 8192> buffer{};
        constexpr size_t limit = codec::kRequestHeader + kMaxKeySize + kMaxValueSize;
        while (true) {
            if (stopping.load(std::memory_order_relaxed)) { close_client(id); return; }
            const size_t space = std::min(buffer.size(), limit - client.input.size());
            const ssize_t count = ::recv(client.fd, buffer.data(), space, 0);
            if (count < 0) {
                if (errno == EINTR) continue;
                if (errno == EAGAIN || errno == EWOULDBLOCK) return;
                close_client(id);
                return;
            }
            if (count == 0) { close_client(id); return; }
            client.input.append(buffer.data(), static_cast<size_t>(count));
            client.active = Clock::now();
            if (dispatch(id)) return;
        }
    }

    void write_client(uint64_t id) {
        auto& client = clients_.at(id);
        while (client.sent < client.output.size()) {
            const ssize_t count = ::send(client.fd, client.output.data() + client.sent,
                                         client.output.size() - client.sent, MSG_NOSIGNAL);
            if (count < 0) {
                if (errno == EINTR) continue;
                if (errno == EAGAIN || errno == EWOULDBLOCK) return;
                close_client(id);
                return;
            }
            if (count == 0) { close_client(id); return; }
            client.sent += static_cast<size_t>(count);
            client.active = Clock::now();
        }
        client.output.clear();
        if (draining_ || stopping.load(std::memory_order_relaxed) || client.close_after_write) {
            close_client(id);
            return;
        }
        if (!dispatch(id)) arm(id, EPOLLIN);
    }

    void finish_requests() {
        uint64_t count;
        while (::read(completed_fd_, &count, sizeof(count)) == sizeof(count)) {}
        std::vector<Completion> ready;
        {
            std::lock_guard<std::mutex> lock(completion_mutex_);
            ready.swap(completions_);
        }
        for (auto& completion : ready) {
            // IDs, unlike descriptors, cannot be reused by a later connection.
            if (clients_.find(completion.id) == clients_.end()) continue;
            respond(completion.id, std::move(completion.response));
            if (clients_.find(completion.id) != clients_.end()) write_client(completion.id);
        }
    }

    std::string stats_json() const {
        // Sample independently, never nest pool/state locks or touch reactor-
        // owned containers from a worker. No paths, errors or user data escape.
        const auto engine = engine_.stats();
        const auto pool = pool_.stats();
        std::ostringstream out;
        out << std::boolalpha << "{\"schema_version\":1,\"engine\":{\"wal_mode\":\""
            << (engine.wal_mode == WalMode::Reliable ? "reliable" : "throughput") << '"';
        const auto field = [&](const char* name, auto value) { out << ",\"" << name << "\":" << value; };
        field("keys", engine.keys);
        field("applied_sequence", engine.applied_sequence);
        field("durable_sequence", engine.durable_sequence);
        field("wal_pending_bytes", engine.wal_pending_bytes);
        field("wal_inflight_bytes", engine.wal_inflight_bytes);
        field("wal_queued_records", engine.wal_queued_records);
        field("wal_queue_capacity_bytes", engine.wal_queue_capacity_bytes);
        field("wal_commits_total", engine.wal_commits_total);
        field("wal_commit_failures_total", engine.wal_commit_failures_total);
        field("wal_commit_duration_ns_total", engine.wal_commit_duration_ns_total);
        field("wal_commit_last_duration_ns", engine.wal_commit_last_duration_ns);
        field("snapshot_successes_total", engine.snapshot_successes_total);
        field("snapshot_failures_total", engine.snapshot_failures_total);
        field("snapshot_in_progress", engine.snapshot_in_progress);
        field("snapshot_sequence", engine.snapshot_sequence);
        field("snapshot_capture_duration_ns_total", engine.snapshot_capture_duration_ns_total);
        field("snapshot_write_duration_ns_total", engine.snapshot_write_duration_ns_total);
        field("snapshot_compact_duration_ns_total", engine.snapshot_compact_duration_ns_total);
        field("io_failed", engine.io_failed);
        field("stopping", engine.stopping);
        out << "},\"server\":{\"connections\":" << connections_.load(std::memory_order_relaxed);
        field("connection_capacity", max_connections_);
        field("request_queue_depth", pool.queued);
        field("request_queue_capacity", pool.capacity);
        field("workers_active", pool.active);
        field("workers_capacity", pool.workers);
        field("requests_rejected_total", requests_rejected_.load(std::memory_order_relaxed));
        field("connections_rejected_total", connections_rejected_.load(std::memory_order_relaxed));
        out << "}}";
        return out.str();
    }

    void release() noexcept {
        for (const auto& entry : clients_) ::close(entry.second.fd);
        clients_.clear();
        connections_.store(0, std::memory_order_relaxed);
        if (listen_fd_ >= 0) { ::close(listen_fd_); listen_fd_ = -1; }
        if (completed_fd_ >= 0) { ::close(completed_fd_); completed_fd_ = -1; }
        if (epoll_fd_ >= 0) { ::close(epoll_fd_); epoll_fd_ = -1; }
    }

    Engine& engine_;
    ThreadPool pool_;
    // A bounded control query can run while every data worker awaits WAL sync.
    ThreadPool stats_pool_{1, 1};
    size_t max_connections_;
    std::chrono::milliseconds idle_timeout_;
    bool draining_ = false;
    Clock::time_point shutdown_deadline_;
    int listen_fd_ = -1, completed_fd_ = -1, epoll_fd_ = -1;
    uint64_t next_id_ = 3;
    std::unordered_map<uint64_t, Client> clients_;
    std::atomic<uint64_t> connections_{0};
    std::atomic<uint64_t> requests_rejected_{0};
    std::atomic<uint64_t> connections_rejected_{0};
    std::mutex completion_mutex_;
    std::vector<Completion> completions_;
};
} // namespace

int main(int argc, char** argv) {
    try {
        EngineConfig config;
        if (argc == 2 && std::string(argv[1]) == "--import-legacy") config.import_legacy = true;
        else if (argc != 1) {
            std::cerr << "usage: engine [--import-legacy]\n";
            return 2;
        }
        config.data_dir = env_string("MINIKV_DATA_DIR", "./data");
        const auto mode = env_string("MINIKV_WAL_MODE", "throughput");
        if (mode != "throughput" && mode != "reliable") throw std::invalid_argument("MINIKV_WAL_MODE must be throughput or reliable");
        config.wal_mode = mode == "reliable" ? WalMode::Reliable : WalMode::Throughput;
        config.wal_batch_size = env_int("MINIKV_WAL_BATCH_SIZE", 512, 1, 65536);
        config.wal_queue_bytes = env_int("MINIKV_WAL_QUEUE_BYTES", 16 * 1024 * 1024, 2 * 1024 * 1024, 1024 * 1024 * 1024);
        config.wal_flush_interval = std::chrono::milliseconds(env_int("MINIKV_WAL_FLUSH_MS", 100, 1, 60000));
        config.snapshot_interval = std::chrono::milliseconds(env_int("MINIKV_SNAPSHOT_INTERVAL_MS", 1200000, 0, 86400000));
        Engine engine(config);
        if (config.import_legacy) {
            engine.close();
            std::cout << "Legacy import complete; original data.db and wal.log retained.\n";
            return 0;
        }
        std::signal(SIGTERM, stop_server);
        std::signal(SIGINT, stop_server);
        {
            Server server(engine);
            server.run();
        }
        engine.close();
    } catch (const std::exception& error) {
        std::cerr << "MiniKV: " << error.what() << '\n';
        return 1;
    }
    return 0;
}
