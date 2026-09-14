#include "engine.h"
#include "codec.h"
#include "threadpool.h"

#include <arpa/inet.h>
#include <array>
#include <atomic>
#include <cassert>
#include <cerrno>
#include <csignal>
#include <cstdlib>
#include <cstring>
#include <exception>
#include <iostream>
#include <memory>
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

class CompletionSink;

struct RequestTicket {
    std::weak_ptr<CompletionSink> sink;
    bool stats_request = false;
    ~RequestTicket();
};

struct Completion {
    uint64_t id;
    Response response;
    std::shared_ptr<RequestTicket> ticket;
};

// A callback owns this target independently of Server and never accesses a
// socket. Tickets cover queued, executing, durable-waiting and completed work,
// including requests whose connection has already gone away.
class CompletionSink : public std::enable_shared_from_this<CompletionSink> {
public:
    explicit CompletionSink(size_t capacity) : capacity_(capacity) {
        completions_.reserve(capacity_ + 2);
        fd_ = ::eventfd(0, EFD_NONBLOCK | EFD_CLOEXEC);
        if (fd_ < 0) network_error("eventfd");
    }

    ~CompletionSink() { if (fd_ >= 0) ::close(fd_); }

    int fd() const { return fd_; }
    size_t capacity() const { return capacity_; }

    std::shared_ptr<RequestTicket> acquire(bool stats_request) {
        {
            std::lock_guard<std::mutex> lock(mutex_);
            auto& used = stats_request ? stats_inflight_ : data_inflight_;
            if (used == (stats_request ? 2 : capacity_)) return {};
            ++used;
        }
        try {
            // Construct in place: a temporary ticket would release the permit.
            auto ticket = std::make_shared<RequestTicket>();
            ticket->sink = shared_from_this();
            ticket->stats_request = stats_request;
            return ticket;
        } catch (...) {
            release(stats_request);
            throw;
        }
    }

    void release(bool stats_request) noexcept {
        std::lock_guard<std::mutex> lock(mutex_);
        auto& used = stats_request ? stats_inflight_ : data_inflight_;
        assert(used != 0);
        --used;
    }

    size_t inflight() const {
        std::lock_guard<std::mutex> lock(mutex_);
        return data_inflight_;
    }

    void post(Completion completion) noexcept {
        {
            std::lock_guard<std::mutex> lock(mutex_);
            // Every entry retains its admission ticket. Both buffers reserve
            // the full bound, so publishing never allocates or waits for space.
            assert(completions_.size() < capacity_ + 2);
            completions_.push_back(std::move(completion));
        }
        const uint64_t one = 1;
        ssize_t written;
        do { written = ::write(fd_, &one, sizeof(one)); } while (written < 0 && errno == EINTR);
        // A saturated eventfd is already readable; all other errors are raised
        // by the reactor, never from an Engine completion callback.
        if (written < 0 && errno != EAGAIN) notification_error_.store(errno, std::memory_order_relaxed);
    }

    void take(std::vector<Completion>& ready) {
        assert(ready.empty());
        std::lock_guard<std::mutex> lock(mutex_);
        ready.swap(completions_);
    }

    void check_notification() const {
        const int error = notification_error_.load(std::memory_order_relaxed);
        if (error) throw std::runtime_error(std::string("completion eventfd: ") + std::strerror(error));
    }

private:
    const size_t capacity_;
    size_t data_inflight_ = 0, stats_inflight_ = 0;
    int fd_ = -1;
    mutable std::mutex mutex_;
    std::vector<Completion> completions_;
    std::atomic<int> notification_error_{0};
};

RequestTicket::~RequestTicket() {
    if (auto target = sink.lock()) target->release(stats_request);
}

// The reactor owns every socket and buffer. Workers receive complete requests
// only; idle connections and partial frames never occupy a worker.
class Server {
public:
    Server(Engine& engine, size_t workers, size_t queue_capacity)
        : engine_(engine),
          asynchronous_replies_(engine.stats().wal_mode == WalMode::Reliable),
          pool_(workers, queue_capacity),
          sink_(std::make_shared<CompletionSink>(workers + queue_capacity)),
          max_connections_(env_int("MINIKV_MAX_CONNECTIONS", 256, 1, 65536)),
          idle_timeout_(env_int("MINIKV_CLIENT_IDLE_MS", 30000, 1, 3600000)) {
        try {
            ready_completions_.reserve(sink_->capacity() + 2);
            epoll_fd_ = ::epoll_create1(EPOLL_CLOEXEC);
            if (epoll_fd_ < 0) network_error("epoll_create1");
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
            add_fd(sink_->fd(), 2, EPOLLIN);
            std::cout << "MiniKV engine listening on " << host << ':' << port << std::endl;
        } catch (...) {
            release();
            throw;
        }
    }

    ~Server() {
        try { shutdown(); }
        catch (const std::exception& error) { std::cerr << "MiniKV shutdown: " << error.what() << '\n'; }
        catch (...) { std::cerr << "MiniKV shutdown: unknown failure\n"; }
    }

    void run() {
        std::exception_ptr failure;
        try { reactor_loop(); }
        catch (...) {
            failure = std::current_exception();
            // A failed reactor still attempts the remaining bounded response
            // grace period. Broken network infrastructure may end it early.
            begin_shutdown();
            try { reactor_loop(); } catch (...) {}
        }
        try { shutdown(); }
        catch (...) {
            if (!failure) throw;
            try { throw; }
            catch (const std::exception& error) { std::cerr << "MiniKV shutdown: " << error.what() << '\n'; }
            catch (...) { std::cerr << "MiniKV shutdown: unknown failure\n"; }
        }
        if (failure) std::rethrow_exception(failure);
    }

private:
    void reactor_loop() {
        std::array<epoll_event, 128> events{};
        while (true) {
            sink_->check_notification();
            if (stopping.load(std::memory_order_relaxed) && !draining_) begin_shutdown();
            if (draining_ && (clients_.empty() || Clock::now() >= shutdown_deadline_)) break;
            if (!ready_completions_.empty()) finish_requests();
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
    }

    void shutdown() {
        if (shutdown_complete_) return;
        begin_shutdown();
        while (!clients_.empty()) close_client(clients_.begin()->first);
        pool_.shutdown();
        stats_pool_.shutdown();
        // Joining submission workers no longer waits for durable responses.
        // The target and its reserved buffers must survive every callback,
        // including callbacks from disconnected clients and final I/O failure.
        std::exception_ptr failure;
        try { engine_.drain_async(); } catch (...) { failure = std::current_exception(); }
        try { engine_.close(); } catch (...) { if (!failure) failure = std::current_exception(); }
        ready_completions_.clear();
        sink_->take(ready_completions_);
        ready_completions_.clear();
        release();
        shutdown_complete_ = true;
        if (failure) std::rethrow_exception(failure);
    }

    void begin_shutdown() {
        if (draining_) return;
        draining_ = true;
        shutdown_deadline_ = Clock::now() + std::chrono::seconds(5);
        ::epoll_ctl(epoll_fd_, EPOLL_CTL_DEL, listen_fd_, nullptr);
        ::close(listen_fd_);
        listen_fd_ = -1;
        for (auto it = clients_.begin(); it != clients_.end();) {
            const auto current = it++;
            if (!current->second.busy && current->second.output.empty()) close_client(current->first);
        }
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
        std::shared_ptr<RequestTicket> ticket;
        bool admitted = false;
        try {
            ticket = sink_->acquire(stats_request);
            if (ticket) {
                const auto deliver = [sink = sink_, ticket, id](Response response) noexcept {
                    sink->post({id, std::move(response), ticket});
                };
                admitted = target_pool.enqueue([this, request = std::move(request), deliver] {
                    try {
                        if (request.operation == Operation::Stats) {
                            deliver({Status::Value, stats_json()});
                        } else if (!asynchronous_replies_) {
                            deliver(engine_.execute(request));
                        } else {
                            // An immediate result never invokes the callback;
                            // nullopt transfers exactly one completion to Engine.
                            auto response = engine_.execute_async(request, deliver);
                            if (response) deliver(std::move(*response));
                        }
                    } catch (...) {
                        // Engine can throw only before taking callback ownership.
                        // An empty failure response also works after bad_alloc.
                        deliver({Status::IOError, {}});
                    }
                });
            }
        } catch (...) {
            ticket.reset();
            respond(id, {Status::IOError, {}}, true);
            return true;
        }
        if (!admitted) {
            ticket.reset();
            if (!stats_request) requests_rejected_.fetch_add(1, std::memory_order_relaxed);
            respond(id, {Status::Busy, "request capacity is full"});
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
        if (ready_completions_.empty()) {
            uint64_t count;
            while (::read(sink_->fd(), &count, sizeof(count)) == sizeof(count)) {}
            sink_->take(ready_completions_);
        }
        // Resuming an older batch after a response exception must not consume
        // the notification belonging to newer entries still held by the sink.
        for (auto& entry : ready_completions_) {
            if (!entry.ticket) continue;
            // Move out each consumed item so a failed response does not retain
            // its value or replay it when the shutdown grace loop resumes.
            auto completion = std::move(entry);
            // IDs, unlike descriptors, cannot be reused by a later connection.
            if (clients_.find(completion.id) == clients_.end()) {
                completion.ticket.reset();
                continue;
            }
            try {
                respond(completion.id, std::move(completion.response));
                // Return capacity before write_client can dispatch the next
                // request. A disconnection alone never returns a ticket.
                completion.ticket.reset();
                if (clients_.find(completion.id) != clients_.end()) write_client(completion.id);
            } catch (...) {
                close_client(completion.id);
                throw;
            }
        }
        ready_completions_.clear();
    }

    std::string stats_json() const {
        // Sample independently, never nest pool/state locks or touch reactor-
        // owned containers from a worker. No paths, errors or user data escape.
        const auto engine = engine_.stats();
        const auto pool = pool_.stats();
        const auto inflight = sink_->inflight();
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
        field("wal_capacity_waiters", engine.wal_capacity_waiters);
        field("wal_capacity_waits_total", engine.wal_capacity_waits_total);
        field("wal_capacity_wait_duration_ns_total", engine.wal_capacity_wait_duration_ns_total);
        field("wal_durable_waiters", engine.wal_durable_waiters);
        field("wal_durable_waits_total", engine.wal_durable_waits_total);
        field("wal_durable_wait_duration_ns_total", engine.wal_durable_wait_duration_ns_total);
        field("async_requests_inflight", engine.async_requests_inflight);
        field("async_requests_capacity", engine.async_requests_capacity);
        field("async_callback_failures_total", engine.async_callback_failures_total);
        field("wal_commits_total", engine.wal_commits_total);
        field("wal_commit_failures_total", engine.wal_commit_failures_total);
        field("wal_commit_duration_ns_total", engine.wal_commit_duration_ns_total);
        field("wal_commit_last_duration_ns", engine.wal_commit_last_duration_ns);
        field("snapshot_successes_total", engine.snapshot_successes_total);
        field("snapshot_failures_total", engine.snapshot_failures_total);
        field("snapshot_in_progress", engine.snapshot_in_progress);
        field("snapshot_sequence", engine.snapshot_sequence);
        field("snapshot_capture_duration_ns_total", engine.snapshot_capture_duration_ns_total);
        field("snapshot_capture_state_lock_acquisitions_total", engine.snapshot_capture_state_lock_acquisitions_total);
        field("snapshot_capture_state_lock_duration_ns_total", engine.snapshot_capture_state_lock_duration_ns_total);
        field("snapshot_capture_state_lock_duration_ns_max", engine.snapshot_capture_state_lock_duration_ns_max);
        field("snapshot_write_duration_ns_total", engine.snapshot_write_duration_ns_total);
        field("snapshot_file_write_calls_total", engine.snapshot_file_write_calls_total);
        field("snapshot_file_written_bytes_total", engine.snapshot_file_written_bytes_total);
        field("snapshot_file_installed_bytes_total", engine.snapshot_file_installed_bytes_total);
        field("snapshot_compact_duration_ns_total", engine.snapshot_compact_duration_ns_total);
        field("snapshot_compact_written_bytes_total", engine.snapshot_compact_written_bytes_total);
        field("io_failed", engine.io_failed);
        field("stopping", engine.stopping);
        out << "},\"server\":{\"connections\":" << connections_.load(std::memory_order_relaxed);
        field("connection_capacity", max_connections_);
        field("request_queue_depth", pool.queued);
        field("request_queue_capacity", pool.capacity);
        field("workers_active", pool.active);
        field("workers_capacity", pool.workers);
        field("requests_inflight", inflight);
        field("requests_capacity", sink_->capacity());
        field("requests_started_total", pool.started_total);
        field("request_queue_wait_duration_ns_total", pool.queue_wait_duration_ns_total);
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
        if (epoll_fd_ >= 0) { ::close(epoll_fd_); epoll_fd_ = -1; }
        sink_.reset();
    }

    Engine& engine_;
    const bool asynchronous_replies_;
    ThreadPool pool_;
    // Control queries have their own workers and two whole-request permits.
    ThreadPool stats_pool_{1, 1};
    std::shared_ptr<CompletionSink> sink_;
    size_t max_connections_;
    std::chrono::milliseconds idle_timeout_;
    bool draining_ = false;
    bool shutdown_complete_ = false;
    Clock::time_point shutdown_deadline_;
    int listen_fd_ = -1, epoll_fd_ = -1;
    uint64_t next_id_ = 3;
    std::unordered_map<uint64_t, Client> clients_;
    std::atomic<uint64_t> connections_{0};
    std::atomic<uint64_t> requests_rejected_{0};
    std::atomic<uint64_t> connections_rejected_{0};
    std::vector<Completion> ready_completions_;
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
        // Import never starts a server and historically ignored server-only
        // settings, so preserve that behavior while parsing runtime limits once.
        const size_t workers = config.import_legacy ? 20 : env_int("MINIKV_WORKERS", 20, 1, 1024);
        const size_t queue_capacity = config.import_legacy ? 128 : env_int("MINIKV_REQUEST_QUEUE_SIZE", 128, 1, 65536);
        config.max_async_requests = workers + queue_capacity;
        Engine engine(config);
        if (config.import_legacy) {
            engine.close();
            std::cout << "Legacy import complete; original data.db and wal.log retained.\n";
            return 0;
        }
        std::signal(SIGTERM, stop_server);
        std::signal(SIGINT, stop_server);
        {
            Server server(engine, workers, queue_capacity);
            server.run();
        }
    } catch (const std::exception& error) {
        std::cerr << "MiniKV: " << error.what() << '\n';
        return 1;
    }
    return 0;
}
