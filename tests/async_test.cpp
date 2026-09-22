#include "engine.h"
#include "codec.h"

#include <atomic>
#include <array>
#include <chrono>
#include <condition_variable>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <future>
#include <iostream>
#include <memory>
#include <mutex>
#include <stdexcept>
#include <thread>
#include <unistd.h>
#include <vector>

using namespace minikv;
using namespace std::chrono_literals;
namespace fs = std::filesystem;

namespace {

void require(bool condition, const std::string& message) {
    if (!condition) throw std::runtime_error(message);
}

template <class Fn> void must_fail(Fn fn, const std::string& message) {
    bool failed = false;
    try { fn(); } catch (const std::exception&) { failed = true; }
    require(failed, message);
}

struct TempDir {
    std::string path;
    TempDir() {
        std::string pattern = (fs::temp_directory_path() / "minikv-async-XXXXXX").string();
        const auto created = ::mkdtemp(pattern.data());
        if (!created) throw std::runtime_error("mkdtemp failed");
        path = created;
    }
    ~TempDir() { std::error_code error; fs::remove_all(path, error); }
};

EngineConfig config_for(const TempDir& dir) {
    EngineConfig config;
    config.data_dir = dir.path;
    config.wal_mode = WalMode::Reliable;
    config.wal_batch_size = 1;
    config.wal_flush_interval = 1ms;
    config.snapshot_interval = 0ms;
    return config;
}

// A real I/O/callback boundary, with bounded cleanup if a test fails early.
struct Gate {
    std::mutex mutex;
    std::condition_variable changed;
    bool entered = false;
    bool released = false;

    bool block() {
        std::unique_lock<std::mutex> lock(mutex);
        entered = true;
        changed.notify_all();
        return changed.wait_for(lock, 5s, [&] { return released; });
    }
    void pause() { if (!block()) throw std::runtime_error("test gate release timed out"); }
    bool wait() {
        std::unique_lock<std::mutex> lock(mutex);
        return changed.wait_for(lock, 2s, [&] { return entered; });
    }
    void release() {
        std::lock_guard<std::mutex> lock(mutex);
        released = true;
        changed.notify_all();
    }
};

struct Reply {
    struct State {
        std::mutex mutex;
        std::condition_variable changed;
        std::vector<Response> responses;
    };
    std::shared_ptr<State> state = std::make_shared<State>();

    AsyncCompletion callback() const {
        return [state = state](Response response) {
            std::lock_guard<std::mutex> lock(state->mutex);
            state->responses.push_back(std::move(response));
            state->changed.notify_all();
        };
    }
    size_t count() const {
        std::lock_guard<std::mutex> lock(state->mutex);
        return state->responses.size();
    }
    Response get() const {
        std::unique_lock<std::mutex> lock(state->mutex);
        require(state->changed.wait_for(lock, 2s, [&] { return !state->responses.empty(); }), "async reply timed out");
        return state->responses.front();
    }
};

template <class Predicate> bool await_stats(Engine& engine, Predicate predicate) {
    const auto deadline = std::chrono::steady_clock::now() + 2s;
    do {
        if (predicate(engine.stats())) return true;
        std::this_thread::sleep_for(1ms);
    } while (std::chrono::steady_clock::now() < deadline);
    return predicate(engine.stats());
}

void require_reset(const EngineStats& stats) {
    require(stats.async_requests_inflight == 0 && stats.async_callback_failures_total == 0 &&
            stats.wal_capacity_waiters == 0 && stats.wal_capacity_waits_total == 0 &&
            stats.wal_capacity_wait_duration_ns_total == 0 && stats.wal_durable_waiters == 0 &&
            stats.wal_durable_waits_total == 0 && stats.wal_durable_wait_duration_ns_total == 0,
            "new/recovered engine inherited request accounting");
}

void immediate_responses_and_validation() {
    TempDir dir;
    auto config = config_for(dir);
    config.max_async_requests = 0;
    must_fail([&] { Engine invalid(config); }, "zero async capacity was accepted");
    config.max_async_requests = 2;
    Engine engine(config);
    Reply unused;
    must_fail([&] { engine.execute_async({Operation::Put, "key", "value"}, {}); }, "empty completion was accepted");
    auto invalid = engine.execute_async({Operation::Stats, {}, {}}, unused.callback());
    require(invalid && invalid->status == Status::Invalid && engine.stats().applied_sequence == 0,
            "invalid/empty-callback request changed state");
    auto missing = engine.execute_async({Operation::Get, "key", {}}, unused.callback());
    require(missing && missing->status == Status::NotFound, "empty reliable GET was deferred");
    require_reset(engine.stats());
    require(engine.execute({Operation::Put, "key", "value"}).status == Status::Ok, "synchronous API failed");
    const auto before = engine.stats();
    auto found = engine.execute_async({Operation::Get, "key", {}}, unused.callback());
    auto absent = engine.execute_async({Operation::Get, "missing", {}}, unused.callback());
    engine.drain_async();
    const auto after = engine.stats();
    require(found && found->status == Status::Value && found->value == "value" &&
            absent && absent->status == Status::NotFound && unused.count() == 0,
            "immediate response also invoked its callback");
    require(after.wal_durable_waits_total == before.wal_durable_waits_total &&
            after.wal_durable_wait_duration_ns_total == before.wal_durable_wait_duration_ns_total &&
            after.async_requests_capacity == 2 && after.async_requests_inflight == 0,
            "already durable GET added a wait or async slot");
    engine.close();
    auto stopped = engine.execute_async({Operation::Put, "after-close", "value"}, unused.callback());
    require(stopped && stopped->status == Status::Busy && unused.count() == 0, "closed engine accepted an async callback");

    TempDir throughput_dir;
    config = config_for(throughput_dir);
    config.wal_mode = WalMode::Throughput;
    Engine throughput(config);
    auto written = throughput.execute_async({Operation::Put, "key", "value"}, unused.callback());
    auto deleted = throughput.execute_async({Operation::Delete, "missing", {}}, unused.callback());
    throughput.drain_async();
    require(written && written->status == Status::Ok && deleted && deleted->status == Status::NotFound &&
            unused.count() == 0, "throughput operation was deferred");
    require_reset(throughput.stats());
}

void throughput_drain_does_not_wait_for_wal() {
    TempDir dir;
    auto config = config_for(dir);
    config.wal_mode = WalMode::Throughput;
    Gate first_sync;
    std::atomic<bool> paused{false};
    config.io_hook = [&](const std::string& point) {
        if (point == "wal.sync" && !paused.exchange(true)) first_sync.pause();
    };
    Engine engine(config);
    Reply unused;
    const auto first = engine.execute({Operation::Put, "first", "one"});
    const bool syncing = first_sync.wait();
    auto writing = std::async(std::launch::async, [&] {
        return engine.execute({Operation::Put, "second", "two"});
    });
    auto submitting = std::async(std::launch::async, [&] {
        return engine.execute_async({Operation::Put, "third", "three"}, unused.callback());
    });
    auto reading = std::async(std::launch::async, [&] {
        return engine.execute_async({Operation::Get, "first", {}}, unused.callback());
    });
    const bool written = writing.wait_for(300ms) == std::future_status::ready;
    const bool submitted = submitting.wait_for(300ms) == std::future_status::ready;
    const bool read = reading.wait_for(300ms) == std::future_status::ready;
    if (!syncing || !written || !submitted || !read) first_sync.release();
    require(syncing && written && submitted && read, "throughput request waited for paused WAL durability");
    const auto second = writing.get();
    const auto third = submitting.get();
    const auto visible = reading.get();
    const auto before_drain = engine.stats();
    // All submitting calls have returned: drain only waits for transferred
    // callbacks, and must leave this throughput WAL batch untouched.
    auto draining = std::async(std::launch::async, [&] { engine.drain_async(); });
    const bool drained = draining.wait_for(300ms) == std::future_status::ready;
    if (!drained) first_sync.release();
    require(drained, "throughput drain waited for paused WAL durability");
    draining.get();
    const auto after_drain = engine.stats();
    auto closing = std::async(std::launch::async, [&] { engine.close(); });
    const bool close_started = await_stats(engine, [](const EngineStats& stats) { return stats.stopping; });
    const bool close_waited = closing.wait_for(50ms) == std::future_status::timeout;
    first_sync.release();
    closing.get();
    require(first.status == Status::Ok && second.status == Status::Ok &&
            third && third->status == Status::Ok && visible && visible->status == Status::Value &&
            visible->value == "one" && unused.count() == 0,
            "throughput immediate response transferred a callback or lost visible state");
    require(before_drain.applied_sequence == 3 && before_drain.durable_sequence == 0 &&
            before_drain.wal_pending_bytes > 0 && before_drain.wal_inflight_bytes > 0 &&
            after_drain.applied_sequence == 3 && after_drain.durable_sequence == 0 &&
            after_drain.wal_pending_bytes == before_drain.wal_pending_bytes &&
            after_drain.wal_inflight_bytes == before_drain.wal_inflight_bytes && !after_drain.stopping,
            "throughput drain flushed WAL or stopped the engine");
    require_reset(before_drain);
    require_reset(after_drain);
    const auto closed = engine.stats();
    require(close_started && close_waited && closed.applied_sequence == 3 && closed.durable_sequence == 3 &&
            closed.wal_pending_bytes == 0 && closed.wal_inflight_bytes == 0 &&
            closed.stopping && !closed.io_failed,
            "throughput close did not wait for and flush its remaining WAL");
    engine.close();
    engine.drain_async();
    engine.drain_async();
    require_reset(engine.stats());
    config.io_hook = {};
    Engine recovered(config);
    require(recovered.execute({Operation::Get, "first", {}}).value == "one" &&
            recovered.execute({Operation::Get, "second", {}}).value == "two" &&
            recovered.execute({Operation::Get, "third", {}}).value == "three" &&
            recovered.stats().durable_sequence == 3,
            "throughput close did not preserve all immediate responses across recovery");
    require_reset(recovered.stats());
}

void batches_and_captured_reads() {
    TempDir dir;
    auto config = config_for(dir);
    Gate first_sync, second_sync;
    std::atomic<unsigned> syncs{0};
    config.io_hook = [&](const std::string& point) {
        if (point != "wal.sync") return;
        const auto batch = syncs.fetch_add(1);
        if (batch == 0) first_sync.pause();
        else if (batch == 1) second_sync.pause();
    };
    Engine engine(config);
    Reply first, old_read, second, missing_delete, new_read;
    require(!engine.execute_async({Operation::Put, "shared", "one"}, first.callback()), "reliable PUT did not transfer its callback");
    const bool first_syncing = first_sync.wait();
    require(!engine.execute_async({Operation::Get, "shared", {}}, old_read.callback()), "undurable GET returned immediately");
    require(!engine.execute_async({Operation::Put, "shared", "two"}, second.callback()), "second PUT was not deferred");
    require(!engine.execute_async({Operation::Delete, "absent", {}}, missing_delete.callback()), "DELETE miss bypassed confirmation");
    require(!engine.execute_async({Operation::Get, "shared", {}}, new_read.callback()), "dependent GET returned immediately");
    const auto pending = engine.stats();
    auto draining = std::async(std::launch::async, [&] { engine.drain_async(); });
    const bool drain_waited = draining.wait_for(50ms) == std::future_status::timeout;
    const bool no_early_reply = first.count() + old_read.count() + second.count() + missing_delete.count() + new_read.count() == 0;
    first_sync.release();
    const bool second_syncing = second_sync.wait();
    const auto first_result = first.get(), old_value = old_read.get();
    const auto partial = engine.stats();
    const bool later_waited = second.count() == 0 && missing_delete.count() == 0 && new_read.count() == 0;
    second_sync.release();
    const auto second_result = second.get(), deleted = missing_delete.get(), new_value = new_read.get();
    draining.get();
    require(first_syncing && second_syncing && drain_waited && no_early_reply && later_waited,
            "async reply/drain bypassed its WAL batch");
    require(pending.applied_sequence == 3 && pending.durable_sequence == 0 &&
            pending.async_requests_inflight == 5 && pending.wal_durable_waiters == 5 &&
            pending.wal_durable_waits_total == 0 && pending.wal_durable_wait_duration_ns_total == 0,
            "registered waits did not retain their targets or live accounting");
    require(first_result.status == Status::Ok && old_value.status == Status::Value && old_value.value == "one" &&
            partial.durable_sequence == 1 && partial.wal_durable_waits_total == 2 && partial.wal_durable_waiters == 3,
            "first commit acknowledged later state or GET reread a newer value");
    require(second_result.status == Status::Ok && deleted.status == Status::NotFound &&
            new_value.status == Status::Value && new_value.value == "two", "deferred response changed operation semantics");
    const auto done = engine.stats();
    require(done.applied_sequence == 3 && done.durable_sequence == 3 && done.async_requests_inflight == 0 &&
            done.wal_durable_waiters == 0 && done.wal_durable_waits_total == 5 &&
            done.wal_durable_wait_duration_ns_total > 0 && !done.stopping,
            "drain stopped the engine or lost completed waits");
    require(first.count() == 1 && old_read.count() == 1 && second.count() == 1 &&
            missing_delete.count() == 1 && new_read.count() == 1, "callback was invoked more than once");
    engine.close();
    config.io_hook = {};
    Engine recovered(config);
    Reply unused;
    const auto recovered_value = recovered.execute_async({Operation::Get, "shared", {}}, unused.callback());
    require(recovered_value && recovered_value->value == "two" && recovered.stats().durable_sequence == 3 &&
            unused.count() == 0, "asynchronously confirmed data did not recover");
    require_reset(recovered.stats());
}

void slots_cover_callback_resources() {
    TempDir dir;
    auto config = config_for(dir);
    config.max_async_requests = 1;
    Gate callback_gate, destructor_gate;
    std::atomic<uint64_t> destructor_inflight{0};
    std::atomic<bool> destructor_timeout{false};
    Engine engine(config);
    struct Capture {
        Engine& engine;
        Gate& gate;
        std::atomic<uint64_t>& inflight;
        std::atomic<bool>& timeout;
        Capture(Engine& owner, Gate& release, std::atomic<uint64_t>& observed, std::atomic<bool>& timed_out)
            : engine(owner), gate(release), inflight(observed), timeout(timed_out) {}
        ~Capture() {
            inflight = engine.stats().async_requests_inflight;
            if (!gate.block()) timeout = true;
        }
    };
    auto capture = std::make_shared<Capture>(engine, destructor_gate, destructor_inflight, destructor_timeout);
    // Construct directly, without retaining another callback/capture owner.
    auto pending = engine.execute_async({Operation::Put, "first", "one"},
        [capture = std::move(capture), &callback_gate](Response response) {
            require(response.status == Status::Ok, "callback received wrong status");
            callback_gate.pause();
        });
    const bool executing = callback_gate.wait();
    const auto during = engine.stats();
    Reply unused;
    auto rejected = engine.execute_async({Operation::Put, "rejected", "value"}, unused.callback());
    const auto after_rejection = engine.stats();
    auto ready_read = engine.execute_async({Operation::Get, "first", {}}, unused.callback());
    // The direct API retains its semantics even when all async slots are held.
    const auto synchronous = engine.execute({Operation::Put, "sync", "value"});
    auto draining = std::async(std::launch::async, [&] { engine.drain_async(); });
    const bool callback_blocked_drain = draining.wait_for(50ms) == std::future_status::timeout;
    callback_gate.release();
    const bool destroying = destructor_gate.wait();
    const bool destructor_blocked_drain = draining.wait_for(50ms) == std::future_status::timeout;
    destructor_gate.release();
    draining.get();
    require(!pending && executing && destroying && callback_blocked_drain && destructor_blocked_drain,
            "async slot/drain did not cover callback execution and destruction");
    require(during.async_requests_inflight == 1 && during.wal_durable_waiters == 0 &&
            during.wal_durable_waits_total == 1 && destructor_inflight == 1 && !destructor_timeout,
            "callback resources were released under the state lock or after releasing their slot");
    require(rejected && rejected->status == Status::Busy &&
            after_rejection.applied_sequence == during.applied_sequence && after_rejection.keys == during.keys &&
            after_rejection.wal_pending_bytes == during.wal_pending_bytes && unused.count() == 0,
            "async admission rejection applied a write or invoked a callback");
    require(ready_read && ready_read->value == "one" && synchronous.status == Status::Ok &&
            engine.execute({Operation::Get, "rejected", {}}).status == Status::NotFound,
            "async capacity changed immediate reads or the synchronous API");
    Reply later;
    require(!engine.execute_async({Operation::Delete, "sync", {}}, later.callback()), "released async slot could not be reused");
    require(later.get().status == Status::Ok, "later callback was lost");
    engine.drain_async();
    require(engine.stats().async_requests_inflight == 0 && engine.stats().async_callback_failures_total == 0,
            "successful callback left a slot or failure behind");
}

void wal_failures_complete_callbacks() {
    for (const std::string failure_point : {"wal.write", "wal.sync"}) {
        TempDir dir;
        auto config = config_for(dir);
        Gate failing_io;
        config.io_hook = [&](const std::string& point) {
            if (point == failure_point) { failing_io.pause(); throw std::runtime_error("injected async WAL failure"); }
        };
        Engine engine(config);
        Reply put, get, missing_delete, unused;
        require(!engine.execute_async({Operation::Put, "key", "value"}, put.callback()), "PUT was not deferred");
        const bool reached_io = failing_io.wait();
        require(!engine.execute_async({Operation::Get, "key", {}}, get.callback()), "GET bypassed pending WAL");
        require(!engine.execute_async({Operation::Delete, "missing", {}}, missing_delete.callback()), "DELETE bypassed pending WAL");
        failing_io.release();
        const auto written = put.get(), read = get.get(), deleted = missing_delete.get();
        engine.drain_async();
        const auto failed = engine.stats();
        auto rejected = engine.execute_async({Operation::Put, "later", "value"}, unused.callback());
        require(reached_io && written.status == Status::IOError && read.status == Status::IOError &&
                deleted.status == Status::IOError && rejected && rejected->status == Status::IOError,
                "WAL failure acknowledged an async request");
        require(failed.io_failed && failed.durable_sequence == 0 && failed.async_requests_inflight == 0 &&
                failed.wal_durable_waiters == 0 && failed.wal_durable_waits_total == 3 &&
                failed.wal_durable_wait_duration_ns_total > 0 && failed.async_callback_failures_total == 0 &&
                put.count() == 1 && get.count() == 1 && missing_delete.count() == 1 && unused.count() == 0,
                "WAL failure lost/doubled callbacks or confused callback and I/O failures");
        must_fail([&] { engine.close(); }, "close hid async WAL failure");
        require(engine.stats().async_requests_inflight == 0, "failed close returned before callbacks were released");
    }
}

void capacity_wait_reservations() {
    for (bool fail : {false, true}) {
        TempDir dir;
        auto config = config_for(dir);
        config.max_async_requests = 2;
        config.wal_queue_bytes = codec::kWalRecordHeader + kMaxKeySize + kMaxValueSize + 4;
        Gate sync_gate;
        std::atomic<bool> first_sync{true};
        config.io_hook = [&](const std::string& point) {
            if (point == "wal.sync" && first_sync.exchange(false)) {
                sync_gate.pause();
                if (fail) throw std::runtime_error("injected reserved submission failure");
            }
        };
        Engine engine(config);
        Reply first, second, unused;
        require(!engine.execute_async({Operation::Put, std::string(kMaxKeySize, 'k'), std::string(kMaxValueSize, 'v')},
                                      first.callback()), "large PUT was not deferred");
        const bool syncing = sync_gate.wait();
        auto submitting = std::async(std::launch::async, [&] {
            return engine.execute_async({Operation::Put, "second", "two"}, second.callback());
        });
        const bool reserved = await_stats(engine, [](const EngineStats& stats) {
            return stats.async_requests_inflight == 2 && stats.wal_capacity_waiters == 1 && stats.wal_durable_waiters == 1;
        });
        auto rejected = engine.execute_async({Operation::Delete, "absent", {}}, unused.callback());
        const auto before = engine.stats();
        const bool caller_waited = submitting.wait_for(50ms) == std::future_status::timeout;
        sync_gate.release();
        const auto submitted = submitting.get();
        const auto first_result = first.get();
        Response second_result = submitted ? *submitted : second.get();
        engine.drain_async();
        const auto after = engine.stats();
        require(syncing && reserved && caller_waited && rejected && rejected->status == Status::Busy &&
                before.applied_sequence == 1 && unused.count() == 0,
                "WAL capacity wait did not retain its pre-apply async reservation");
        require(after.async_requests_inflight == 0 && after.wal_capacity_waiters == 0 && after.wal_capacity_waits_total == 1 &&
                after.wal_capacity_wait_duration_ns_total > 0 && after.wal_durable_waiters == 0 &&
                after.wal_durable_waits_total == (fail ? 1U : 2U), "reserved submission lost or duplicated wait accounting");
        if (fail) {
            require(submitted && first_result.status == Status::IOError && second_result.status == Status::IOError &&
                    second.count() == 0 && after.applied_sequence == 1,
                    "failed pre-transfer submission invoked a callback or applied a write");
            must_fail([&] { engine.close(); }, "close hid reservation WAL failure");
        } else {
            require(!submitted && first_result.status == Status::Ok && second_result.status == Status::Ok &&
                    first.count() == 1 && second.count() == 1 && after.durable_sequence == 2,
                    "successful capacity waiter did not transfer its reserved callback");
            engine.close();
        }
    }
}

void snapshot_commit_and_failure_boundaries() {
    for (bool terminal : {false, true}) {
        TempDir dir;
        auto config = config_for(dir);
        config.wal_batch_size = 64;
        config.wal_flush_interval = 60s;
        Gate sync_gate, snapshot_gate;
        std::atomic<bool> armed{false};
        config.io_hook = [&](const std::string& point) {
            if (!armed) return;
            if (point == "wal.sync") sync_gate.pause();
            if (point == "snapshot.write") {
                snapshot_gate.pause();
                if (!terminal) throw std::runtime_error("injected snapshot write failure");
            }
            if (terminal && point == "wal.compact.after_replace") throw std::runtime_error("injected compaction failure");
        };
        Engine engine(config);
        armed = true;
        Reply first, read, later;
        require(!engine.execute_async({Operation::Put, "first", "one"}, first.callback()), "PUT was not deferred");
        auto snapshot = std::async(std::launch::async, [&] {
            try { engine.snapshot(); return false; } catch (const std::exception&) { return true; }
        });
        const bool syncing = sync_gate.wait();
        require(!engine.execute_async({Operation::Get, "first", {}}, read.callback()), "GET bypassed snapshot WAL commit");
        sync_gate.release();
        const bool writing_snapshot = snapshot_gate.wait();
        const auto written = first.get(), value = read.get();
        if (terminal) {
            require(!engine.execute_async({Operation::Put, "later", "two"}, later.callback()), "post-capture PUT was not deferred");
        }
        snapshot_gate.release();
        const bool snapshot_failed = snapshot.get();
        if (terminal) require(later.get().status == Status::IOError, "compaction failure left a callback waiting");
        engine.drain_async();
        const auto after = engine.stats();
        require(syncing && writing_snapshot && snapshot_failed && written.status == Status::Ok &&
                value.status == Status::Value && value.value == "one", "snapshot commit did not release its async targets");
        require(after.io_failed == terminal && after.async_requests_inflight == 0 &&
                after.wal_durable_waiters == 0 && after.wal_durable_waits_total == (terminal ? 3U : 2U),
                "snapshot failure used the wrong terminal/async wait boundary");
        if (terminal) must_fail([&] { engine.close(); }, "close hid compaction failure");
        else {
            Reply unused;
            const auto ready = engine.execute_async({Operation::Get, "first", {}}, unused.callback());
            require(ready && ready->value == "one" && unused.count() == 0, "nonterminal snapshot failure disabled reads");
            engine.close();
        }
    }
}

void close_wakes_reserved_and_registered_requests() {
    TempDir dir;
    auto config = config_for(dir);
    config.max_async_requests = 3;
    config.wal_queue_bytes = codec::kWalRecordHeader + kMaxKeySize + kMaxValueSize + 4;
    Gate sync_gate;
    config.io_hook = [&](const std::string& point) { if (point == "wal.sync") sync_gate.pause(); };
    Engine engine(config);
    const std::string key(kMaxKeySize, 'k');
    Reply write, read, unused_delete;
    require(!engine.execute_async({Operation::Put, key, std::string(kMaxValueSize, 'v')}, write.callback()), "large PUT was not deferred");
    const bool syncing = sync_gate.wait();
    require(!engine.execute_async({Operation::Get, key, {}}, read.callback()), "GET was not deferred");
    auto deleting = std::async(std::launch::async, [&] {
        return engine.execute_async({Operation::Delete, key, {}}, unused_delete.callback());
    });
    const bool reserved = await_stats(engine, [](const EngineStats& stats) {
        return stats.async_requests_inflight == 3 && stats.wal_capacity_waiters == 1 && stats.wal_durable_waiters == 2;
    });
    auto closing = std::async(std::launch::async, [&] { engine.close(); });
    const bool woken = await_stats(engine, [](const EngineStats& stats) {
        return stats.stopping && stats.async_requests_inflight == 0;
    });
    const auto stopped = engine.stats();
    const bool close_waited = closing.wait_for(50ms) == std::future_status::timeout;
    sync_gate.release();
    const auto deleted = deleting.get();
    const auto written = write.get(), value = read.get();
    closing.get();
    require(syncing && reserved && woken && close_waited, "close did not drain reserved submissions and registered callbacks");
    require(deleted && deleted->status == Status::Busy && written.status == Status::IOError &&
            value.status == Status::IOError && unused_delete.count() == 0 && write.count() == 1 && read.count() == 1,
            "close changed synchronous capacity rejection or async error ownership");
    require(stopped.durable_sequence == 0 && stopped.wal_capacity_waiters == 0 && stopped.wal_capacity_waits_total == 1 &&
            stopped.wal_durable_waiters == 0 && stopped.wal_durable_waits_total == 2 &&
            stopped.wal_capacity_wait_duration_ns_total > 0 && stopped.wal_durable_wait_duration_ns_total > 0,
            "close lost capacity/durable wait accounting");
    require(engine.stats().applied_sequence == 1 && engine.stats().durable_sequence == 1 &&
            engine.stats().async_requests_inflight == 0, "close admitted the blocked DELETE or did not join callbacks");
    config.io_hook = {};
    Engine recovered(config);
    require(recovered.execute({Operation::Get, key, {}}).value.size() == kMaxValueSize, "close lost admitted data");
    require_reset(recovered.stats());
}

void callback_reentry_and_exceptions() {
    TempDir dir;
    auto config = config_for(dir);
    Gate sync_gate;
    std::atomic<bool> first_sync{true};
    config.io_hook = [&](const std::string& point) {
        if (point == "wal.sync" && first_sync.exchange(false)) sync_gate.pause();
    };
    std::atomic<unsigned> rejections{0};
    std::atomic<uint64_t> observed_inflight{0};
    Engine engine(config);
    Reply later;
    require(!engine.execute_async({Operation::Put, "first", "one"}, [&](Response response) {
        observed_inflight = engine.stats().async_requests_inflight;
        try { engine.drain_async(); } catch (const std::logic_error&) { ++rejections; }
        try { engine.close(); } catch (const std::logic_error&) { ++rejections; }
        require(response.status == Status::Ok, "callback status changed");
        throw std::runtime_error("injected callback failure");
    }), "PUT was not deferred");
    const bool syncing = sync_gate.wait();
    require(!engine.execute_async({Operation::Put, "second", "two"}, later.callback()), "second PUT was not deferred");
    sync_gate.release();
    const auto later_result = later.get();
    engine.drain_async();
    const auto after = engine.stats();
    require(syncing && observed_inflight >= 1 && rejections == 2 && later_result.status == Status::Ok,
            "callback stats/reentry or a later callback deadlocked");
    require(after.async_callback_failures_total == 1 && after.async_requests_inflight == 0 &&
            !after.io_failed && after.wal_durable_waits_total == 2,
            "callback exception leaked a slot or marked storage failed");
    require(engine.execute({Operation::Put, "after", "value"}).status == Status::Ok, "callback exception disabled the engine");
    engine.close();
    config.io_hook = {};
    Engine recovered(config);
    require(recovered.execute({Operation::Get, "second", {}}).value == "two", "callback exception damaged durable data");
    require_reset(recovered.stats());
}

void callback_close_reentry_during_external_close() {
    TempDir dir;
    auto config = config_for(dir);
    Gate sync_gate;
    config.io_hook = [&](const std::string& point) { if (point == "wal.sync") sync_gate.pause(); };
    std::atomic<bool> rejected{false};
    Engine engine(config);
    Reply completion;
    auto collect = completion.callback();
    require(!engine.execute_async({Operation::Put, "key", "value"}, [&, collect](Response response) {
        // External close holds close_mutex_ while the WAL writer is paused.
        // Reentry must be rejected before attempting to acquire that mutex.
        try { engine.close(); } catch (const std::logic_error&) { rejected = true; }
        collect(std::move(response));
    }), "PUT was not deferred");
    const bool syncing = sync_gate.wait();
    auto closing = std::async(std::launch::async, [&] { engine.close(); });
    const auto response = completion.get();
    const bool close_waited = closing.wait_for(50ms) == std::future_status::timeout;
    sync_gate.release();
    closing.get();
    require(syncing && rejected && close_waited && response.status == Status::IOError && completion.count() == 1,
            "callback close reentry acquired the lifecycle lock or changed close semantics");
    require(engine.stats().async_requests_inflight == 0 && engine.stats().async_callback_failures_total == 0,
            "external close returned before callback completion");
}

void concurrent_registration_across_capacity() {
    TempDir dir;
    auto config = config_for(dir);
    config.max_async_requests = 9;
    config.wal_queue_bytes = codec::kWalRecordHeader + kMaxKeySize + kMaxValueSize + 4;
    Gate first_sync;
    std::atomic<bool> pause_first{true};
    config.io_hook = [&](const std::string& point) {
        if (point == "wal.sync" && pause_first.exchange(false)) first_sync.pause();
    };
    // These callback observations must outlive Engine's failure-path drain too.
    std::array<std::atomic<uint64_t>, 8> acknowledged_at;
    for (auto& sequence : acknowledged_at) sequence = 0;
    Engine engine(config);
    Reply first;
    std::array<Reply, 8> replies;
    require(!engine.execute_async({Operation::Put, std::string(kMaxKeySize, 'k'), std::string(kMaxValueSize, 'v')},
                                  first.callback()), "large PUT was not deferred");
    const bool syncing = first_sync.wait();
    std::vector<std::future<std::optional<Response>>> submissions;
    for (size_t i = 0; i < replies.size(); ++i) {
        submissions.push_back(std::async(std::launch::async, [&, i] {
            return engine.execute_async({Operation::Put, "parallel" + std::to_string(i), std::string(256 * 1024, 'a' + i)},
                [&, i, collect = replies[i].callback()](Response response) {
                    acknowledged_at[i] = engine.durable_sequence();
                    collect(std::move(response));
                });
        }));
    }
    const bool reserved = await_stats(engine, [](const EngineStats& stats) {
        return stats.async_requests_inflight == 9 && stats.wal_capacity_waiters == 8;
    });
    first_sync.release();
    bool all_deferred = true;
    for (auto& submission : submissions) all_deferred = !submission.get() && all_deferred;
    require(first.get().status == Status::Ok, "initial PUT failed");
    for (auto& reply : replies) require(reply.get().status == Status::Ok, "concurrent PUT failed");
    engine.drain_async();
    const auto done = engine.stats();
    require(syncing && reserved && all_deferred && done.applied_sequence == 9 && done.durable_sequence == 9 &&
            done.async_requests_inflight == 0 && done.wal_capacity_waits_total == 8 &&
            done.wal_durable_waits_total == 9 && done.wal_commits_total >= 3,
            "concurrent capacity waiters lost registration, ordering or wait accounting");
    require(first.count() == 1, "initial callback was repeated");
    for (const auto& reply : replies) require(reply.count() == 1, "concurrent callback was repeated");
    engine.close();
    // Compare callback-time durability against each request's actual WAL order,
    // rather than assuming thread launch order determined its sequence number.
    std::ifstream file(dir.path + "/wal.v1", std::ios::binary);
    const std::string wal{std::istreambuf_iterator<char>(file), std::istreambuf_iterator<char>()};
    codec::validate_wal_file_header(std::string_view(wal).substr(0, codec::kWalFileHeader));
    size_t offset = codec::kWalFileHeader, checked = 0;
    while (offset < wal.size()) {
        const auto remaining = std::string_view(wal).substr(offset);
        const auto size = codec::wal_record_size(remaining);
        const auto bytes = remaining.substr(0, size);
        const auto request = codec::decode_wal_record(bytes);
        if (request.key.compare(0, 8, "parallel") == 0) {
            const auto index = static_cast<size_t>(std::stoul(request.key.substr(8)));
            require(index < acknowledged_at.size() && acknowledged_at[index] >= codec::u64(bytes, 5),
                    "concurrent callback ran before its own WAL sequence was durable");
            ++checked;
        }
        offset += size;
    }
    require(checked == replies.size(), "concurrent WAL records were missing");
    config.io_hook = {};
    Engine recovered(config);
    for (size_t i = 0; i < replies.size(); ++i) {
        require(recovered.execute({Operation::Get, "parallel" + std::to_string(i), {}}).value == std::string(256 * 1024, 'a' + i),
                "concurrent asynchronous write did not recover");
    }
}

void reserved_callback_resources_during_failure_and_close() {
    for (bool fail : {false, true}) {
        TempDir dir;
        auto config = config_for(dir);
        config.max_async_requests = 2;
        config.wal_queue_bytes = codec::kWalRecordHeader + kMaxKeySize + kMaxValueSize + 4;
        Gate sync_gate, destructor_gate;
        std::atomic<uint64_t> destructor_inflight{0};
        std::atomic<bool> destructor_timeout{false};
        config.io_hook = [&](const std::string& point) {
            if (point == "wal.sync") {
                sync_gate.pause();
                if (fail) throw std::runtime_error("injected pre-transfer failure");
            }
        };
        Engine engine(config);
        struct Capture {
            Engine& engine;
            Gate& gate;
            std::atomic<uint64_t>& inflight;
            std::atomic<bool>& timeout;
            Capture(Engine& e, Gate& g, std::atomic<uint64_t>& i, std::atomic<bool>& t) : engine(e), gate(g), inflight(i), timeout(t) {}
            ~Capture() { inflight = engine.stats().async_requests_inflight; if (!gate.block()) timeout = true; }
        };
        Reply first, unused;
        require(!engine.execute_async({Operation::Put, std::string(kMaxKeySize, 'k'), std::string(kMaxValueSize, 'v')},
                                      first.callback()), "large PUT was not deferred");
        const bool syncing = sync_gate.wait();
        auto submitting = std::async(std::launch::async, [&] {
            auto capture = std::make_shared<Capture>(engine, destructor_gate, destructor_inflight, destructor_timeout);
            return engine.execute_async({Operation::Put, "blocked", "value"},
                [capture = std::move(capture), collect = unused.callback()](Response response) { collect(std::move(response)); });
        });
        const bool reserved = await_stats(engine, [](const EngineStats& stats) {
            return stats.async_requests_inflight == 2 && stats.wal_capacity_waiters == 1;
        });
        auto draining = std::async(std::launch::async, [&] { engine.drain_async(); });
        bool io_failed = true;
        if (fail) {
            sync_gate.release();
            io_failed = await_stats(engine, [](const EngineStats& stats) { return stats.io_failed; });
        }
        auto closing = std::async(std::launch::async, [&] {
            try { engine.close(); return false; } catch (const std::exception&) { return true; }
        });
        const bool destroying = destructor_gate.wait();
        const auto first_result = first.get();
        sync_gate.release();
        const bool close_waited = closing.wait_for(50ms) == std::future_status::timeout;
        const bool drain_waited = draining.wait_for(50ms) == std::future_status::timeout;
        const auto while_destroying = engine.stats();
        destructor_gate.release();
        const auto response = submitting.get();
        draining.get();
        const bool close_failed = closing.get();
        require(syncing && reserved && io_failed && destroying && close_waited && drain_waited &&
                destructor_inflight >= 1 && while_destroying.async_requests_inflight >= 1 && !destructor_timeout,
                "pre-transfer resource destruction escaped async accounting or held the state lock");
        require(response && response->status == (fail ? Status::IOError : Status::Busy) &&
                first_result.status == Status::IOError && unused.count() == 0 && close_failed == fail &&
                engine.stats().async_requests_inflight == 0 && engine.stats().applied_sequence == 1,
                "failed reserved submission transferred its callback or changed close semantics");
    }
}

} // namespace

int main(int argc, char** argv) {
    try {
        const std::vector<std::pair<const char*, void(*)()>> tests = {
            {"immediate responses", immediate_responses_and_validation},
            {"throughput drain and close", throughput_drain_does_not_wait_for_wal},
            {"batches and captured reads", batches_and_captured_reads},
            {"callback resource lifetime", slots_cover_callback_resources},
            {"WAL failures", wal_failures_complete_callbacks},
            {"capacity reservations", capacity_wait_reservations},
            {"snapshot boundaries", snapshot_commit_and_failure_boundaries},
            {"close reserved requests", close_wakes_reserved_and_registered_requests},
            {"callback exceptions and reentry", callback_reentry_and_exceptions},
            {"callback close reentry", callback_close_reentry_during_external_close},
            {"concurrent registration", concurrent_registration_across_capacity},
            {"reserved callback resources", reserved_callback_resources_during_failure_and_close},
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
