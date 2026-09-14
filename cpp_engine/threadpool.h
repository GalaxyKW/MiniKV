#pragma once

#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <functional>
#include <mutex>
#include <queue>
#include <thread>
#include <vector>

namespace minikv {

class ThreadPool {
public:
    struct Stats {
        size_t queued;
        size_t capacity;
        size_t active;
        size_t workers;
        // Count and completed queue residence of accepted tasks at dequeue.
        // Rejections and time executing a task do not contribute.
        uint64_t started_total;
        uint64_t queue_wait_duration_ns_total;
    };

    ThreadPool(size_t workers, size_t capacity) : capacity_(capacity) {
        try {
            for (size_t i = 0; i < workers; ++i) {
                workers_.emplace_back([this] {
                    while (true) {
                        std::function<void()> task;
                        {
                            std::unique_lock<std::mutex> lock(mutex_);
                            ready_.wait(lock, [this] { return stopping_ || !tasks_.empty(); });
                            if (tasks_.empty()) return;
                            auto& queued = tasks_.front();
                            queue_wait_duration_ns_total_ += static_cast<uint64_t>(
                                std::chrono::duration_cast<std::chrono::nanoseconds>(Clock::now() - queued.enqueued_at).count());
                            ++started_total_;
                            task = std::move(queued.function);
                            tasks_.pop();
                            ++active_;
                        }
                        task();
                        {
                            std::lock_guard<std::mutex> lock(mutex_);
                            --active_;
                        }
                    }
                });
            }
        } catch (...) {
            shutdown();
            throw;
        }
    }

    ~ThreadPool() { shutdown(); }

    bool enqueue(std::function<void()> task) {
        std::lock_guard<std::mutex> lock(mutex_);
        if (stopping_ || tasks_.size() >= capacity_) return false;
        tasks_.push({std::move(task), {}});
        // Timestamp successful admission, excluding queue insertion work.
        // Workers cannot observe the node before this mutex is released.
        tasks_.back().enqueued_at = Clock::now();
        ready_.notify_one();
        return true;
    }

    Stats stats() const {
        std::lock_guard<std::mutex> lock(mutex_);
        return {tasks_.size(), capacity_, active_, workers_.size(), started_total_, queue_wait_duration_ns_total_};
    }

    void shutdown() {
        {
            std::lock_guard<std::mutex> lock(mutex_);
            stopping_ = true;
        }
        ready_.notify_all();
        for (auto& worker : workers_) if (worker.joinable()) worker.join();
    }

private:
    using Clock = std::chrono::steady_clock;
    struct QueuedTask {
        std::function<void()> function;
        Clock::time_point enqueued_at;
    };

    size_t capacity_;
    size_t active_ = 0;
    uint64_t started_total_ = 0;
    uint64_t queue_wait_duration_ns_total_ = 0;
    bool stopping_ = false;
    mutable std::mutex mutex_;
    std::condition_variable ready_;
    std::queue<QueuedTask> tasks_;
    std::vector<std::thread> workers_;
};

} // namespace minikv
