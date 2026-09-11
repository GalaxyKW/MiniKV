#pragma once

#include <condition_variable>
#include <functional>
#include <mutex>
#include <queue>
#include <thread>
#include <vector>

namespace minikv {

class ThreadPool {
public:
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
                            task = std::move(tasks_.front());
                            tasks_.pop();
                        }
                        task();
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
        tasks_.push(std::move(task));
        ready_.notify_one();
        return true;
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
    size_t capacity_;
    bool stopping_ = false;
    std::mutex mutex_;
    std::condition_variable ready_;
    std::queue<std::function<void()>> tasks_;
    std::vector<std::thread> workers_;
};

} // namespace minikv
