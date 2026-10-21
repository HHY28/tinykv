#include "tinykv/thread_pool.hpp"

#include <stdexcept>
#include <utility>

namespace tinykv {

ThreadPool::ThreadPool(std::size_t thread_count, std::size_t queue_limit)
    : queue_limit_(queue_limit) {
    if (thread_count == 0 || queue_limit == 0) {
        throw std::invalid_argument("thread count and queue limit must be positive");
    }
    workers_.reserve(thread_count);
    for (std::size_t i = 0; i < thread_count; ++i) {
        workers_.emplace_back([this] { Worker(); });
    }
}

ThreadPool::~ThreadPool() {
    {
        std::lock_guard lock(mutex_);
        stopping_ = true;
    }
    ready_.notify_all();
    for (auto& worker : workers_) {
        if (worker.joinable()) worker.join();
    }
}

bool ThreadPool::Submit(std::function<void()> task) {
    {
        std::lock_guard lock(mutex_);
        if (stopping_ || tasks_.size() >= queue_limit_) return false;
        tasks_.push(std::move(task));
    }
    ready_.notify_one();
    return true;
}

std::size_t ThreadPool::QueueSize() const {
    std::lock_guard lock(mutex_);
    return tasks_.size();
}

void ThreadPool::Worker() {
    while (true) {
        std::function<void()> task;
        {
            std::unique_lock lock(mutex_);
            ready_.wait(lock, [this] { return stopping_ || !tasks_.empty(); });
            if (stopping_ && tasks_.empty()) return;
            task = std::move(tasks_.front());
            tasks_.pop();
        }
        try {
            task();
        } catch (...) {
            // A failed request must not terminate a worker thread.
        }
    }
}

}  // namespace tinykv
