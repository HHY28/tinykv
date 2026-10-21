#pragma once

#include <condition_variable>
#include <cstddef>
#include <functional>
#include <mutex>
#include <queue>
#include <thread>
#include <vector>

namespace tinykv {

class ThreadPool {
public:
    ThreadPool(std::size_t thread_count, std::size_t queue_limit);
    ~ThreadPool();
    ThreadPool(const ThreadPool&) = delete;
    ThreadPool& operator=(const ThreadPool&) = delete;

    bool Submit(std::function<void()> task);
    std::size_t QueueSize() const;

private:
    void Worker();
    const std::size_t queue_limit_;
    mutable std::mutex mutex_;
    std::condition_variable ready_;
    std::queue<std::function<void()>> tasks_;
    std::vector<std::thread> workers_;
    bool stopping_{false};
};

}  // namespace tinykv
