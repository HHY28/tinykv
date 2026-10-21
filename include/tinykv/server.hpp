#pragma once

#include "tinykv/raft.hpp"
#include "tinykv/store.hpp"
#include "tinykv/thread_pool.hpp"

#include <cstdint>
#include <mutex>
#include <string>
#include <unordered_map>
#include <vector>

namespace tinykv {

class Server {
public:
    Server(ShardedStore& store, RaftNode& raft, ThreadPool& workers, ThreadPool& raft_workers,
           std::string bind_address, int port);
    int Run();

private:
    struct Connection {
        std::string input;
        std::string output;
        bool in_flight{false};
        bool closing{false};
        bool close_after_response{false};
    };
    struct Completion {
        int fd{};
        std::string response;
        bool close_after_response{false};
    };

    void HandleClientEvent(int fd, std::uint32_t events);
    void ReadClient(int fd);
    void WriteClient(int fd);
    void DispatchNext(int fd);
    void ProcessCompletions();
    void EnqueueCompletion(Completion completion);
    void UpdateInterest(int fd);
    void MaybeClose(int fd);
    std::string Execute(std::string line, bool& close_after_response);

    ShardedStore& store_;
    RaftNode& raft_;
    ThreadPool& workers_;
    ThreadPool& raft_workers_;
    std::string bind_address_;
    int port_;
    int epoll_fd_{-1};
    int listen_fd_{-1};
    int completion_fd_{-1};
    std::unordered_map<int, Connection> connections_;
    std::mutex completion_mutex_;
    std::vector<Completion> completions_;
};

}  // namespace tinykv
