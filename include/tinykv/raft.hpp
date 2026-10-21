#pragma once

#include "tinykv/store.hpp"

#include <atomic>
#include <chrono>
#include <filesystem>
#include <mutex>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>

namespace tinykv {

struct Peer {
    int id{};
    std::string address;  // host:port
};

struct RaftConfig {
    int node_id{};
    std::filesystem::path data_dir;
    std::vector<Peer> peers;
};

class RaftNode {
public:
    RaftNode(RaftConfig config, ShardedStore& store);
    ~RaftNode();
    RaftNode(const RaftNode&) = delete;
    RaftNode& operator=(const RaftNode&) = delete;

    void Start();
    void Stop();
    std::string Propose(Operation operation, const std::string& key, const std::string& value);
    bool IsLeader() const;
    std::string Status() const;
    std::string HandleVote(const std::vector<std::string>& args);
    std::string HandleAppend(const std::vector<std::string>& args);
    bool SaveSnapshot();

private:
    struct Entry {
        std::uint64_t index{};
        std::uint64_t term{};
        Operation operation{Operation::kSet};
        std::string key;
        std::string value;
    };
    enum class Role { kFollower, kCandidate, kLeader };

    void RunTicker();
    void StartElection();
    void BecomeFollowerLocked(std::uint64_t term);
    void BecomeLeaderLocked();
    bool ReplicatePeer(int peer_id, std::uint64_t target_index, int max_rounds);
    void AdvanceCommitIndex();
    void ApplyCommitted();
    bool LoadPersistentState();
    bool AppendLogFile(const Entry& entry);
    bool RewriteLogFile(const std::vector<Entry>& entries);
    void PersistStateLocked();
    std::string EncodeEntry(const Entry& entry) const;
    static bool DecodeEntry(const std::vector<std::string>& args, std::size_t offset, Entry& entry);
    static std::string SendRpc(const std::string& address, const std::string& request);
    std::uint64_t LastLogIndexLocked() const;
    std::uint64_t LastLogTermLocked() const;
    void MaybeSnapshot();

    RaftConfig config_;
    ShardedStore& store_;
    mutable std::mutex mutex_;
    std::mutex proposal_mutex_;
    std::mutex apply_mutex_;
    std::vector<Entry> entries_;  // retained from index 1; snapshots do not compact this teaching log
    std::uint64_t current_term_{0};
    int voted_for_{-1};
    std::uint64_t commit_index_{0};
    std::uint64_t last_applied_{0};
    std::uint64_t snapshot_index_{0};
    Role role_{Role::kFollower};
    int leader_id_{-1};
    std::unordered_map<int, std::uint64_t> next_index_;
    std::unordered_map<int, std::uint64_t> match_index_;
    std::chrono::steady_clock::time_point last_contact_;
    std::atomic<bool> stopping_{false};
    std::thread ticker_;
};

}  // namespace tinykv
