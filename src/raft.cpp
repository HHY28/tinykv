#include "tinykv/raft.hpp"

#include "tinykv/command.hpp"

#include <algorithm>
#include <cerrno>
#include <charconv>
#include <cstdio>
#include <fcntl.h>
#include <fstream>
#include <iostream>
#include <poll.h>
#include <sstream>
#include <utility>
#include <sys/time.h>
#include <utility>
#include <sys/socket.h>
#include <sys/types.h>
#include <unistd.h>

#include <arpa/inet.h>
#include <netdb.h>

namespace tinykv {
namespace {

bool ParseU64(const std::string& text, std::uint64_t& value) {
    const auto result = std::from_chars(text.data(), text.data() + text.size(), value);
    return result.ec == std::errc{} && result.ptr == text.data() + text.size();
}

bool ParseInt(const std::string& text, int& value) {
    const auto result = std::from_chars(text.data(), text.data() + text.size(), value);
    return result.ec == std::errc{} && result.ptr == text.data() + text.size();
}

bool WriteAll(int fd, std::string_view data) {
    std::size_t offset = 0;
    while (offset < data.size()) {
        const ssize_t written = ::write(fd, data.data() + offset, data.size() - offset);
        if (written < 0 && errno == EINTR) continue;
        if (written <= 0) return false;
        offset += static_cast<std::size_t>(written);
    }
    return true;
}

bool SyncFile(const std::filesystem::path& path) {
    const int fd = ::open(path.c_str(), O_RDONLY);
    if (fd < 0) return false;
    const bool ok = ::fsync(fd) == 0;
    ::close(fd);
    return ok;
}

bool SyncDirectory(const std::filesystem::path& path) {
    const int fd = ::open(path.c_str(), O_RDONLY | O_DIRECTORY);
    if (fd < 0) return false;
    const bool ok = ::fsync(fd) == 0;
    ::close(fd);
    return ok;
}

bool SendSocketAll(int fd, std::string_view data) {
    std::size_t offset = 0;
    while (offset < data.size()) {
        const ssize_t written = ::send(fd, data.data() + offset, data.size() - offset, MSG_NOSIGNAL);
        if (written < 0 && errno == EINTR) continue;
        if (written <= 0) return false;
        offset += static_cast<std::size_t>(written);
    }
    return true;
}

std::string RoleName(int role) {
    switch (role) {
        case 0: return "follower";
        case 1: return "candidate";
        default: return "leader";
    }
}

}  // namespace

RaftNode::RaftNode(RaftConfig config, ShardedStore& store)
    : config_(std::move(config)), store_(store), last_contact_(std::chrono::steady_clock::now()) {
    std::filesystem::create_directories(config_.data_dir);
    LoadPersistentState();
}

RaftNode::~RaftNode() { Stop(); }

void RaftNode::Start() {
    if (ticker_.joinable()) return;
    stopping_.store(false);
    ticker_ = std::thread([this] { RunTicker(); });
}

void RaftNode::Stop() {
    stopping_.store(true);
    if (ticker_.joinable()) ticker_.join();
}

std::uint64_t RaftNode::LastLogIndexLocked() const {
    return entries_.empty() ? snapshot_index_ : entries_.back().index;
}

std::uint64_t RaftNode::LastLogTermLocked() const {
    return entries_.empty() ? 0 : entries_.back().term;
}

bool RaftNode::IsLeader() const {
    std::lock_guard lock(mutex_);
    return role_ == Role::kLeader;
}

std::string RaftNode::Status() const {
    std::lock_guard lock(mutex_);
    return "STATUS id=" + std::to_string(config_.node_id) + " role=" +
        RoleName(static_cast<int>(role_)) + " term=" + std::to_string(current_term_) +
        " leader=" + std::to_string(leader_id_) + " commit=" + std::to_string(commit_index_) +
        " last_log=" + std::to_string(LastLogIndexLocked());
}

std::string RaftNode::EncodeEntry(const Entry& entry) const {
    const char* op = entry.operation == Operation::kSet ? "SET" : "DEL";
    const std::string key = HexEncode(entry.key).empty() ? "-" : HexEncode(entry.key);
    const std::string value = HexEncode(entry.value).empty() ? "-" : HexEncode(entry.value);
    return std::to_string(entry.index) + " " + std::to_string(entry.term) + " " + op + " " +
        key + " " + value;
}

bool RaftNode::DecodeEntry(const std::vector<std::string>& args, std::size_t offset, Entry& entry) {
    if (args.size() < offset + 5) return false;
    if (!ParseU64(args[offset], entry.index) || !ParseU64(args[offset + 1], entry.term)) return false;
    if (args[offset + 2] == "SET") entry.operation = Operation::kSet;
    else if (args[offset + 2] == "DEL") entry.operation = Operation::kDel;
    else return false;
    const std::string key = args[offset + 3] == "-" ? "" : args[offset + 3];
    const std::string value = args[offset + 4] == "-" ? "" : args[offset + 4];
    return HexDecode(key, entry.key) && HexDecode(value, entry.value);
}

bool RaftNode::AppendLogFile(const Entry& entry) {
    const auto path = config_.data_dir / "raft.log";
    std::error_code exists_error;
    const bool existed = std::filesystem::exists(path, exists_error);
    const int fd = ::open(path.c_str(), O_CREAT | O_WRONLY | O_APPEND, 0644);
    if (fd < 0) return false;
    const std::string line = EncodeEntry(entry) + "\n";
    bool ok = WriteAll(fd, line) && ::fsync(fd) == 0;
    ::close(fd);
    if (ok && !existed) ok = SyncDirectory(config_.data_dir);
    return ok;
}

bool RaftNode::RewriteLogFile(const std::vector<Entry>& entries) {
    const auto path = config_.data_dir / "raft.log";
    const auto temp = config_.data_dir / "raft.log.tmp";
    const int fd = ::open(temp.c_str(), O_CREAT | O_WRONLY | O_TRUNC, 0644);
    if (fd < 0) return false;
    bool ok = true;
    for (const auto& entry : entries) {
        if (!WriteAll(fd, EncodeEntry(entry) + "\n")) {
            ok = false;
            break;
        }
    }
    if (ok) ok = ::fsync(fd) == 0;
    ::close(fd);
    if (!ok) return false;
    return ::rename(temp.c_str(), path.c_str()) == 0 && SyncDirectory(config_.data_dir);
}

void RaftNode::PersistStateLocked() {
    const auto path = config_.data_dir / "raft.state";
    const auto temp = config_.data_dir / "raft.state.tmp";
    std::ofstream output(temp, std::ios::trunc);
    output << current_term_ << ' ' << voted_for_ << ' ' << commit_index_ << '\n';
    output.flush();
    output.close();
    if (!SyncFile(temp) || ::rename(temp.c_str(), path.c_str()) != 0 || !SyncDirectory(config_.data_dir)) {
        std::cerr << "warning: unable to persist raft.state\n";
    }
}

bool RaftNode::LoadPersistentState() {
    std::uint64_t snap_index = 0;
    if (store_.LoadSnapshot(config_.data_dir / "snapshot.dat", snap_index)) {
        snapshot_index_ = snap_index;
        last_applied_ = snap_index;
    }

    std::ifstream log(config_.data_dir / "raft.log");
    std::string line;
    std::uint64_t expected_index = 1;
    bool malformed_tail = false;
    while (std::getline(log, line)) {
        std::istringstream input(line);
        std::vector<std::string> args;
        std::string arg;
        while (input >> arg) args.push_back(arg);
        Entry entry;
        if (!DecodeEntry(args, 0, entry) || entry.index != expected_index) {
            malformed_tail = true;
            break;
        }
        entries_.push_back(std::move(entry));
        ++expected_index;
    }
    if (malformed_tail && !RewriteLogFile(entries_)) {
        std::cerr << "warning: unable to truncate incomplete raft.log tail\n";
    }

    std::ifstream state(config_.data_dir / "raft.state");
    if (state) state >> current_term_ >> voted_for_ >> commit_index_;
    commit_index_ = std::min(commit_index_, LastLogIndexLocked());
    ApplyCommitted();
    return true;
}

void RaftNode::BecomeFollowerLocked(std::uint64_t term) {
    role_ = Role::kFollower;
    leader_id_ = -1;
    if (term > current_term_) {
        current_term_ = term;
        voted_for_ = -1;
    }
    last_contact_ = std::chrono::steady_clock::now();
    PersistStateLocked();
}

void RaftNode::BecomeLeaderLocked() {
    role_ = Role::kLeader;
    leader_id_ = config_.node_id;
    const auto next = LastLogIndexLocked() + 1;
    for (const auto& peer : config_.peers) {
        next_index_[peer.id] = next;
        match_index_[peer.id] = 0;
    }
}

std::string RaftNode::SendRpc(const std::string& address, const std::string& request) {
    const auto separator = address.rfind(':');
    if (separator == std::string::npos) return {};
    const std::string host = address.substr(0, separator);
    const std::string service = address.substr(separator + 1);
    addrinfo hints{};
    hints.ai_family = AF_INET;
    hints.ai_socktype = SOCK_STREAM;
    addrinfo* results = nullptr;
    if (::getaddrinfo(host.c_str(), service.c_str(), &hints, &results) != 0) return {};

    int fd = -1;
    for (addrinfo* item = results; item != nullptr; item = item->ai_next) {
        fd = ::socket(item->ai_family, item->ai_socktype | SOCK_CLOEXEC, item->ai_protocol);
        if (fd < 0) continue;
        const int old_flags = ::fcntl(fd, F_GETFL, 0);
        if (old_flags < 0 || ::fcntl(fd, F_SETFL, old_flags | O_NONBLOCK) < 0) {
            ::close(fd);
            fd = -1;
            continue;
        }
        int connected = ::connect(fd, item->ai_addr, item->ai_addrlen);
        if (connected < 0 && errno == EINPROGRESS) {
            pollfd poll_fd{fd, POLLOUT, 0};
            connected = ::poll(&poll_fd, 1, 250);
            if (connected > 0) {
                int socket_error = 0;
                socklen_t error_size = sizeof(socket_error);
                if (::getsockopt(fd, SOL_SOCKET, SO_ERROR, &socket_error, &error_size) < 0 || socket_error != 0) {
                    errno = socket_error;
                    connected = -1;
                } else {
                    connected = 0;
                }
            } else {
                connected = -1;
            }
        }
        if (connected == 0) {
            ::fcntl(fd, F_SETFL, old_flags);
        } else {
            ::close(fd);
            fd = -1;
            continue;
        }
        timeval timeout{0, 250000};
        ::setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &timeout, sizeof(timeout));
        ::setsockopt(fd, SOL_SOCKET, SO_SNDTIMEO, &timeout, sizeof(timeout));
        break;
    }
    ::freeaddrinfo(results);
    if (fd < 0) return {};
    if (!SendSocketAll(fd, request + "\n")) {
        ::close(fd);
        return {};
    }
    std::string response;
    char ch;
    while (response.size() < 4096) {
        const ssize_t count = ::recv(fd, &ch, 1, 0);
        if (count <= 0) break;
        if (ch == '\n') break;
        if (ch != '\r') response.push_back(ch);
    }
    ::close(fd);
    return response;
}

std::string RaftNode::HandleVote(const std::vector<std::string>& args) {
    std::uint64_t term = 0, last_index = 0, last_term = 0;
    int candidate = -1;
    if (args.size() != 4 || !ParseU64(args[0], term) || !ParseInt(args[1], candidate) ||
        !ParseU64(args[2], last_index) || !ParseU64(args[3], last_term)) return "ERR bad vote request";

    std::lock_guard lock(mutex_);
    if (term < current_term_) return "VOTE " + std::to_string(current_term_) + " 0";
    if (term > current_term_) BecomeFollowerLocked(term);
    const auto my_index = LastLogIndexLocked();
    const auto my_term = LastLogTermLocked();
    const bool up_to_date = last_term > my_term || (last_term == my_term && last_index >= my_index);
    const bool can_vote = voted_for_ == -1 || voted_for_ == candidate;
    const bool granted = can_vote && up_to_date;
    if (granted) {
        voted_for_ = candidate;
        last_contact_ = std::chrono::steady_clock::now();
        PersistStateLocked();
    }
    return "VOTE " + std::to_string(current_term_) + (granted ? " 1" : " 0");
}

std::string RaftNode::HandleAppend(const std::vector<std::string>& args) {
    std::uint64_t term = 0, prev_index = 0, prev_term = 0, leader_commit = 0;
    int leader = -1, has_entry = 0;
    if (args.size() < 6 || !ParseU64(args[0], term) || !ParseInt(args[1], leader) ||
        !ParseU64(args[2], prev_index) || !ParseU64(args[3], prev_term) ||
        !ParseU64(args[4], leader_commit) || !ParseInt(args[5], has_entry) ||
        (has_entry != 0 && has_entry != 1) || args.size() != (has_entry ? 11u : 6u)) {
        return "ERR bad append request";
    }

    Entry incoming;
    if (has_entry && (!DecodeEntry(args, 6, incoming) || incoming.index != prev_index + 1)) {
        return "ERR bad log entry";
    }

    std::unique_lock lock(mutex_);
    if (term < current_term_) {
        return "APPEND " + std::to_string(current_term_) + " 0 " + std::to_string(LastLogIndexLocked());
    }
    if (term > current_term_ || role_ != Role::kFollower) BecomeFollowerLocked(term);
    leader_id_ = leader;
    last_contact_ = std::chrono::steady_clock::now();

    const auto last_index = LastLogIndexLocked();
    if (prev_index > last_index) {
        return "APPEND " + std::to_string(current_term_) + " 0 " + std::to_string(last_index);
    }
    if (prev_index > 0) {
        const auto found = std::find_if(entries_.begin(), entries_.end(), [prev_index](const Entry& entry) {
            return entry.index == prev_index;
        });
        if (found == entries_.end() || found->term != prev_term) {
            return "APPEND " + std::to_string(current_term_) + " 0 " + std::to_string(prev_index ? prev_index - 1 : 0);
        }
    }

    if (has_entry) {
        if (incoming.index <= commit_index_) {
            const auto found = std::find_if(entries_.begin(), entries_.end(), [&incoming](const Entry& entry) {
                return entry.index == incoming.index;
            });
            if (found != entries_.end() && found->term != incoming.term) {
                return "APPEND " + std::to_string(current_term_) + " 0 " + std::to_string(commit_index_);
            }
        }
        auto existing = std::find_if(entries_.begin(), entries_.end(), [&incoming](const Entry& entry) {
            return entry.index == incoming.index;
        });
        if (existing != entries_.end() && existing->term != incoming.term) {
            entries_.erase(existing, entries_.end());
            if (!RewriteLogFile(entries_)) return "ERR log rewrite failed";
            existing = entries_.end();
        }
        if (existing == entries_.end()) {
            if (!AppendLogFile(incoming)) return "ERR log append failed";
            entries_.push_back(incoming);
        }
    }

    const auto new_commit = std::min(leader_commit, LastLogIndexLocked());
    if (new_commit > commit_index_) {
        commit_index_ = new_commit;
        PersistStateLocked();
    }
    const auto matched = has_entry ? incoming.index : prev_index;
    const auto response = "APPEND " + std::to_string(current_term_) + " 1 " + std::to_string(matched);
    lock.unlock();
    ApplyCommitted();
    return response;
}

void RaftNode::StartElection() {
    std::uint64_t term, last_index, last_term;
    {
        std::lock_guard lock(mutex_);
        role_ = Role::kCandidate;
        ++current_term_;
        voted_for_ = config_.node_id;
        leader_id_ = -1;
        term = current_term_;
        last_index = LastLogIndexLocked();
        last_term = LastLogTermLocked();
        last_contact_ = std::chrono::steady_clock::now();
        PersistStateLocked();
    }

    int votes = 1;
    const int majority = static_cast<int>((config_.peers.size() + 1) / 2 + 1);
    for (const auto& peer : config_.peers) {
        const std::string request = "RAFT_VOTE " + std::to_string(term) + " " +
            std::to_string(config_.node_id) + " " + std::to_string(last_index) + " " + std::to_string(last_term);
        const auto response = SendRpc(peer.address, request);
        std::istringstream input(response);
        std::string marker;
        std::uint64_t response_term = 0;
        int granted = 0;
        input >> marker >> response_term >> granted;
        if (marker != "VOTE") continue;
        if (response_term > term) {
            std::lock_guard lock(mutex_);
            if (response_term > current_term_) BecomeFollowerLocked(response_term);
            return;
        }
        if (response_term == term && granted != 0) ++votes;
    }
    if (votes >= majority) {
        std::lock_guard lock(mutex_);
        if (current_term_ == term && role_ == Role::kCandidate) BecomeLeaderLocked();
    }
}

bool RaftNode::ReplicatePeer(int peer_id, std::uint64_t target_index, int max_rounds) {
    const auto peer_it = std::find_if(config_.peers.begin(), config_.peers.end(), [peer_id](const Peer& peer) {
        return peer.id == peer_id;
    });
    if (peer_it == config_.peers.end()) return false;

    for (int round = 0; round < max_rounds; ++round) {
        std::string request;
        std::uint64_t sent_term = 0;
        std::uint64_t sent_index = 0;
        {
            std::lock_guard lock(mutex_);
            if (role_ != Role::kLeader) return false;
            sent_term = current_term_;
            auto& next = next_index_[peer_id];
            if (next == 0) next = LastLogIndexLocked() + 1;
            if (next > LastLogIndexLocked() + 1) next = LastLogIndexLocked() + 1;
            const std::uint64_t prev_index = next - 1;
            std::uint64_t prev_term = 0;
            if (prev_index > 0) {
                auto prev = std::find_if(entries_.begin(), entries_.end(), [prev_index](const Entry& entry) {
                    return entry.index == prev_index;
                });
                if (prev == entries_.end()) return false;
                prev_term = prev->term;
            }
            const auto commit = commit_index_;
            if (next <= LastLogIndexLocked()) {
                auto entry = std::find_if(entries_.begin(), entries_.end(), [next](const Entry& item) {
                    return item.index == next;
                });
                if (entry == entries_.end()) return false;
                sent_index = entry->index;
                request = "RAFT_APPEND " + std::to_string(sent_term) + " " + std::to_string(config_.node_id) + " " +
                    std::to_string(prev_index) + " " + std::to_string(prev_term) + " " + std::to_string(commit) + " 1 " +
                    EncodeEntry(*entry);
            } else {
                sent_index = prev_index;
                request = "RAFT_APPEND " + std::to_string(sent_term) + " " + std::to_string(config_.node_id) + " " +
                    std::to_string(prev_index) + " " + std::to_string(prev_term) + " " + std::to_string(commit) + " 0";
            }
        }

        const auto response = SendRpc(peer_it->address, request);
        std::istringstream input(response);
        std::string marker;
        std::uint64_t response_term = 0, match_index = 0;
        int success = 0;
        input >> marker >> response_term >> success >> match_index;
        std::lock_guard lock(mutex_);
        if (response_term > current_term_) {
            BecomeFollowerLocked(response_term);
            return false;
        }
        if (role_ != Role::kLeader || current_term_ != sent_term) return false;
        if (marker == "APPEND" && success != 0) {
            match_index_[peer_id] = std::max(match_index_[peer_id], match_index);
            next_index_[peer_id] = match_index_[peer_id] + 1;
            if (match_index >= target_index) return true;
        } else {
            auto& next = next_index_[peer_id];
            next = next > 1 ? next - 1 : 1;
        }
    }
    return false;
}

void RaftNode::AdvanceCommitIndex() {
    std::lock_guard lock(mutex_);
    if (role_ != Role::kLeader) return;
    const int majority = static_cast<int>((config_.peers.size() + 1) / 2 + 1);
    const auto last = LastLogIndexLocked();
    for (std::uint64_t index = last; index > commit_index_; --index) {
        const auto entry = std::find_if(entries_.begin(), entries_.end(), [index](const Entry& item) {
            return item.index == index;
        });
        if (entry == entries_.end() || entry->term != current_term_) continue;
        int replicated = 1;
        for (const auto& [peer, match] : match_index_) {
            (void)peer;
            if (match >= index) ++replicated;
        }
        if (replicated >= majority) {
            commit_index_ = index;
            PersistStateLocked();
            break;
        }
    }
}

void RaftNode::ApplyCommitted() {
    std::lock_guard apply_lock(apply_mutex_);
    while (true) {
        Entry entry;
        {
            std::lock_guard lock(mutex_);
            if (last_applied_ >= commit_index_) break;
            const auto next = last_applied_ + 1;
            auto found = std::find_if(entries_.begin(), entries_.end(), [next](const Entry& item) {
                return item.index == next;
            });
            if (found == entries_.end()) break;
            entry = *found;
        }
        store_.Apply(entry.operation, entry.key, entry.value);
        {
            std::lock_guard lock(mutex_);
            last_applied_ = entry.index;
        }
        MaybeSnapshot();
    }
}

void RaftNode::MaybeSnapshot() {
    std::uint64_t applied;
    {
        std::lock_guard lock(mutex_);
        if (last_applied_ < snapshot_index_ + 1000) return;
        applied = last_applied_;
    }
    if (store_.SaveSnapshot(config_.data_dir / "snapshot.dat", applied)) {
        std::lock_guard lock(mutex_);
        snapshot_index_ = std::max(snapshot_index_, applied);
    }
}

bool RaftNode::SaveSnapshot() {
    std::uint64_t applied;
    {
        std::lock_guard lock(mutex_);
        applied = last_applied_;
    }
    if (!store_.SaveSnapshot(config_.data_dir / "snapshot.dat", applied)) return false;
    std::lock_guard lock(mutex_);
    snapshot_index_ = std::max(snapshot_index_, applied);
    return true;
}

std::string RaftNode::Propose(Operation operation, const std::string& key, const std::string& value) {
    std::lock_guard proposal_lock(proposal_mutex_);
    Entry entry;
    {
        std::lock_guard lock(mutex_);
        if (role_ != Role::kLeader) return "NOT_LEADER " + std::to_string(leader_id_);
        entry.index = LastLogIndexLocked() + 1;
        entry.term = current_term_;
        entry.operation = operation;
        entry.key = key;
        entry.value = value;
        if (!AppendLogFile(entry)) return "ERR WAL append failed";
        entries_.push_back(entry);
    }

    int replicated = 1;
    for (const auto& peer : config_.peers) {
        if (ReplicatePeer(peer.id, entry.index, 8)) ++replicated;
    }
    const int majority = static_cast<int>((config_.peers.size() + 1) / 2 + 1);
    if (replicated < majority) return "NO_QUORUM";

    {
        std::lock_guard lock(mutex_);
        if (role_ != Role::kLeader || current_term_ != entry.term) return "NOT_LEADER " + std::to_string(leader_id_);
        commit_index_ = std::max(commit_index_, entry.index);
        PersistStateLocked();
    }
    ApplyCommitted();
    return "OK";
}

void RaftNode::RunTicker() {
    while (!stopping_.load()) {
        bool leader = false;
        bool election_due = false;
        {
            std::lock_guard lock(mutex_);
            leader = role_ == Role::kLeader;
            const auto timeout = std::chrono::milliseconds(350 + (config_.node_id % 5) * 70);
            election_due = !leader && std::chrono::steady_clock::now() - last_contact_ > timeout;
        }
        if (leader) {
            std::vector<int> peer_ids;
            {
                std::lock_guard lock(mutex_);
                for (const auto& peer : config_.peers) peer_ids.push_back(peer.id);
            }
            for (int peer : peer_ids) ReplicatePeer(peer, 0, 8);
            AdvanceCommitIndex();
            ApplyCommitted();
        } else if (election_due) {
            StartElection();
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(50));
    }
}

}  // namespace tinykv
