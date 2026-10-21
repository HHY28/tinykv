#include "tinykv/server.hpp"

#include "tinykv/command.hpp"

#include <cerrno>
#include <cstring>
#include <exception>
#include <iostream>
#include <sstream>
#include <utility>
#include <sys/epoll.h>
#include <sys/eventfd.h>
#include <sys/socket.h>
#include <unistd.h>

#include <arpa/inet.h>
#include <fcntl.h>

namespace tinykv {
namespace {
constexpr std::size_t kMaxInput = 256 * 1024;
constexpr std::size_t kMaxOutput = 1024 * 1024;
constexpr std::size_t kMaxConnections = 2048;
constexpr int kMaxEvents = 128;

bool SendAllNonBlocking(int fd, std::string& output) {
    while (!output.empty()) {
        const ssize_t sent = ::send(fd, output.data(), output.size(), MSG_NOSIGNAL);
        if (sent < 0) {
            if (errno == EINTR) continue;
            if (errno == EAGAIN || errno == EWOULDBLOCK) return true;
            return false;
        }
        if (sent == 0) return false;
        output.erase(0, static_cast<std::size_t>(sent));
    }
    return true;
}

}  // namespace

Server::Server(ShardedStore& store, RaftNode& raft, ThreadPool& workers, ThreadPool& raft_workers,
               std::string bind_address, int port)
    : store_(store), raft_(raft), workers_(workers), raft_workers_(raft_workers),
      bind_address_(std::move(bind_address)), port_(port) {}

int Server::Run() {
    listen_fd_ = ::socket(AF_INET, SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0);
    if (listen_fd_ < 0) {
        std::cerr << "socket: " << std::strerror(errno) << '\n';
        return 1;
    }
    int reuse = 1;
    ::setsockopt(listen_fd_, SOL_SOCKET, SO_REUSEADDR, &reuse, sizeof(reuse));
    sockaddr_in address{};
    address.sin_family = AF_INET;
    address.sin_port = htons(static_cast<std::uint16_t>(port_));
    if (::inet_pton(AF_INET, bind_address_.c_str(), &address.sin_addr) != 1) {
        std::cerr << "invalid IPv4 bind address: " << bind_address_ << '\n';
        return 1;
    }
    if (::bind(listen_fd_, reinterpret_cast<sockaddr*>(&address), sizeof(address)) < 0 ||
        ::listen(listen_fd_, SOMAXCONN) < 0) {
        std::cerr << "bind/listen: " << std::strerror(errno) << '\n';
        return 1;
    }
    epoll_fd_ = ::epoll_create1(EPOLL_CLOEXEC);
    completion_fd_ = ::eventfd(0, EFD_NONBLOCK | EFD_CLOEXEC);
    if (epoll_fd_ < 0 || completion_fd_ < 0) {
        std::cerr << "epoll/eventfd: " << std::strerror(errno) << '\n';
        return 1;
    }
    epoll_event listen_event{};
    listen_event.events = EPOLLIN;
    listen_event.data.fd = listen_fd_;
    ::epoll_ctl(epoll_fd_, EPOLL_CTL_ADD, listen_fd_, &listen_event);
    epoll_event completion_event{};
    completion_event.events = EPOLLIN;
    completion_event.data.fd = completion_fd_;
    ::epoll_ctl(epoll_fd_, EPOLL_CTL_ADD, completion_fd_, &completion_event);

    std::cout << "TinyKV node listening on " << bind_address_ << ':' << port_ << '\n';
    epoll_event events[kMaxEvents];
    while (true) {
        const int count = ::epoll_wait(epoll_fd_, events, kMaxEvents, -1);
        if (count < 0) {
            if (errno == EINTR) continue;
            std::cerr << "epoll_wait: " << std::strerror(errno) << '\n';
            break;
        }
        for (int i = 0; i < count; ++i) {
            const int fd = events[i].data.fd;
            if (fd == listen_fd_) {
                while (true) {
                    const int client = ::accept4(listen_fd_, nullptr, nullptr, SOCK_NONBLOCK | SOCK_CLOEXEC);
                    if (client < 0) {
                        if (errno == EINTR) continue;
                        if (errno == EAGAIN || errno == EWOULDBLOCK) break;
                        std::cerr << "accept: " << std::strerror(errno) << '\n';
                        break;
                    }
                    if (connections_.size() >= kMaxConnections) {
                        (void)::send(client, "BUSY\n", 5, MSG_NOSIGNAL);
                        ::close(client);
                        continue;
                    }
                    connections_.emplace(client, Connection{});
                    epoll_event client_event{};
                    client_event.events = EPOLLIN | EPOLLRDHUP;
                    client_event.data.fd = client;
                    ::epoll_ctl(epoll_fd_, EPOLL_CTL_ADD, client, &client_event);
                }
            } else if (fd == completion_fd_) {
                std::uint64_t value;
                while (::read(completion_fd_, &value, sizeof(value)) > 0) {}
                ProcessCompletions();
            } else {
                HandleClientEvent(fd, events[i].events);
            }
        }
    }
    return 0;
}

void Server::HandleClientEvent(int fd, std::uint32_t events) {
    auto found = connections_.find(fd);
    if (found == connections_.end()) return;
    if (events & EPOLLIN) ReadClient(fd);
    found = connections_.find(fd);
    if (found == connections_.end()) return;
    if (events & EPOLLOUT) WriteClient(fd);
    found = connections_.find(fd);
    if (found == connections_.end()) return;
    if (events & (EPOLLERR | EPOLLHUP | EPOLLRDHUP)) found->second.closing = true;
    MaybeClose(fd);
    if (connections_.contains(fd)) UpdateInterest(fd);
}

void Server::ReadClient(int fd) {
    auto found = connections_.find(fd);
    if (found == connections_.end()) return;
    char buffer[8192];
    while (true) {
        const ssize_t count = ::recv(fd, buffer, sizeof(buffer), 0);
        if (count < 0) {
            if (errno == EINTR) continue;
            if (errno == EAGAIN || errno == EWOULDBLOCK) break;
            found->second.closing = true;
            return;
        }
        if (count == 0) {
            found->second.closing = true;
            break;
        }
        found->second.input.append(buffer, static_cast<std::size_t>(count));
        if (found->second.input.size() > kMaxInput) {
            found->second.output += "ERR input buffer limit exceeded\n";
            found->second.closing = true;
            break;
        }
    }
    DispatchNext(fd);
    if (connections_.contains(fd)) UpdateInterest(fd);
}

void Server::WriteClient(int fd) {
    auto found = connections_.find(fd);
    if (found == connections_.end()) return;
    if (!SendAllNonBlocking(fd, found->second.output)) found->second.closing = true;
    if (found->second.output.empty() && !found->second.in_flight &&
        !found->second.closing && !found->second.close_after_response) {
        DispatchNext(fd);
    }
    if (connections_.contains(fd)) UpdateInterest(fd);
    MaybeClose(fd);
}

void Server::DispatchNext(int fd) {
    auto found = connections_.find(fd);
    if (found == connections_.end()) return;
    auto& connection = found->second;
    if (connection.in_flight || connection.close_after_response) return;
    const auto newline = connection.input.find('\n');
    if (newline == std::string::npos) return;
    std::string line = connection.input.substr(0, newline);
    connection.input.erase(0, newline + 1);
    if (!line.empty() && line.back() == '\r') line.pop_back();
    connection.in_flight = true;

    const auto command_type = ParseCommand(line).type;
    ThreadPool& pool = (command_type == CommandType::kRaftVote || command_type == CommandType::kRaftAppend)
        ? raft_workers_ : workers_;
    const bool accepted = pool.Submit([this, fd, line = std::move(line)]() mutable {
        bool close_after = false;
        std::string response;
        try {
            response = Execute(std::move(line), close_after);
        } catch (const std::exception& error) {
            response = std::string("ERR internal: ") + error.what() + "\n";
        } catch (...) {
            response = "ERR internal failure\n";
        }
        EnqueueCompletion(Completion{fd, std::move(response), close_after});
    });
    if (!accepted) {
        connection.in_flight = false;
        connection.input.clear();
        connection.output += "BUSY\n";
    }
}

void Server::EnqueueCompletion(Completion completion) {
    {
        std::lock_guard lock(completion_mutex_);
        completions_.push_back(std::move(completion));
    }
    const std::uint64_t one = 1;
    (void)::write(completion_fd_, &one, sizeof(one));
}

void Server::ProcessCompletions() {
    std::vector<Completion> ready;
    {
        std::lock_guard lock(completion_mutex_);
        ready.swap(completions_);
    }
    for (auto& completion : ready) {
        auto found = connections_.find(completion.fd);
        if (found == connections_.end()) continue;
        auto& connection = found->second;
        connection.in_flight = false;
        connection.output += std::move(completion.response);
        connection.close_after_response = completion.close_after_response;
        if (connection.output.size() > kMaxOutput) connection.closing = true;
        if (!connection.close_after_response && !connection.closing) DispatchNext(completion.fd);
        UpdateInterest(completion.fd);
        MaybeClose(completion.fd);
    }
}

void Server::UpdateInterest(int fd) {
    const auto found = connections_.find(fd);
    if (found == connections_.end()) return;
    epoll_event event{};
    event.events = EPOLLRDHUP;
    if (!found->second.closing) event.events |= EPOLLIN;
    if (!found->second.output.empty()) event.events |= EPOLLOUT;
    event.data.fd = fd;
    if (::epoll_ctl(epoll_fd_, EPOLL_CTL_MOD, fd, &event) < 0 && errno != ENOENT) {
        found->second.closing = true;
    }
}

void Server::MaybeClose(int fd) {
    const auto found = connections_.find(fd);
    if (found == connections_.end()) return;
    auto& connection = found->second;
    if (connection.close_after_response && connection.output.empty()) connection.closing = true;
    if (connection.closing && !connection.in_flight && connection.output.empty()) {
        ::epoll_ctl(epoll_fd_, EPOLL_CTL_DEL, fd, nullptr);
        ::close(fd);
        connections_.erase(found);
        return;
    }
}

std::string Server::Execute(std::string line, bool& close_after_response) {
    Command command = ParseCommand(line);
    switch (command.type) {
        case CommandType::kPing: return "PONG\n";
        case CommandType::kSet: {
            const auto result = raft_.Propose(Operation::kSet, command.key, command.value);
            return result + "\n";
        }
        case CommandType::kGet: {
            if (!raft_.IsLeader()) return "NOT_LEADER\n";
            std::string value;
            return store_.Get(command.key, value) ? "VALUE " + value + "\n" : "NOT_FOUND\n";
        }
        case CommandType::kDel: {
            const auto result = raft_.Propose(Operation::kDel, command.key, {});
            return result + "\n";
        }
        case CommandType::kSave: return raft_.SaveSnapshot() ? "OK SNAPSHOT\n" : "ERR SNAPSHOT_FAILED\n";
        case CommandType::kStatus: return raft_.Status() + "\n";
        case CommandType::kQuit:
            close_after_response = true;
            return "BYE\n";
        case CommandType::kRaftVote:
            return raft_.HandleVote(command.args) + "\n";
        case CommandType::kRaftAppend:
            return raft_.HandleAppend(command.args) + "\n";
        case CommandType::kInvalid:
            return "ERR " + command.error + "\n";
    }
    return "ERR unsupported command\n";
}

}  // namespace tinykv
