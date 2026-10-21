#include "tinykv/raft.hpp"
#include "tinykv/server.hpp"
#include "tinykv/store.hpp"
#include "tinykv/thread_pool.hpp"

#include <charconv>
#include <cstdlib>
#include <filesystem>
#include <iostream>
#include <stdexcept>
#include <string>
#include <unordered_set>
#include <vector>

namespace {

int ParseInt(const std::string& value, const char* name) {
    int out = 0;
    const auto parsed = std::from_chars(value.data(), value.data() + value.size(), out);
    if (parsed.ec != std::errc{} || parsed.ptr != value.data() + value.size()) {
        throw std::invalid_argument(std::string("invalid value for ") + name);
    }
    return out;
}

struct Options {
    int id{1};
    int port{8001};
    int threads{4};
    int queue_limit{1024};
    std::string bind{"127.0.0.1"};
    std::filesystem::path data{"data-1"};
    std::vector<tinykv::Peer> peers;
};

Options ParseOptions(int argc, char* argv[]) {
    Options options;
    bool data_given = false;
    for (int i = 1; i < argc; ++i) {
        const std::string flag = argv[i];
        auto value = [&]() -> std::string {
            if (++i >= argc) throw std::invalid_argument("missing value after " + flag);
            return argv[i];
        };
        if (flag == "--id") options.id = ParseInt(value(), "--id");
        else if (flag == "--port") options.port = ParseInt(value(), "--port");
        else if (flag == "--bind") options.bind = value();
        else if (flag == "--data") { options.data = value(); data_given = true; }
        else if (flag == "--threads") options.threads = ParseInt(value(), "--threads");
        else if (flag == "--queue") options.queue_limit = ParseInt(value(), "--queue");
        else if (flag == "--peer") {
            const std::string peer = value();
            const auto at = peer.find('@');
            if (at == std::string::npos || at == 0 || at + 1 == peer.size()) {
                throw std::invalid_argument("peer format must be id@host:port");
            }
            options.peers.push_back(tinykv::Peer{ParseInt(peer.substr(0, at), "peer id"), peer.substr(at + 1)});
        } else if (flag == "--help") {
            std::cout << "usage: kv_server [--id N] [--port N] [--bind IPv4] [--data DIR] "
                         "[--peer id@host:port]... [--threads N] [--queue N]\n";
            std::exit(0);
        } else {
            throw std::invalid_argument("unknown option: " + flag);
        }
    }
    if (options.id <= 0 || options.port <= 0 || options.port > 65535 || options.threads <= 0 || options.queue_limit <= 0) {
        throw std::invalid_argument("id, port, threads and queue must be positive (port <= 65535)");
    }
    std::unordered_set<int> peer_ids;
    for (const auto& peer : options.peers) {
        if (peer.id <= 0 || peer.id == options.id || !peer_ids.insert(peer.id).second ||
            peer.address.find(':') == std::string::npos) {
            throw std::invalid_argument("peer IDs must be positive, unique, non-self IDs with host:port addresses");
        }
    }
    if (!data_given) options.data = "data-" + std::to_string(options.id);
    return options;
}

}  // namespace

int main(int argc, char* argv[]) {
    try {
        const Options options = ParseOptions(argc, argv);
        tinykv::ShardedStore store(16);
        tinykv::RaftNode raft(tinykv::RaftConfig{options.id, options.data, options.peers}, store);
        tinykv::ThreadPool workers(static_cast<std::size_t>(options.threads), static_cast<std::size_t>(options.queue_limit));
        tinykv::ThreadPool raft_workers(2, 128);
        tinykv::Server server(store, raft, workers, raft_workers, options.bind, options.port);
        raft.Start();
        std::cout << raft.Status() << '\n';
        return server.Run();
    } catch (const std::exception& error) {
        std::cerr << error.what() << '\n';
        return EXIT_FAILURE;
    }
}
