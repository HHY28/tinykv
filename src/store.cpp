#include "tinykv/store.hpp"

#include "tinykv/command.hpp"

#include <fstream>
#include <sstream>
#include <fcntl.h>
#include <unistd.h>
#include <system_error>

namespace tinykv {

ShardedStore::ShardedStore(std::size_t shard_count) {
    if (shard_count == 0) shard_count = 1;
    shards_.reserve(shard_count);
    for (std::size_t i = 0; i < shard_count; ++i) {
        shards_.push_back(std::make_unique<Shard>());
    }
}

std::size_t ShardedStore::ShardFor(std::string_view key) const {
    return std::hash<std::string_view>{}(key) % shards_.size();
}

bool ShardedStore::Get(std::string_view key, std::string& value) const {
    std::shared_lock snapshot_lock(snapshot_gate_);
    const auto& shard = *shards_[ShardFor(key)];
    std::lock_guard shard_lock(shard.mutex);
    const auto it = shard.values.find(std::string(key));
    if (it == shard.values.end()) return false;
    value = it->second;
    return true;
}

void ShardedStore::Apply(Operation operation, const std::string& key, const std::string& value) {
    std::shared_lock snapshot_lock(snapshot_gate_);
    auto& shard = *shards_[ShardFor(key)];
    std::lock_guard shard_lock(shard.mutex);
    if (operation == Operation::kSet) shard.values[key] = value;
    else shard.values.erase(key);
}

bool ShardedStore::SaveSnapshot(const std::filesystem::path& path, std::uint64_t index) const {
    std::unique_lock snapshot_lock(snapshot_gate_);
    const auto temporary = path.string() + ".tmp";
    std::ofstream output(temporary, std::ios::binary | std::ios::trunc);
    if (!output) return false;
    output << "TINYKV_SNAPSHOT 1 " << index << '\n';
    for (const auto& shard_ptr : shards_) {
        const auto& shard = *shard_ptr;
        std::lock_guard shard_lock(shard.mutex);
        for (const auto& [key, value] : shard.values) {
            output << HexEncode(key) << ' ' << HexEncode(value) << '\n';
        }
    }
    output.flush();
    if (!output) return false;
    output.close();
    const int snapshot_fd = ::open(temporary.c_str(), O_RDONLY);
    if (snapshot_fd < 0) return false;
    const bool synced = ::fsync(snapshot_fd) == 0;
    ::close(snapshot_fd);
    if (!synced) return false;
    std::error_code error;
    std::filesystem::rename(temporary, path, error);
    if (error) {
        std::filesystem::remove(path, error);
        error.clear();
        std::filesystem::rename(temporary, path, error);
    }
    if (error) return false;
    const int directory_fd = ::open(path.parent_path().c_str(), O_RDONLY | O_DIRECTORY);
    if (directory_fd < 0) return false;
    const bool directory_synced = ::fsync(directory_fd) == 0;
    ::close(directory_fd);
    return directory_synced;
}

bool ShardedStore::LoadSnapshot(const std::filesystem::path& path, std::uint64_t& index) {
    std::ifstream input(path, std::ios::binary);
    if (!input) return false;
    std::string header;
    if (!std::getline(input, header)) return false;
    std::istringstream header_stream(header);
    std::string marker;
    int version = 0;
    if (!(header_stream >> marker >> version >> index) || marker != "TINYKV_SNAPSHOT" || version != 1) {
        return false;
    }

    std::vector<std::unordered_map<std::string, std::string>> restored(shards_.size());
    std::string line;
    while (std::getline(input, line)) {
        const auto separator = line.find(' ');
        if (separator == std::string::npos) return false;
        std::string key;
        std::string value;
        if (!HexDecode(std::string_view(line).substr(0, separator), key) ||
            !HexDecode(std::string_view(line).substr(separator + 1), value)) return false;
        restored[ShardFor(key)][std::move(key)] = std::move(value);
    }

    std::unique_lock snapshot_lock(snapshot_gate_);
    for (std::size_t i = 0; i < shards_.size(); ++i) {
        std::lock_guard shard_lock(shards_[i]->mutex);
        shards_[i]->values = std::move(restored[i]);
    }
    return true;
}

}  // namespace tinykv
