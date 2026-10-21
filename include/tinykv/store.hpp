#pragma once

#include <array>
#include <cstdint>
#include <filesystem>
#include <memory>
#include <mutex>
#include <shared_mutex>
#include <string>
#include <string_view>
#include <unordered_map>
#include <vector>

namespace tinykv {

enum class Operation { kSet, kDel };

class ShardedStore {
public:
    explicit ShardedStore(std::size_t shard_count = 16);
    bool Get(std::string_view key, std::string& value) const;
    void Apply(Operation operation, const std::string& key, const std::string& value);
    bool SaveSnapshot(const std::filesystem::path& path, std::uint64_t index) const;
    bool LoadSnapshot(const std::filesystem::path& path, std::uint64_t& index);

private:
    struct Shard {
        mutable std::mutex mutex;
        std::unordered_map<std::string, std::string> values;
    };
    std::size_t ShardFor(std::string_view key) const;
    mutable std::shared_mutex snapshot_gate_;
    std::vector<std::unique_ptr<Shard>> shards_;
};

}  // namespace tinykv
