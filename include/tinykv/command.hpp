#pragma once

#include <string>
#include <string_view>
#include <vector>

namespace tinykv {

enum class CommandType {
    kPing,
    kSet,
    kGet,
    kDel,
    kSave,
    kStatus,
    kQuit,
    kRaftVote,
    kRaftAppend,
    kInvalid,
};

struct Command {
    CommandType type{CommandType::kInvalid};
    std::string key;
    std::string value;
    std::vector<std::string> args;
    std::string error;
};

Command ParseCommand(std::string_view line);
std::string HexEncode(std::string_view value);
bool HexDecode(std::string_view input, std::string& output);

}  // namespace tinykv
