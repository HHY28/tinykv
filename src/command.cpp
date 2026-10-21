#include "tinykv/command.hpp"

#include <cctype>
#include <sstream>
#include <utility>

namespace tinykv {
namespace {

std::string_view TrimLeft(std::string_view text) {
    const auto start = text.find_first_not_of(" \t");
    return start == std::string_view::npos ? std::string_view{} : text.substr(start);
}

std::string Upper(std::string text) {
    for (char& ch : text) {
        ch = static_cast<char>(std::toupper(static_cast<unsigned char>(ch)));
    }
    return text;
}

std::vector<std::string> SplitWords(std::string_view text) {
    std::istringstream input{std::string(text)};
    std::vector<std::string> words;
    std::string word;
    while (input >> word) {
        words.push_back(std::move(word));
    }
    return words;
}

}  // namespace

Command ParseCommand(std::string_view line) {
    line = TrimLeft(line);
    const auto end = line.find_first_of(" \t");
    const std::string name = Upper(std::string(line.substr(0, end)));
    const std::string_view rest = end == std::string_view::npos ? std::string_view{} : TrimLeft(line.substr(end));
    Command result;

    if (name == "PING") result.type = CommandType::kPing;
    else if (name == "SET") result.type = CommandType::kSet;
    else if (name == "GET") result.type = CommandType::kGet;
    else if (name == "DEL") result.type = CommandType::kDel;
    else if (name == "SAVE") result.type = CommandType::kSave;
    else if (name == "STATUS") result.type = CommandType::kStatus;
    else if (name == "QUIT") result.type = CommandType::kQuit;
    else if (name == "RAFT_VOTE") result.type = CommandType::kRaftVote;
    else if (name == "RAFT_APPEND") result.type = CommandType::kRaftAppend;
    else {
        result.error = "unknown command";
        return result;
    }

    if (result.type == CommandType::kSet) {
        const auto key_end = rest.find_first_of(" \t");
        if (rest.empty() || key_end == std::string_view::npos) {
            result.type = CommandType::kInvalid;
            result.error = "usage: SET <key> <value>";
            return result;
        }
        result.key = std::string(rest.substr(0, key_end));
        result.value = std::string(TrimLeft(rest.substr(key_end)));
        return result;
    }

    if (result.type == CommandType::kGet || result.type == CommandType::kDel) {
        result.args = SplitWords(rest);
        if (result.args.size() != 1) {
            result.type = CommandType::kInvalid;
            result.error = result.error.empty() ? "expected exactly one key" : result.error;
        } else {
            result.key = result.args.front();
        }
        return result;
    }

    if (result.type == CommandType::kRaftVote || result.type == CommandType::kRaftAppend) {
        result.args = SplitWords(rest);
        return result;
    }

    if (!rest.empty()) {
        result.type = CommandType::kInvalid;
        result.error = "command takes no arguments";
    }
    return result;
}

std::string HexEncode(std::string_view value) {
    constexpr char digits[] = "0123456789abcdef";
    std::string out;
    out.reserve(value.size() * 2);
    for (unsigned char byte : value) {
        out.push_back(digits[byte >> 4]);
        out.push_back(digits[byte & 0x0f]);
    }
    return out;
}

bool HexDecode(std::string_view input, std::string& output) {
    if (input.size() % 2 != 0) return false;
    auto digit = [](char ch) -> int {
        if (ch >= '0' && ch <= '9') return ch - '0';
        if (ch >= 'a' && ch <= 'f') return ch - 'a' + 10;
        if (ch >= 'A' && ch <= 'F') return ch - 'A' + 10;
        return -1;
    };
    output.clear();
    output.reserve(input.size() / 2);
    for (std::size_t i = 0; i < input.size(); i += 2) {
        const int high = digit(input[i]);
        const int low = digit(input[i + 1]);
        if (high < 0 || low < 0) return false;
        output.push_back(static_cast<char>((high << 4) | low));
    }
    return true;
}

}  // namespace tinykv
