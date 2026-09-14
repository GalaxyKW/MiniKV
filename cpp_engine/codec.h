#pragma once

#include "engine.h"

#include <array>
#include <stdexcept>
#include <string>
#include <string_view>

namespace minikv::codec {

inline void append_u32(std::string& out, uint32_t value) {
    for (int shift = 24; shift >= 0; shift -= 8) out.push_back(static_cast<char>(value >> shift));
}

inline void append_u64(std::string& out, uint64_t value) {
    for (int shift = 56; shift >= 0; shift -= 8) out.push_back(static_cast<char>(value >> shift));
}

inline uint32_t u32(std::string_view bytes, size_t offset) {
    uint32_t value = 0;
    for (size_t i = 0; i < 4; ++i) value = (value << 8) | static_cast<unsigned char>(bytes.at(offset + i));
    return value;
}

inline uint64_t u64(std::string_view bytes, size_t offset) {
    uint64_t value = 0;
    for (size_t i = 0; i < 8; ++i) value = (value << 8) | static_cast<unsigned char>(bytes.at(offset + i));
    return value;
}

inline uint32_t crc32(std::string_view bytes) {
    static const auto table = [] {
        std::array<uint32_t, 256> values{};
        for (uint32_t i = 0; i < values.size(); ++i) {
            uint32_t c = i;
            for (int bit = 0; bit < 8; ++bit) c = (c >> 1) ^ ((c & 1) ? 0xedb88320U : 0);
            values[i] = c;
        }
        return values;
    }();
    uint32_t crc = 0xffffffffU;
    for (unsigned char c : bytes) crc = table[(crc ^ c) & 0xff] ^ (crc >> 8);
    return crc ^ 0xffffffffU;
}

inline bool valid_request(const Request& request) {
    // Engine::execute handles only KV operations. Stats is dispatched by the
    // server and must never reach the mutation path or persistent record codec.
    return !request.key.empty() && request.key.size() <= kMaxKeySize &&
           request.value.size() <= kMaxValueSize &&
           (request.operation == Operation::Put ||
            ((request.operation == Operation::Get || request.operation == Operation::Delete) && request.value.empty()));
}

constexpr size_t kRequestHeader = 16;

inline size_t request_size(std::string_view header) {
    if (header.size() < kRequestHeader || header.substr(0, 4) != "MKV1" ||
        header[5] != 0 || header[6] != 0 || header[7] != 0) {
        throw std::runtime_error("invalid protocol header");
    }
    const auto op = static_cast<Operation>(header[4]);
    const uint32_t key_size = u32(header, 8), value_size = u32(header, 12);
    if (op == Operation::Stats) {
        if (key_size != 0 || value_size != 0) throw std::runtime_error("stats request must have no payload");
        return kRequestHeader;
    }
    if (key_size == 0 || key_size > kMaxKeySize || value_size > kMaxValueSize ||
        (op != Operation::Put && op != Operation::Get && op != Operation::Delete) ||
        (op != Operation::Put && value_size != 0)) {
        throw std::runtime_error("invalid operation or key/value length");
    }
    return kRequestHeader + key_size + value_size;
}

inline Request decode_request(std::string_view frame) {
    if (frame.size() != request_size(frame)) throw std::runtime_error("incomplete request");
    const auto key_size = u32(frame, 8);
    return {static_cast<Operation>(frame[4]), std::string(frame.substr(kRequestHeader, key_size)),
            std::string(frame.substr(kRequestHeader + key_size))};
}

inline std::string response(const Response& result) {
    std::string bytes = "MKR1";
    bytes.push_back(static_cast<char>(result.status));
    bytes.append(3, '\0');
    append_u32(bytes, static_cast<uint32_t>(result.value.size()));
    bytes += result.value;
    return bytes;
}

// A storage record has a 21-byte header, raw key/value bytes, and a CRC32.
constexpr size_t kRecordHeader = 21;

inline std::string record(uint64_t sequence, Operation op, const std::string& key, const std::string& value) {
    if (op != Operation::Put && op != Operation::Delete) throw std::runtime_error("invalid persistent operation");
    std::string bytes = "MKL1";
    bytes.push_back(static_cast<char>(op));
    append_u64(bytes, sequence);
    append_u32(bytes, static_cast<uint32_t>(key.size()));
    append_u32(bytes, static_cast<uint32_t>(value.size()));
    bytes += key;
    bytes += value;
    append_u32(bytes, crc32(bytes));
    return bytes;
}

inline size_t record_size(std::string_view header) {
    if (header.size() < kRecordHeader || header.substr(0, 4) != "MKL1") throw std::runtime_error("corrupt record header");
    const auto op = static_cast<Operation>(header[4]);
    const auto key_size = u32(header, 13), value_size = u32(header, 17);
    if (key_size == 0 || key_size > kMaxKeySize || value_size > kMaxValueSize ||
        (op != Operation::Put && op != Operation::Delete) || (op == Operation::Delete && value_size != 0)) {
        throw std::runtime_error("corrupt record lengths or operation");
    }
    return kRecordHeader + key_size + value_size + 4;
}

inline Request decode_record(std::string_view bytes) {
    if (bytes.size() != record_size(bytes) || u32(bytes, bytes.size() - 4) != crc32(bytes.substr(0, bytes.size() - 4))) {
        throw std::runtime_error("record checksum mismatch");
    }
    const auto key_size = u32(bytes, 13);
    return {static_cast<Operation>(bytes[4]), std::string(bytes.substr(kRecordHeader, key_size)),
            std::string(bytes.substr(kRecordHeader + key_size, u32(bytes, 17)))};
}

} // namespace minikv::codec
