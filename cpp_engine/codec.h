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

// Snapshot and legacy WAL records retain their original layout. The snapshot
// reader never repairs incomplete entries, so its existing framing stays safe.
constexpr size_t kRecordHeader = 21;
constexpr size_t kWalRecordHeader = 25;
constexpr size_t kWalFileHeader = 24;

inline std::string wal_file_header() {
    std::string bytes = "MKVWAL02";
    append_u32(bytes, 0); // flags
    append_u64(bytes, 0); // reserved
    append_u32(bytes, crc32(bytes));
    return bytes;
}

inline void validate_wal_file_header(std::string_view bytes) {
    if (bytes.size() != kWalFileHeader || bytes.substr(0, 8) != "MKVWAL02" ||
        u32(bytes, 8) != 0 || u64(bytes, 12) != 0 || u32(bytes, 20) != crc32(bytes.substr(0, 20))) {
        throw std::runtime_error("corrupt WAL file header");
    }
}

inline size_t record_payload_size(std::string_view header) {
    const auto op = static_cast<Operation>(header[4]);
    const auto key_size = u32(header, 13), value_size = u32(header, 17);
    if (key_size == 0 || key_size > kMaxKeySize || value_size > kMaxValueSize ||
        (op != Operation::Put && op != Operation::Delete) || (op == Operation::Delete && value_size != 0)) {
        throw std::runtime_error("corrupt record lengths or operation");
    }
    return key_size + value_size;
}

inline std::string encode_record(uint64_t sequence, Operation op, const std::string& key,
                                 const std::string& value, bool protected_header) {
    if (op != Operation::Put && op != Operation::Delete) throw std::runtime_error("invalid persistent operation");
    std::string bytes = protected_header ? "MKL2" : "MKL1";
    bytes.push_back(static_cast<char>(op));
    append_u64(bytes, sequence);
    append_u32(bytes, static_cast<uint32_t>(key.size()));
    append_u32(bytes, static_cast<uint32_t>(value.size()));
    if (protected_header) append_u32(bytes, crc32(bytes));
    bytes += key;
    bytes += value;
    append_u32(bytes, crc32(bytes));
    return bytes;
}

inline std::string record(uint64_t sequence, Operation op, const std::string& key, const std::string& value) {
    return encode_record(sequence, op, key, value, false);
}

inline std::string wal_record(uint64_t sequence, Operation op, const std::string& key, const std::string& value) {
    return encode_record(sequence, op, key, value, true);
}

inline size_t record_size(std::string_view header) {
    if (header.size() < kRecordHeader || header.substr(0, 4) != "MKL1") throw std::runtime_error("corrupt record header");
    return kRecordHeader + record_payload_size(header) + 4;
}

inline size_t wal_record_size(std::string_view header) {
    // The fixed header is verified before its lengths can turn corruption into
    // apparent EOF. A short body is repairable only after this check succeeds.
    if (header.size() < kWalRecordHeader || header.substr(0, 4) != "MKL2" ||
        u32(header, 21) != crc32(header.substr(0, 21))) {
        throw std::runtime_error("corrupt WAL record header");
    }
    return kWalRecordHeader + record_payload_size(header) + 4;
}

inline Request decode_record_body(std::string_view bytes, size_t header_size, size_t record_bytes) {
    if (bytes.size() != record_bytes || u32(bytes, bytes.size() - 4) != crc32(bytes.substr(0, bytes.size() - 4))) {
        throw std::runtime_error("record checksum mismatch");
    }
    const auto key_size = u32(bytes, 13);
    return {static_cast<Operation>(bytes[4]), std::string(bytes.substr(header_size, key_size)),
            std::string(bytes.substr(header_size + key_size, u32(bytes, 17)))};
}

inline Request decode_record(std::string_view bytes) {
    return decode_record_body(bytes, kRecordHeader, record_size(bytes));
}

inline Request decode_wal_record(std::string_view bytes) {
    return decode_record_body(bytes, kWalRecordHeader, wal_record_size(bytes));
}

} // namespace minikv::codec
