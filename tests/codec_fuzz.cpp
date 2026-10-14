#include "codec.h"

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <limits>
#include <stdexcept>
#include <string_view>

namespace {

// The input is borrowed. A successful decoder allocates at most one 4096-byte
// key and one 1 MiB value; the independent reference uses only views/scalars.
// Keep a few bytes beyond the largest valid frame to exercise trailing input.
constexpr size_t kInputLimit = 1052706;
constexpr uint32_t kKeyLimit = 4096;
constexpr uint32_t kValueLimit = 1048576;

void require(bool condition) {
    if (!condition) std::abort(); // Remains active in release/NDEBUG builds.
}

uint8_t byte_at(std::string_view bytes, size_t offset) {
    require(offset < bytes.size());
    return static_cast<uint8_t>(bytes[offset]);
}

uint64_t integer_at(std::string_view bytes, size_t offset, size_t width) {
    require(offset <= bytes.size() && width <= bytes.size() - offset && width <= 8);
    uint64_t number = 0;
    for (size_t index = 0; index < width; ++index) number = number * 256 + byte_at(bytes, offset + index);
    return number;
}

// An independent bitwise IEEE CRC32 reference, not the codec's lookup table.
uint32_t reference_crc(std::string_view bytes) {
    uint32_t checksum = UINT32_C(0xffffffff);
    for (const unsigned char byte : bytes) {
        checksum ^= byte;
        for (unsigned bit = 0; bit < 8; ++bit) {
            const bool low_bit = (checksum & 1) != 0;
            checksum /= 2;
            if (low_bit) checksum ^= UINT32_C(0xedb88320);
        }
    }
    return ~checksum;
}

bool checksum_matches(std::string_view bytes, size_t checksum_offset) {
    require(checksum_offset <= bytes.size());
    if (bytes.size() == checksum_offset) return true;
    const uint32_t expected = reference_crc(bytes.substr(0, checksum_offset));
    for (size_t index = 0; index < 4 && checksum_offset + index < bytes.size(); ++index) {
        const auto expected_byte = static_cast<uint8_t>(expected >> ((3 - index) * 8));
        if (byte_at(bytes, checksum_offset + index) != expected_byte) return false;
    }
    return true;
}

// Complete missing bytes with 0x00 and 0xff to obtain the possible interval.
// This differs from the codec's comparison of shifted min/max prefixes.
bool prefix_in_range(std::string_view bytes, size_t offset, size_t width,
                     uint64_t minimum, uint64_t maximum) {
    require(width >= 1 && width <= 8 && minimum <= maximum);
    uint64_t lowest = 0, highest = 0;
    for (size_t index = 0; index < width; ++index) {
        const bool present = offset < bytes.size() && index < bytes.size() - offset;
        const uint8_t byte = present ? byte_at(bytes, offset + index) : 0;
        lowest = lowest * 256 + byte;
        highest = highest * 256 + (present ? byte : 255);
    }
    return lowest <= maximum && highest >= minimum;
}

struct Layout {
    bool valid = false;
    uint8_t operation = 0;
    size_t payload_offset = 0;
    size_t key_size = 0;
    size_t value_size = 0;
};

Layout request_layout(std::string_view bytes) {
    if (bytes.size() < 16 || bytes.substr(0, 4) != "MKV1" ||
        byte_at(bytes, 5) != 0 || byte_at(bytes, 6) != 0 || byte_at(bytes, 7) != 0) return {};
    const auto operation = byte_at(bytes, 4);
    const auto key_size = integer_at(bytes, 8, 4);
    const auto value_size = integer_at(bytes, 12, 4);
    if (operation == 4) {
        if (key_size != 0 || value_size != 0 || bytes.size() != 16) return {};
    } else {
        if ((operation != 1 && operation != 2 && operation != 3) ||
            key_size == 0 || key_size > kKeyLimit || value_size > kValueLimit ||
            (operation != 1 && value_size != 0) || bytes.size() != 16 + key_size + value_size) return {};
    }
    return {true, operation, 16, static_cast<size_t>(key_size), static_cast<size_t>(value_size)};
}

Layout record_layout(std::string_view bytes, bool protected_header) {
    const size_t header_size = protected_header ? 25 : 21;
    if (bytes.size() < header_size + 4 || bytes.substr(0, 4) != (protected_header ? "MKL2" : "MKL1")) return {};
    const auto operation = byte_at(bytes, 4);
    const auto key_size = integer_at(bytes, 13, 4);
    const auto value_size = integer_at(bytes, 17, 4);
    if ((operation != 1 && operation != 3) || key_size == 0 || key_size > kKeyLimit ||
        value_size > kValueLimit || (operation == 3 && value_size != 0) ||
        bytes.size() != header_size + key_size + value_size + 4) return {};
    if (protected_header && integer_at(bytes, 21, 4) != reference_crc(bytes.substr(0, 21))) return {};
    if (integer_at(bytes, bytes.size() - 4, 4) != reference_crc(bytes.substr(0, bytes.size() - 4))) return {};
    // Sequence zero/gaps depend on the replay state. These decoders only check
    // record framing, operation, lengths and checksums, just like the format.
    return {true, operation, header_size, static_cast<size_t>(key_size), static_cast<size_t>(value_size)};
}

bool file_header_valid(std::string_view bytes) {
    if (bytes.size() != 24 || bytes.substr(0, 8) != "MKVWAL02") return false;
    for (size_t offset = 8; offset < 20; ++offset) {
        if (byte_at(bytes, offset) != 0) return false;
    }
    return integer_at(bytes, 20, 4) == reference_crc(bytes.substr(0, 20));
}

bool record_prefix_valid(std::string_view bytes) {
    constexpr std::string_view magic = "MKL2";
    for (size_t offset = 0; offset < magic.size() && offset < bytes.size(); ++offset) {
        if (bytes[offset] != magic[offset]) return false;
    }
    if (bytes.size() <= 4) return true;
    const auto operation = byte_at(bytes, 4);
    if (operation != 1 && operation != 3) return false;
    if (!prefix_in_range(bytes, 13, 4, 1, kKeyLimit) ||
        !prefix_in_range(bytes, 17, 4, 0, operation == 1 ? kValueLimit : 0)) return false;
    return bytes.size() <= 21 || checksum_matches(bytes, 21);
}

template <class Decode>
void check_decode(std::string_view bytes, const Layout& reference, Decode decode) {
    bool accepted = false;
    try {
        const auto request = decode(bytes);
        accepted = true;
        require(reference.valid);
        require(static_cast<uint8_t>(request.operation) == reference.operation);
        require(std::string_view(request.key) == bytes.substr(reference.payload_offset, reference.key_size));
        require(std::string_view(request.value) ==
                bytes.substr(reference.payload_offset + reference.key_size, reference.value_size));
    } catch (const std::runtime_error&) {
        // Invalid external frames are expected. Allocation failures, bounds
        // exceptions and logic errors deliberately escape to the fuzzer.
    }
    require(accepted == reference.valid);
}

template <class Validate>
void check_validation(std::string_view bytes, bool expected, Validate validate) {
    bool accepted = false;
    try {
        validate(bytes);
        accepted = true;
    } catch (const std::runtime_error&) {
        // Same narrow rejection contract as check_decode.
    }
    require(accepted == expected);
}

void check_prefix_helpers(std::string_view bytes) {
    constexpr auto maximum = std::numeric_limits<uint64_t>::max();
    struct Range { size_t offset, width; uint64_t minimum, maximum; };
    const Range ranges[] = {
        {5, 8, 1, maximum}, {5, 8, 1, 1}, {5, 8, maximum, maximum},
        {13, 4, 1, kKeyLimit}, {17, 4, 0, kValueLimit}, {17, 4, 0, 0},
    };
    for (const auto& range : ranges) {
        require(minikv::codec::unsigned_prefix_in_range(bytes, range.offset, range.width,
                    range.minimum, range.maximum) ==
                prefix_in_range(bytes, range.offset, range.width, range.minimum, range.maximum));
    }
    // Derive an exact and an adjacent sequence interval from available input
    // to exercise boundaries other than the fixed minimum/maximum cases.
    if (bytes.size() >= 13) {
        const auto sequence = integer_at(bytes, 5, 8);
        const auto next = sequence == maximum ? maximum : sequence + 1;
        require(minikv::codec::unsigned_prefix_in_range(bytes, 5, 8, next, next) ==
                prefix_in_range(bytes, 5, 8, next, next));
    }
    if (bytes.size() >= 21) {
        require(minikv::codec::checksum_prefix_matches(bytes, 21) == checksum_matches(bytes, 21));
    }
}

} // namespace

extern "C" int LLVMFuzzerTestOneInput(const uint8_t* data, size_t size) {
    if (size == 0 || size > kInputLimit) return 0;
    const auto selector = data[0] % 5;
    const std::string_view bytes(reinterpret_cast<const char*>(data + 1), size - 1);
    switch (selector) {
    case 0:
        check_decode(bytes, request_layout(bytes), minikv::codec::decode_request);
        break;
    case 1:
        check_decode(bytes, record_layout(bytes, false), minikv::codec::decode_record);
        break;
    case 2:
        check_decode(bytes, record_layout(bytes, true), minikv::codec::decode_wal_record);
        break;
    case 3:
        check_validation(bytes, file_header_valid(bytes), minikv::codec::validate_wal_file_header);
        // Check the emitted canonical header against independent format/CRC
        // rules as well; never use the codec's validator as its own oracle.
        require(file_header_valid(minikv::codec::wal_file_header()));
        break;
    case 4: {
        // This helper checks only the fixed header; selector 2 exercises whole
        // records. Payload bytes must not change the prefix validator's input.
        const auto header = bytes.substr(0, 25);
        check_validation(header, record_prefix_valid(header), minikv::codec::validate_wal_record_prefix);
        check_prefix_helpers(header);
        break;
    }
    }
    return 0;
}
