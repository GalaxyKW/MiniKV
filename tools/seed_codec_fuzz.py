#!/usr/bin/env python3
"""Write deterministic, independently encoded seeds for codec_fuzz.

Existing unrelated corpus files are retained. Only the named seeds produced by
this script are installed or updated; identical seeds are left untouched.
"""

import argparse
from pathlib import Path
import struct
import zlib


MAX_KEY = 4096
MAX_VALUE = 1024 * 1024
MAX_INPUT = 1052706


def crc(data):
    return data + struct.pack("!I", zlib.crc32(data))


def request_header(operation, key_size, value_size):
    return struct.pack("!4sB3sII", b"MKV1", operation, b"\x00\x00\x00", key_size, value_size)


def request(operation, key=b"", value=b""):
    return request_header(operation, len(key), len(value)) + key + value


def record_header(version, sequence, operation, key_size, value_size):
    header = struct.pack("!4sBQII", b"MKL1" if version == 1 else b"MKL2",
                         operation, sequence, key_size, value_size)
    return header if version == 1 else crc(header)


def record(version, sequence, operation, key, value=b""):
    return crc(record_header(version, sequence, operation, len(key), len(value)) + key + value)


def file_header(flags=0, reserved=0):
    return crc(struct.pack("!8sIQ", b"MKVWAL02", flags, reserved))


def changed(data, offset):
    return data[:offset] + bytes([data[offset] ^ 1]) + data[offset + 1:]


def seed(name, selector, frame):
    return name + ".bin", bytes([selector]) + frame


def short_cuts(name, selector, frame):
    # Enumerating a 1 MiB boundary frame would create hundreds of GiB of seeds.
    # Keep every byte boundary only for small representative frames.
    if len(frame) > 128:
        raise ValueError("only small frames may be enumerated for truncation")
    for cut in range(len(frame)):
        yield seed("{}-cut-{:02d}".format(name, cut), selector, frame[:cut])


def corpus():
    put = request(1, b"key", b"value")
    stats = request(4)
    requests = (
        ("request-put", put),
        ("request-get", request(2, b"key")),
        ("request-delete", request(3, b"key")),
        ("request-stats", stats),
        ("request-empty-value", request(1, b"key")),
        ("request-binary", request(1, b"k\x00\xff", b"\x00\r\n\xff")),
    )
    for name, frame in requests:
        yield seed(name, 0, frame)
    for name, frame in (("request-put", put), ("request-stats", stats)):
        yield from short_cuts(name, 0, frame)
    yield seed("request-bad-magic", 0, changed(put, 0))
    for offset in (5, 6, 7):
        yield seed("request-reserved-{}".format(offset), 0, changed(put, offset))
    for name, frame in (
        ("request-unknown-operation", request(255, b"key")),
        ("request-empty-key", request(1)),
        ("request-get-with-value", request(2, b"key", b"value")),
        ("request-delete-with-value", request(3, b"key", b"value")),
        ("request-stats-with-key", request(4, b"key")),
        ("request-stats-with-value", request(4, value=b"value")),
        ("request-key-limit-plus-one", request_header(1, MAX_KEY + 1, 0)),
        ("request-value-limit-plus-one", request_header(1, 1, MAX_VALUE + 1)),
        ("request-length-uint32-maximum", request_header(1, (1 << 32) - 1, (1 << 32) - 1)),
        ("request-trailing-byte", put + b"\x00"),
    ):
        yield seed(name, 0, frame)

    for version in (1, 2):
        name = "record-v{}".format(version)
        put_record = record(version, 1, 1, b"key", b"value")
        delete_record = record(version, 2, 3, b"key")
        for suffix, frame in (
            ("put", put_record),
            ("delete", delete_record),
            ("empty-value", record(version, 1, 1, b"key")),
            ("binary", record(version, 1, 1, b"k\x00\xff", b"\x00\r\n\xff")),
            # The codec accepts sequence zero (e.g. imported snapshots); WAL
            # replay, outside this harness, enforces its own sequence policy.
            ("sequence-zero", record(version, 0, 1, b"key", b"value")),
            ("sequence-maximum", record(version, (1 << 64) - 1, 1, b"key", b"value")),
            ("bad-magic", changed(put_record, 0)),
            ("bad-record-crc", changed(put_record, len(put_record) - 1)),
            ("unknown-operation", record(version, 1, 2, b"key", b"value")),
            ("empty-key", record(version, 1, 1, b"", b"value")),
            ("delete-with-value", record(version, 1, 3, b"key", b"value")),
            ("key-limit-plus-one", record_header(version, 1, 1, MAX_KEY + 1, 0)),
            ("value-limit-plus-one", record_header(version, 1, 1, 1, MAX_VALUE + 1)),
            ("length-uint32-maximum", record_header(version, 1, 1, (1 << 32) - 1, (1 << 32) - 1)),
            ("trailing-byte", put_record + b"\x00"),
        ):
            yield seed(name + "-" + suffix, version, frame)
        yield from short_cuts(name + "-put", version, put_record)
        yield from short_cuts(name + "-delete", version, delete_record)
        if version == 2:
            yield seed(name + "-bad-header-crc", 2, changed(put_record, 21))
            for available in range(1, 4):
                partial = put_record[:len(put_record) - 4 + available]
                yield seed(name + "-bad-partial-record-crc-{}".format(available), 2,
                           changed(partial, len(partial) - 1))

    header = file_header()
    yield seed("wal-v2-header", 3, header)
    yield from short_cuts("wal-v2-header", 3, header)
    for name, frame in (
        ("wal-v2-header-bad-magic", changed(header, 0)),
        ("wal-v2-header-bad-crc", changed(header, 23)),
        ("wal-v2-header-flags", file_header(flags=1)),
        ("wal-v2-header-reserved", file_header(reserved=1)),
        ("wal-v2-header-trailing-byte", header + b"\x00"),
    ):
        yield seed(name, 3, frame)

    put_header = record_header(2, 1, 1, 3, 5)
    yield seed("partial-v2-magic", 4, b"MK")
    yield seed("partial-v2-crc", 4, put_header[:22])
    for label, frame in (
        ("put", put_header),
        ("delete", record_header(2, 1, 3, 3, 0)),
        ("maximum", record_header(2, (1 << 64) - 1, 1, MAX_KEY, MAX_VALUE)),
    ):
        for cut in range(len(frame) + 1):
            yield seed("partial-v2-{}-{:02d}".format(label, cut), 4, frame[:cut])
    yield seed("partial-v2-sequence-zero", 4, record_header(2, 0, 1, 1, 1))
    for cut in range(1, 5):
        yield seed("partial-v2-bad-magic-{}".format(cut), 4, changed(put_header[:cut], cut - 1))
    for operation in (0, 2, 4, 255):
        yield seed("partial-v2-bad-operation-{}".format(operation), 4,
                   record_header(2, 1, operation, 1, 0)[:5])
    for label, frame, cut in (
        ("key-high-byte", record_header(2, 1, 1, 1 << 24, 0), 14),
        ("key-zero", record_header(2, 1, 1, 0, 0), 17),
        ("key-limit-plus-one", record_header(2, 1, 1, MAX_KEY + 1, 0), 17),
        ("value-high-byte", record_header(2, 1, 1, 1, 1 << 24), 18),
        ("value-limit-plus-one", record_header(2, 1, 1, 1, MAX_VALUE + 1), 21),
        ("delete-value-high-byte", record_header(2, 1, 3, 1, 1 << 24), 18),
        ("delete-with-value", record_header(2, 1, 3, 1, 1), 21),
    ):
        yield seed("partial-v2-bad-" + label, 4, frame[:cut])
    for cut in range(22, 26):
        yield seed("partial-v2-bad-crc-{}".format(cut), 4, changed(put_header[:cut], cut - 1))

    # Three complete large frames cover the exact key/value limits in each
    # decoder. All oversized seeds above need only their independently encoded
    # length fields; no large truncation family is emitted.
    key = b"k" * (MAX_KEY - 1) + b"\x00"
    value = b"v" * (MAX_VALUE - 1) + b"\xff"
    yield seed("request-max-fields", 0, request(1, key, value))
    for version in (1, 2):
        yield seed("record-v{}-max-fields".format(version), version, record(version, 1, 1, key, value))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("output", type=Path, help="corpus directory; unrelated existing files are retained")
    args = parser.parse_args()
    args.output.mkdir(parents=True, exist_ok=True)
    names = set()
    total_bytes = 0
    for name, data in corpus():
        if name in names:
            raise ValueError("duplicate corpus seed: " + name)
        if not data or len(data) > MAX_INPUT:
            raise ValueError("corpus seed exceeds the harness input bound: " + name)
        names.add(name)
        target = args.output / name
        if not target.exists() or target.read_bytes() != data:
            target.write_bytes(data)
        total_bytes += len(data)
    print("Generated {} seeds ({} bytes) in {}".format(len(names), total_bytes, args.output))


if __name__ == "__main__":
    main()
