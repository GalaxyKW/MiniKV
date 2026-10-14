package main

import (
	"bytes"
	"io"
	"testing"
)

// Count attempts to read past the fixed header, including reads that hit EOF.
// This distinguishes rejecting an invalid length from trying to read its body.
type responseFuzzReader struct {
	data         []byte
	offset       int
	chunk        int
	payloadReads int
}

func (r *responseFuzzReader) Read(p []byte) (int, error) {
	if r.offset >= 12 && len(p) != 0 {
		r.payloadReads++
	}
	if r.offset == len(r.data) {
		return 0, io.EOF
	}
	if r.chunk != 0 && len(p) > r.chunk {
		p = p[:r.chunk]
	}
	n := copy(p, r.data[r.offset:])
	r.offset += n
	return n, nil
}

// Encode corpus examples directly from the wire specification. Do not use a
// production encoder or another test's responseFrame as the decoding oracle.
func responseFuzzSeed(status byte, payload []byte) []byte {
	n := uint32(len(payload))
	frame := []byte{'M', 'K', 'R', '1', status, 0, 0, 0,
		byte(n >> 24), byte(n >> 16), byte(n >> 8), byte(n)}
	return append(frame, payload...)
}

func FuzzReadResponse(f *testing.F) {
	f.Add([]byte{})
	for status := byte(0); status <= 5; status++ {
		f.Add(responseFuzzSeed(status, nil))
		// Nonempty OK and NOT_FOUND are invalid; other statuses permit payloads.
		f.Add(responseFuzzSeed(status, []byte{0, ' ', '\n', 0xff, 0x80, 'x'}))
	}
	valid := responseFuzzSeed(1, []byte("value\x00\n"))
	for end := 1; end < len(valid); end++ {
		f.Add(bytes.Clone(valid[:end]))
	}
	for _, offset := range []int{0, 1, 2, 3, 5, 6, 7} {
		invalid := bytes.Clone(valid)
		invalid[offset] ^= 1
		f.Add(invalid)
	}
	f.Add(responseFuzzSeed(6, nil))
	f.Add(responseFuzzSeed(255, nil))
	for _, declared := range []uint32{1, (1 << 20) - 1, 1 << 20, (1 << 20) + 1, ^uint32(0)} {
		frame := responseFuzzSeed(1, []byte("unread"))
		frame[8], frame[9], frame[10], frame[11] = byte(declared>>24), byte(declared>>16), byte(declared>>8), byte(declared)
		f.Add(frame)
	}
	f.Add(responseFuzzSeed(1, bytes.Repeat([]byte{0xa5}, 1<<20)))
	f.Add(append(bytes.Clone(valid), responseFuzzSeed(2, nil)...))
	f.Add(append(responseFuzzSeed(0, nil), valid...))

	f.Fuzz(func(t *testing.T, data []byte) {
		// Two maximum-size frames cover concatenation while bounding copies of
		// the payload and unread tail made by this harness, including io.ReadAll.
		if len(data) > 2*(12+(1<<20)) {
			t.Skip("input exceeds two maximum-size response frames")
		}
		// Compute the expected result independently of readResponse. These are
		// the documented wire limits, not values inferred from decoder output.
		validHeader := false
		frameSize := 12
		if len(data) >= 12 {
			length := uint32(data[8])<<24 | uint32(data[9])<<16 | uint32(data[10])<<8 | uint32(data[11])
			validHeader = data[0] == 'M' && data[1] == 'K' && data[2] == 'R' && data[3] == '1' &&
				data[4] <= 5 && data[5] == 0 && data[6] == 0 && data[7] == 0 && length <= 1<<20 &&
				((data[4] != 0 && data[4] != 2) || length == 0)
			if validHeader {
				frameSize += int(length)
			}
		}
		for _, chunk := range []int{0, 3} {
			reader := &responseFuzzReader{data: data, chunk: chunk}
			response, err := readResponse(reader)
			switch {
			case len(data) < 12:
				if err == nil || reader.offset != len(data) || reader.payloadReads != 0 {
					t.Fatalf("chunk=%d: incomplete header accepted or overread: error=%v consumed=%d payload_reads=%d",
						chunk, err, reader.offset, reader.payloadReads)
				}
			case !validHeader:
				if err == nil || reader.offset != 12 || reader.payloadReads != 0 {
					t.Fatalf("chunk=%d: invalid header reached payload: error=%v consumed=%d payload_reads=%d",
						chunk, err, reader.offset, reader.payloadReads)
				}
			case len(data) < frameSize:
				if err == nil || reader.offset != len(data) {
					t.Fatalf("chunk=%d: incomplete payload accepted or underread: error=%v consumed=%d available=%d",
						chunk, err, reader.offset, len(data))
				}
			default:
				if err != nil || response.status != data[4] || response.value != string(data[12:frameSize]) || reader.offset != frameSize {
					t.Fatalf("chunk=%d: valid frame changed: error=%v status=%d want=%d consumed=%d want=%d payload_bytes=%d",
						chunk, err, response.status, data[4], reader.offset, frameSize, len(response.value))
				}
				remaining, readErr := io.ReadAll(reader)
				if readErr != nil || !bytes.Equal(remaining, data[frameSize:]) {
					t.Fatalf("chunk=%d: bytes after the first response changed: error=%v", chunk, readErr)
				}
			}
		}
	})
}
