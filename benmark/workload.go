package main

import (
	"math/bits"
	"math/rand/v2"
	"strconv"
	"strings"
)

// Change the version whenever the mapping from (config, index) to a request
// changes. Reproducibility covers request contents, not server arrival order.
const workloadVersion = "indexed-pcg-v1"

type workloadRequest struct {
	operation string
	key       string
	value     string
}

// requestFor uses a small, local PCG stream for each request. No worker identity,
// shared random state or previous request affects its output. cfg must already
// be validated and index must be a nonnegative request sequence number.
func requestFor(cfg benchConfig, index int) workloadRequest {
	var source rand.PCG
	source.Seed(uint64(cfg.seed), uint64(index))
	// Draw the key first so changing operation ratios retains the same key trace.
	key := workloadUint64N(&source, uint64(cfg.keyspace))
	request := workloadRequest{operation: cfg.op}
	if cfg.op == "mixed" {
		draw := workloadUint64N(&source, 100)
		switch {
		case draw < uint64(cfg.writeRatio):
			request.operation = "put"
		case draw < uint64(cfg.writeRatio+cfg.deleteRatio):
			request.operation = "delete"
		default:
			request.operation = "get"
		}
	}
	var keyBuffer [21]byte // "k" and any uint64 in decimal.
	keyBuffer[0] = 'k'
	request.key = string(strconv.AppendUint(keyBuffer[:1], key, 10))
	if request.operation == "put" {
		request.value = workloadValue(cfg.seed, index, cfg.valueSize)
	}
	return request
}

// Multiply-high reduction with rejection removes modulo bias. Keeping the
// fixed-width arithmetic here makes the stream identical on 32/64-bit targets,
// and a concrete PCG pointer keeps the generator's 16-byte state on the stack.
func workloadUint64N(source *rand.PCG, n uint64) uint64 {
	if n == 0 {
		panic("workload keyspace must be positive")
	}
	hi, lo := bits.Mul64(source.Uint64(), n)
	if lo < n {
		threshold := -n % n
		for lo < threshold {
			hi, lo = bits.Mul64(source.Uint64(), n)
		}
	}
	return hi
}

func workloadValue(seed int64, index, size int) string {
	if size == 0 {
		return ""
	}
	var prefixBuffer [42]byte // v, signed seed, separator, nonnegative int64 index.
	prefixBuffer[0] = 'v'
	prefix := strconv.AppendInt(prefixBuffer[:1], seed, 10)
	prefix = append(prefix, '-')
	prefix = strconv.AppendInt(prefix, int64(index), 10)
	if len(prefix) >= size {
		return string(prefix[:size])
	}
	var value strings.Builder
	value.Grow(size)
	_, _ = value.Write(prefix)
	const padding = "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"
	for remaining := size - len(prefix); remaining > 0; {
		count := min(remaining, len(padding))
		_, _ = value.WriteString(padding[:count])
		remaining -= count
	}
	return value.String()
}
