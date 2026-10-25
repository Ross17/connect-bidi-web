// Copyright 2026 The RossB project
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package bidiprotocol

import (
	"bytes"
	"runtime"
	"testing"

	"connectrpc.com/connect/v2"
	"github.com/sudorandom/connect-bidi-web/internal/connectprotocol"
)

func gzipCompressor(t *testing.T) connect.Compressor {
	t.Helper()
	compressors, names := connectprotocol.DefaultCompressors()
	return compressors[names[0]]
}

func gzipped(t *testing.T, c connect.Compressor, plain []byte) []byte {
	t.Helper()
	out, err := compress(c, plain)
	if err != nil {
		t.Fatalf("compress: %v", err)
	}
	return out
}

func TestDecompressLimitBoundary(t *testing.T) {
	c := gzipCompressor(t)
	const limit = 4096
	exact := gzipped(t, c, bytes.Repeat([]byte("x"), limit))
	got, err := decompress(c, exact, limit)
	if err != nil || len(got) != limit {
		t.Fatalf("exactly %d bytes: len %d, err %v", limit, len(got), err)
	}
	over := gzipped(t, c, bytes.Repeat([]byte("x"), limit+1))
	if _, err := decompress(c, over, limit); connect.CodeOf(err) != connect.CodeResourceExhausted {
		t.Fatalf("%d bytes: err %v, want resource_exhausted", limit+1, err)
	}
}

func TestDecompressNoLimitKeepsUpstreamBehaviour(t *testing.T) {
	c := gzipCompressor(t)
	plain := bytes.Repeat([]byte("y"), 1<<20)
	for _, limit := range []int{0, -1} {
		got, err := decompress(c, gzipped(t, c, plain), limit)
		if err != nil || !bytes.Equal(got, plain) {
			t.Fatalf("limit %d: len %d, err %v", limit, len(got), err)
		}
	}
}

// A small compressed message that inflates to 64 MiB must be refused
// without allocating anything close to 64 MiB.
func TestDecompressBombIsBounded(t *testing.T) {
	c := gzipCompressor(t)
	bomb := gzipped(t, c, make([]byte, 64<<20))
	if len(bomb) > 1<<20 {
		t.Fatalf("bomb is %d bytes, expected a small compressed payload", len(bomb))
	}
	const limit = 64 << 10
	var before, after runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&before)
	_, err := decompress(c, bomb, limit)
	runtime.ReadMemStats(&after)
	if connect.CodeOf(err) != connect.CodeResourceExhausted {
		t.Fatalf("err %v, want resource_exhausted", err)
	}
	if allocated := after.TotalAlloc - before.TotalAlloc; allocated > 4<<20 {
		t.Fatalf("allocated %d bytes while refusing the bomb, want under 4 MiB", allocated)
	}
}

func TestDecompressRejectsCorruptInput(t *testing.T) {
	c := gzipCompressor(t)
	if _, err := decompress(c, []byte("not gzip"), 1024); connect.CodeOf(err) != connect.CodeInternal {
		t.Fatalf("err %v, want internal", err)
	}
}
