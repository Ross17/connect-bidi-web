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

package connectwebsocket_test

import (
	"context"
	"strings"
	"testing"

	"connectrpc.com/connect/v2"
	"github.com/coder/websocket"
	"github.com/sudorandom/connect-bidi-web/connectwebsocket"
	pingv1 "github.com/sudorandom/connect-bidi-web/internal/gen/connectbidi/ping/v1"
	pingv1connect "github.com/sudorandom/connect-bidi-web/internal/gen/connectbidi/ping/v1/pingv1connect"
)

// A PingRequest with only Text set encodes as 1 tag byte, a 2-byte length
// (for 128..16383 bytes) and the text, so the data frame is 9+3+len(text).
const frameOverhead = 9 + 3

func pingText(t *testing.T, wsURL string, n int) error {
	t.Helper()
	client := pingv1connect.NewPingServiceClient(connect.NewClient(connectwebsocket.NewTransport(wsURL)))
	resp, err := client.Ping(context.Background(), &pingv1.PingRequest{Text: strings.Repeat("a", n)})
	if err == nil && len(resp.GetText()) != n {
		t.Fatalf("echo length = %d, want %d", len(resp.GetText()), n)
	}
	return err
}

func TestMaxFrameBytesBoundary(t *testing.T) {
	const limit = 1024
	wsURL := newServerWithOptions(t, testPingServer{}, connectwebsocket.WithMaxFrameBytes(limit))
	if err := pingText(t, wsURL, limit-frameOverhead); err != nil {
		t.Fatalf("frame of exactly %d bytes refused: %v", limit, err)
	}
	if err := pingText(t, wsURL, limit-frameOverhead+1); err == nil {
		t.Fatalf("frame of %d bytes accepted, limit %d", limit+1, limit)
	}
}

func TestMaxFrameBytesClosesWith1009(t *testing.T) {
	wsURL := newServerWithOptions(t, testPingServer{}, connectwebsocket.WithMaxFrameBytes(256))
	raw := dialRaw(t, wsURL)
	_ = raw.write(1, 0x06, make([]byte, 4096))
	raw.expectClose(websocket.StatusMessageTooBig)
}

// Without the option the handler keeps upstream behaviour: no limit, even
// past the WebSocket library's own 32 KiB default.
func TestNoMaxFrameBytesKeepsUpstreamBehaviour(t *testing.T) {
	wsURL := newServerWithOptions(t, testPingServer{})
	if err := pingText(t, wsURL, 100_000); err != nil {
		t.Fatalf("100 KB ping refused without a limit: %v", err)
	}
}

// Zero and negative values mean "no limit".
func TestMaxFrameBytesNonPositiveMeansOff(t *testing.T) {
	for _, n := range []int64{0, -1} {
		wsURL := newServerWithOptions(t, testPingServer{}, connectwebsocket.WithMaxFrameBytes(n))
		if err := pingText(t, wsURL, 100_000); err != nil {
			t.Fatalf("WithMaxFrameBytes(%d): 100 KB ping refused: %v", n, err)
		}
	}
}
