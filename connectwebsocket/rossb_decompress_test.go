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
	"github.com/sudorandom/connect-bidi-web/connectwebsocket"
	pingv1 "github.com/sudorandom/connect-bidi-web/internal/gen/connectbidi/ping/v1"
	pingv1connect "github.com/sudorandom/connect-bidi-web/internal/gen/connectbidi/ping/v1/pingv1connect"
)

// The server's ReadMaxBytes must bound the decompressed request, not only
// the compressed bytes on the wire.
func TestServerReadMaxBytesBoundsDecompressedRequest(t *testing.T) {
	wsURL := newServerWithOptions(t, testPingServer{}, connectwebsocket.WithReadMaxBytes(1024))
	transport := connectwebsocket.NewTransport(wsURL, connectwebsocket.WithSendCompressor("gzip"))
	client := pingv1connect.NewPingServiceClient(connect.NewClient(transport))
	_, err := client.Ping(context.Background(), &pingv1.PingRequest{Text: strings.Repeat("a", 100_000)})
	if connect.CodeOf(err) != connect.CodeResourceExhausted {
		t.Fatalf("err %v, want resource_exhausted", err)
	}
	// A small compressed request still works.
	if _, err := client.Ping(context.Background(), &pingv1.PingRequest{Text: "ok"}); err != nil {
		t.Fatalf("small ping failed: %v", err)
	}
}

// The client's ReadMaxBytes must bound the decompressed response too.
func TestClientReadMaxBytesBoundsDecompressedResponse(t *testing.T) {
	wsURL := newServerWithOptions(t, testPingServer{})
	transport := connectwebsocket.NewTransport(wsURL, connectwebsocket.WithReadMaxBytes(1024))
	client := pingv1connect.NewPingServiceClient(connect.NewClient(transport))
	_, err := client.Ping(context.Background(), &pingv1.PingRequest{Text: strings.Repeat("b", 100_000)})
	if connect.CodeOf(err) != connect.CodeResourceExhausted {
		t.Fatalf("err %v, want resource_exhausted", err)
	}
}
