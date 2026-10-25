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
	"encoding/binary"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"connectrpc.com/connect/v2"
	"github.com/coder/websocket"
	"github.com/sudorandom/connect-bidi-web/connectwebsocket"
	pingv1connect "github.com/sudorandom/connect-bidi-web/internal/gen/connectbidi/ping/v1/pingv1connect"
)

// newServerWithOptions starts a handler with extra options and returns its
// ws:// URL.
func newServerWithOptions(t *testing.T, impl pingv1connect.PingServiceHandler, opts ...connectwebsocket.Option) string {
	t.Helper()
	connectServer := connect.NewServer()
	pingv1connect.RegisterPingServiceHandler(connectServer, impl)
	all := append([]connectwebsocket.Option{connectwebsocket.WithAcceptOptions(&websocket.AcceptOptions{
		InsecureSkipVerify: true,
	})}, opts...)
	wsHandler := connectwebsocket.NewHandler(connectServer, all...)
	server := httptest.NewServer(wsHandler)
	t.Cleanup(server.Close)
	return "ws" + strings.TrimPrefix(server.URL, "http")
}

// rawConn is a hand-driven WebSocket client for hostile-input tests.
type rawConn struct {
	t    *testing.T
	ctx  context.Context
	conn *websocket.Conn
}

func dialRaw(t *testing.T, wsURL string) *rawConn {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	t.Cleanup(cancel)
	conn, _, err := websocket.Dial(ctx, wsURL, nil) //nolint:bodyclose // coder/websocket closes the handshake response body itself
	if err != nil {
		t.Fatalf("dial failed: %v", err)
	}
	conn.SetReadLimit(-1)
	t.Cleanup(func() { _ = conn.CloseNow() })
	return &rawConn{t: t, ctx: ctx, conn: conn}
}

func buildRawFrame(id uint32, flag byte, payload []byte) []byte {
	frame := make([]byte, 9+len(payload))
	binary.BigEndian.PutUint32(frame[0:4], id)
	frame[4] = flag
	binary.BigEndian.PutUint32(frame[5:9], uint32(len(payload))) //nolint:gosec // test payloads are small
	copy(frame[9:], payload)
	return frame
}

func headersFor(procedure string) []byte {
	return []byte(`{"metadata":{":path":["` + procedure + `"],"content-type":["application/connect+proto"]}}`)
}

// write sends one frame; a write error is returned, not fatal, because the
// server may already have closed the connection.
func (r *rawConn) write(id uint32, flag byte, payload []byte) error {
	return r.conn.Write(r.ctx, websocket.MessageBinary, buildRawFrame(id, flag, payload))
}

func (r *rawConn) mustWrite(id uint32, flag byte, payload []byte) {
	r.t.Helper()
	if err := r.write(id, flag, payload); err != nil {
		r.t.Fatalf("write frame failed: %v", err)
	}
}

// readFrame reads one frame or returns the read error.
func (r *rawConn) readFrame() (uint32, byte, []byte, error) {
	_, data, err := r.conn.Read(r.ctx)
	if err != nil {
		return 0, 0, nil, err
	}
	if len(data) < 9 {
		r.t.Fatalf("frame too short: %d bytes", len(data))
	}
	return binary.BigEndian.Uint32(data[0:4]), data[4], data[9:], nil
}

// expectClose reads until the connection fails and checks the close status.
func (r *rawConn) expectClose(want websocket.StatusCode) {
	r.t.Helper()
	for {
		_, _, _, err := r.readFrame()
		if err == nil {
			continue
		}
		if got := websocket.CloseStatus(err); got != want {
			r.t.Fatalf("close status = %v (err %v), want %v", got, err, want)
		}
		return
	}
}

// expectEndStream reads frames until an end-stream for id arrives.
func (r *rawConn) expectEndStream(id uint32) {
	r.t.Helper()
	for {
		gotID, flag, _, err := r.readFrame()
		if err != nil {
			r.t.Fatalf("read failed while waiting for end-stream on %d: %v", id, err)
		}
		if gotID == id && flag == 0x02 {
			return
		}
	}
}
