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
	"testing"

	"github.com/coder/websocket"
	"github.com/sudorandom/connect-bidi-web/connectwebsocket"
	pingv1connect "github.com/sudorandom/connect-bidi-web/internal/gen/connectbidi/ping/v1/pingv1connect"
)

var (
	cumSumHeaders = headersFor(pingv1connect.PingServiceCumSumProcedure)
	pingHeaders   = headersFor(pingv1connect.PingServicePingProcedure)
)

// cumSumRequest encodes CumSumRequest{Number: n} for 0 < n < 128.
func cumSumRequest(n byte) []byte { return []byte{0x08, n} }

// expectData reads frames until a data frame for id arrives.
func (r *rawConn) expectData(id uint32) []byte {
	r.t.Helper()
	for {
		gotID, flag, payload, err := r.readFrame()
		if err != nil {
			r.t.Fatalf("read failed while waiting for data on %d: %v", id, err)
		}
		if gotID == id && flag == 0x00 {
			return payload
		}
	}
}

// pingOn runs one unary Ping by hand on stream id and waits for its end.
func (r *rawConn) pingOn(id uint32) {
	r.t.Helper()
	r.mustWrite(id, 0x06, pingHeaders)
	r.mustWrite(id, 0x00, nil)
	r.mustWrite(id, 0x02, nil)
	r.expectEndStream(id)
}

func TestMaxStreamsClosesConnectionOverLimit(t *testing.T) {
	wsURL := newServerWithOptions(t, testPingServer{}, connectwebsocket.WithMaxStreams(3))
	raw := dialRaw(t, wsURL)
	for id := uint32(1); id <= 3; id++ {
		raw.mustWrite(id, 0x06, cumSumHeaders)
	}
	// The three streams are live: each answers.
	for id := uint32(1); id <= 3; id++ {
		raw.mustWrite(id, 0x00, cumSumRequest(byte(id)))
		raw.expectData(id)
	}
	_ = raw.write(4, 0x06, cumSumHeaders)
	raw.expectClose(websocket.StatusPolicyViolation)
}

// A reset frees a slot: the read loop deregisters the stream before it
// reads the next frame, so the next stream fits.
func TestMaxStreamsSlotFreedByReset(t *testing.T) {
	wsURL := newServerWithOptions(t, testPingServer{}, connectwebsocket.WithMaxStreams(2))
	raw := dialRaw(t, wsURL)
	raw.mustWrite(1, 0x06, cumSumHeaders)
	raw.mustWrite(2, 0x06, cumSumHeaders)
	raw.mustWrite(1, 0x07, nil)
	raw.mustWrite(3, 0x06, cumSumHeaders)
	raw.mustWrite(3, 0x00, cumSumRequest(9))
	raw.expectData(3)
	_ = raw.write(4, 0x06, cumSumHeaders)
	raw.expectClose(websocket.StatusPolicyViolation)
}

// Without the option there is no cap, as upstream.
func TestNoMaxStreamsKeepsUpstreamBehaviour(t *testing.T) {
	for _, opts := range [][]connectwebsocket.Option{nil, {connectwebsocket.WithMaxStreams(0)}} {
		wsURL := newServerWithOptions(t, testPingServer{}, opts...)
		raw := dialRaw(t, wsURL)
		for id := uint32(1); id <= 100; id++ {
			raw.mustWrite(id, 0x06, cumSumHeaders)
		}
		raw.pingOn(101)
	}
}
