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
	"bytes"
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/coder/websocket"
	"github.com/sudorandom/connect-bidi-web/connectwebsocket"
	pingv1connect "github.com/sudorandom/connect-bidi-web/internal/gen/connectbidi/ping/v1/pingv1connect"
)

// recorder is a ControlHandler that remembers what it was given and can echo,
// refuse or block.
type recorder struct {
	mu      sync.Mutex
	frames  []recordedFrame
	echo    bool
	refuse  error
	release chan struct{}
	seen    chan struct{}
}

type recordedFrame struct {
	flag    uint8
	payload []byte
}

func newRecorder() *recorder {
	return &recorder{seen: make(chan struct{}, 64)}
}

func (r *recorder) HandleControl(_ context.Context, stream connectwebsocket.ControlStream, flag uint8, payload []byte) error {
	r.mu.Lock()
	r.frames = append(r.frames, recordedFrame{flag: flag, payload: bytes.Clone(payload)})
	echo, refuse, release := r.echo, r.refuse, r.release
	r.mu.Unlock()
	select {
	case r.seen <- struct{}{}:
	default:
	}
	if release != nil {
		<-release
	}
	if refuse != nil {
		return refuse
	}
	if echo {
		return stream.Send(flag, append([]byte("echo:"), payload...))
	}
	return nil
}

func (r *recorder) got() []recordedFrame {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]recordedFrame(nil), r.frames...)
}

func (r *recorder) waitFor(t *testing.T, n int) {
	t.Helper()
	deadline := time.After(5 * time.Second)
	for len(r.got()) < n {
		select {
		case <-r.seen:
		case <-deadline:
			t.Fatalf("saw %d control frames, want %d", len(r.got()), n)
		}
	}
}

func TestControlHandlerReceivesStreamZero(t *testing.T) {
	rec := newRecorder()
	rec.echo = true
	wsURL := newServerWithOptions(t, testPingServer{}, connectwebsocket.WithControlHandler(rec))
	raw := dialRaw(t, wsURL)

	// A flag other than 0x00, so an echo that dropped the flag would show.
	const controlFlag = uint8(0x0a)
	raw.mustWrite(connectwebsocket.ControlStreamID, controlFlag, []byte("hello"))
	gotID, gotFlag, payload, err := raw.readFrame()
	if err != nil {
		t.Fatalf("read echo: %v", err)
	}
	if gotID != connectwebsocket.ControlStreamID || gotFlag != controlFlag || string(payload) != "echo:hello" {
		t.Fatalf("echo = id %d flag 0x%02x %q", gotID, gotFlag, payload)
	}
	// Ordinary streams keep working alongside it.
	raw.pingOn(1)

	rec.waitFor(t, 1)
	if frames := rec.got(); len(frames) != 1 || string(frames[0].payload) != "hello" {
		t.Fatalf("handler saw %+v", frames)
	}
}

// Every flag on stream 0 reaches the handler, including flags the RPC path
// would refuse, and an empty payload.
func TestControlHandlerReceivesEveryFlag(t *testing.T) {
	rec := newRecorder()
	wsURL := newServerWithOptions(t, testPingServer{}, connectwebsocket.WithControlHandler(rec))
	raw := dialRaw(t, wsURL)
	flags := []uint8{0x00, 0x01, 0x02, 0x06, 0x07, 0x0a, 0xff}
	for _, f := range flags {
		raw.mustWrite(connectwebsocket.ControlStreamID, f, nil)
	}
	rec.waitFor(t, len(flags))
	got := rec.got()
	for i, f := range flags {
		if got[i].flag != f {
			t.Fatalf("frame %d flag 0x%02x, want 0x%02x", i, got[i].flag, f)
		}
		if len(got[i].payload) != 0 {
			t.Fatalf("frame %d payload %q, want empty", i, got[i].payload)
		}
	}
	// The connection is still usable.
	raw.pingOn(1)
}

// A headers frame on stream 0 must not start an RPC when a control handler is
// set: the handler gets it instead.
func TestControlHandlerPreventsRPCOnStreamZero(t *testing.T) {
	rec := newRecorder()
	wsURL := newServerWithOptions(t, testPingServer{}, connectwebsocket.WithControlHandler(rec))
	raw := dialRaw(t, wsURL)
	raw.mustWrite(connectwebsocket.ControlStreamID, 0x06, pingHeaders)
	raw.mustWrite(connectwebsocket.ControlStreamID, 0x00, nil)
	raw.mustWrite(connectwebsocket.ControlStreamID, 0x02, nil)
	rec.waitFor(t, 3)
	// An RPC on stream 0 would answer on stream 0. Run one on stream 1 and
	// check that nothing at all comes back for stream 0 before it finishes.
	raw.mustWrite(1, 0x06, pingHeaders)
	raw.mustWrite(1, 0x00, nil)
	raw.mustWrite(1, 0x02, nil)
	for {
		id, flag, _, err := raw.readFrame()
		if err != nil {
			t.Fatalf("read: %v", err)
		}
		if id == connectwebsocket.ControlStreamID {
			t.Fatalf("stream 0 answered with flag 0x%02x, so an RPC ran there", flag)
		}
		if id == 1 && flag == 0x02 {
			return
		}
	}
}

// A refusal from the handler closes the connection with 1008 and stops the
// read loop.
func TestControlHandlerRefusalCloses(t *testing.T) {
	rec := newRecorder()
	rec.refuse = errors.New("no")
	wsURL := newServerWithOptions(t, testPingServer{}, connectwebsocket.WithControlHandler(rec))
	raw := dialRaw(t, wsURL)
	raw.mustWrite(connectwebsocket.ControlStreamID, 0x00, []byte("bad"))
	raw.expectClose(websocket.StatusPolicyViolation)
}

// The control stream is outside WithMaxStreams and WithIncreasingStreamIDs.
func TestControlStreamIsOutsideStreamLimits(t *testing.T) {
	rec := newRecorder()
	wsURL := newServerWithOptions(t, testPingServer{},
		connectwebsocket.WithControlHandler(rec),
		connectwebsocket.WithMaxStreams(1),
		connectwebsocket.WithIncreasingStreamIDs())
	raw := dialRaw(t, wsURL)
	for range 20 {
		raw.mustWrite(connectwebsocket.ControlStreamID, 0x00, []byte("x"))
	}
	rec.waitFor(t, 20)
	// Stream 0 did not use the single slot, and did not raise the highest ID.
	raw.pingOn(1)
	raw.mustWrite(connectwebsocket.ControlStreamID, 0x00, []byte("after"))
	rec.waitFor(t, 21)
}

// Without the option, stream 0 behaves as upstream: a headers frame opens an
// RPC there.
func TestNoControlHandlerKeepsUpstreamBehaviour(t *testing.T) {
	wsURL := newServerWithOptions(t, testPingServer{})
	raw := dialRaw(t, wsURL)
	raw.mustWrite(connectwebsocket.ControlStreamID, 0x06, headersFor(pingv1connect.PingServicePingProcedure))
	raw.mustWrite(connectwebsocket.ControlStreamID, 0x00, nil)
	raw.mustWrite(connectwebsocket.ControlStreamID, 0x02, nil)
	raw.expectEndStream(connectwebsocket.ControlStreamID)
}

// Send after the connection ends returns an error instead of panicking.
func TestControlStreamSendAfterClose(t *testing.T) {
	var (
		mu     sync.Mutex
		stream connectwebsocket.ControlStream
	)
	h := controlFunc(func(_ context.Context, s connectwebsocket.ControlStream, _ uint8, _ []byte) error {
		mu.Lock()
		stream = s
		mu.Unlock()
		return nil
	})
	wsURL := newServerWithOptions(t, testPingServer{}, connectwebsocket.WithControlHandler(h))
	raw := dialRaw(t, wsURL)
	raw.mustWrite(connectwebsocket.ControlStreamID, 0x00, []byte("x"))
	// Wait until the handler has run, then drop the connection.
	deadline := time.After(5 * time.Second)
	for {
		mu.Lock()
		s := stream
		mu.Unlock()
		if s != nil {
			_ = raw.conn.CloseNow()
			// The read loop notices and terminates the connection.
			for range 100 {
				if err := s.Send(0x00, []byte("late")); err != nil {
					return
				}
				time.Sleep(10 * time.Millisecond)
			}
			t.Fatal("Send kept succeeding after the connection closed")
		}
		select {
		case <-deadline:
			t.Fatal("handler never ran")
		case <-time.After(5 * time.Millisecond):
		}
	}
}

// controlFunc adapts a function to ControlHandler.
type controlFunc func(context.Context, connectwebsocket.ControlStream, uint8, []byte) error

func (f controlFunc) HandleControl(ctx context.Context, s connectwebsocket.ControlStream, flag uint8, payload []byte) error {
	return f(ctx, s, flag, payload)
}
