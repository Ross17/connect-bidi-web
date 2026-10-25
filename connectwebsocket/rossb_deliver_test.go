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
	"errors"
	"testing"
	"time"

	"connectrpc.com/connect/v2"
	"github.com/coder/websocket"
	"github.com/sudorandom/connect-bidi-web/connectwebsocket"
	pingv1 "github.com/sudorandom/connect-bidi-web/internal/gen/connectbidi/ping/v1"
	pingv1connect "github.com/sudorandom/connect-bidi-web/internal/gen/connectbidi/ping/v1/pingv1connect"
)

// stuckServer answers the first CumSum message and then stops reading
// until its context ends or the test releases it.
type stuckServer struct {
	testPingServer
	release chan struct{}
	ended   chan error
}

func newStuckServer(t *testing.T) *stuckServer {
	t.Helper()
	s := &stuckServer{release: make(chan struct{}), ended: make(chan error, 1)}
	return s
}

func (s *stuckServer) CumSum(ctx context.Context, stream pingv1connect.PingServiceCumSumServerStream) error {
	req, err := stream.Receive()
	if err != nil {
		return err
	}
	if err := stream.Send(&pingv1.CumSumResponse{Sum: req.GetNumber()}); err != nil {
		return err
	}
	select {
	case <-ctx.Done():
		s.ended <- context.Cause(ctx)
		return ctx.Err()
	case <-s.release:
		s.ended <- nil
		return nil
	}
}

// fillStuckStream opens stream 1, waits for its first answer, then sends
// more frames than its inbox holds (64), so the next delivery blocks.
func fillStuckStream(raw *rawConn) {
	raw.mustWrite(1, 0x06, cumSumHeaders)
	raw.mustWrite(1, 0x00, cumSumRequest(1))
	raw.expectData(1)
	for range 70 {
		raw.mustWrite(1, 0x00, cumSumRequest(2))
	}
}

func TestDeliverTimeoutResetsSlowStream(t *testing.T) {
	impl := newStuckServer(t)
	wsURL := newServerWithOptions(t, impl, connectwebsocket.WithDeliverTimeout(100*time.Millisecond))
	t.Cleanup(func() { close(impl.release) })
	raw := dialRaw(t, wsURL)
	start := time.Now()
	fillStuckStream(raw)
	// The server resets the stuck stream ...
	for {
		id, flag, _, err := raw.readFrame()
		if err != nil {
			t.Fatalf("read failed waiting for reset: %v", err)
		}
		if id == 1 && flag == 0x07 {
			break
		}
	}
	if elapsed := time.Since(start); elapsed < 100*time.Millisecond {
		t.Fatalf("reset after %v, before the 100ms timeout", elapsed)
	}
	// ... cancels its handler ...
	select {
	case err := <-impl.ended:
		if err == nil {
			t.Fatal("handler was released, not canceled")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("handler of the reset stream was not canceled")
	}
	// ... and keeps serving the other streams on the connection.
	raw.pingOn(3)
}

// Normal traffic never waits for the timer.
func TestDeliverTimeoutNormalTraffic(t *testing.T) {
	wsURL := newServerWithOptions(t, testPingServer{}, connectwebsocket.WithDeliverTimeout(50*time.Millisecond))
	client := pingv1connect.NewPingServiceClient(connect.NewClient(connectwebsocket.NewTransport(wsURL)))
	stream, err := client.CumSum(context.Background())
	if err != nil {
		t.Fatalf("CumSum: %v", err)
	}
	defer stream.Close()
	var sum int64
	for i := int64(1); i <= 300; i++ {
		if err := stream.Send(&pingv1.CumSumRequest{Number: i}); err != nil {
			t.Fatalf("Send %d: %v", i, err)
		}
		res, err := stream.Receive()
		if err != nil {
			t.Fatalf("Receive %d: %v", i, err)
		}
		sum += i
		if res.GetSum() != sum {
			t.Fatalf("sum %d, want %d", res.GetSum(), sum)
		}
	}
}

// Without the option, upstream behaviour is kept: one stuck stream blocks
// the read loop, so a ping on another stream gets no answer.
func TestNoDeliverTimeoutKeepsUpstreamBehaviour(t *testing.T) {
	for _, opts := range [][]connectwebsocket.Option{nil, {connectwebsocket.WithDeliverTimeout(0)}} {
		impl := newStuckServer(t)
		wsURL := newServerWithOptions(t, impl, opts...)
		raw := dialRaw(t, wsURL)
		fillStuckStream(raw)
		raw.mustWrite(3, 0x06, pingHeaders)
		raw.mustWrite(3, 0x00, nil)
		raw.mustWrite(3, 0x02, nil)
		ctx, cancel := context.WithTimeout(context.Background(), 400*time.Millisecond)
		for {
			_, data, err := raw.conn.Read(ctx)
			if err != nil {
				if !errors.Is(err, context.DeadlineExceeded) && websocket.CloseStatus(err) != -1 {
					t.Fatalf("unexpected read error: %v", err)
				}
				break
			}
			if len(data) >= 9 && data[3] == 3 && data[4] == 0x02 {
				t.Fatal("ping on another stream finished; expected head-of-line blocking without the option")
			}
		}
		cancel()
		close(impl.release)
	}
}

// deafServer is like stuckServer but ignores cancellation: only the test
// can release it.
type deafServer struct {
	testPingServer
	release chan struct{}
}

func (s *deafServer) CumSum(_ context.Context, stream pingv1connect.PingServiceCumSumServerStream) error {
	req, err := stream.Receive()
	if err != nil {
		return err
	}
	if err := stream.Send(&pingv1.CumSumResponse{Sum: req.GetNumber()}); err != nil {
		return err
	}
	<-s.release
	return nil
}

// A reset for a slow consumer frees its WithMaxStreams slot at once, as a
// client reset does, even if the handler has not returned yet.
func TestDeliverTimeoutFreesStreamSlot(t *testing.T) {
	impl := &deafServer{release: make(chan struct{})}
	wsURL := newServerWithOptions(t, impl,
		connectwebsocket.WithDeliverTimeout(100*time.Millisecond),
		connectwebsocket.WithMaxStreams(1))
	t.Cleanup(func() { close(impl.release) })
	raw := dialRaw(t, wsURL)
	fillStuckStream(raw)
	for {
		id, flag, _, err := raw.readFrame()
		if err != nil {
			t.Fatalf("read failed waiting for reset: %v", err)
		}
		if id == 1 && flag == 0x07 {
			break
		}
	}
	raw.pingOn(3)
}
