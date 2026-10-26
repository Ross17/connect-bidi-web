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
	"net/http"
	"sync"
	"testing"
	"time"

	"github.com/coder/websocket"
	"github.com/sudorandom/connect-bidi-web/connectwebsocket"
	pingv1 "github.com/sudorandom/connect-bidi-web/internal/gen/connectbidi/ping/v1"
	pingv1connect "github.com/sudorandom/connect-bidi-web/internal/gen/connectbidi/ping/v1/pingv1connect"
)

// --- a server whose handler reports every message and every ending ---

type watchedServer struct {
	testPingServer
	got   chan int64
	ended chan error
}

func newWatchedServer() *watchedServer {
	return &watchedServer{got: make(chan int64, 64), ended: make(chan error, 8)}
}

// CumSum keeps a running total, so a reply after a reconnect proves the same
// handler kept its state.
func (s *watchedServer) CumSum(ctx context.Context, stream pingv1connect.PingServiceCumSumServerStream) error {
	var sum int64
	for {
		req, err := stream.Receive()
		if err != nil {
			s.ended <- err
			return err
		}
		sum += req.GetNumber()
		select {
		case s.got <- sum:
		default:
		}
		if err := stream.Send(&pingv1.CumSumResponse{Sum: sum}); err != nil {
			s.ended <- err
			return err
		}
	}
}

// --- an owner that keeps every stream and records what it sees ---

type testOwner struct {
	mu       sync.Mutex
	persist  bool
	headers  []http.Header
	sent     int
	received int
	streams  []*connectwebsocket.PersistentStream
	// raw keeps the unwrapped stream connections, so a test can write
	// through one directly.
	raw           []connectwebsocket.StreamConn
	detached      chan *connectwebsocket.PersistentStream
	detachedConns chan *ownedConn
	// holdWrites makes the wrapper wait instead of failing while detached,
	// as a resumable layer would.
	holdWrites bool
	resumed    chan struct{}
}

func newTestOwner(persist bool) *testOwner {
	return &testOwner{
		persist:       persist,
		detached:      make(chan *connectwebsocket.PersistentStream, 8),
		detachedConns: make(chan *ownedConn, 8),
		resumed:       make(chan struct{}),
	}
}

type ctxKey struct{}

func (o *testOwner) OpenStream(ctx context.Context, id uint32, header http.Header, conn connectwebsocket.StreamConn) (context.Context, connectwebsocket.StreamConn, bool) {
	o.mu.Lock()
	o.headers = append(o.headers, header)
	o.raw = append(o.raw, conn)
	o.mu.Unlock()
	return context.WithValue(ctx, ctxKey{}, id), &ownedConn{StreamConn: conn, o: o}, o.persist
}

func (o *testOwner) DetachStream(s *connectwebsocket.PersistentStream, conn connectwebsocket.StreamConn) {
	o.mu.Lock()
	o.streams = append(o.streams, s)
	own, isOwn := conn.(*ownedConn)
	o.mu.Unlock()
	if !isOwn || own == nil {
		panic("DetachStream did not hand back the wrapper OpenStream returned")
	}
	o.detachedConns <- own
	o.detached <- s
}

func (o *testOwner) counts() (sent, received int) {
	o.mu.Lock()
	defer o.mu.Unlock()
	return o.sent, o.received
}

// waitCounts waits until the owner has seen at least the given numbers. The
// counters are updated by the RPC's goroutine after each frame reaches the
// socket, so a client that has just read a frame may still be ahead of them;
// sampling once is a race.
func (o *testOwner) waitCounts(t *testing.T, wantSent, wantReceived int) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for {
		sent, received := o.counts()
		if sent >= wantSent && received >= wantReceived {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("owner saw %d sent and %d received, want at least %d and %d", sent, received, wantSent, wantReceived)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func (o *testOwner) lastRaw(t *testing.T) connectwebsocket.StreamConn {
	t.Helper()
	o.mu.Lock()
	defer o.mu.Unlock()
	if len(o.raw) == 0 {
		t.Fatal("OpenStream was never called")
	}
	return o.raw[len(o.raw)-1]
}

func (o *testOwner) lastHeader(t *testing.T) http.Header {
	t.Helper()
	o.mu.Lock()
	defer o.mu.Unlock()
	if len(o.headers) == 0 {
		t.Fatal("OpenStream was never called")
	}
	return o.headers[len(o.headers)-1]
}

// ownedConn is the wrapper the owner puts around each stream.
type ownedConn struct {
	connectwebsocket.StreamConn
	o *testOwner
}

func (c *ownedConn) ReadEnvelope() (uint8, []byte, error) {
	flag, payload, err := c.StreamConn.ReadEnvelope()
	if err == nil {
		c.o.mu.Lock()
		c.o.received++
		c.o.mu.Unlock()
	}
	return flag, payload, err
}

func (c *ownedConn) WriteEnvelope(flag uint8, payload []byte) error {
	for {
		err := c.StreamConn.WriteEnvelope(flag, payload)
		c.o.mu.Lock()
		hold := c.o.holdWrites
		c.o.mu.Unlock()
		if errors.Is(err, connectwebsocket.ErrDetached) && hold {
			// A resumable layer waits for the next socket instead of
			// failing the handler.
			select {
			case <-c.o.resumed:
				continue
			case <-time.After(5 * time.Second):
				return err
			}
		}
		if err == nil {
			c.o.mu.Lock()
			c.o.sent++
			c.o.mu.Unlock()
		}
		return err
	}
}

// nilAdopter offers Adopt a stream that is not there.
type nilAdopter struct{ done chan error }

func (n nilAdopter) HandleControl(_ context.Context, stream connectwebsocket.ControlStream, _ uint8, _ []byte) error {
	n.done <- stream.Adopt(nil)
	return nil
}

// adopter is a control handler that adopts the streams an owner is holding.
type adopter struct {
	o       *testOwner
	mu      sync.Mutex
	lastErr error
	done    chan struct{}
}

func newAdopter(o *testOwner) *adopter { return &adopter{o: o, done: make(chan struct{}, 8)} }

func (a *adopter) HandleControl(_ context.Context, stream connectwebsocket.ControlStream, _ uint8, _ []byte) error {
	a.o.mu.Lock()
	streams := append([]*connectwebsocket.PersistentStream(nil), a.o.streams...)
	a.o.mu.Unlock()
	var err error
	for _, s := range streams {
		if e := stream.Adopt(s); e != nil {
			err = e
		}
	}
	a.mu.Lock()
	a.lastErr = err
	a.mu.Unlock()
	a.done <- struct{}{}
	return nil
}

func (a *adopter) err() error {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.lastErr
}

// cumSumOn starts a CumSum stream by hand and sends one number.
func (r *rawConn) cumSumOn(id uint32, n byte) {
	r.t.Helper()
	r.mustWrite(id, 0x06, cumSumHeaders)
	r.mustWrite(id, 0x00, cumSumRequest(n))
}

func waitSum(t *testing.T, srv *watchedServer, want int64) {
	t.Helper()
	select {
	case got := <-srv.got:
		if got != want {
			t.Fatalf("running sum %d, want %d", got, want)
		}
	case <-time.After(5 * time.Second):
		t.Fatalf("handler never reported sum %d", want)
	}
}

// The whole point of the patch: a dropped socket detaches the stream, a new
// socket adopts it, and the same handler carries on with its state.
func TestPersistentStreamSurvivesAndResumes(t *testing.T) {
	srv := newWatchedServer()
	owner := newTestOwner(true)
	owner.holdWrites = true
	ad := newAdopter(owner)
	wsURL := newServerWithOptions(t, srv,
		connectwebsocket.WithStreamOwner(owner),
		connectwebsocket.WithControlHandler(ad))

	first := dialRaw(t, wsURL)
	first.cumSumOn(7, 5)
	waitSum(t, srv, 5)
	first.expectData(7)

	// The socket dies.
	_ = first.conn.CloseNow()
	var held *connectwebsocket.PersistentStream
	select {
	case held = <-owner.detached:
	case <-time.After(5 * time.Second):
		t.Fatal("DetachStream was never called")
	}
	if held.ID() != 7 || held.Attached() {
		t.Fatalf("detached stream: id %d attached %v", held.ID(), held.Attached())
	}
	// The handler is still waiting, not ended.
	select {
	case err := <-srv.ended:
		t.Fatalf("handler ended on socket loss: %v", err)
	case <-time.After(200 * time.Millisecond):
	}

	// A new socket adopts it.
	second := dialRaw(t, wsURL)
	second.mustWrite(connectwebsocket.ControlStreamID, 0x00, []byte("resume"))
	<-ad.done
	if err := ad.err(); err != nil {
		t.Fatalf("Adopt: %v", err)
	}
	if !held.Attached() {
		t.Fatal("stream is not attached after Adopt")
	}
	close(owner.resumed)

	// The same handler continues: 5 + 4 = 9, so its state survived.
	second.mustWrite(7, 0x00, cumSumRequest(4))
	waitSum(t, srv, 9)
	second.expectData(7)
}

// Without persistence, a dropped socket ends the handler, as upstream does.
func TestNonPersistentStreamStillEndsWithTheSocket(t *testing.T) {
	srv := newWatchedServer()
	owner := newTestOwner(false)
	wsURL := newServerWithOptions(t, srv, connectwebsocket.WithStreamOwner(owner))
	raw := dialRaw(t, wsURL)
	raw.cumSumOn(7, 5)
	waitSum(t, srv, 5)
	_ = raw.conn.CloseNow()
	select {
	case <-srv.ended:
	case <-time.After(5 * time.Second):
		t.Fatal("handler outlived the socket without persistence")
	}
	select {
	case s := <-owner.detached:
		t.Fatalf("DetachStream called for a stream that is not persistent: %d", s.ID())
	default:
	}
}

// The owner sees the stream's headers and every frame it sends and receives.
func TestOwnerObservesStream(t *testing.T) {
	srv := newWatchedServer()
	owner := newTestOwner(false)
	wsURL := newServerWithOptions(t, srv, connectwebsocket.WithStreamOwner(owner))
	raw := dialRaw(t, wsURL)
	raw.cumSumOn(7, 5)
	waitSum(t, srv, 5)
	raw.expectData(7)
	if got := owner.lastHeader(t).Get("Content-Type"); got != "application/connect+proto" {
		t.Fatalf("header Content-Type = %q", got)
	}
	// Received: the headers frame and one data frame. Sent: response headers
	// and one data frame.
	owner.waitCounts(t, 2, 2)
}

// A write on a detached stream fails with ErrDetached rather than hanging or
// panicking, which is the signal an owner waits on to hold the frame.
func TestWriteWhileDetachedFails(t *testing.T) {
	srv := newWatchedServer()
	owner := newTestOwner(true) // holdWrites stays false
	wsURL := newServerWithOptions(t, srv, connectwebsocket.WithStreamOwner(owner))
	raw := dialRaw(t, wsURL)
	raw.cumSumOn(7, 5)
	waitSum(t, srv, 5)
	raw.expectData(7)
	conn := owner.lastRaw(t)
	// Attached: an ordinary write reaches the socket.
	if err := conn.WriteEnvelope(0x00, nil); err != nil {
		t.Fatalf("write while attached: %v", err)
	}

	_ = raw.conn.CloseNow()
	held := <-owner.detached
	if held.Attached() {
		t.Fatal("stream reports attached after its socket died")
	}
	// Detached: the same write reports ErrDetached, every time.
	for i := range 3 {
		if err := conn.WriteEnvelope(0x00, []byte("x")); !errors.Is(err, connectwebsocket.ErrDetached) {
			t.Fatalf("write %d while detached: %v, want ErrDetached", i, err)
		}
	}
	// Closing a detached stream is allowed and must not panic.
	if err := conn.Close(); err != nil {
		t.Fatalf("Close while detached: %v", err)
	}
	select {
	case <-srv.ended:
	case <-time.After(5 * time.Second):
		t.Fatal("Close did not end the handler")
	}
	if held.Attached() {
		t.Fatal("a closed stream reports attached")
	}
	held.Expire(nil)
}

func TestExpireEndsTheHandler(t *testing.T) {
	srv := newWatchedServer()
	owner := newTestOwner(true)
	wsURL := newServerWithOptions(t, srv, connectwebsocket.WithStreamOwner(owner))
	raw := dialRaw(t, wsURL)
	raw.cumSumOn(7, 5)
	waitSum(t, srv, 5)
	_ = raw.conn.CloseNow()
	held := <-owner.detached
	want := errors.New("session expired")
	held.Expire(want)
	select {
	case <-srv.ended:
	case <-time.After(5 * time.Second):
		t.Fatal("handler still running after Expire")
	}
	// Expiring twice, or after the end, is harmless.
	held.Expire(want)
	held.Expire(nil)
	if held.Attached() {
		t.Fatal("an expired stream reports attached")
	}
}

// Adopt refuses a stream that is already attached, one that has ended, and
// one whose ID is in use on the new connection.
func TestAdoptRefusals(t *testing.T) {
	srv := newWatchedServer()
	owner := newTestOwner(true)
	owner.holdWrites = true
	ad := newAdopter(owner)
	wsURL := newServerWithOptions(t, srv,
		connectwebsocket.WithStreamOwner(owner),
		connectwebsocket.WithControlHandler(ad))

	first := dialRaw(t, wsURL)
	first.cumSumOn(7, 1)
	waitSum(t, srv, 1)
	first.expectData(7)
	_ = first.conn.CloseNow()
	held := <-owner.detached

	// The ID is already in use on the new connection.
	second := dialRaw(t, wsURL)
	second.cumSumOn(7, 2)
	waitSum(t, srv, 2)
	second.expectData(7)
	second.mustWrite(connectwebsocket.ControlStreamID, 0x00, nil)
	<-ad.done
	if ad.err() == nil {
		t.Fatal("Adopt accepted a stream whose ID is already in use")
	}
	if held.Attached() {
		t.Fatal("the refused stream reports attached")
	}

	// An ended stream cannot be adopted either.
	held.Expire(nil)
	third := dialRaw(t, wsURL)
	third.mustWrite(connectwebsocket.ControlStreamID, 0x00, nil)
	<-ad.done
	if ad.err() == nil {
		t.Fatal("Adopt accepted a stream that had ended")
	}
}

// Adopting a stream that is attached to a live connection is refused, so a
// second socket cannot steal a stream from the first.
func TestAdoptRefusesAttachedStream(t *testing.T) {
	srv := newWatchedServer()
	owner := newTestOwner(true)
	owner.holdWrites = true
	ad := newAdopter(owner)
	wsURL := newServerWithOptions(t, srv,
		connectwebsocket.WithStreamOwner(owner),
		connectwebsocket.WithControlHandler(ad))
	first := dialRaw(t, wsURL)
	first.cumSumOn(7, 1)
	waitSum(t, srv, 1)
	first.expectData(7)
	_ = first.conn.CloseNow()
	held := <-owner.detached

	second := dialRaw(t, wsURL)
	second.mustWrite(connectwebsocket.ControlStreamID, 0x00, nil)
	<-ad.done
	if ad.err() != nil {
		t.Fatalf("first Adopt: %v", ad.err())
	}
	// A third connection must not take it away from the second.
	third := dialRaw(t, wsURL)
	third.mustWrite(connectwebsocket.ControlStreamID, 0x00, nil)
	<-ad.done
	if ad.err() == nil {
		t.Fatal("a second connection adopted a stream that was already attached")
	}
	if !held.Attached() {
		t.Fatal("the refused adopt detached the stream from its connection")
	}
}

// A persistent stream must not hold the HTTP handler open after its socket
// closes, or the server would leak a goroutine per dropped connection.
func TestServeHTTPReturnsWithPersistentStreamRunning(t *testing.T) {
	srv := newWatchedServer()
	owner := newTestOwner(true)
	done := make(chan struct{})
	wsURL := newServerWithHandlerWrapper(t, srv, func(next http.Handler, w http.ResponseWriter, r *http.Request) {
		next.ServeHTTP(w, r)
		close(done)
	}, connectwebsocket.WithStreamOwner(owner))

	raw := dialRaw(t, wsURL)
	raw.cumSumOn(7, 5)
	waitSum(t, srv, 5)
	_ = raw.conn.CloseNow()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("ServeHTTP did not return while a persistent stream was running")
	}
	held := <-owner.detached
	held.Expire(nil)
}

// An owner that keeps nothing leaves every upstream guarantee in place,
// including the stream caps.
func TestOwnerWithoutPersistenceKeepsLimits(t *testing.T) {
	srv := newWatchedServer()
	owner := newTestOwner(false)
	wsURL := newServerWithOptions(t, srv,
		connectwebsocket.WithStreamOwner(owner),
		connectwebsocket.WithMaxStreams(2))
	raw := dialRaw(t, wsURL)
	raw.cumSumOn(1, 1)
	raw.cumSumOn(2, 1)
	waitSum(t, srv, 1)
	waitSum(t, srv, 1)
	_ = raw.write(3, 0x06, cumSumHeaders)
	raw.expectClose(websocket.StatusPolicyViolation)
}

// When an adopted stream ends, it must be unregistered from the connection
// it ended on, not the one it started on. Opening a stream with the same ID
// afterwards proves it: a forgotten registration would swallow the frames.
//
// The handler unregisters itself as it unwinds, which happens shortly after
// it reports that it ended, so the test keeps offering the new stream until
// it is accepted rather than racing that unwind once.
func TestAdoptedStreamIsUnregisteredFromItsNewConnection(t *testing.T) {
	srv := newWatchedServer()
	owner := newTestOwner(true)
	owner.holdWrites = true
	ad := newAdopter(owner)
	wsURL := newServerWithOptions(t, srv,
		connectwebsocket.WithStreamOwner(owner),
		connectwebsocket.WithControlHandler(ad))

	first := dialRaw(t, wsURL)
	first.cumSumOn(7, 5)
	waitSum(t, srv, 5)
	first.expectData(7)
	_ = first.conn.CloseNow()
	<-owner.detached

	second := dialRaw(t, wsURL)
	second.mustWrite(connectwebsocket.ControlStreamID, 0x00, nil)
	<-ad.done
	if err := ad.err(); err != nil {
		t.Fatalf("Adopt: %v", err)
	}
	close(owner.resumed)

	// End the adopted stream.
	second.mustWrite(7, 0x02, nil)
	select {
	case <-srv.ended:
	case <-time.After(5 * time.Second):
		t.Fatal("the adopted handler did not end")
	}

	// Offer a fresh stream on the same ID until it starts. A fresh handler
	// counts from zero, so the reply is 3, not 8.
	stop := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-stop:
				return
			default:
			}
			if err := second.write(7, 0x06, cumSumHeaders); err != nil {
				return
			}
			if err := second.write(7, 0x00, cumSumRequest(3)); err != nil {
				return
			}
			time.Sleep(20 * time.Millisecond)
		}
	}()
	waitSum(t, srv, 3)
	close(stop)
	wg.Wait()
}

// Adopt refuses a nil stream instead of panicking.
func TestAdoptNilRefused(t *testing.T) {
	srv := newWatchedServer()
	owner := newTestOwner(true)
	h := nilAdopter{done: make(chan error, 1)}
	wsURL := newServerWithOptions(t, srv,
		connectwebsocket.WithStreamOwner(owner),
		connectwebsocket.WithControlHandler(h))
	raw := dialRaw(t, wsURL)
	raw.mustWrite(connectwebsocket.ControlStreamID, 0x00, nil)
	select {
	case err := <-h.done:
		if err == nil {
			t.Fatal("Adopt(nil) was accepted")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("control handler never ran")
	}
	// The connection is still usable.
	raw.cumSumOn(1, 2)
	waitSum(t, srv, 2)
}

// valueServer reports what it finds in its context, so a test can check that
// the owner's per-stream values reached the handler.
type valueServer struct {
	testPingServer
	got chan any
}

func (s *valueServer) CumSum(ctx context.Context, stream pingv1connect.PingServiceCumSumServerStream) error {
	if _, err := stream.Receive(); err != nil {
		return err
	}
	s.got <- ctx.Value(ctxKey{})
	return stream.Send(&pingv1.CumSumResponse{Sum: 1})
}

// The context an owner returns from OpenStream becomes the RPC's parent.
func TestOwnerContextReachesTheHandler(t *testing.T) {
	srv := &valueServer{got: make(chan any, 1)}
	owner := newTestOwner(false)
	wsURL := newServerWithOptions(t, srv, connectwebsocket.WithStreamOwner(owner))
	raw := dialRaw(t, wsURL)
	raw.cumSumOn(9, 1)
	select {
	case got := <-srv.got:
		if id, ok := got.(uint32); !ok || id != 9 {
			t.Fatalf("handler saw %#v, want the stream ID 9", got)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("handler never ran")
	}
}

// Stream IDs are chosen per connection, so two sessions can both hold a
// stream 1. DetachStream must hand back the wrapper that opened each one,
// or an owner cannot tell them apart.
func TestDetachStreamIdentifiesTheStreamByItsWrapper(t *testing.T) {
	srv := newWatchedServer()
	owner := newTestOwner(true)
	wsURL := newServerWithOptions(t, srv, connectwebsocket.WithStreamOwner(owner))

	first := dialRaw(t, wsURL)
	first.cumSumOn(1, 5)
	waitSum(t, srv, 5)
	firstConn := owner.lastRaw(t)

	second := dialRaw(t, wsURL)
	second.cumSumOn(1, 7) // the same stream ID on another connection
	waitSum(t, srv, 7)
	secondConn := owner.lastRaw(t)
	if firstConn == secondConn {
		t.Fatal("the two connections shared a stream object")
	}

	_ = second.conn.CloseNow()
	gotConn := <-owner.detachedConns
	got := <-owner.detached
	if got.ID() != 1 {
		t.Fatalf("detached stream ID %d, want 1", got.ID())
	}
	if gotConn.StreamConn != secondConn {
		t.Fatal("DetachStream handed back the wrong connection's stream")
	}
	got.Expire(nil)

	_ = first.conn.CloseNow()
	gotConn = <-owner.detachedConns
	got = <-owner.detached
	if gotConn.StreamConn != firstConn {
		t.Fatal("the second detach handed back the wrong stream")
	}
	got.Expire(nil)
}
