// Copyright 2021-2026 The Connect Authors
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
//
// Modified by the RossB project, 2026: stream count for WithMaxStreams,
// bounded delivery for WithDeliverTimeout, 32-bit build fix, control stream,
// streams that outlive their connection (WithStreamOwner).

package connectwebsocket

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math"
	"sync"
	"sync/atomic"
	"time"

	"connectrpc.com/connect/v2"
	"github.com/coder/websocket"
	"github.com/sudorandom/connect-bidi-web/internal/bidiprotocol"
)

// Wire framing: every binary WebSocket message carries exactly one envelope
// belonging to exactly one stream:
//
//	[4-byte big-endian stream ID][1-byte flag][4-byte big-endian length][payload]
//
// The stream ID lets several RPCs share one connection: receivers use it to
// match each frame to the appropriate caller. IDs are assigned by the
// client, start at 1, increase by one per stream, and are never reused
// within a connection. Everything after the stream ID is a standard Connect
// envelope.

// ControlStreamID is the stream ID reserved for the control stream by
// WithControlHandler. The transport in this package never allocates it: its
// own stream IDs start at 1.
const ControlStreamID uint32 = 0

const (
	streamIDLen  = 4
	frameHeadLen = streamIDLen + 5 // stream ID + Connect envelope head
)

var (
	errConnClosed   = errors.New("websocket connection closed")
	errStreamClosed = errors.New("websocket stream closed")
	// ErrDetached is returned by a write on a persistent stream that has no
	// connection: the socket is gone and no new one has adopted it yet.
	ErrDetached = errors.New("connectwebsocket: stream detached")
	// errStreamExpired ends a persistent stream whose owner gave up on it.
	errStreamExpired = errors.New("connectwebsocket: stream expired")
	// errAdoptRefused is returned by ControlStream.Adopt.
	errAdoptRefused = errors.New("connectwebsocket: stream cannot be adopted")
	// errDeliverTimeout ends a stream whose consumer stopped reading.
	errDeliverTimeout = connect.NewError(connect.CodeResourceExhausted, "websocket stream consumer too slow")
)

// writeFrame writes one WebSocket message carrying one envelope for one
// stream. Callers must serialize calls (muxConn.writeFrame does).
func writeFrame(ctx context.Context, conn *websocket.Conn, streamID uint32, flag uint8, payload []byte) error {
	if uint64(len(payload)) > math.MaxUint32 {
		return errors.New("payload too large")
	}
	writer, err := conn.Writer(ctx, websocket.MessageBinary)
	if err != nil {
		return err
	}
	var head [frameHeadLen]byte
	binary.BigEndian.PutUint32(head[0:4], streamID)
	head[4] = flag
	//nolint:gosec // the payload length is bounded to MaxUint32 above
	binary.BigEndian.PutUint32(head[5:9], uint32(len(payload)))
	if _, err := writer.Write(head[:]); err != nil {
		_ = writer.Close()
		return err
	}
	if len(payload) > 0 {
		if _, err := writer.Write(payload); err != nil {
			_ = writer.Close()
			return err
		}
	}
	return writer.Close()
}

// readFrame reads one WebSocket message and splits it into stream ID, flag,
// and payload. A message must contain exactly one complete envelope.
func readFrame(ctx context.Context, conn *websocket.Conn) (streamID uint32, flag uint8, payload []byte, err error) {
	msgType, data, err := conn.Read(ctx)
	if err != nil {
		return 0, 0, nil, err
	}
	if msgType != websocket.MessageBinary {
		return 0, 0, nil, errors.New("received non-binary websocket message")
	}
	if len(data) < frameHeadLen {
		return 0, 0, nil, fmt.Errorf("frame too short: %d bytes", len(data))
	}
	streamID = binary.BigEndian.Uint32(data[0:4])
	flag = data[4]
	length := binary.BigEndian.Uint32(data[5:9])
	if int64(length) != int64(len(data)-frameHeadLen) {
		return 0, 0, nil, fmt.Errorf("envelope declares %d payload bytes but frame carries %d", length, len(data)-frameHeadLen)
	}
	return streamID, flag, data[frameHeadLen:], nil
}

// muxConn multiplexes streams onto a single WebSocket connection. Both the
// client transport and the server handler use it: a single read loop routes
// each incoming frame to the inbox of the stream it belongs to, and outgoing
// frames from all streams are serialized onto the connection.
type muxConn struct {
	conn *websocket.Conn
	// writeCtx lives as long as the connection and is used for every write.
	// Stream or RPC contexts must never reach a write: coder/websocket
	// treats a context canceled mid-write as fatal to the connection (the
	// frame may be half-written), which would tear down every other stream.
	// Per-stream cancellation is checked before writing instead.
	writeCtx context.Context //nolint:containedctx // see above; the connection outlives any caller context

	writeMu sync.Mutex

	mu       sync.Mutex
	streams  map[uint32]*muxStream
	nextID   uint32
	closed   bool
	closeErr error
}

func newMuxConn(writeCtx context.Context, conn *websocket.Conn) *muxConn {
	return &muxConn{
		conn:     conn,
		writeCtx: writeCtx,
		streams:  make(map[uint32]*muxStream),
	}
}

// newStream registers the next client-initiated stream. A dedicated stream
// owns the connection outright: closing the stream closes the connection.
func (mc *muxConn) newStream(ctx context.Context, dedicated bool) (*muxStream, error) {
	mc.mu.Lock()
	defer mc.mu.Unlock()
	if mc.closed {
		return nil, mc.closeErr
	}
	for i := uint64(0); i < math.MaxUint32; i++ {
		mc.nextID++
		if mc.nextID == 0 {
			mc.nextID = 1
		}
		if _, exists := mc.streams[mc.nextID]; !exists {
			stream := newMuxStream(ctx, mc, mc.nextID)
			stream.dedicated = dedicated
			mc.streams[stream.id] = stream
			return stream, nil
		}
	}
	return nil, errors.New("stream ID space exhausted")
}

// register adds a peer-initiated stream (server side). It reports false if
// the connection is closed or the ID is already in use.
func (mc *muxConn) register(stream *muxStream) bool {
	mc.mu.Lock()
	defer mc.mu.Unlock()
	if mc.closed {
		return false
	}
	if _, exists := mc.streams[stream.id]; exists {
		return false
	}
	mc.streams[stream.id] = stream
	return true
}

func (mc *muxConn) deregister(streamID uint32) {
	mc.mu.Lock()
	delete(mc.streams, streamID)
	mc.mu.Unlock()
}

// count reports how many streams are registered.
func (mc *muxConn) count() int {
	mc.mu.Lock()
	defer mc.mu.Unlock()
	return len(mc.streams)
}

func (mc *muxConn) lookup(streamID uint32) *muxStream {
	mc.mu.Lock()
	defer mc.mu.Unlock()
	return mc.streams[streamID]
}

// conn returns the connection the stream is attached to, or nil.
func (s *muxStream) conn() *muxConn {
	s.connMu.Lock()
	defer s.connMu.Unlock()
	return s.mc
}

func (mc *muxConn) writeFrame(streamID uint32, flag uint8, payload []byte) error {
	mc.writeMu.Lock()
	defer mc.writeMu.Unlock()
	return writeFrame(mc.writeCtx, mc.conn, streamID, flag, payload)
}

// controlStream is the ControlStream handed to a ControlHandler. It holds
// only the connection, so it stays valid for the connection's lifetime.
type controlStream struct{ mc *muxConn }

// Adopt implements ControlStream.
func (c controlStream) Adopt(stream *PersistentStream) error {
	if stream == nil || stream.stream == nil {
		return errAdoptRefused
	}
	c.mc.mu.Lock()
	closed, closeErr := c.mc.closed, c.mc.closeErr
	c.mc.mu.Unlock()
	if closed {
		return closeErr
	}
	if !stream.stream.adopt(c.mc) {
		return errAdoptRefused
	}
	return nil
}

// Send implements ControlStream.
func (c controlStream) Send(flag uint8, payload []byte) error {
	c.mc.mu.Lock()
	closed, closeErr := c.mc.closed, c.mc.closeErr
	c.mc.mu.Unlock()
	if closed {
		return closeErr
	}
	return c.mc.writeFrame(ControlStreamID, flag, payload)
}

// readLoopClient routes incoming frames to the client streams that opened
// them until the connection fails or is closed. Frames for unknown streams
// (already closed on this side) are dropped.
func (mc *muxConn) readLoopClient(ctx context.Context) {
	for {
		streamID, flag, payload, err := readFrame(ctx, mc.conn)
		if err != nil {
			mc.terminateAll(connReadError(err))
			_ = mc.conn.CloseNow()
			return
		}
		stream := mc.lookup(streamID)
		if stream == nil {
			continue
		}
		if flag == bidiprotocol.FlagEnvelopeReset {
			mc.deregister(streamID)
			stream.terminate(connect.Errorf(connect.CodeCanceled, "stream reset by peer"))
			continue
		}
		stream.deliver(flag, payload)
	}
}

// terminateAll marks the connection closed and terminates every stream. A
// persistent stream is detached instead: it is removed from the connection
// and returned, still running, for its owner to re-attach or expire.
func (mc *muxConn) terminateAll(err error) []*muxStream {
	mc.mu.Lock()
	if !mc.closed {
		mc.closed = true
		mc.closeErr = err
	}
	streams := make([]*muxStream, 0, len(mc.streams))
	detached := make([]*muxStream, 0)
	for _, stream := range mc.streams {
		if stream.persistent {
			detached = append(detached, stream)
			continue
		}
		streams = append(streams, stream)
	}
	clear(mc.streams)
	mc.mu.Unlock()
	for _, stream := range detached {
		stream.detach()
	}
	for _, stream := range streams {
		stream.terminate(err)
	}
	return detached
}

// shutdown terminates every stream and closes the connection gracefully.
func (mc *muxConn) shutdown() error {
	_ = mc.terminateAll(errConnClosed)
	return mc.conn.Close(websocket.StatusNormalClosure, "")
}

// connReadError converts a read-loop failure into the error surfaced by the
// streams that were cut off by it.
func connReadError(err error) error {
	if errors.Is(err, io.EOF) {
		return io.EOF
	}
	switch status := websocket.CloseStatus(err); status {
	case -1:
		return connect.Errorf(connect.CodeUnavailable, "websocket read: %v", err)
	case websocket.StatusNormalClosure, websocket.StatusGoingAway:
		return io.EOF
	default:
		return connect.Errorf(connect.CodeUnavailable, "websocket closed with status %v", status)
	}
}

// envelope is one frame routed to a stream's inbox.
type envelope struct {
	flag    uint8
	payload []byte
}

// muxStream is one logical stream on a muxConn, implementing
// bidiprotocol.Conn. The connection's read loop delivers this stream's
// frames to inbox; writes go back through the shared connection with this
// stream's ID.
type muxStream struct {
	// connMu guards mc, which changes when a persistent stream is adopted
	// by another connection.
	connMu sync.Mutex
	mc     *muxConn

	id  uint32
	ctx context.Context //nolint:containedctx // carries the RPC context into bidiprotocol.Conn's context-free interface

	inbox chan envelope

	terminateOnce sync.Once
	terminated    chan struct{}
	terminalErr   error

	// cancel aborts the server-side handler for this stream; nil on clients.
	cancel context.CancelFunc

	// dedicated marks a client stream that owns its connection outright.
	dedicated bool

	// rxEnd records that the peer finished this stream with an end-stream
	// envelope, making a reset frame on close unnecessary.
	rxEnd atomic.Bool

	// persistent marks a stream that the connection's end detaches instead
	// of terminating; see WithStreamOwner.
	persistent bool
	// wrapper is what the owner returned from OpenStream, handed back to it
	// at DetachStream so it can recognise the stream.
	wrapper StreamConn
	// attached is false while a persistent stream has no connection.
	attached atomic.Bool
}

var _ bidiprotocol.Conn = (*muxStream)(nil)

func newMuxStream(ctx context.Context, mc *muxConn, id uint32) *muxStream {
	return &muxStream{
		mc:         mc,
		id:         id,
		ctx:        ctx,
		inbox:      make(chan envelope, 64),
		terminated: make(chan struct{}),
	}
}

// deliver hands a frame to the stream's consumer. It blocks until the
// consumer accepts it, providing connection-wide backpressure, and drops the
// frame if the stream terminates first.
func (s *muxStream) deliver(flag uint8, payload []byte) {
	select {
	case s.inbox <- envelope{flag: flag, payload: payload}:
	case <-s.terminated:
	}
}

// deliverWithin is deliver with a bound: it reports false if the consumer
// did not accept the frame within d. A frame for a terminated stream is
// dropped and reported as delivered, as in deliver. The timer is only
// created when the inbox is full, so the common path does not allocate.
func (s *muxStream) deliverWithin(flag uint8, payload []byte, d time.Duration) bool {
	env := envelope{flag: flag, payload: payload}
	select {
	case s.inbox <- env:
		return true
	case <-s.terminated:
		return true
	default:
	}
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case s.inbox <- env:
		return true
	case <-s.terminated:
		return true
	case <-timer.C:
		return false
	}
}

// detach unhooks a persistent stream from its connection. The handler keeps
// running: its context is not canceled and its inbox is kept, so frames that
// arrived before the drop are still delivered. Writes fail until the stream
// is adopted again.
func (s *muxStream) detach() {
	s.connMu.Lock()
	s.mc = nil
	s.connMu.Unlock()
	s.attached.Store(false)
}

// adopt attaches a detached persistent stream to mc under its own ID. It
// reports false if the stream has ended, is still attached, or the ID is
// taken on mc.
func (s *muxStream) adopt(mc *muxConn) bool {
	select {
	case <-s.terminated:
		return false
	default:
	}
	s.connMu.Lock()
	if s.mc != nil {
		s.connMu.Unlock()
		return false
	}
	if !mc.register(s) {
		s.connMu.Unlock()
		return false
	}
	s.mc = mc
	s.connMu.Unlock()
	s.attached.Store(true)
	return true
}

// terminate ends the stream with err as the terminal error for both
// directions, and cancels the server-side handler if there is one.
func (s *muxStream) terminate(err error) {
	s.terminateOnce.Do(func() {
		s.terminalErr = err
		close(s.terminated)
		if s.cancel != nil {
			s.cancel()
		}
	})
}

// ReadEnvelope implements bidiprotocol.Conn.
func (s *muxStream) ReadEnvelope() (uint8, []byte, error) {
	// Prefer a frame delivered before termination, so an end-stream racing a
	// connection failure isn't lost.
	select {
	case env := <-s.inbox:
		return s.acceptEnvelope(env)
	default:
	}
	select {
	case env := <-s.inbox:
		return s.acceptEnvelope(env)
	case <-s.terminated:
		return 0, nil, s.terminalErr
	case <-s.ctx.Done():
		return 0, nil, s.ctx.Err()
	}
}

func (s *muxStream) acceptEnvelope(env envelope) (uint8, []byte, error) {
	if env.flag == bidiprotocol.FlagEnvelopeEndStream {
		s.rxEnd.Store(true)
	}
	return env.flag, env.payload, nil
}

// WriteEnvelope implements bidiprotocol.Conn. Stream cancellation is checked
// before the write; the write itself runs under the connection's context so
// a cancellation can never poison the shared connection mid-frame.
func (s *muxStream) WriteEnvelope(flag uint8, payload []byte) error {
	select {
	case <-s.terminated:
		return s.terminalErr
	case <-s.ctx.Done():
		return s.ctx.Err()
	default:
	}
	mc := s.conn()
	if mc == nil {
		// A persistent stream with no connection. The owner decides what to
		// do with frames written while detached; this package does not hold
		// them.
		return ErrDetached
	}
	return mc.writeFrame(s.id, flag, payload)
}

// CloseSend implements bidiprotocol.Conn by writing an explicit end-stream
// envelope: a WebSocket has no per-stream half-close of its own.
func (s *muxStream) CloseSend() error {
	return s.WriteEnvelope(bidiprotocol.FlagEnvelopeEndStream, nil)
}

// Close implements bidiprotocol.Conn, releasing the client side of the
// stream. If the peer hasn't finished the stream, a reset frame tells it to
// stop work; a dedicated connection is closed outright instead.
func (s *muxStream) Close() error {
	mc := s.conn()
	if mc == nil {
		s.terminate(errStreamClosed)
		return nil
	}
	mc.deregister(s.id)
	finished := s.rxEnd.Load()
	s.terminate(errStreamClosed)
	if s.dedicated {
		return mc.conn.Close(websocket.StatusNormalClosure, "")
	}
	if !finished {
		_ = mc.writeFrame(s.id, bidiprotocol.FlagEnvelopeReset, nil)
	}
	return nil
}
