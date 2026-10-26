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
// Modified by the RossB project, 2026: read limit (WithMaxFrameBytes),
// stream cap (WithMaxStreams), increasing IDs (WithIncreasingStreamIDs),
// slow-consumer reset (WithDeliverTimeout), control stream (WithControlHandler),
// persistent streams (WithStreamOwner).

package connectwebsocket

import (
	"context"
	"log/slog"
	"net/http"
	"sync"

	"connectrpc.com/connect/v2"
	"github.com/coder/websocket"
	"github.com/sudorandom/connect-bidi-web/internal/bidiprotocol"
)

// Handler serves Connect RPCs over WebSocket connections. Each accepted
// connection carries any number of concurrent RPCs, demultiplexed by the
// stream ID on every frame.
type Handler struct {
	server *connect.Server
	opts   serverOptions
}

// NewHandler creates a new Handler for serving Connect RPCs over WebSockets.
func NewHandler(server *connect.Server, opts ...Option) *Handler {
	sOpts := serverOptions{Options: bidiprotocol.NewServerOptions()}
	for _, opt := range opts {
		opt.applyServer(&sOpts)
	}
	sOpts.Finalize()

	return &Handler{
		server: server,
		opts:   sOpts,
	}
}

// ServeHTTP implements http.Handler by upgrading the request to a WebSocket
// connection and serving RPCs on it until the client disconnects. A frame
// with an unknown stream ID and a headers flag starts a new RPC; a reset
// frame cancels an in-flight one.
func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	conn, err := websocket.Accept(w, r, h.opts.acceptOptions)
	if err != nil {
		return
	}
	if h.opts.maxFrameBytes > 0 {
		conn.SetReadLimit(h.opts.maxFrameBytes)
	} else {
		conn.SetReadLimit(-1)
	}

	ctx := r.Context()
	mc := newMuxConn(ctx, conn)
	var handlers sync.WaitGroup
	closeStatus := websocket.StatusNormalClosure
	var highestID uint32 // highest stream ID started; 0 before the first
	for {
		streamID, flag, payload, err := readFrame(ctx, conn)
		if err != nil {
			slog.DebugContext(ctx, "bidi server: websocket read ended", "error", err)
			break
		}
		if h.opts.control != nil && streamID == ControlStreamID {
			if err := h.opts.control.HandleControl(ctx, controlStream{mc: mc}, flag, payload); err != nil {
				slog.DebugContext(ctx, "bidi server: control handler refused a frame", "error", err)
				closeStatus = websocket.StatusPolicyViolation
				break
			}
			continue
		}
		stream := mc.lookup(streamID)
		if stream == nil {
			if flag != bidiprotocol.FlagEnvelopeHeaders {
				// A frame for a stream that already finished; drop it.
				continue
			}
			if h.opts.increasingStreamIDs {
				if streamID <= highestID {
					slog.DebugContext(ctx, "bidi server: stream ID not increasing", "stream_id", streamID)
					closeStatus = websocket.StatusPolicyViolation
					break
				}
				highestID = streamID
			}
			if h.opts.maxStreams > 0 && mc.count() >= h.opts.maxStreams {
				slog.DebugContext(ctx, "bidi server: stream limit reached", "limit", h.opts.maxStreams)
				closeStatus = websocket.StatusPolicyViolation
				break
			}
			h.startStream(ctx, mc, streamID, payload, &handlers, r.RemoteAddr)
			continue
		}
		if flag == bidiprotocol.FlagEnvelopeReset {
			mc.deregister(streamID)
			stream.terminate(context.Canceled)
			continue
		}
		if h.opts.deliverTimeout > 0 {
			if !stream.deliverWithin(flag, payload, h.opts.deliverTimeout) {
				slog.DebugContext(ctx, "bidi server: stream consumer too slow, resetting", "stream_id", streamID)
				mc.deregister(streamID)
				stream.terminate(errDeliverTimeout)
				_ = mc.writeFrame(streamID, bidiprotocol.FlagEnvelopeReset, nil)
			}
			continue
		}
		stream.deliver(flag, payload)
	}
	detached := mc.terminateAll(errConnClosed)
	handlers.Wait()
	_ = conn.Close(closeStatus, "")
	// Hand the surviving streams back last, once this connection is done
	// with them, so the owner can adopt them onto another one.
	for _, stream := range detached {
		h.opts.owner.DetachStream(&PersistentStream{stream: stream}, stream.wrapper)
	}
}

// startStream registers a new stream and dispatches its RPC on its own
// goroutine, seeded with the headers envelope that opened it.
func (h *Handler) startStream(
	ctx context.Context,
	mc *muxConn,
	streamID uint32,
	headersPayload []byte,
	handlers *sync.WaitGroup,
	remoteAddr string,
) {
	stream := newMuxStream(ctx, mc, streamID)
	// The inbox always has room for the headers envelope on a fresh stream.
	stream.deliver(bidiprotocol.FlagEnvelopeHeaders, headersPayload)

	var conn bidiprotocol.Conn = stream
	if h.opts.owner != nil {
		if header, err := bidiprotocol.UnmarshalHeaders(headersPayload); err == nil {
			streamValues, wrapped, persist := h.opts.owner.OpenStream(ctx, streamID, header, stream)
			if wrapped != nil {
				conn = wrapped
				stream.wrapper = wrapped
			}
			if streamValues != nil {
				ctx = streamValues
			}
			stream.persistent = persist
		}
		// A headers frame this package cannot parse is left to HandleRPC,
		// which refuses it; the owner never sees such a stream.
	}

	// A persistent stream outlives this connection, so its context must not
	// be canceled when the connection's is. Values (trace identifiers and
	// the like) are kept.
	parent := ctx
	if stream.persistent {
		parent = context.WithoutCancel(ctx)
	}
	streamCtx, cancel := context.WithCancel(parent)
	stream.ctx = streamCtx
	stream.cancel = cancel
	// A fresh stream is attached. No caller can observe this before the
	// connection ends, because the handle only reaches the owner at
	// DetachStream, but it keeps the flag's meaning (the stream has a
	// connection) true at all times.
	stream.attached.Store(true)

	if !mc.register(stream) {
		cancel()
		return
	}
	if !stream.persistent {
		handlers.Add(1)
	}
	go func() {
		if !stream.persistent {
			defer handlers.Done()
		}
		defer func() {
			if live := stream.conn(); live != nil {
				live.deregister(stream.id)
			}
		}()
		// Terminating unblocks the read loop if it is delivering a frame to
		// this stream, and cancels streamCtx.
		defer stream.terminate(errStreamClosed)
		bidiprotocol.HandleRPC(streamCtx, conn, h.server, h.opts.Options, "websocket", remoteAddr)
	}()
}
