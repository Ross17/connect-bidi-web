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
// Modified by the RossB project, 2026: added WithMaxFrameBytes, WithMaxStreams,
// WithIncreasingStreamIDs, WithDeliverTimeout, WithControlHandler.

// Package connectwebsocket provides a connect.Transport and an http.Handler
// that carry Connect RPCs over WebSocket connections, enabling full
// bidirectional streaming from environments such as web browsers. Every
// frame carries a stream ID, so any number of concurrent RPCs are
// multiplexed onto one shared WebSocket connection; WithConnectionPerStream
// gives each streaming RPC a dedicated connection instead.
package connectwebsocket

import (
	"context"
	"errors"
	"sync"
	"time"

	"connectrpc.com/connect/v2"
	"github.com/coder/websocket"
	"github.com/sudorandom/connect-bidi-web/internal/bidiprotocol"
)

// Option configures NewTransport and NewHandler.
type Option interface {
	applyTransport(*transportOptions)
	applyServer(*serverOptions)
}

type transportOptions struct {
	bidiprotocol.Options

	dialOptions         *websocket.DialOptions
	connectionPerStream bool
}

type serverOptions struct {
	bidiprotocol.Options

	acceptOptions *websocket.AcceptOptions

	// maxFrameBytes bounds each WebSocket message the server reads; 0 or
	// less keeps the upstream behaviour (no limit).
	maxFrameBytes int64

	// maxStreams bounds concurrent streams per connection; 0 or less keeps
	// the upstream behaviour (no limit).
	maxStreams int

	// increasingStreamIDs requires each new stream ID to be greater than
	// every earlier one on the connection.
	increasingStreamIDs bool

	// deliverTimeout bounds how long the read loop waits for one stream's
	// consumer; 0 or less keeps the upstream behaviour (wait forever).
	deliverTimeout time.Duration

	// control, when set, receives every frame on stream 0 instead of
	// letting it open an RPC.
	control ControlHandler
}

type optionFunc func(*transportOptions, *serverOptions)

func (f optionFunc) applyTransport(opts *transportOptions) { f(opts, nil) }
func (f optionFunc) applyServer(opts *serverOptions)       { f(nil, opts) }

// WithSendCodec selects the codec (by name) used for outgoing client requests.
func WithSendCodec(name string) Option {
	return optionFunc(func(topts *transportOptions, _ *serverOptions) {
		if topts != nil {
			topts.SendCodecName = name
		}
	})
}

// WithCodecs registers connect.Codec values.
func WithCodecs(codecs ...connect.Codec) Option {
	return optionFunc(func(topts *transportOptions, sopts *serverOptions) {
		if topts != nil {
			topts.AddCodecs(codecs...)
		}
		if sopts != nil {
			sopts.AddCodecs(codecs...)
		}
	})
}

// WithSendCompressor selects the compressor (by name) to apply to outgoing
// client payloads.
func WithSendCompressor(name string) Option {
	return optionFunc(func(topts *transportOptions, _ *serverOptions) {
		if topts != nil {
			topts.SendCompressor = name
		}
	})
}

// WithCompressors registers connect.Compressor values.
func WithCompressors(compressors ...connect.Compressor) Option {
	return optionFunc(func(topts *transportOptions, sopts *serverOptions) {
		if topts != nil {
			topts.AddCompressors(compressors...)
		}
		if sopts != nil {
			sopts.AddCompressors(compressors...)
		}
	})
}

// WithReadMaxBytes limits the size of a message that can be read.
func WithReadMaxBytes(maxBytes int) Option {
	return optionFunc(func(topts *transportOptions, sopts *serverOptions) {
		if topts != nil {
			topts.ReadMaxBytes = maxBytes
		}
		if sopts != nil {
			sopts.ReadMaxBytes = maxBytes
		}
	})
}

// WithSendMaxBytes limits the size of a message that can be sent.
func WithSendMaxBytes(maxBytes int) Option {
	return optionFunc(func(topts *transportOptions, sopts *serverOptions) {
		if topts != nil {
			topts.SendMaxBytes = maxBytes
		}
		if sopts != nil {
			sopts.SendMaxBytes = maxBytes
		}
	})
}

// WithMaxFrameBytes limits the size of each WebSocket message the handler
// reads, including the 9-byte frame head (stream ID and envelope head). The
// limit is enforced by the WebSocket library while the message is being
// read, so an oversized message is never fully buffered: the connection is
// closed with status 1009 (message too big). WithReadMaxBytes, by contrast,
// is checked only after a whole message is in memory. A value of 0 or less
// means no limit, which is the default. It applies to NewHandler only.
func WithMaxFrameBytes(maxBytes int64) Option {
	return optionFunc(func(_ *transportOptions, sopts *serverOptions) {
		if sopts != nil {
			sopts.maxFrameBytes = maxBytes
		}
	})
}

// WithMaxStreams limits how many streams may be open at once on one
// WebSocket connection. A headers frame that would open one more stream
// closes the whole connection with status 1008 (policy violation); a
// well-behaved client never exceeds the limit it was configured for. A
// value of 0 or less means no limit, which is the default. It applies to
// NewHandler only.
func WithMaxStreams(maxStreams int) Option {
	return optionFunc(func(_ *transportOptions, sopts *serverOptions) {
		if sopts != nil {
			sopts.maxStreams = maxStreams
		}
	})
}

// WithIncreasingStreamIDs makes the handler enforce the wire rule that
// stream IDs on a connection only increase and are never reused. Without
// it, a finished stream's ID is forgotten and a later headers frame with
// the same ID starts a new RPC. With it, a headers frame whose ID is not
// greater than every ID seen before (including 0) closes the connection
// with status 1008. The transport in this package already allocates IDs
// this way. It applies to NewHandler only.
func WithIncreasingStreamIDs() Option {
	return optionFunc(func(_ *transportOptions, sopts *serverOptions) {
		if sopts != nil {
			sopts.increasingStreamIDs = true
		}
	})
}

// WithDeliverTimeout bounds how long the handler's read loop waits for one
// stream's handler to accept a frame. Without it, a handler that stops
// reading blocks the read loop, which stalls every other stream on the
// connection, including their reset frames (head-of-line blocking). With
// it, a stream whose inbox stays full for longer than d is reset: the
// handler's context is canceled with a resource-exhausted error, a reset
// frame is sent to the client, and the read loop carries on. A value of 0
// or less means wait forever, which is the default. It applies to
// NewHandler only.
func WithDeliverTimeout(d time.Duration) Option {
	return optionFunc(func(_ *transportOptions, sopts *serverOptions) {
		if sopts != nil {
			sopts.deliverTimeout = d
		}
	})
}

// ControlStream writes frames on the control stream. It is safe for
// concurrent use and may be kept for as long as the connection lives; a write
// after the connection ends returns an error.
type ControlStream interface {
	// Send writes one frame on the control stream.
	Send(flag uint8, payload []byte) error
}

// ControlHandler receives the frames of one connection's control stream.
//
// HandleControl is called from the connection's read loop, once per frame, in
// arrival order and never concurrently for the same connection, so it must
// return promptly: while it runs, no other stream on that connection is read.
// It must not retain payload after returning. Returning an error is a
// protocol violation: the connection closes with status 1008 and no further
// frames are read.
type ControlHandler interface {
	HandleControl(ctx context.Context, stream ControlStream, flag uint8, payload []byte) error
}

// WithControlHandler reserves stream 0 as a control stream and sends its
// frames to handler. Without it, stream 0 is an ordinary stream ID on which a
// headers frame opens an RPC, which is the upstream behaviour.
//
// A control stream gives an extension somewhere to put its own messages
// without a flag of its own and without disturbing RPC streams. Frames on
// stream 0 are never counted against WithMaxStreams and never checked by
// WithIncreasingStreamIDs. It applies to NewHandler only.
func WithControlHandler(handler ControlHandler) Option {
	return optionFunc(func(_ *transportOptions, sopts *serverOptions) {
		if sopts != nil {
			sopts.control = handler
		}
	})
}

// WithDialOptions sets custom websocket.DialOptions. The options are used for
// every connection opened by the transport.
func WithDialOptions(dialOpts *websocket.DialOptions) Option {
	return optionFunc(func(topts *transportOptions, _ *serverOptions) {
		if topts == nil {
			return
		}
		if dialOpts == nil {
			topts.dialOptions = nil
			return
		}
		cloned := *dialOpts
		topts.dialOptions = &cloned
	})
}

// WithConnectionPerStream configures the transport to dial a dedicated
// WebSocket connection for each streaming RPC instead of multiplexing all
// RPCs onto one shared connection. A shared connection is subject to
// head-of-line blocking: one stream with a large message or a slow consumer
// delays every other stream behind it. Dedicated connections trade a
// WebSocket handshake per streaming RPC for full isolation. Unary RPCs
// always use the shared multiplexed connection.
func WithConnectionPerStream() Option {
	return optionFunc(func(topts *transportOptions, _ *serverOptions) {
		if topts != nil {
			topts.connectionPerStream = true
		}
	})
}

// WithAcceptOptions sets custom websocket.AcceptOptions.
func WithAcceptOptions(acceptOpts *websocket.AcceptOptions) Option {
	return optionFunc(func(_ *transportOptions, sopts *serverOptions) {
		if sopts != nil {
			sopts.acceptOptions = acceptOpts
		}
	})
}

type transport struct {
	url  string
	opts transportOptions

	mu     sync.Mutex
	shared *muxConn
}

// NewTransport returns a connect.Transport that dispatches RPCs over
// WebSocket. RPCs are multiplexed onto one shared WebSocket connection,
// dialed lazily on first use and re-dialed if it fails; every frame carries
// the stream ID of the RPC it belongs to. With WithConnectionPerStream, each
// streaming RPC dials a dedicated connection instead.
//
// The returned Transport also implements io.Closer: Close closes the shared
// connection, terminating any RPCs still running on it. The transport
// remains usable afterwards.
func NewTransport(url string, opts ...Option) connect.Transport {
	tOpts := transportOptions{Options: bidiprotocol.NewClientOptions()}
	for _, opt := range opts {
		opt.applyTransport(&tOpts)
	}
	tOpts.Finalize()

	return &transport{
		url:  url,
		opts: tOpts,
	}
}

func (t *transport) NewClientStream(ctx context.Context, spec connect.Spec) (connect.ClientStream, error) {
	if t.url == "" {
		return nil, errors.New("connectwebsocket: empty URL")
	}

	callInfo, _ := connect.CallInfoForClientContext(ctx)
	if callInfo != nil {
		callInfo.Protocol = "websocket"
	}

	var stream *muxStream
	if t.opts.connectionPerStream && spec.StreamType != connect.StreamTypeUnary {
		mc, err := t.dial(ctx)
		if err != nil {
			return nil, err
		}
		stream, err = mc.newStream(ctx, true)
		if err != nil {
			_ = mc.shutdown()
			return nil, err
		}
	} else {
		var err error
		stream, err = t.sharedStream(ctx)
		if err != nil {
			return nil, err
		}
	}

	return bidiprotocol.NewClientStream(
		ctx,
		spec,
		stream,
		callInfo,
		t.opts.Options,
	), nil
}

// Close closes the shared multiplexed connection, if one is open,
// terminating any RPCs still running on it. The next RPC dials a new
// connection.
func (t *transport) Close() error {
	t.mu.Lock()
	shared := t.shared
	t.shared = nil
	t.mu.Unlock()
	if shared == nil {
		return nil
	}
	return shared.shutdown()
}

func (t *transport) dial(ctx context.Context) (*muxConn, error) {
	conn, _, err := websocket.Dial(ctx, t.url, t.opts.dialOptions) //nolint:bodyclose // coder/websocket closes the handshake response body itself
	if err != nil {
		return nil, connect.Errorf(connect.CodeUnavailable, "failed to dial WebSocket: %v", err)
	}
	conn.SetReadLimit(-1)
	// The connection outlives any single RPC, so neither the read loop nor
	// writes use an RPC context; closing the connection ends them.
	mc := newMuxConn(context.Background(), conn)
	go mc.readLoopClient(context.Background()) //nolint:contextcheck,gosec // G118: the shared connection deliberately outlives the RPC that dialed it
	return mc, nil
}

// sharedStream opens a stream on the shared multiplexed connection, dialing
// it on first use and replacing it if it has failed. Dialing holds the
// transport lock, so concurrent RPCs wait for one shared connection instead
// of racing to dial several.
func (t *transport) sharedStream(ctx context.Context) (*muxStream, error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.shared != nil {
		stream, err := t.shared.newStream(ctx, false)
		if err == nil {
			return stream, nil
		}
		// The shared connection failed; dial a replacement.
	}
	mc, err := t.dial(ctx)
	if err != nil {
		return nil, err
	}
	t.shared = mc
	return mc.newStream(ctx, false)
}
