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

package connectwebsocket

import (
	"context"
	"testing"
	"time"
)

// The common path (inbox has room) must not allocate a timer.
func TestDeliverWithinFastPathDoesNotAllocate(t *testing.T) {
	s := newMuxStream(context.Background(), nil, 1)
	payload := []byte{1}
	allocs := testing.AllocsPerRun(1000, func() {
		if !s.deliverWithin(0, payload, time.Second) {
			t.Fatal("delivery with room in the inbox timed out")
		}
		<-s.inbox
	})
	if allocs != 0 {
		t.Fatalf("deliverWithin allocated %v times per call, want 0", allocs)
	}
}

func TestDeliverWithinTimesOutWhenFull(t *testing.T) {
	s := newMuxStream(context.Background(), nil, 1)
	for range cap(s.inbox) {
		s.deliver(0, nil)
	}
	start := time.Now()
	if s.deliverWithin(0, nil, 20*time.Millisecond) {
		t.Fatal("delivery to a full inbox reported success")
	}
	if elapsed := time.Since(start); elapsed < 20*time.Millisecond {
		t.Fatalf("gave up after %v, before the 20ms bound", elapsed)
	}
}

func TestDeliverWithinDropsForTerminatedStream(t *testing.T) {
	s := newMuxStream(context.Background(), nil, 1)
	for range cap(s.inbox) {
		s.deliver(0, nil)
	}
	s.terminate(errStreamClosed)
	if !s.deliverWithin(0, nil, time.Hour) {
		t.Fatal("frame for a terminated stream not dropped")
	}
}
