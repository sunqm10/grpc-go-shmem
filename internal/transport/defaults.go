/*
 *
 * Copyright 2018 gRPC authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package transport

import (
	"math"
	"time"
)

const (
	// The default value of flow control window size in HTTP2 spec.
	defaultWindowSize = 65535
	// The initial window size for flow control.
	initialWindowSize             = defaultWindowSize // for an RPC
	infinity                      = time.Duration(math.MaxInt64)
	defaultClientKeepaliveTime    = infinity
	defaultClientKeepaliveTimeout = 20 * time.Second
	defaultMaxStreamsClient       = 100
	defaultMaxConnectionIdle      = infinity
	defaultMaxConnectionAge       = infinity
	defaultMaxConnectionAgeGrace  = infinity
	defaultServerKeepaliveTime    = 2 * time.Hour
	defaultServerKeepaliveTimeout = 20 * time.Second
	defaultKeepalivePolicyMinTime = 5 * time.Minute
	// max window limit set by HTTP2 Specs.
	maxWindowSize = math.MaxInt32
	// defaultWriteQuota is the default value for number of data
	// bytes that each stream can schedule before some of it being
	// flushed out.
	defaultWriteQuota              = 64 * 1024
	defaultClientMaxHeaderListSize = uint32(16 << 20)
	defaultServerMaxHeaderListSize = uint32(16 << 20)
	upcomingDefaultHeaderListSize  = uint32(8 << 10)

	// shmClientMaxMessageBurst caps the number of MESSAGE frames the client
	// reader will deliver to recvBuffer between cooperative yields. The
	// reader normally yields after every frame to let the app goroutine
	// pick up the recv on the same M (avoids cross-M wakep + futex), but
	// at high stream concurrency that yield costs N park/unparks per RPC
	// round. The burst cap lets the reader drain a sub-batch from the
	// ring before yielding, then yield so the woken app goroutines get
	// scheduled. 32 is empirically large enough that one frame per
	// stream at N <= 32 never hits the cap, and small enough that
	// medium-payload streaming at N=100 with multi-DATA-frame messages
	// does not starve app receivers.
	shmClientMaxMessageBurst = 32
	// shmServerMaxMessageBurst — same as the client cap, see comment above.
	shmServerMaxMessageBurst = 32
)

// MaxStreamID is the upper bound for the stream ID before the current
// transport gracefully closes and new transport is created for subsequent RPCs.
// This is set to 75% of 2^31-1. Streams are identified with an unsigned 31-bit
// integer. It's exported so that tests can override it.
var MaxStreamID = uint32(math.MaxInt32 * 3 / 4)
