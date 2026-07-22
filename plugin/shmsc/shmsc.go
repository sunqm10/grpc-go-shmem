/*
 *
 * Copyright 2026 gRPC authors.
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

// Package shmsc is a SELF-CONTAINED shared-memory (SHM) gRPC transport plugin.
//
// Unlike the in-tree POC bridge (google.golang.org/grpc/plugin/shm), this module
// owns its ENTIRE SHM engine and depends on grpc-go ONLY through the exported,
// experimental pluggable-transport API (google.golang.org/grpc/experimental/
// transport/{client,server}) plus public packages — it imports NO
// google.golang.org/grpc/internal/* package. That independence is enforced by
// the no-internal-import guard test in this package and lets the plugin
// eventually live in its own repository and be upstreamed.
//
// It coexists with the in-tree full-featured SHM transport (which is untouched):
// this plugin registers under the distinct transport type Name ("shmsc").
//
// # Platform scope
//
// Like shared memory itself, this transport targets Linux and Windows only
// (same-host IPC). The engine's ring/segment/wake primitives are built only for
// those OSes; the module is not expected to build for other platforms (e.g.
// darwin), matching the in-tree SHM transport.
//
// STATUS: work in progress. The module boundary + experimental-API wiring are
// established; the SHM engine is being ported in from the monolith with its own
// (non-shared) stream types. Not yet functional end to end.
package shmsc

import (
	transportclient "google.golang.org/grpc/experimental/transport/client"
	transportserver "google.golang.org/grpc/experimental/transport/server"
)

// Name is the resolver.Address.TransportType (and server-side accepted-conn
// transport type) under which this self-contained SHM transport registers. It is
// deliberately distinct from the in-tree "shm" bridge so both can coexist during
// development.
const Name = "shmsc"

// Compile-time proof that the exported, experimental D1 transport API is
// reachable from this standalone module WITHOUT any internal/* import. The real
// client + server builders are registered in a later step.
var (
	_ = transportclient.Get
	_ = transportserver.Get
)
