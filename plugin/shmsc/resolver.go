//go:build linux || windows

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

package shmsc

import (
	"fmt"

	"google.golang.org/grpc/plugin/shmsc/internal/engine"
	"google.golang.org/grpc/resolver"
)

type resolverBuilder struct{}

func (resolverBuilder) Build(target resolver.Target, cc resolver.ClientConn, _ resolver.BuildOptions) (resolver.Resolver, error) {
	if target.URL.Scheme != Name {
		return nil, fmt.Errorf("shmsc: resolver received target with scheme %q, want %q", target.URL.Scheme, Name)
	}
	if target.URL.OmitHost {
		return nil, fmt.Errorf("shmsc: non-canonical target form is not supported; use %s:///<segment>", Name)
	}
	if target.URL.User != nil {
		return nil, fmt.Errorf("shmsc: target userinfo is not supported; use %s:///<segment>", Name)
	}
	if target.URL.Host != "" {
		return nil, fmt.Errorf("shmsc: target authority %q is not supported; use %s:///<segment>", target.URL.Host, Name)
	}
	if target.URL.Opaque != "" {
		return nil, fmt.Errorf("shmsc: opaque target %q is not supported; use %s:///<segment>", target.URL.Opaque, Name)
	}
	if target.URL.RawPath != "" || target.URL.EscapedPath() != target.URL.Path {
		return nil, fmt.Errorf("shmsc: percent-encoded segment names are not supported")
	}
	if target.URL.ForceQuery || target.URL.RawQuery != "" {
		return nil, fmt.Errorf("shmsc: target query parameters are not supported")
	}
	// url.Parse discards a trailing empty fragment delimiter, so Build cannot
	// distinguish "shmsc:///segment#" from "shmsc:///segment". Non-empty
	// fragments remain observable and are rejected.
	if target.URL.Fragment != "" || target.URL.RawFragment != "" {
		return nil, fmt.Errorf("shmsc: target fragments are not supported")
	}

	segmentName := target.Endpoint()
	if err := engine.ValidateSegmentName(segmentName); err != nil {
		return nil, fmt.Errorf("shmsc: invalid target %q: %w", target.String(), err)
	}

	addr := resolver.Address{Addr: segmentName, TransportType: Name}
	if err := cc.UpdateState(resolver.State{Addresses: []resolver.Address{addr}}); err != nil {
		return nil, err
	}
	return nopResolver{}, nil
}

func (resolverBuilder) Scheme() string { return Name }

func (resolverBuilder) OverrideAuthority(resolver.Target) string { return "localhost" }

type nopResolver struct{}

func (nopResolver) ResolveNow(resolver.ResolveNowOptions) {}

func (nopResolver) Close() {}

func init() {
	resolver.Register(resolverBuilder{})
}
