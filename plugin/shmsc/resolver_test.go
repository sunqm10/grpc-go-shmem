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
	"errors"
	"net/url"
	"strings"
	"testing"

	"google.golang.org/grpc/resolver"
)

type resolverTestClientConn struct {
	resolver.ClientConn
	state resolver.State
	err   error
}

func (c *resolverTestClientConn) UpdateState(state resolver.State) error {
	c.state = state
	return c.err
}

func resolverTestTarget(t *testing.T, target string) resolver.Target {
	t.Helper()
	u, err := url.Parse(target)
	if err != nil {
		t.Fatalf("url.Parse(%q) failed: %v", target, err)
	}
	return resolver.Target{URL: *u}
}

func TestResolverRegistration(t *testing.T) {
	builder := resolver.Get(Name)
	if builder == nil {
		t.Fatalf("resolver.Get(%q) = nil; want registered shmsc resolver", Name)
	}
	if got := builder.Scheme(); got != Name {
		t.Fatalf("builder.Scheme() = %q; want %q", got, Name)
	}
	overrider, ok := builder.(resolver.AuthorityOverrider)
	if !ok {
		t.Fatalf("registered builder %T does not implement resolver.AuthorityOverrider", builder)
	}
	if got := overrider.OverrideAuthority(resolverTestTarget(t, "shmsc:///segment")); got != "localhost" {
		t.Fatalf("OverrideAuthority() = %q; want localhost", got)
	}
}

func TestResolverBuild(t *testing.T) {
	name200 := strings.Repeat("a", 200)
	tests := []struct {
		name   string
		target string
		want   string
	}{
		{name: "canonical", target: "shmsc:///segment", want: "segment"},
		{name: "empty fragment is parse equivalent", target: "shmsc:///segment#", want: "segment"},
		{name: "allowed characters", target: "shmsc:///A-z_09.v1", want: "A-z_09.v1"},
		{name: "one byte", target: "shmsc:///a", want: "a"},
		{name: "maximum length", target: "shmsc:///" + name200, want: name200},
	}
	builder := resolverBuilder{}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			cc := &resolverTestClientConn{}
			r, err := builder.Build(resolverTestTarget(t, test.target), cc, resolver.BuildOptions{})
			if err != nil {
				t.Fatalf("Build(%q) failed: %v", test.target, err)
			}
			defer r.Close()
			if got := len(cc.state.Addresses); got != 1 {
				t.Fatalf("resolved address count = %d; want 1", got)
			}
			got := cc.state.Addresses[0]
			if got.Addr != test.want {
				t.Errorf("resolved Addr = %q; want %q", got.Addr, test.want)
			}
			if got.TransportType != Name {
				t.Errorf("resolved TransportType = %q; want %q", got.TransportType, Name)
			}
			if got.ServerName != "" {
				t.Errorf("resolved ServerName = %q; want empty", got.ServerName)
			}
			if got.Attributes != nil || got.BalancerAttributes != nil {
				t.Errorf("resolved address has unexpected attributes: %+v", got)
			}
		})
	}
}

func TestResolverRejectsInvalidTarget(t *testing.T) {
	name201 := strings.Repeat("a", 201)
	tests := []struct {
		name   string
		target string
		want   string
	}{
		{name: "wrong scheme", target: "other:///segment", want: "scheme"},
		{name: "single slash", target: "shmsc:/segment", want: "non-canonical"},
		{name: "host form", target: "shmsc://segment", want: "authority"},
		{name: "host and path", target: "shmsc://host/segment", want: "authority"},
		{name: "userinfo", target: "shmsc://user@host/segment", want: "userinfo"},
		{name: "opaque", target: "shmsc:segment", want: "opaque"},
		{name: "query", target: "shmsc:///segment?x=y", want: "query"},
		{name: "empty query", target: "shmsc:///segment?", want: "query"},
		{name: "fragment", target: "shmsc:///segment#fragment", want: "fragment"},
		{name: "empty", target: "shmsc:///", want: "must not be empty"},
		{name: "path separator", target: "shmsc:///a/b", want: "invalid character"},
		{name: "encoded alias", target: "shmsc:///%73egment", want: "percent-encoded"},
		{name: "encoded path separator", target: "shmsc:///a%2Fb", want: "percent-encoded"},
		{name: "encoded nul", target: "shmsc:///a%00b", want: "percent-encoded"},
		{name: "encoded traversal", target: "shmsc:///a%2E%2Eb", want: "percent-encoded"},
		{name: "unicode", target: "shmsc:///%E6%AE%B5", want: "percent-encoded"},
		{name: "port-like name", target: "shmsc:///segment:80", want: "invalid character"},
		{name: "too long", target: "shmsc:///" + name201, want: "too long"},
		{name: "reserved control suffix", target: "shmsc:///segment_ctl", want: "reserved suffix"},
		{name: "reserved lock suffix", target: "shmsc:///segment.lock", want: "reserved suffix"},
		{name: "reserved socket suffix", target: "shmsc:///segment.fds.sock", want: "reserved suffix"},
	}
	builder := resolverBuilder{}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			cc := &resolverTestClientConn{}
			r, err := builder.Build(resolverTestTarget(t, test.target), cc, resolver.BuildOptions{})
			if r != nil {
				r.Close()
				t.Fatalf("Build(%q) returned resolver %T; want nil", test.target, r)
			}
			if err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("Build(%q) error = %v; want error containing %q", test.target, err, test.want)
			}
			if len(cc.state.Addresses) != 0 {
				t.Fatalf("Build(%q) published addresses after rejecting target: %+v", test.target, cc.state.Addresses)
			}
		})
	}
}

func TestResolverBuildReturnsUpdateStateError(t *testing.T) {
	wantErr := errors.New("update rejected")
	cc := &resolverTestClientConn{err: wantErr}
	r, err := (resolverBuilder{}).Build(resolverTestTarget(t, "shmsc:///segment"), cc, resolver.BuildOptions{})
	if r != nil {
		t.Fatalf("Build() returned resolver %T; want nil", r)
	}
	if !errors.Is(err, wantErr) {
		t.Fatalf("Build() error = %v; want %v", err, wantErr)
	}
}
