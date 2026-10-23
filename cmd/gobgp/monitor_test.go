// Copyright (C) 2026 Nippon Telegraph and Telephone Corporation.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package main

import (
	"context"
	"io"
	"net/netip"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"

	"github.com/osrg/gobgp/v4/api"
	"github.com/osrg/gobgp/v4/pkg/apiutil"
	"github.com/osrg/gobgp/v4/pkg/packet/bgp"
)

// sliceStream replays the given responses and then reports io.EOF.
type sliceStream[T any] struct {
	grpc.ClientStream
	items []*T
}

func (s *sliceStream[T]) Recv() (*T, error) {
	if len(s.items) == 0 {
		return nil, io.EOF
	}
	item := s.items[0]
	s.items = s.items[1:]
	return item, nil
}

type testMonitorClient struct {
	api.GoBgpServiceClient
	peers        []*api.Peer
	listPeer     *api.ListPeerRequest
	watchEvent   *api.WatchEventRequest
	watchReplies []*api.WatchEventResponse
}

func (c *testMonitorClient) ListPeer(_ context.Context, request *api.ListPeerRequest, _ ...grpc.CallOption) (grpc.ServerStreamingClient[api.ListPeerResponse], error) {
	c.listPeer = request
	replies := make([]*api.ListPeerResponse, 0, len(c.peers))
	for _, p := range c.peers {
		if request.Address == "" || p.Conf.NeighborAddress == request.Address || p.Conf.NeighborInterface == request.Address {
			replies = append(replies, &api.ListPeerResponse{Peer: p})
		}
	}
	return &sliceStream[api.ListPeerResponse]{items: replies}, nil
}

func (c *testMonitorClient) WatchEvent(_ context.Context, request *api.WatchEventRequest, _ ...grpc.CallOption) (grpc.ServerStreamingClient[api.WatchEventResponse], error) {
	c.watchEvent = request
	return &sliceStream[api.WatchEventResponse]{items: c.watchReplies}, nil
}

func newTestMonitorClient(t *testing.T, peers ...*api.Peer) *testMonitorClient {
	t.Helper()
	previousClient := client
	previousContext := ctx
	fake := &testMonitorClient{peers: peers}
	client = fake
	ctx = context.Background()
	t.Cleanup(func() {
		client = previousClient
		ctx = previousContext
	})
	return fake
}

func TestMonitorAdjInRequest(t *testing.T) {
	numbered := &api.Peer{
		Conf:  &api.PeerConf{NeighborAddress: "10.0.0.2"},
		State: &api.PeerState{NeighborAddress: "10.0.0.2"},
	}
	unnumbered := &api.Peer{
		Conf:  &api.PeerConf{NeighborInterface: "eth0"},
		State: &api.PeerState{NeighborAddress: "fe80::1"},
	}

	tests := []struct {
		name        string
		args        []string
		peerAddress string
		init        bool
		batchSize   uint32
	}{
		{
			name: "all neighbors",
			args: []string{"adj-in"},
		},
		{
			name:        "neighbor by address",
			args:        []string{"adj-in", "10.0.0.2"},
			peerAddress: "10.0.0.2",
		},
		{
			name:        "neighbor by interface resolves to the session address",
			args:        []string{"adj-in", "eth0"},
			peerAddress: "fe80::1",
		},
		{
			name:        "neighbor by address that is not configured yet",
			args:        []string{"adj-in", "10.0.0.9"},
			peerAddress: "10.0.0.9",
		},
		{
			name:        "current and batch size are passed through",
			args:        []string{"adj-in", "10.0.0.2", "--current", "--batch-size", "7"},
			peerAddress: "10.0.0.2",
			init:        true,
			batchSize:   7,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fake := newTestMonitorClient(t, numbered, unnumbered)
			command := newMonitorCmd()
			command.SetArgs(tt.args)
			require.NoError(t, command.Execute())

			require.NotNil(t, fake.watchEvent)
			require.Len(t, fake.watchEvent.GetTable().GetFilters(), 1)
			filter := fake.watchEvent.GetTable().GetFilters()[0]
			assert.Equal(t, api.WatchEventRequest_Table_Filter_TYPE_ADJIN, filter.Type)
			assert.Equal(t, tt.peerAddress, filter.PeerAddress)
			assert.Equal(t, tt.init, filter.Init)
			assert.Equal(t, tt.batchSize, fake.watchEvent.BatchSize)
		})
	}
}

// captureStdout runs fn and returns what it printed to os.Stdout.
func captureStdout(t *testing.T, fn func()) string {
	t.Helper()
	r, w, err := os.Pipe()
	require.NoError(t, err)
	previous := os.Stdout
	os.Stdout = w
	// Also runs when fn fails the test, so the reader does not hang.
	defer func() {
		os.Stdout = previous
		w.Close()
	}()

	done := make(chan string, 1)
	go func() {
		b, _ := io.ReadAll(r)
		r.Close()
		done <- string(b)
	}()
	fn()
	require.NoError(t, w.Close())
	return <-done
}

func TestMonitorPrintsPathIdentifierForAdjIn(t *testing.T) {
	nlri, err := bgp.NewIPAddrPrefix(netip.MustParsePrefix("10.2.0.0/24"))
	require.NoError(t, err)
	nexthop, err := bgp.NewPathAttributeNextHop(netip.MustParseAddr("192.0.2.1"))
	require.NoError(t, err)
	path, err := apiutil.NewPath(bgp.RF_IPv4_UC, nlri, false, []bgp.PathAttributeInterface{
		bgp.NewPathAttributeOrigin(bgp.BGP_ORIGIN_ATTR_TYPE_IGP),
		nexthop,
	}, time.Now())
	require.NoError(t, err)
	path.Identifier = 2
	event := &api.WatchEventResponse{
		Event: &api.WatchEventResponse_Table{
			Table: &api.WatchEventResponse_TableEvent{Paths: []*api.Path{path}},
		},
	}

	tests := []struct {
		name string
		args []string
		want string
	}{
		{name: "adj-in shows the identifier", args: []string{"adj-in"}, want: " [ROUTE] 2:10.2.0.0/24 via 192.0.2.1 "},
		{name: "global rib does not", args: []string{"global", "rib"}, want: " [ROUTE] 10.2.0.0/24 via 192.0.2.1 "},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fake := newTestMonitorClient(t)
			fake.watchReplies = []*api.WatchEventResponse{event}
			command := newMonitorCmd()
			command.SetArgs(tt.args)
			out := captureStdout(t, func() {
				require.NoError(t, command.Execute())
			})
			assert.Contains(t, out, tt.want)
		})
	}
}

func TestMonitorNeighbor(t *testing.T) {
	numbered := &api.Peer{
		Conf:  &api.PeerConf{NeighborAddress: "10.0.0.2"},
		State: &api.PeerState{NeighborAddress: "10.0.0.2", SessionState: api.PeerState_SESSION_STATE_ESTABLISHED},
	}
	unnumbered := &api.Peer{
		Conf:  &api.PeerConf{NeighborInterface: "eth0"},
		State: &api.PeerState{NeighborAddress: "fe80::1", SessionState: api.PeerState_SESSION_STATE_ESTABLISHED},
	}
	numberedDown := &api.Peer{
		Conf:  numbered.Conf,
		State: &api.PeerState{NeighborAddress: "10.0.0.2", SessionState: api.PeerState_SESSION_STATE_IDLE},
	}
	peerEvent := func(typ api.WatchEventResponse_PeerEvent_Type, p *api.Peer) *api.WatchEventResponse {
		return &api.WatchEventResponse{
			Event: &api.WatchEventResponse_Peer{
				Peer: &api.WatchEventResponse_PeerEvent{Type: typ, Peer: p},
			},
		}
	}
	// What the server sends: a snapshot of every neighbor, the end marker
	// without a peer, then live state changes.
	replies := []*api.WatchEventResponse{
		peerEvent(api.WatchEventResponse_PeerEvent_TYPE_INIT, numbered),
		peerEvent(api.WatchEventResponse_PeerEvent_TYPE_INIT, unnumbered),
		peerEvent(api.WatchEventResponse_PeerEvent_TYPE_END_OF_INIT, nil),
		peerEvent(api.WatchEventResponse_PeerEvent_TYPE_STATE, numberedDown),
		peerEvent(api.WatchEventResponse_PeerEvent_TYPE_STATE, unnumbered),
	}

	tests := []struct {
		name string
		args []string
		want []string
	}{
		{
			name: "all neighbors, live events only",
			args: []string{"neighbor"},
			want: []string{
				"[NEIGH] 10.0.0.2 fsm: SESSION_STATE_IDLE",
				"[NEIGH] fe80::1(eth0) fsm: SESSION_STATE_ESTABLISHED",
			},
		},
		{
			name: "all neighbors with the current snapshot",
			args: []string{"neighbor", "--current"},
			want: []string{
				"[NEIGH] 10.0.0.2 fsm: SESSION_STATE_ESTABLISHED",
				"[NEIGH] fe80::1(eth0) fsm: SESSION_STATE_ESTABLISHED",
				"[NEIGH] 10.0.0.2 fsm: SESSION_STATE_IDLE",
				"[NEIGH] fe80::1(eth0) fsm: SESSION_STATE_ESTABLISHED",
			},
		},
		{
			name: "one neighbor by address",
			args: []string{"neighbor", "10.0.0.2"},
			want: []string{"[NEIGH] 10.0.0.2 fsm: SESSION_STATE_IDLE"},
		},
		{
			name: "one neighbor by interface with the current snapshot",
			args: []string{"neighbor", "eth0", "--current"},
			want: []string{
				"[NEIGH] fe80::1(eth0) fsm: SESSION_STATE_ESTABLISHED",
				"[NEIGH] fe80::1(eth0) fsm: SESSION_STATE_ESTABLISHED",
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fake := newTestMonitorClient(t, numbered, unnumbered)
			fake.watchReplies = replies
			command := newMonitorCmd()
			command.SetArgs(tt.args)
			out := captureStdout(t, func() {
				require.NoError(t, command.Execute())
			})
			lines := strings.Split(strings.TrimSpace(out), "\n")
			require.Len(t, lines, len(tt.want), out)
			for i, want := range tt.want {
				assert.Contains(t, lines[i], want)
			}
		})
	}
}

func TestMonitorRejectsExtraArgs(t *testing.T) {
	tests := []struct {
		name string
		args []string
	}{
		{name: "adj-in takes one address", args: []string{"adj-in", "10.0.0.2", "10.0.0.3"}},
		{name: "global rib takes none", args: []string{"global", "rib", "10.0.0.2"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fake := newTestMonitorClient(t)
			command := newMonitorCmd()
			command.SetArgs(tt.args)
			command.SetOut(io.Discard)
			command.SetErr(io.Discard)
			require.Error(t, command.Execute())
			assert.Nil(t, fake.watchEvent)
		})
	}
}
