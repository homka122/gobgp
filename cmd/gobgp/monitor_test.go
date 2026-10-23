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
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"

	"github.com/osrg/gobgp/v4/api"
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
