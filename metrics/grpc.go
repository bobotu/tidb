// Copyright 2019 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package metrics

import (
	"context"
	"math"
	_ "unsafe"

	"github.com/prometheus/client_golang/prometheus"
	channelzgrpc "google.golang.org/grpc/channelz/grpc_channelz_v1"
)

type grpcCollector struct {
	cz channelzgrpc.ChannelzServer

	callsDesc      *prometheus.Desc
	channelCntDesc *prometheus.Desc
	stateDesc      *prometheus.Desc
}

func newGRPCCollector() *grpcCollector {
	return &grpcCollector{
		cz: newChannelz(),
		callsDesc: prometheus.NewDesc(
			"tidb_grpc_channel_calls",
			"gRPC channel connection state",
			[]string{"target", "state"}, nil),
		channelCntDesc: prometheus.NewDesc(
			"tidb_grpc_channel_count",
			"gRPC channel count by connection states",
			[]string{"state"}, nil),
		stateDesc: prometheus.NewDesc(
			"tidb_grpc_channel_state",
			"gRPC connection state of channel",
			[]string{"target", "state"}, nil),
	}
}

func (g *grpcCollector) Describe(descs chan<- *prometheus.Desc) {
	descs <- g.callsDesc
	descs <- g.channelCntDesc
	descs <- g.stateDesc
}

func (g *grpcCollector) Collect(metrics chan<- prometheus.Metric) {
	var start int64

	channelStates := make([]int, len(channelzgrpc.ChannelConnectivityState_State_name))

	for {
		resp, err := g.cz.GetTopChannels(context.Background(), &channelzgrpc.GetTopChannelsRequest{
			StartChannelId: start,
			MaxResults:     math.MaxInt64,
		})
		if err != nil {
			return
		}

		for _, ch := range resp.Channel {
			start = ch.Ref.ChannelId

			channelStates[ch.Data.State.State]++
			metrics <- prometheus.MustNewConstMetric(g.stateDesc, prometheus.GaugeValue,
				1, ch.Data.Target, ch.Data.State.State.String())

			metrics <- prometheus.MustNewConstMetric(g.callsDesc, prometheus.GaugeValue,
				float64(ch.Data.CallsSucceeded), ch.Data.Target, "succeeded")
			metrics <- prometheus.MustNewConstMetric(g.callsDesc, prometheus.GaugeValue,
				float64(ch.Data.CallsFailed), ch.Data.Target, "failed")
		}

		if resp.End {
			break
		}
	}

	for st, cnt := range channelStates {
		metrics <- prometheus.MustNewConstMetric(g.channelCntDesc, prometheus.GaugeValue,
			float64(cnt), channelzgrpc.ChannelConnectivityState_State_name[int32(st)])
	}
}

//go:linkname newChannelz google.golang.org/grpc/channelz/service.newCZServer
func newChannelz() channelzgrpc.ChannelzServer
