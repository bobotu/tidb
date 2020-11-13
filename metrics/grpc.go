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
	"time"
	_ "unsafe"

	"github.com/pingcap/tidb/util/logutil"
	"go.uber.org/zap"
	channelzgrpc "google.golang.org/grpc/channelz/grpc_channelz_v1"
)

func init() {
	cz := newChannelz()
	go func() {
		for {
			time.Sleep(100 * time.Millisecond)
			resp, err := cz.GetTopChannels(context.Background(), &channelzgrpc.GetTopChannelsRequest{})
			if err != nil {
				logutil.BgLogger().Error("???", zap.Error(err))
				continue
			}
			logutil.BgLogger().Info("top channels", zap.Stringer("resp", resp))
		}
	}()
}

//go:linkname newChannelz google.golang.org/grpc/channelz/service.newCZServer
func newChannelz() channelzgrpc.ChannelzServer
