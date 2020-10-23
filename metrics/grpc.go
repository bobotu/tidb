// Copyright 2020 PingCAP, Inc.
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
	"regexp"

	"github.com/prometheus/client_golang/prometheus"
	"google.golang.org/grpc/grpclog"
)

var (
	gRPCConnectivityChangedCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "tidb",
			Subsystem: "grpc",
			Name:      "connectivity_changed_num",
			Help:      "Number of gRPC channel connectivity changed events",
		}, []string{"state"})
)

type grpcLogInspector struct{}

func SetupGRPCInspector() {
	grpclog.SetLoggerV2(grpcLogInspector{})
}

func (l grpcLogInspector) Info(args ...interface{}) {
	for _, p := range infoLogPatterns {
		if p.Process(args...) {
			return
		}
	}
}

func (l grpcLogInspector) Infoln(args ...interface{}) {
	for _, p := range infoLogPatterns {
		if p.Process(args...) {
			return
		}
	}
}

func (l grpcLogInspector) Infof(format string, args ...interface{}) {}

func (l grpcLogInspector) Warning(args ...interface{}) {}

func (l grpcLogInspector) Warningln(args ...interface{}) {}

func (l grpcLogInspector) Warningf(format string, args ...interface{}) {}

func (l grpcLogInspector) Error(args ...interface{}) {}

func (l grpcLogInspector) Errorln(args ...interface{}) {}

func (l grpcLogInspector) Errorf(format string, args ...interface{}) {}

func (l grpcLogInspector) Fatal(args ...interface{}) {}

func (l grpcLogInspector) Fatalln(args ...interface{}) {}

func (l grpcLogInspector) Fatalf(format string, args ...interface{}) {}

func (l grpcLogInspector) V(v int) bool { return false }

type logPattern struct {
	Process    func(args ...interface{}) bool
	FmtProcess func(format string, args ...interface{}) bool
}

var (
	// Connectivity change log is INFO level, and will be logged through grpclog.Info.
	connectivityChangeRegexp = regexp.MustCompile(`Channel Connectivity change to (\S+)`)
)

var infoLogPatterns = []logPattern{
	{
		Process: func(args ...interface{}) bool {
			if len(args) != 1 {
				return false
			}
			str, ok := args[0].(string)
			if !ok {
				return false
			}
			matches := connectivityChangeRegexp.FindStringSubmatch(str)
			if len(matches) == 0 {
				return false
			}
			gRPCConnectivityChangedCounter.WithLabelValues(matches[1]).Inc()
			return true
		},
	},
}
