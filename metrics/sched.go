package metrics

import (
	"runtime"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

var (
	SchedulerLoadGauge = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Namespace: "tidb",
			Subsystem: "runtime",
			Name:      "scheduler_load",
			Help:      "Load",
		})
)

func init() {
	var load float64 = 0
	const exp = 0.9966722160545234
	go func() {
		tk := time.NewTicker(50 * time.Millisecond)
		for {
			<-tk.C
			var stats runtime.SchedStats
			runtime.ReadSchedStats(&stats, runtime.SchedStatsStates)
			load *= exp
			load += float64(stats.States.Runnable + stats.States.Running) * (1 - exp)
			SchedulerLoadGauge.Set(load)
		}
	}()
}
