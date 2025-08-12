package metrics

import (
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
)

// Metrics contains the registered metrics for the labeler.
// Please remember to add them to `New` when you add new fields, otherwise you will cause nil panics.
type Metrics struct {
	iterations      prometheus.CounterVec   // Iterations, per entry
	iterationTime   prometheus.HistogramVec // Time each iteration takes, per entry
	iterationPeriod prometheus.GaugeVec     // How often the watcher of a each node_label will tick
	labelOperations prometheus.CounterVec   // Labels added/removed, per entry and maybe op (add/remove)
	labeledNodes    prometheus.GaugeVec     // % of nodes labeled
}

// We use functions rather than accessing the fields so when we change labels, we will break compilation.

func (m *Metrics) MarkIteration(nodeLabel string) {
	m.iterations.WithLabelValues(nodeLabel).Inc()
}

func (m *Metrics) MarkWatcherTickerInterval(nodeLabel string, interval time.Duration) {
	m.iterationPeriod.WithLabelValues(nodeLabel).Set(float64(interval.Seconds()))
}

func (m *Metrics) MarkIterationComplete(nodeLabel string, duration time.Duration) {
	m.iterationTime.WithLabelValues(nodeLabel).Observe(float64(duration.Seconds()))
}

func (m *Metrics) MarkPodsLabeledRatio(nodeLabel string, labeled, total int) {
	ratio := float64(labeled) / float64(total)
	m.labeledNodes.WithLabelValues(nodeLabel).Set(ratio)
}

func (m *Metrics) MarkLabelOperation(nodeLabel string) {
	m.labelOperations.WithLabelValues(nodeLabel).Inc()
}

const (
	ns             = "kube_node_labeler"
	nodeLabelLabel = "node_label"
)

// NewRegistry creates a registry with some default collectors registered. This is intended for use in production.
func NewRegistry() *prometheus.Registry {
	reg := prometheus.NewRegistry()
	reg.MustRegister(collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}))
	reg.MustRegister(collectors.NewGoCollector())
	reg.MustRegister(collectors.NewBuildInfoCollector())

	return reg
}

// New creates a new Metrics instance and registers the metrics to the given registry.
//
// This will panic if called twice on the same registry!
func New(reg *prometheus.Registry) *Metrics {
	m := &Metrics{
		iterations: *prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: ns,
			Name:      "iterations_total",
		}, []string{nodeLabelLabel}),
		iterationTime: *prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Namespace: ns,
			Name:      "iteration_time_seconds",
		}, []string{nodeLabelLabel}),
		iterationPeriod: *prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: ns,
			Name:      "iteration_period_seconds",
		}, []string{nodeLabelLabel}),
		labelOperations: *prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: ns,
			Name:      "label_operations_total",
		}, []string{nodeLabelLabel}),
		labeledNodes: *prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: ns,
			Name:      "labeled_nodes_fraction",
		}, []string{nodeLabelLabel}),
	}

	reg.MustRegister(m.iterations)
	reg.MustRegister(m.iterationTime)
	reg.MustRegister(m.iterationPeriod)
	reg.MustRegister(m.labelOperations)
	reg.MustRegister(m.labeledNodes)

	return m
}
