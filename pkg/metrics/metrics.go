package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
)

// Metrics contains the registered metrics for the labeler.
// Please remember to add them to `New` when you add new fields, otherwise you will cause nil panics.
type Metrics struct {
	Iterations      prometheus.CounterVec   // Iterations, per entry
	IterationTime   prometheus.HistogramVec // Time each iteration takes, per entry
	IterationPeriod prometheus.GaugeVec     // How often the watcher of a each node_label will tick
	LabelOperations prometheus.CounterVec   // Labels added/removed, per entry and maybe op (add/remove)
	LabeledNodes    prometheus.GaugeVec     // % of nodes labeled
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
		Iterations: *prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: ns,
			Name:      "iterations_total",
		}, []string{nodeLabelLabel}),
		IterationTime: *prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Namespace: ns,
			Name:      "iteration_time_seconds",
		}, []string{nodeLabelLabel}),
		IterationPeriod: *prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: ns,
			Name:      "iteration_period_seconds",
		}, []string{nodeLabelLabel}),
		LabelOperations: *prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: ns,
			Name:      "label_operations_total",
		}, []string{nodeLabelLabel}),
		LabeledNodes: *prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: ns,
			Name:      "labeled_nodes_fraction",
		}, []string{nodeLabelLabel}),
	}

	reg.MustRegister(m.Iterations)
	reg.MustRegister(m.IterationTime)
	reg.MustRegister(m.IterationPeriod)
	reg.MustRegister(m.LabelOperations)
	reg.MustRegister(m.LabeledNodes)

	return m
}
