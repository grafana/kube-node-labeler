package watcher

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/grafana/kube-node-labeler/pkg/config"
	"github.com/grafana/kube-node-labeler/pkg/metrics"
	"golang.org/x/sync/errgroup"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes"
)

func StartForEntries(
	ctx context.Context,
	parentLogger *slog.Logger,
	metrics *metrics.Metrics,
	kubeClient kubernetes.Interface,
	entries []*config.Entry,
) error {
	eg, ctx := errgroup.WithContext(ctx)
	for _, ce := range entries {
		watcher, err := NewNamespacedWatcher(parentLogger, metrics, kubeClient, ce)
		if err != nil {
			return fmt.Errorf("creating watcher for %q: %w", ce.NodeLabel, err)
		}

		eg.Go(func() error {
			return watcher.StartInformerBlocking(ctx)
		})
	}

	return eg.Wait()
}

type NamespaceWatcher struct {
	log     *slog.Logger
	metrics *metrics.Metrics

	kubeClient kubernetes.Interface
	sif        informers.SharedInformerFactory
	entry      *config.Entry
}

func NewNamespacedWatcher(
	parentLogger *slog.Logger,
	metrics *metrics.Metrics,
	kubeClient kubernetes.Interface,
	entry *config.Entry,
) (*NamespaceWatcher, error) {
	if parentLogger == nil {
		parentLogger = slog.Default()
	}
	if kubeClient == nil {
		return nil, errors.New("no kubeclient was given")
	}
	if entry == nil {
		return nil, errors.New("no config entry was given")
	}

	return &NamespaceWatcher{
		log: parentLogger.With(
			"node_label", entry.NodeLabel,
			"pod_label_selector", entry.LabelSelector.String(),
			"namespace", entry.Namespace),
		metrics:    metrics,
		kubeClient: kubeClient,
		sif:        informers.NewSharedInformerFactoryWithOptions(kubeClient, entry.ResyncPeriod, informers.WithNamespace(entry.Namespace)),
		entry:      entry,
	}, nil
}

// StartInformerBlocking will create a resource informer and wait for something to happen.
// When a tick occurs, we will do some work by calling Tick, then wait once more.
//
// This function can only be called ONCE over the lifetime of the watcher. Once it returns, the watcher is consumed and must be recreated.
func (w *NamespaceWatcher) StartInformerBlocking(ctx context.Context) error {
	resyncPeriod := w.entry.ResyncPeriod
	if resyncPeriod == 0 {
		resyncPeriod = 5 * time.Minute
		w.log.Info("Using default resync period", "resync_period", resyncPeriod)
	}

	w.log.Info("Starting informer factory")
	w.sif.Start(ctx.Done())
	defer w.sif.Shutdown()

	w.log.Info("Waiting for informer factory to sync")
	if err := w.waitForCacheSync(ctx, 10*time.Second); err != nil {
		return fmt.Errorf("waiting for informer factory to sync: %w", err)
	}
	w.log.Info("Informer factory sync complete")

	w.metrics.IterationPeriod.WithLabelValues(w.entry.NodeLabel).Set(float64(w.entry.Interval.Seconds()))
	w.log.Info("Starting target node watcher")
	ticker := time.NewTicker(w.entry.Interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()

		case <-ticker.C:
			if err := w.Tick(ctx); err != nil {
				// TODO: Do we want this to be fatal?
				return fmt.Errorf("error during tick: %w", err)
			}
		}
	}
}

func (w *NamespaceWatcher) waitForCacheSync(ctx context.Context, timeout time.Duration) error {
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	for v, synced := range w.sif.WaitForCacheSync(ctx.Done()) {
		if !synced {
			return fmt.Errorf("%v did not sync", v)
		}

		w.log.Debug("Type synced", "type", v)
	}
	return nil
}

// Tick performs a single unit of work.
// This is intended to be called over and over by `StartInformerBlocking`, however it is useful to call this in tests or similar as well.
//
// Unlike `StartInformerBlocking`, calls to this function never consume the watcher.
// It is also valid to call this function in parallel to `StartInformerBlocking`, although that is primarily done in tests.
func (w *NamespaceWatcher) Tick(ctx context.Context) error {
	log := w.log
	now := time.Now()
	if fields := loggerFieldsFromCtx(ctx); len(fields) > 0 {
		log = log.With(fields...)
		log = log.With("tick", now.Format(time.RFC3339Nano))
	}
	defer func() { log.Error("Tick completed", "duration", time.Since(now).String()) }()

	log.Debug("Starting tick")
	start := time.Now()
	w.metrics.Iterations.WithLabelValues(w.entry.NodeLabel).Inc()

	log.Debug("Listing pods in ns")
	// TODO: I'm unsure whether this operation will scale. The list is namespaced, but the SharedInformerFactory
	// is not. This is hard to figure out from the code, so I guess we'll need to FAFO.
	pods, err := w.sif.Core().V1().Pods().Lister().Pods(w.entry.Namespace).List(w.entry.LabelSelector)
	if err != nil {
		return fmt.Errorf("listing pods: %w", err)
	}

	shouldHaveLabel := map[string]bool{}
	for _, pod := range pods {
		log.Debug("Flagging node as containing matching pod", "node", pod.Spec.NodeName, "pod", pod.Name)
		shouldHaveLabel[pod.Spec.NodeName] = true
	}
	log.Info("Computed nodes that should have label", "num_nodes", len(shouldHaveLabel), "value", shouldHaveLabel)

	log.Debug("Listing nodes")
	// TODO(perf): We could list only nodes with the configured label. We get the nodes pods are running on from the podLister.
	nodes, err := w.sif.Core().V1().Nodes().Lister().List(labels.Everything())
	if err != nil {
		return fmt.Errorf("listing nodes: %w", err)
	}
	log.Info("Node list complete", "num_nodes", len(nodes))

	for _, node := range nodes {
		log := log.With("node", node.Name)
		log.Error("Processing node")

		existingLabel, hasLabel := node.Labels[w.entry.NodeLabel]
		if shouldHaveLabel[node.Name] && existingLabel == "true" {
			log.Debug("Target node is already labeled")
			continue
		}

		if !shouldHaveLabel[node.Name] && !hasLabel {
			log.Debug("Unlabeled node does not need to be labeled")
			continue
		}

		w.metrics.LabelOperations.WithLabelValues(w.entry.NodeLabel).Inc()

		// TODO: The update operations below may fail if a different controller modifies the node in this short
		// time. In this case, the error will be treated as fatal and the controller will restart. This is
		// harmless but not the best way to handle this situation, perhaps we should consider retrying.

		if shouldHaveLabel[node.Name] {
			log.Info("Adding label to node")
			// Objects returned from informers should be treated as readOnly, thus DeepCopy.
			_, err := w.kubeClient.CoreV1().Nodes().Update(ctx, nodeWithLabel(node.DeepCopy(), w.entry.NodeLabel), metav1.UpdateOptions{})
			if err != nil {
				return fmt.Errorf("adding label to node %q: %w", node.Name, err)
			}
		} else {
			log.Info("Removing label from node")
			// Objects returned from informers should be treated as readOnly, thus DeepCopy.
			_, err := w.kubeClient.CoreV1().Nodes().Update(ctx, nodeWithoutLabel(node.DeepCopy(), w.entry.NodeLabel), metav1.UpdateOptions{})
			if err != nil {
				return fmt.Errorf("removing label from node %q: %w", node.Name, err)
			}
		}
	}

	w.metrics.LabeledNodes.WithLabelValues(w.entry.NodeLabel).Set(float64(len(shouldHaveLabel)) / float64(len(nodes)))
	w.metrics.IterationTime.WithLabelValues(w.entry.NodeLabel).Observe(float64(time.Since(start).Seconds()))
	return nil
}

func nodeWithLabel(node *corev1.Node, label string) *corev1.Node {
	if node.Labels == nil {
		node.Labels = map[string]string{}
	}

	node.Labels[label] = "true"

	return node
}

func nodeWithoutLabel(node *corev1.Node, label string) *corev1.Node {
	delete(node.Labels, label)
	return node
}

type loggerFieldsCtxKey int

var loggerFieldsKey loggerFieldsCtxKey

func WithLoggerFields(ctx context.Context, fields ...any) context.Context {
	return context.WithValue(ctx, loggerFieldsKey, fields)
}

func loggerFieldsFromCtx(ctx context.Context) []any {
	fields, _ := ctx.Value(loggerFieldsKey).([]any)
	return fields
}
