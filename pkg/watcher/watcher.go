package watcher

import (
	"context"
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

type NodeLabeler struct {
	Log           *slog.Logger
	Metrics       *metrics.Metrics
	KubeClient    kubernetes.Interface
	ConfigEntries []*config.Entry
}

func (n *NodeLabeler) Start(ctx context.Context) error {
	eg := errgroup.Group{}
	for _, ce := range n.ConfigEntries {
		n.Log.Info("Creating watcher for nodeLabel", "nodeLabel", ce.NodeLabel)

		w := &watcher{
			log:        n.Log,
			metrics:    n.Metrics,
			kubeClient: n.KubeClient,
			entry:      ce,
		}

		eg.Go(func() error {
			return w.Start(ctx)
		})
	}

	return eg.Wait()
}

type watcher struct {
	log        *slog.Logger
	metrics    *metrics.Metrics
	kubeClient kubernetes.Interface
	entry      *config.Entry
}

func (w *watcher) Start(ctx context.Context) error {
	log := w.log.With("nodeLabel", w.entry.NodeLabel, "podLabelSelector", w.entry.LabelSelector.String(), "namespace", w.entry.Namespace)

	if w.entry.ResyncPeriod == 0 {
		w.entry.ResyncPeriod = 5 * time.Minute
		log.Info("Using default resync period", "ResyncPeriod", w.entry.ResyncPeriod)
	}

	log.Info("Starting informer factory")
	sif := informers.NewSharedInformerFactoryWithOptions(w.kubeClient, w.entry.ResyncPeriod, informers.WithNamespace(w.entry.Namespace))
	defer sif.Shutdown()

	pods := sif.Core().V1().Pods().Lister()
	nodes := sif.Core().V1().Nodes().Lister()

	sif.Start(ctx.Done())

	log.Info("Waiting for informer factory to sync")
	waitCtx, waitCancel := context.WithTimeout(ctx, 10*time.Second)
	defer waitCancel()
	for v, syncd := range sif.WaitForCacheSync(waitCtx.Done()) {
		if !syncd {
			return fmt.Errorf("%v did not sync", v)
		}

		log.Debug("Type synced", "type", v)
	}

	log.Info("Informer factory sync complete")

	w.metrics.IterationPeriod.WithLabelValues(w.entry.NodeLabel).Set(float64(w.entry.Interval.Seconds()))

	log.Info("Starting target node watcher")

	for {
		ticker := time.NewTicker(w.entry.Interval)
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			log.Debug("Starting iteration")

			itStart := time.Now()

			w.metrics.Iterations.WithLabelValues(w.entry.NodeLabel).Inc()

			log.Debug("Listing pods in ns")
			// TODO: I'm unsure whether this operation will scale. The list is namespaced, but the SharedInformerFactory
			// is not. This is hard to figure out from the code, so I guess we'll need to FAFO.
			pods, err := pods.Pods(w.entry.Namespace).List(w.entry.LabelSelector)
			if err != nil {
				return fmt.Errorf("listing pods: %w", err)
			}

			log.Debug("Pod list complete", "found", len(pods), "label", w.entry.LabelSelector.String())

			shouldHaveLabel := map[string]bool{}
			for _, pod := range pods {
				log.Debug("Flagging node as containing matching pod", "node", pod.Spec.NodeName, "pod", pod.Name)
				shouldHaveLabel[pod.Spec.NodeName] = true
			}

			log.Debug("Computed nodes that should have label", "numNodes", len(shouldHaveLabel))

			log.Debug("Listing nodes")
			nodes, err := nodes.List(labels.Everything())
			if err != nil {
				return fmt.Errorf("listing nodes: %w", err)
			}
			log.Debug("Node list complete", "numNodes", len(nodes))

			for _, node := range nodes {
				log.Debug("Processing node", "node", node.Name)

				if shouldHaveLabel[node.Name] && node.Labels[w.entry.NodeLabel] != "" {
					log.Debug("Target node is already labeled", "node", node.Name)
					continue
				}

				if !shouldHaveLabel[node.Name] && node.Labels[w.entry.NodeLabel] == "" {
					log.Debug("Unlabeled node does not need to be labeled", "node", node.Name)
					continue
				}

				w.metrics.LabelOperations.WithLabelValues(w.entry.NodeLabel).Inc()

				// TODO: The update operations below may fail if a different controller modifies the node in this short
				// time. In this case, the error will be treated as fatal and the controller will restart. This is
				// harmless but not the best way to handle this situation, perhaps we should consider retrying.

				if shouldHaveLabel[node.Name] {
					log.Info("Adding label to node", "node", node.Name)
					// Objects returned from informers should be treated as readOnly, thus DeepCopy.
					_, err := w.kubeClient.CoreV1().Nodes().Update(ctx, nodeWithLabel(node.DeepCopy(), w.entry.NodeLabel), metav1.UpdateOptions{})
					if err != nil {
						return fmt.Errorf("adding label to node %q: %w", node.Name, err)
					}
				}

				if !shouldHaveLabel[node.Name] {
					log.Info("Removing label from node", "node", node.Name)
					// Objects returned from informers should be treated as readOnly, thus DeepCopy.
					_, err := w.kubeClient.CoreV1().Nodes().Update(ctx, nodeWithoutLabel(node.DeepCopy(), w.entry.NodeLabel), metav1.UpdateOptions{})
					if err != nil {
						return fmt.Errorf("removing label from node %q: %w", node.Name, err)
					}
				}
			}

			w.metrics.LabeledNodes.WithLabelValues(w.entry.NodeLabel).Set(float64(len(shouldHaveLabel)) / float64(len(nodes)))
			w.metrics.IterationTime.WithLabelValues(w.entry.NodeLabel).Observe(float64(time.Since(itStart).Seconds()))
		}
	}
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
