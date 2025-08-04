package kubenodelabeler_test

import (
	"context"
	"errors"
	"log/slog"
	"os"
	"strings"
	"testing"
	"time"

	kubenodelabeler "github.com/grafana/kube-node-labeler"
	"github.com/grafana/kube-node-labeler/config"
	"github.com/grafana/kube-node-labeler/metrics"
	"github.com/prometheus/client_golang/prometheus"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	acorev1 "k8s.io/client-go/applyconfigurations/core/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/fake"
)

func TestKubeNodeLabeler(t *testing.T) {
	t.Parallel()

	const testns = "test"

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	cs := fake.NewClientset()

	const testLabel = "interesting-node"

	log := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelDebug}))
	nl := kubenodelabeler.NodeLabeler{
		Log:        log,
		Metrics:    metrics.New(prometheus.NewRegistry()),
		KubeClient: cs,
		ConfigEntries: []*config.Entry{
			{
				Interval:      time.Second,
				Namespace:     testns,
				LabelSelector: selector(t, "app.k8s.io/name=pod1"),
				NodeLabel:     testLabel,
			},
		},
		// Informers interact weirdly with the k8s fake client. Work around that by making them poll very often.
		// Ref: https://github.com/kubernetes/kubernetes/issues/95372#issuecomment-717016660
		ResyncPeriod: time.Second,
	}

	go func() {
		err := nl.Start(ctx)
		if err != nil && !errors.Is(err, context.Canceled) {
			t.Errorf("nodelabeler returned an error: %v", err)
			t.Fail()
		}
	}()

	t.Log("Creating starting nodes")

	for _, name := range []string{"node1", "node2", "node3"} {
		createNode(t, cs, name)
	}

	t.Log("Creating starting pods inside the first two nodes")

	for _, podNodeNs := range []string{"pod1:node1:" + testns, "pod2:node2:" + testns} {
		createPodInNode(t, cs, podNodeNs)
	}

	// Give our controller time to reconcile.
	time.Sleep(2 * time.Second)

	// Check label presence
	for node, shouldHaveLabel := range map[string]bool{
		"node1": true, // pod1 matches the label, thus node1 should be interesting now
		"node2": false,
		"node3": false,
	} {
		node, err := cs.CoreV1().Nodes().Get(ctx, node, metav1.GetOptions{})
		if err != nil {
			t.Fatalf("getting node: %v", err)
		}

		if _, hasLabel := node.Labels[testLabel]; hasLabel != shouldHaveLabel {
			t.Fatalf("expected hasLabel=%v for %q, but got %v", shouldHaveLabel, node, hasLabel)
		}
	}

	t.Log("Removing interesting pod")

	err := cs.CoreV1().Pods(testns).Delete(ctx, "pod1", metav1.DeleteOptions{})
	if err != nil {
		t.Fatalf("deleting pod: %v", err)
	}

	// Give our controller time to reconcile.
	time.Sleep(2 * time.Second)

	// Check label presence
	for node, shouldHaveLabel := range map[string]bool{
		"node1": false, // All nodes shoudld be unlabeled now.
		"node2": false,
		"node3": false,
	} {
		node, err := cs.CoreV1().Nodes().Get(ctx, node, metav1.GetOptions{})
		if err != nil {
			t.Fatalf("getting node: %v", err)
		}

		if _, hasLabel := node.Labels[testLabel]; hasLabel != shouldHaveLabel {
			t.Fatalf("expected hasLabel=%v for %q, but got %v", shouldHaveLabel, node, hasLabel)
		}
	}

	t.Log("Creating interesting pod elsewhere")

	createPodInNode(t, cs, "pod1:node3:"+testns)

	// Give our controller time to reconcile.
	time.Sleep(2 * time.Second)

	// Check label presence
	for node, shouldHaveLabel := range map[string]bool{
		"node1": false,
		"node2": false,
		"node3": true, // Interesting pod moved here.
	} {
		node, err := cs.CoreV1().Nodes().Get(ctx, node, metav1.GetOptions{})
		if err != nil {
			t.Fatalf("getting node: %v", err)
		}

		if _, hasLabel := node.Labels[testLabel]; hasLabel != shouldHaveLabel {
			t.Fatalf("expected hasLabel=%v for %q, but got %v", shouldHaveLabel, node, hasLabel)
		}
	}
}

func createNode(t *testing.T, cs kubernetes.Interface, name string) {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	t.Cleanup(cancel)
	_, err := cs.CoreV1().Nodes().Apply(ctx, acorev1.Node(name), metav1.ApplyOptions{})
	if err != nil {
		t.Fatalf("creating %q: %v", name, err)
	}
}

func createPodInNode(t *testing.T, cs kubernetes.Interface, podNodeNs string) {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	t.Cleanup(cancel)

	split := strings.Split(podNodeNs, ":")
	pod := split[0]
	node := split[1]
	ns := split[2]

	_, err := cs.CoreV1().Pods(ns).Apply(ctx,
		acorev1.Pod(pod, ns).
			WithLabels(map[string]string{"app.k8s.io/name": pod}).
			WithSpec(acorev1.PodSpec().WithNodeName(node)),
		metav1.ApplyOptions{})
	if err != nil {
		t.Fatalf("creating %q: %v", podNodeNs, err)
	}
}

func selector(t *testing.T, str string) labels.Selector {
	t.Helper()
	sel, err := labels.Parse(str)
	if err != nil {
		t.Fatalf("parsing labelSelector: %v", err)
	}

	return sel
}
