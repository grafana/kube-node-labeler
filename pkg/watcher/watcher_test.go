package watcher_test

import (
	"context"
	"errors"
	"log/slog"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/grafana/kube-node-labeler/pkg/config"
	"github.com/grafana/kube-node-labeler/pkg/metrics"
	"github.com/grafana/kube-node-labeler/pkg/watcher"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	acorev1 "k8s.io/client-go/applyconfigurations/core/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/fake"
	"k8s.io/client-go/tools/clientcmd"
)

func init() {
	slog.SetDefault(slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelDebug})))
}

func TestWatcher(t *testing.T) {
	t.Parallel()

	testWatcherLabelOperations(t, fake.NewClientset())
}

func TestWatcherOnCluster(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping watcher test in short mode")
	}
	kubeConfig := os.Getenv("TEST_KUBECONFIG")
	if kubeConfig == "" {
		t.Skip("TEST_KUBECONFIG not set, skipping watcher test")
	}
	if !filepath.IsAbs(kubeConfig) {
		t.Fatalf("TEST_KUBECONFIG must be an absolute path, got: %s", kubeConfig)
	}

	t.Parallel()

	contents, err := os.ReadFile(kubeConfig)
	require.NoError(t, err, "could not read kubeconfig at: %s", kubeConfig)

	restConfig, err := clientcmd.RESTConfigFromKubeConfig(contents)
	require.NoError(t, err, "could not create REST config from kubeconfig at: %s", kubeConfig)

	client, err := kubernetes.NewForConfig(restConfig)
	require.NoError(t, err, "could not create k8s client")

	testWatcherLabelOperations(t, client)
}

func testWatcherLabelOperations(t testing.TB, client kubernetes.Interface) {
	logger := slog.With("test", t.Name())
	metrics := metrics.New(prometheus.NewRegistry())
	config := &config.Entry{
		Interval:      time.Millisecond * 10,
		Namespace:     "default",
		LabelSelector: selector(t, "grafana.net/interesting=true"),
		NodeLabel:     "grafana.net/look-at-me",
		ResyncPeriod:  time.Millisecond * 200,
	}

	watcher, err := watcher.NewNamespacedWatcher(logger, metrics, client, config)
	require.NoError(t, err, "failed to create watcher")
	// We need to start the informer such that it can receive updates about what's going on in the client.
	go func() {
		err := watcher.StartInformerBlocking(t.Context())
		if errors.Is(err, context.Canceled) {
			return
		}
		require.NoError(t, err, "watcher returned an error")
	}()

	// Do a first tick: this should do nothing as there are no nodes or pods.
	require.NoError(t, watcher.Tick(t.Context()), "tick failed with no nodes or pods")

	// Now, we'll introduce the nodes in the story. They should still have no labels.
	_, err = client.CoreV1().Nodes().Apply(t.Context(), acorev1.Node("node1"), metav1.ApplyOptions{FieldManager: t.Name()})
	require.NoError(t, err, "failed to create node")
	_, err = client.CoreV1().Nodes().Apply(t.Context(), acorev1.Node("node2"), metav1.ApplyOptions{FieldManager: t.Name()})
	require.NoError(t, err, "failed to create node")
	_, err = client.CoreV1().Nodes().Apply(t.Context(), acorev1.Node("node3"), metav1.ApplyOptions{FieldManager: t.Name()})
	require.NoError(t, err, "failed to create node")

	require.NoError(t, watcher.Tick(t.Context()), "tick failed with 3 nodes and no pods")
	require.Empty(t, labelsOfNode(t, client, "node1"), "node1 should not have any labels")
	require.Empty(t, labelsOfNode(t, client, "node2"), "node2 should not have any labels")
	require.Empty(t, labelsOfNode(t, client, "node3"), "node3 should not have any labels")

	// Adding the namespace should likewise do nothing.
	_, err = client.CoreV1().Namespaces().Apply(t.Context(), acorev1.Namespace("default"), metav1.ApplyOptions{FieldManager: t.Name()})
	require.NoError(t, err, "failed to create namespace")

	require.NoError(t, watcher.Tick(t.Context()), "tick failed with 3 nodes, no pods, and a single namespace")
	require.Empty(t, labelsOfNode(t, client, "node1"), "node1 should not have any labels")
	require.Empty(t, labelsOfNode(t, client, "node2"), "node2 should not have any labels")
	require.Empty(t, labelsOfNode(t, client, "node3"), "node3 should not have any labels")

	// And when we add a pod that is not relevant, again, we should see that nothing happens.
	_, err = client.CoreV1().Pods("default").Apply(t.Context(),
		acorev1.Pod("unlabeled-pod", "default").
			WithSpec(acorev1.PodSpec().
				WithNodeName("node1").
				WithContainers(crashingContainer()),
			),
		metav1.ApplyOptions{FieldManager: t.Name()})
	require.NoError(t, err, "failed to create unlabelled pod")

	require.NoError(t, watcher.Tick(t.Context()), "tick failed with 3 nodes and an unlabelled pod")
	require.Empty(t, labelsOfNode(t, client, "node1"), "node1 should not have any labels")
	require.Empty(t, labelsOfNode(t, client, "node2"), "node2 should not have any labels")
	require.Empty(t, labelsOfNode(t, client, "node3"), "node3 should not have any labels")

	// Only when we add one that we say is interesting, should we notice something.
	_, err = client.CoreV1().Pods("default").Apply(t.Context(),
		acorev1.Pod("labeled-pod", "default").
			WithLabels(map[string]string{"grafana.net/interesting": "true"}).
			WithSpec(acorev1.PodSpec().
				WithNodeName("node2").
				WithContainers(longRunningContainer())),
		metav1.ApplyOptions{FieldManager: t.Name()})
	require.NoError(t, err, "failed to create labeled pod")
	require.EventuallyWithT(t, func(collect *assert.CollectT) {
		pod, err := client.CoreV1().Pods("default").Get(t.Context(), "labeled-pod", metav1.GetOptions{})
		if assert.NoError(collect, err, "failed getting pod") {
			assert.Contains(collect, pod.Labels, "grafana.net/interesting", "pod should have the label")
		}
	}, 5*time.Second, time.Millisecond*50)

	require.NoError(t, watcher.Tick(t.Context()), "tick failed with 3 nodes and a labeled pod")
	require.EventuallyWithT(t, func(collect *assert.CollectT) {
		assert.Empty(collect, labelsOfNode(t, client, "node1"), "node1 should not have any labels")
		assert.Equal(collect, map[string]string{"grafana.net/look-at-me": "true"}, labelsOfNode(t, client, "node2"), "node2 should have the label")
		assert.Empty(collect, labelsOfNode(t, client, "node3"), "node3 should not have any labels")
	}, 5*time.Second, time.Millisecond*50)

	// Let's remove it, and see that the node is unlabeled.
	err = client.CoreV1().Pods("default").Delete(t.Context(), "labeled-pod", metav1.DeleteOptions{})
	require.NoError(t, err, "failed to delete labeled pod")

	require.NoError(t, watcher.Tick(t.Context()), "tick failed with 3 nodes and a deleted pod")
	require.EventuallyWithT(t, func(collect *assert.CollectT) {
		assert.Empty(collect, labelsOfNode(t, client, "node1"), "node1 should not have any labels")
		assert.Empty(collect, labelsOfNode(t, client, "node2"), "node2 should not have any labels")
		assert.Empty(collect, labelsOfNode(t, client, "node3"), "node3 should not have any labels")
	}, 5*time.Second, time.Millisecond*50)

	// And if we pretend as if Kubernetes reschedules it on another node (node3 in this case), we'll see it occurs there, too.
	_, err = client.CoreV1().Pods("default").Apply(t.Context(),
		acorev1.Pod("labeled-pod", "default").
			WithLabels(map[string]string{"grafana.net/interesting": "true"}).
			WithSpec(acorev1.PodSpec().
				WithNodeName("node3").
				WithContainers(longRunningContainer())),
		metav1.ApplyOptions{FieldManager: t.Name()})
	require.NoError(t, err, "failed to create labeled pod")
	require.EventuallyWithT(t, func(collect *assert.CollectT) {
		pod, err := client.CoreV1().Pods("default").Get(t.Context(), "labeled-pod", metav1.GetOptions{})
		if assert.NoError(collect, err, "failed getting pod") {
			assert.Contains(collect, pod.Labels, "grafana.net/interesting", "pod should have the label")
		}
	}, 5*time.Second, time.Millisecond*50)

	require.NoError(t, watcher.Tick(t.Context()), "tick failed with 3 nodes and a labeled pod")
	require.EventuallyWithT(t, func(collect *assert.CollectT) {
		assert.Empty(collect, labelsOfNode(t, client, "node1"), "node1 should not have any labels")
		assert.Empty(collect, labelsOfNode(t, client, "node2"), "node2 should not have any labels")
		assert.Equal(collect, map[string]string{"grafana.net/look-at-me": "true"}, labelsOfNode(t, client, "node3"), "node3 should have the label")
	}, 5*time.Second, time.Millisecond*50)

	// Look at what happens now: we will schedule move the pod on a node that doesn't exist (i.e. in a real K8s setup, it would never schedule).
	// This should simply remove the label from the old node, as the new pod can't be scheduled.
	err = client.CoreV1().Pods("default").Delete(t.Context(), "labeled-pod", metav1.DeleteOptions{})
	require.NoError(t, err, "failed to delete labeled pod")
	_, err = client.CoreV1().Pods("default").Apply(t.Context(),
		acorev1.Pod("labeled-pod", "default").
			WithLabels(map[string]string{"grafana.net/interesting": "true"}).
			WithSpec(acorev1.PodSpec().
				WithNodeName("node999").
				WithContainers(longRunningContainer())),
		metav1.ApplyOptions{FieldManager: t.Name()})
	require.NoError(t, err, "failed to create labeled pod")
	require.EventuallyWithT(t, func(collect *assert.CollectT) {
		pod, err := client.CoreV1().Pods("default").Get(t.Context(), "labeled-pod", metav1.GetOptions{})
		if assert.NoError(collect, err, "failed getting pod") {
			assert.Contains(collect, pod.Labels, "grafana.net/interesting", "pod should have the label")
		}
	}, 5*time.Second, time.Millisecond*50)

	require.NoError(t, watcher.Tick(t.Context()), "tick failed with 3 nodes and a labeled pod")
	require.EventuallyWithT(t, func(collect *assert.CollectT) {
		assert.Empty(collect, labelsOfNode(t, client, "node1"), "node1 should not have any labels")
		assert.Empty(collect, labelsOfNode(t, client, "node2"), "node2 should not have any labels")
		assert.Empty(collect, labelsOfNode(t, client, "node3"), "node3 should not have any labels")
	}, 5*time.Second, time.Millisecond*50)
}

func selector(t testing.TB, str string) labels.Selector {
	t.Helper()
	sel, err := labels.Parse(str)
	if err != nil {
		t.Fatalf("parsing labelSelector: %v", err)
	}

	return sel
}

func labelsOfNode(t testing.TB, cs kubernetes.Interface, node string) map[string]string {
	t.Helper()

	nodeObj, err := cs.CoreV1().Nodes().Get(t.Context(), node, metav1.GetOptions{})
	if err != nil {
		t.Fatalf("getting node %q: %v", node, err)
	}
	return nodeObj.Labels
}

func crashingContainer() *acorev1.ContainerApplyConfiguration {
	return acorev1.Container().
		WithName("crashing").
		WithImage("busybox:latest").
		WithCommand("/bin/false")
}

func longRunningContainer() *acorev1.ContainerApplyConfiguration {
	return acorev1.Container().
		WithName("long-running").
		WithImage("busybox:latest").
		WithCommand("/bin/sleep", "120")
}
