package watcher_test

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/docker/go-connections/nat"
	"github.com/grafana/kube-node-labeler/pkg/config"
	"github.com/grafana/kube-node-labeler/pkg/metrics"
	"github.com/grafana/kube-node-labeler/pkg/watcher"
	"github.com/lmittmann/tint"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	acorev1 "k8s.io/client-go/applyconfigurations/core/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/fake"
	"k8s.io/client-go/tools/clientcmd"
)

func init() {
	//slog.SetDefault(slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelDebug})))
	slog.SetDefault(slog.New(tint.NewHandler(os.Stderr, &tint.Options{Level: slog.LevelDebug})))
}

func TestWatcher(t *testing.T) {
	t.Parallel()
	t.Skip("use cluster-watcher if possible")
	// Using a fake client, while handy in some situations, is generally super flaky.
	// Kwok, in comparison, generally always passes. And it is a _real_ Kubernetes setup (albeit very small and funky in its own ways).
	// This means this is super useful for rapid unit-test-backed development, but shouldn't be run in CI/CD.

	testWatcherLabelOperations(t, fake.NewClientset())
}

func TestWatcherOnCluster(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping watcher test in short mode")
	}
	t.Parallel()

	var contents []byte
	kubeConfig := os.Getenv("TEST_KUBECONFIG")
	if kubeConfig == "" {
		container, err := testcontainers.Run(t.Context(),
			"registry.k8s.io/kwok/cluster:v0.7.0-k8s.v1.33.0",
			testcontainers.WithExposedPorts("8080"),
			testcontainers.WithWaitStrategy(wait.ForLog("Start cluster and keep alive")))
		require.NoError(t, err, "could not start kwok container")

		port, err := nat.NewPort("tcp", "8080")
		require.NoError(t, err, "could not create port")

		endpoint, err := container.PortEndpoint(t.Context(), port, "http")
		require.NoError(t, err, "could not get port endpoint")

		contents = fmt.Appendf(nil, `
apiVersion: v1
clusters:
- cluster:
    server: %s
  name: kwok
contexts:
- context:
    cluster: kwok
  name: kwok
current-context: kwok
kind: Config
preferences: {}
users: null
`, endpoint)
	} else if !filepath.IsAbs(kubeConfig) {
		t.Fatalf("TEST_KUBECONFIG must be an absolute path, got: %s", kubeConfig)
	} else {
		var err error
		contents, err = os.ReadFile(kubeConfig)
		require.NoError(t, err, "could not read kubeconfig at: %s", kubeConfig)
	}

	restConfig, err := clientcmd.RESTConfigFromKubeConfig(contents)
	require.NoError(t, err, "could not create REST config from kubeconfig at: %s", kubeConfig)

	client, err := kubernetes.NewForConfig(restConfig)
	require.NoError(t, err, "could not create k8s client")

	time.Sleep(5 * time.Second)

	testWatcherLabelOperations(t, client)
}

func testWatcherLabelOperations(t testing.TB, client kubernetes.Interface) {
	t.Helper()

	uniquenessID := fmt.Sprintf("%d-", time.Now().Unix()) // handy for running with -count=2
	namespace := uniquenessID + "kube-node-labeler-watcher-test"
	nodePrefix := uniquenessID + "kube-node-labeler-watcher-test-node-"
	ctx := t.Context()
	ctx = watcher.WithLoggerFields(ctx, slog.String("MANUAL_TICK", uniquenessID))

	logger := slog.With("test", t.Name())
	metrics := metrics.New(prometheus.NewRegistry())
	config := &config.Entry{
		Interval:      time.Millisecond * 10,
		Namespace:     namespace,
		LabelSelector: selector(t, "grafana.net/interesting=true"),
		NodeLabel:     "grafana.net/look-at-me",
		ResyncPeriod:  time.Millisecond * 200,
	}

	watcher, err := watcher.NewNamespacedWatcher(logger, metrics, client, config)
	require.NoError(t, err, "failed to create watcher")
	// We need to start the informer such that it can receive updates about what's going on in the client.
	go func() {
		err := watcher.StartInformerBlocking(ctx)
		if errors.Is(err, context.Canceled) {
			return
		}
		require.NoError(t, err, "watcher returned an error")
	}()

	// Do a first tick: this should do nothing as there are no nodes or pods.
	require.NoError(t, watcher.Tick(ctx), "tick failed with no nodes or pods")

	// Now, we'll introduce the nodes in the story. They should still have no labels.
	t.Cleanup(func() {
		_ = client.CoreV1().Nodes().Delete(ctx, nodePrefix+"1", metav1.DeleteOptions{})
		_ = client.CoreV1().Nodes().Delete(ctx, nodePrefix+"2", metav1.DeleteOptions{})
		_ = client.CoreV1().Nodes().Delete(ctx, nodePrefix+"3", metav1.DeleteOptions{})
	})
	_, err = client.CoreV1().Nodes().Apply(ctx, acorev1.Node(nodePrefix+"1"), metav1.ApplyOptions{FieldManager: t.Name()})
	require.NoError(t, err, "failed to create node")
	_, err = client.CoreV1().Nodes().Apply(ctx, acorev1.Node(nodePrefix+"2"), metav1.ApplyOptions{FieldManager: t.Name()})
	require.NoError(t, err, "failed to create node")
	_, err = client.CoreV1().Nodes().Apply(ctx, acorev1.Node(nodePrefix+"3"), metav1.ApplyOptions{FieldManager: t.Name()})
	require.NoError(t, err, "failed to create node")

	time.Sleep(config.ResyncPeriod * 2) // give the informer some time

	require.NoError(t, watcher.Tick(ctx), "tick failed with 3 nodes and no pods")
	require.Empty(t, labelsOfNode(t, client, nodePrefix+"1"), "node1 should not have any labels")
	require.Empty(t, labelsOfNode(t, client, nodePrefix+"2"), "node2 should not have any labels")
	require.Empty(t, labelsOfNode(t, client, nodePrefix+"3"), "node3 should not have any labels")

	// Adding the namespace should likewise do nothing.
	_, err = client.CoreV1().Namespaces().Apply(ctx, acorev1.Namespace(namespace), metav1.ApplyOptions{FieldManager: t.Name()})
	require.NoError(t, err, "failed to create namespace")
	// If this is a real Kubernetes setup somehow (e.g. kwok, minikube, k3s), deleting the namespace should be enough to remove any pods inside. So we don't need to clean those up.
	t.Cleanup(func() {
		_ = client.CoreV1().Namespaces().Delete(ctx, namespace, metav1.DeleteOptions{})
	})

	require.NoError(t, watcher.Tick(ctx), "tick failed with 3 nodes, no pods, and a single namespace")
	require.Empty(t, labelsOfNode(t, client, nodePrefix+"1"), "node1 should not have any labels")
	require.Empty(t, labelsOfNode(t, client, nodePrefix+"2"), "node2 should not have any labels")
	require.Empty(t, labelsOfNode(t, client, nodePrefix+"3"), "node3 should not have any labels")

	// And when we add a pod that is not relevant, again, we should see that nothing happens.
	_, err = client.CoreV1().Pods(namespace).Apply(ctx,
		acorev1.Pod("unlabeled-pod", namespace).
			WithSpec(acorev1.PodSpec().
				WithNodeName(nodePrefix+"1").
				WithContainers(crashingContainer()),
			),
		metav1.ApplyOptions{FieldManager: t.Name()})
	require.NoError(t, err, "failed to create unlabelled pod")

	require.NoError(t, watcher.Tick(ctx), "tick failed with 3 nodes and an unlabelled pod")
	require.Empty(t, labelsOfNode(t, client, nodePrefix+"1"), "node1 should not have any labels")
	require.Empty(t, labelsOfNode(t, client, nodePrefix+"2"), "node2 should not have any labels")
	require.Empty(t, labelsOfNode(t, client, nodePrefix+"3"), "node3 should not have any labels")

	// Only when we add one that we say is interesting, should we notice something.
	_, err = client.CoreV1().Pods(namespace).Apply(ctx,
		acorev1.Pod("labeled-pod", namespace).
			WithLabels(map[string]string{"grafana.net/interesting": "true"}).
			WithSpec(acorev1.PodSpec().
				WithNodeName(nodePrefix+"2").
				WithContainers(longRunningContainer())),
		metav1.ApplyOptions{FieldManager: t.Name()})
	require.NoError(t, err, "failed to create labeled pod")
	require.EventuallyWithT(t, func(collect *assert.CollectT) {
		pod, err := client.CoreV1().Pods(namespace).Get(ctx, "labeled-pod", metav1.GetOptions{})
		if assert.NoError(collect, err, "failed getting pod") {
			assert.Contains(collect, pod.Labels, "grafana.net/interesting", "pod should have the label")
		}
	}, 5*time.Second, time.Millisecond*50, "UID: %s", uniquenessID)

	require.NoError(t, watcher.Tick(ctx), "tick failed with 3 nodes and a labeled pod")
	require.EventuallyWithT(t, func(collect *assert.CollectT) {
		assert.Empty(collect, labelsOfNode(t, client, nodePrefix+"1"), "node1 should not have any labels")
		assert.Equal(collect, map[string]string{"grafana.net/look-at-me": "true"}, labelsOfNode(t, client, nodePrefix+"2"), "node2 should have the label")
		assert.Empty(collect, labelsOfNode(t, client, nodePrefix+"3"), "node3 should not have any labels")
	}, 5*time.Second, time.Millisecond*50, "UID: %s", uniquenessID)

	// Let's remove it, and see that the node is unlabeled.
	err = client.CoreV1().Pods(namespace).Delete(ctx, "labeled-pod", metav1.DeleteOptions{})
	require.NoError(t, err, "failed to delete labeled pod")

	require.NoError(t, watcher.Tick(ctx), "tick failed with 3 nodes and a deleted pod")
	require.EventuallyWithT(t, func(collect *assert.CollectT) {
		assert.Empty(collect, labelsOfNode(t, client, nodePrefix+"1"), "node1 should not have any labels")
		assert.Empty(collect, labelsOfNode(t, client, nodePrefix+"2"), "node2 should not have any labels")
		assert.Empty(collect, labelsOfNode(t, client, nodePrefix+"3"), "node3 should not have any labels")
	}, 5*time.Second, time.Millisecond*50, "UID: %s", uniquenessID)

	// And if we pretend as if Kubernetes reschedules it on another node (node3 in this case), we'll see it occurs there, too.
	_, err = client.CoreV1().Pods(namespace).Apply(ctx,
		acorev1.Pod("labeled-pod", namespace).
			WithLabels(map[string]string{"grafana.net/interesting": "true"}).
			WithSpec(acorev1.PodSpec().
				WithNodeName(nodePrefix+"3").
				WithContainers(longRunningContainer())),
		metav1.ApplyOptions{FieldManager: t.Name()})
	require.NoError(t, err, "failed to create labeled pod")
	require.EventuallyWithT(t, func(collect *assert.CollectT) {
		pod, err := client.CoreV1().Pods(namespace).Get(ctx, "labeled-pod", metav1.GetOptions{})
		if assert.NoError(collect, err, "failed getting pod") {
			assert.Contains(collect, pod.Labels, "grafana.net/interesting", "pod should have the label")
		}
	}, 5*time.Second, time.Millisecond*50, "UID: %s", uniquenessID)

	require.NoError(t, watcher.Tick(ctx), "tick failed with 3 nodes and a labeled pod")
	require.EventuallyWithT(t, func(collect *assert.CollectT) {
		assert.Empty(collect, labelsOfNode(t, client, nodePrefix+"1"), "node1 should not have any labels")
		assert.Empty(collect, labelsOfNode(t, client, nodePrefix+"2"), "node2 should not have any labels")
		assert.Equal(collect, map[string]string{"grafana.net/look-at-me": "true"}, labelsOfNode(t, client, nodePrefix+"3"), "node3 should have the label")
	}, 5*time.Second, time.Millisecond*50, "UID: %s", uniquenessID)

	// Look at what happens now: we will schedule move the pod on a node that doesn't exist (i.e. in a real K8s setup, it would never schedule).
	// This should simply remove the label from the old node, as the new pod can't be scheduled.
	err = client.CoreV1().Pods(namespace).Delete(ctx, "labeled-pod", metav1.DeleteOptions{})
	require.NoError(t, err, "failed to delete labeled pod")
	_, err = client.CoreV1().Pods(namespace).Apply(ctx,
		acorev1.Pod("labeled-pod", namespace).
			WithLabels(map[string]string{"grafana.net/interesting": "true"}).
			WithSpec(acorev1.PodSpec().
				WithNodeName(nodePrefix+"999").
				WithContainers(longRunningContainer())),
		metav1.ApplyOptions{FieldManager: t.Name()})
	require.NoError(t, err, "failed to create labeled pod")
	require.EventuallyWithT(t, func(collect *assert.CollectT) {
		pod, err := client.CoreV1().Pods(namespace).Get(ctx, "labeled-pod", metav1.GetOptions{})
		if assert.NoError(collect, err, "failed getting pod") {
			assert.Contains(collect, pod.Labels, "grafana.net/interesting", "pod should have the label")
		}
	}, 5*time.Second, time.Millisecond*50, "UID: %s", uniquenessID)

	require.NoError(t, watcher.Tick(ctx), "tick failed with 3 nodes and a labeled pod")
	require.EventuallyWithT(t, func(collect *assert.CollectT) {
		assert.Empty(collect, labelsOfNode(t, client, nodePrefix+"1"), "node1 should not have any labels")
		assert.Empty(collect, labelsOfNode(t, client, nodePrefix+"2"), "node2 should not have any labels")
		assert.Empty(collect, labelsOfNode(t, client, nodePrefix+"3"), "node3 should not have any labels")
	}, 5*time.Second, time.Millisecond*50, "UID: %s", uniquenessID)
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
