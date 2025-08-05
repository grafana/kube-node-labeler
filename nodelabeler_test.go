package kubenodelabeler_test

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"strings"
	"sync"
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
	"k8s.io/client-go/tools/clientcmd"
)

func TestKubeNodeLabeler(t *testing.T) {
	t.Parallel()

	t.Run("with kubernetes fake clientset", func(t *testing.T) {
		t.Parallel()
		testMigratingPodSequence(t, true)
	})

	t.Run("with TEST_KUBECONFIG", func(t *testing.T) {
		t.Parallel()

		kc := os.Getenv("TEST_KUBECONFIG")
		if kc == "" {
			t.Skipf("Skipping test against real cluster as TEST_KUBECONFIG is not defined")
		}

		testMigratingPodSequence(t, false)
	})
}

// testMigratingPodSequence tests the main controller loop by creating an initial state with a target pod on a known
// node, and then moving the pod somewhere else, checking after both steps that the nodes are labeled as they should.
//
// The magic fakeCS boolean tweaks the test either towards the kubernetes fake client (if true), or towards a real-ish
// cluster. For the fake client, the test gives less time for the (non-existent) cluster to initialize, and causes it
// to create node objects.
func testMigratingPodSequence(t *testing.T, fakeCS bool) {
	var cs kubernetes.Interface

	if fakeCS {
		cs = fake.NewClientset()
	} else {
		kc, err := os.ReadFile(os.Getenv("TEST_KUBECONFIG"))
		if err != nil {
			t.Fatalf("cannot read kubeconfig from TEST_KUBECONFIG: %v", err)
		}

		rest, err := clientcmd.RESTConfigFromKubeConfig(kc)
		if err != nil {
			t.Fatalf("creating kubernetes rest config: %v", err)
		}

		if os.Getenv("TEST_KUBE_LOG") != "" {
			rest.WrapTransport = func(rt http.RoundTripper) http.RoundTripper {
				return logRoundTripper{inner: rt, log: *slog.Default()}
			}
		}

		cs, err = kubernetes.NewForConfig(rest)
		if err != nil {
			t.Fatalf("creating kubernetes client: %v", err)
		}
	}

	// Timings for the test.
	nlInterval := time.Second
	resyncPeriod := 400 * time.Millisecond
	reconcileTime := 1500 * time.Millisecond
	if !fakeCS {
		// We're working with a real cluster, be friendlier and/or more lenient
		const slowness = 3
		nlInterval *= slowness
		resyncPeriod *= slowness
		reconcileTime *= 10 // This includes the time to create and delete pods, which is slow.
	}

	const testns = "test"

	const testLabel = "interesting-node"

	log := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelDebug}))
	nl := kubenodelabeler.NodeLabeler{
		Log:        log,
		Metrics:    metrics.New(prometheus.NewRegistry()),
		KubeClient: cs,
		ConfigEntries: []*config.Entry{
			{
				Interval:      nlInterval,
				Namespace:     testns,
				LabelSelector: selector(t, "app.k8s.io/name=pod1"),
				NodeLabel:     testLabel,
			},
		},
		// Informers interact weirdly with the k8s fake client. Work around that by making them poll very often.
		// Ref: https://github.com/kubernetes/kubernetes/issues/95372#issuecomment-717016660
		ResyncPeriod: resyncPeriod,
	}

	// WG to wait for the nodeLabeler goroutine before ending the test. Wait() must be deferred before cancelling the context.
	nlWg := &sync.WaitGroup{}
	defer nlWg.Wait()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	nlWg.Add(1)
	go func() {
		defer nlWg.Done()
		err := nl.Start(ctx)
		if err != nil && !errors.Is(err, context.Canceled) {
			t.Errorf("nodelabeler returned an error: %v", err)
			t.Fail()
		}
	}()

	if fakeCS {
		t.Log("Running on the fake client, creating starting nodes")
		for _, name := range []string{"node1", "node2", "node3"} {
			createNode(t, cs, name)
		}
	}

	nodes, err := cs.CoreV1().Nodes().List(ctx, metav1.ListOptions{})
	if err != nil {
		t.Fatalf("Listing nodes: %v", err)
	}

	if len(nodes.Items) != 3 {
		t.Fatalf("This test must be run on a cluster with 3 nodes, found: %d", len(nodes.Items))
	}

	t.Log("Creating test namespace")
	_, err = cs.CoreV1().Namespaces().Apply(ctx, acorev1.Namespace(testns), metav1.ApplyOptions{FieldManager: testFM})
	if err != nil {
		t.Fatalf("cannot create test namespace %q: %v", testns, err)
	}

	t.Log("Creating starting pods inside the first two nodes")

	podNames := []string{"pod1", "pod2", "pod3"} // One per node.
	for i, node := range nodes.Items {
		createPodInNode(t, cs, podNames[i], node.Name, testns)
	}

	// Give our controller time to reconcile.
	time.Sleep(reconcileTime)

	// Check label presence
	for node, shouldHaveLabel := range map[string]bool{
		nodes.Items[0].Name: true, // pod1 matches the label, thus node1 should be interesting now
		nodes.Items[1].Name: false,
		nodes.Items[2].Name: false,
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

	err = cs.CoreV1().Pods(testns).Delete(ctx, "pod1", metav1.DeleteOptions{})
	if err != nil {
		t.Fatalf("deleting pod: %v", err)
	}

	// Give our controller time to reconcile.
	time.Sleep(reconcileTime)

	// Check label presence
	for node, shouldHaveLabel := range map[string]bool{
		nodes.Items[0].Name: false, // Interesting pod deleted, all nodes should be unlabeled.
		nodes.Items[1].Name: false,
		nodes.Items[2].Name: false,
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

	createPodInNode(t, cs, podNames[0], nodes.Items[2].Name, testns)

	// Give our controller time to reconcile.
	time.Sleep(reconcileTime)

	// Check label presence
	for node, shouldHaveLabel := range map[string]bool{
		nodes.Items[0].Name: false,
		nodes.Items[1].Name: false,
		nodes.Items[2].Name: true, // Interesting pod moved here.
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

const testFM = "node-manager-test"

func createNode(t *testing.T, cs kubernetes.Interface, name string) {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	t.Cleanup(cancel)
	_, err := cs.CoreV1().Nodes().Apply(ctx, acorev1.Node(name), metav1.ApplyOptions{FieldManager: testFM})
	if err != nil {
		t.Fatalf("creating %q: %v", name, err)
	}
}

func createPodInNode(t *testing.T, cs kubernetes.Interface, pod, node, ns string) {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	t.Cleanup(cancel)

	_, err := cs.CoreV1().Pods(ns).Apply(ctx,
		acorev1.Pod(pod, ns).
			WithLabels(map[string]string{"app.k8s.io/name": pod}).
			WithSpec(
				acorev1.PodSpec().
					WithNodeName(node).
					WithTerminationGracePeriodSeconds(1).
					WithContainers(
						acorev1.Container().
							WithName(pod).
							WithImage("alpine:latest").
							WithArgs("sleep", "infinity"),
					),
			),
		metav1.ApplyOptions{FieldManager: testFM})
	if err != nil {
		t.Fatalf("creating %s/%s in %s: %v", ns, pod, node, err)
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

type logRoundTripper struct {
	log   slog.Logger
	inner http.RoundTripper
}

func (rt logRoundTripper) RoundTrip(r *http.Request) (resp *http.Response, err error) {
	requestLine := fmt.Sprintf("%s %s", r.Method, r.URL.String())

	log := rt.log.With()
	if host := r.Header["Host"]; len(host) > 0 {
		log = log.With("host", strings.Join(host, ","))
	}

	log.Info(requestLine, "state", "request")

	resp, err = rt.inner.RoundTrip(r)
	if err != nil {
		log.Error(requestLine)
		return
	}

	log.Info(requestLine, "state", "response", "statusCode", resp.StatusCode)
	return
}
