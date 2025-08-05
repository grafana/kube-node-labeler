package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"

	kubenodelabeler "github.com/grafana/kube-node-labeler"
	"github.com/grafana/kube-node-labeler/config"
	"github.com/grafana/kube-node-labeler/metrics"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"golang.org/x/sync/errgroup"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
)

var log = slog.Default()

func run() error {
	levelStr := flag.String("log-level", slog.LevelInfo.String(), "slog level")
	configPath := flag.String("config", "", "path to config file")
	flag.Parse()

	var lvl slog.Level
	err := lvl.UnmarshalText([]byte(*levelStr))
	if err != nil {
		return fmt.Errorf("parsing log level: %w", err)
	}

	log = slog.New(slog.NewJSONHandler(os.Stderr, &slog.HandlerOptions{Level: lvl}))

	if *configPath == "" {
		return errors.New("path to config must be provided, see -h")
	}

	configFile, err := os.Open(*configPath)
	if err != nil {
		return fmt.Errorf("reading config file: %w", err)
	}

	defer configFile.Close() //nolint:errcheck

	cfg, err := config.Read(configFile)
	if err != nil {
		return fmt.Errorf("parsing config: %w", err)
	}

	kconfig, err := rest.InClusterConfig()
	if err != nil {
		return fmt.Errorf("reading in-cluster config: %w", err)
	}

	clientset, err := kubernetes.NewForConfig(kconfig)
	if err != nil {
		return fmt.Errorf("creating clientset: %w", err)
	}

	reg := prometheus.NewRegistry()
	handler := promhttp.HandlerFor(reg, promhttp.HandlerOpts{})

	mux := http.NewServeMux()
	mux.Handle("GET /metrics", handler)

	nl := &kubenodelabeler.NodeLabeler{
		Log:           log,
		Metrics:       metrics.New(prometheus.NewRegistry()), // FIXME: Pass prometheus registry
		KubeClient:    clientset,
		ConfigEntries: cfg.Entries,
	}

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	eg := errgroup.Group{}
	eg.Go(func() error {
		return nl.Start(ctx)
	})
	eg.Go(func() error {
		return http.ListenAndServe(cfg.MetricsAddr, mux)
	})

	return eg.Wait()
}

func main() {
	err := run()
	if err != nil {
		log.Error("running application", "err", err)
		os.Exit(1)
	}
}
