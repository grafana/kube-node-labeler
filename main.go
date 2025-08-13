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

	"github.com/grafana/kube-node-labeler/pkg/config"
	"github.com/grafana/kube-node-labeler/pkg/metrics"
	"github.com/grafana/kube-node-labeler/pkg/watcher"
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

	reg := metrics.NewRegistry()
	metrics := metrics.New(reg)
	handler := promhttp.HandlerFor(reg, promhttp.HandlerOpts{Registry: reg})

	mux := http.NewServeMux()
	mux.Handle("GET /metrics", handler)

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	eg, ctx := errgroup.WithContext(ctx)
	eg.Go(func() error {
		return watcher.StartForEntries(ctx, log, metrics, clientset, cfg.Entries)
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
