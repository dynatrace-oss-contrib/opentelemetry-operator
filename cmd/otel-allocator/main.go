// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/oklog/run"
	monitoringclient "github.com/prometheus-operator/prometheus-operator/pkg/client/versioned"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/prometheus/discovery"
	otelconf "go.opentelemetry.io/contrib/otelconf/v0.3.0"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	otelprom "go.opentelemetry.io/otel/exporters/prometheus"
	otlpmetricgrpc "go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetricgrpc"
	otlpmetrichttp "go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetrichttp"
	"go.opentelemetry.io/otel/metric"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	"k8s.io/client-go/kubernetes"
	_ "k8s.io/client-go/plugin/pkg/client/auth/gcp"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/certwatcher"

	"github.com/open-telemetry/opentelemetry-operator/cmd/otel-allocator/internal/allocation"
	"github.com/open-telemetry/opentelemetry-operator/cmd/otel-allocator/internal/collector"
	"github.com/open-telemetry/opentelemetry-operator/cmd/otel-allocator/internal/config"
	"github.com/open-telemetry/opentelemetry-operator/cmd/otel-allocator/internal/prehook"
	"github.com/open-telemetry/opentelemetry-operator/cmd/otel-allocator/internal/server"
	"github.com/open-telemetry/opentelemetry-operator/cmd/otel-allocator/internal/target"
	allocatorWatcher "github.com/open-telemetry/opentelemetry-operator/cmd/otel-allocator/internal/watcher"
)

var setupLog = ctrl.Log.WithName("setup")

func main() {
	var (
		// allocatorPrehook will be nil if filterStrategy is not set or
		// unrecognized. No filtering will be used in this case.
		allocatorPrehook prehook.Hook
		allocator        allocation.Allocator
		discoveryManager *discovery.Manager
		collectorWatcher *collector.Watcher
		targetDiscoverer *target.Discoverer
		certWatcher      *certwatcher.CertWatcher

		discoveryCancel context.CancelFunc
		runGroup        run.Group
		eventChan       = make(chan allocatorWatcher.Event)
		eventCloser     = make(chan bool, 1)
		interrupts      = make(chan os.Signal, 1)
		errChan         = make(chan error)
	)
	cfg, loadErr := config.Load(os.Args)
	if loadErr != nil {
		fmt.Printf("Failed to load config: %v", loadErr)
		os.Exit(1)
	}
	ctrl.SetLogger(cfg.RootLogger)

	if validationErr := config.ValidateConfig(cfg); validationErr != nil {
		setupLog.Error(validationErr, "Invalid configuration")
		os.Exit(1)
	}

	cfg.RootLogger.Info("Starting the Target Allocator")
	ctx := context.Background()
	log := ctrl.Log.WithName("allocator")

	k8sClient, err := kubernetes.NewForConfig(cfg.ClusterConfig)
	if err != nil {
		setupLog.Error(err, "Unable to initialize kubernetes client")
		os.Exit(1)
	}
	monitoringClient, err := monitoringclient.NewForConfig(cfg.ClusterConfig)
	if err != nil {
		setupLog.Error(err, "Unable to initialize monitoring client")
		os.Exit(1)
	}

	// Always register the Prometheus pull reader so /metrics keeps working.
	promExporter, promErr := otelprom.New()
	if promErr != nil {
		setupLog.Error(promErr, "Failed to create Prometheus exporter")
		os.Exit(1)
	}
	mpReaders := []sdkmetric.Reader{promExporter}

	if len(cfg.MeterProvider) > 0 {
		// Parse user-supplied meter_provider config (otelconf format) via JSON round-trip.
		otelCfgJSON, jsonErr := json.Marshal(map[string]any{
			"file_format":    "0.3",
			"meter_provider": normalizeYAMLMap(cfg.MeterProvider),
		})
		if jsonErr != nil {
			setupLog.Error(jsonErr, "Failed to marshal meter_provider configuration")
			os.Exit(1)
		}
		var otelCfg otelconf.OpenTelemetryConfiguration
		if jsonErr = json.Unmarshal(otelCfgJSON, &otelCfg); jsonErr != nil {
			setupLog.Error(jsonErr, "Failed to parse meter_provider configuration")
			os.Exit(1)
		}
		if otelCfg.MeterProvider != nil {
			for i, r := range otelCfg.MeterProvider.Readers {
				if r.Periodic == nil {
					continue
				}
				reader, rErr := buildPeriodicReader(ctx, r.Periodic)
				if rErr != nil {
					setupLog.Error(rErr, "Failed to build periodic reader", "reader_index", i)
					os.Exit(1)
				}
				mpReaders = append(mpReaders, reader)
			}
		}
	}

	var readerOpts []sdkmetric.Option
	for _, r := range mpReaders {
		readerOpts = append(readerOpts, sdkmetric.WithReader(r))
	}
	otel.SetMeterProvider(sdkmetric.NewMeterProvider(readerOpts...))

	allocatorPrehook = prehook.New(cfg.FilterStrategy, log)
	allocator, allocErr := allocation.New(cfg.AllocationStrategy, log, allocation.WithFilter(allocatorPrehook), allocation.WithFallbackStrategy(cfg.AllocationFallbackStrategy))
	if allocErr != nil {
		setupLog.Error(allocErr, "Unable to initialize allocation strategy")
		os.Exit(1)
	}

	httpOptions := []server.Option{}
	if cfg.HTTPS.Enabled {
		var tlsConfig *tls.Config
		var confErr error
		tlsConfig, certWatcher, confErr = cfg.HTTPS.NewTLSConfig(log)
		if confErr != nil {
			setupLog.Error(confErr, "Unable to initialize TLS configuration")
			os.Exit(1)
		}
		httpOptions = append(httpOptions, server.WithTLSConfig(tlsConfig, cfg.HTTPS.ListenAddr))
	}
	srv, serverErr := server.NewServer(log, allocator, cfg.ListenAddr, httpOptions...)
	if serverErr != nil {
		panic(serverErr)
	}

	discoveryCtx, discoveryCancel := context.WithCancel(ctx)
	defer discoveryCancel()
	sdMetrics, discErr := discovery.CreateAndRegisterSDMetrics(prometheus.DefaultRegisterer)
	if discErr != nil {
		setupLog.Error(discErr, "Unable to register metrics for Prometheus service discovery")
		os.Exit(1)
	}
	discoveryManager = discovery.NewManager(discoveryCtx, config.NopLogger, prometheus.DefaultRegisterer, sdMetrics)

	targetDiscoverer, targetErr := target.NewDiscoverer(log, discoveryManager, allocatorPrehook, srv, allocator.SetTargets)
	if targetErr != nil {
		panic(targetErr)
	}
	collectorWatcher, collectorWatcherErr := collector.NewCollectorWatcher(log, k8sClient, cfg.CollectorNotReadyGracePeriod)
	if collectorWatcherErr != nil {
		setupLog.Error(collectorWatcherErr, "Unable to initialize collector watcher")
		os.Exit(1)
	}
	signal.Notify(interrupts, os.Interrupt, syscall.SIGINT, syscall.SIGTERM)
	defer close(interrupts)

	if cfg.PrometheusCR.Enabled {
		promWatcher, allocErr := allocatorWatcher.NewPrometheusCRWatcher(
			ctx, setupLog.WithName("prometheus-cr-watcher"), k8sClient, monitoringClient, *cfg)
		if allocErr != nil {
			setupLog.Error(allocErr, "Can't start the prometheus watcher")
			os.Exit(1)
		}
		// apply the initial configuration
		promConfig, loadErr := promWatcher.LoadConfig(ctx)
		if loadErr != nil {
			setupLog.Error(loadErr, "Can't load initial Prometheus configuration from Prometheus CRs")
			os.Exit(1)
		}
		loadErr = targetDiscoverer.ApplyConfig(allocatorWatcher.EventSourcePrometheusCR, promConfig.ScrapeConfigs)
		if loadErr != nil {
			setupLog.Error(loadErr, "Can't load initial scrape targets from Prometheus CRs")
			os.Exit(1)
		}
		runGroup.Add(
			func() error {
				promWatcherErr := promWatcher.Watch(eventChan, errChan)
				setupLog.Info("Prometheus watcher exited")
				return promWatcherErr
			},
			func(_ error) {
				setupLog.Info("Closing prometheus watcher")
				promWatcherErr := promWatcher.Close()
				if promWatcherErr != nil {
					setupLog.Error(promWatcherErr, "prometheus watcher failed to close")
				}
			})
	}
	runGroup.Add(
		func() error {
			discoveryManagerErr := discoveryManager.Run()
			setupLog.Info("Discovery manager exited")
			return discoveryManagerErr
		},
		func(_ error) {
			setupLog.Info("Closing discovery manager")
			discoveryCancel()
		})
	runGroup.Add(
		func() error {
			// Initial loading of the config file's scrape config
			if cfg.PromConfig != nil && len(cfg.PromConfig.ScrapeConfigs) > 0 {
				applyErr := targetDiscoverer.ApplyConfig(allocatorWatcher.EventSourceConfigMap, cfg.PromConfig.ScrapeConfigs)
				if applyErr != nil {
					setupLog.Error(err, "Unable to apply initial configuration")
					return err
				}
			} else {
				setupLog.Info("Prometheus config empty, skipping initial discovery configuration")
			}

			tErr := targetDiscoverer.Run()
			setupLog.Info("Target discoverer exited")
			return tErr
		},
		func(_ error) {
			setupLog.Info("Closing target discoverer")
			targetDiscoverer.Close()
		})
	runGroup.Add(
		func() error {
			watchErr := collectorWatcher.Watch(cfg.CollectorNamespace, cfg.CollectorSelector, allocator.SetCollectors)
			setupLog.Info("Collector watcher exited")
			return watchErr
		},
		func(_ error) {
			setupLog.Info("Closing collector watcher")
			collectorWatcher.Close()
		})
	runGroup.Add(
		func() error {
			startErr := srv.Start()
			setupLog.Info("Server failed to start")
			return startErr
		},
		func(_ error) {
			setupLog.Info("Closing server")
			if shutdownErr := srv.Shutdown(ctx); shutdownErr != nil {
				setupLog.Error(shutdownErr, "Error on server shutdown")
			}
		})
	if cfg.HTTPS.Enabled {
		runGroup.Add(
			func() error {
				startErr := srv.StartHTTPS()
				setupLog.Info("HTTPS Server failed to start")
				return startErr
			},
			func(_ error) {
				setupLog.Info("Closing HTTPS server")
				if shutdownErr := srv.ShutdownHTTPS(ctx); shutdownErr != nil {
					setupLog.Error(shutdownErr, "Error on HTTPS server shutdown")
				}
			})

		// Start certificate watchers for hot-reload
		certWatcherCtx, certWatcherCancel := context.WithCancel(ctx)
		defer certWatcherCancel()
		// Server certificate watcher
		runGroup.Add(
			func() error {
				watchErr := certWatcher.Start(certWatcherCtx)
				setupLog.Info("Certificate watcher exited")
				return watchErr
			},
			func(_ error) {
				setupLog.Info("Closing certificate watcher")
				certWatcherCancel()
			})
	}
	meter := otel.GetMeterProvider().Meter("targetallocator")
	eventsMetric, err := meter.Int64Counter("opentelemetry_allocator_events", metric.WithDescription("Number of events in the channel."))
	if err != nil {
		panic(err)
	}
	runGroup.Add(
		func() error {
			for {
				select {
				case event := <-eventChan:
					eventsMetric.Add(context.Background(), 1, metric.WithAttributes(attribute.String("source", event.Source.String())))
					loadConfig, err := event.Watcher.LoadConfig(ctx)
					if err != nil {
						setupLog.Error(err, "Unable to load configuration")
						continue
					}
					err = targetDiscoverer.ApplyConfig(event.Source, loadConfig.ScrapeConfigs)
					if err != nil {
						setupLog.Error(err, "Unable to apply configuration")
						continue
					}
				case err := <-errChan:
					setupLog.Error(err, "Watcher error")
				case <-eventCloser:
					return nil
				}
			}
		},
		func(_ error) {
			setupLog.Info("Closing watcher loop")
			close(eventCloser)
		})
	runGroup.Add(
		func() error {
			for {
				select {
				case <-interrupts:
					setupLog.Info("Received interrupt")
					return nil
				case <-eventCloser:
					return nil
				}
			}
		},
		func(_ error) {
			setupLog.Info("Closing interrupt loop")
		})
	if runErr := runGroup.Run(); runErr != nil {
		setupLog.Error(runErr, "run group exited")
	}
	setupLog.Info("Target allocator exited.")
}

// normalizeYAMLMap recursively converts map[interface{}]interface{} (produced by
// the go-yaml v2 parser) to map[string]interface{} so that encoding/json can
// marshal the value without error.
func normalizeYAMLMap(v any) any {
	switch val := v.(type) {
	case map[interface{}]interface{}:
		out := make(map[string]any, len(val))
		for k, v2 := range val {
			out[fmt.Sprintf("%v", k)] = normalizeYAMLMap(v2)
		}
		return out
	case map[string]interface{}:
		for k, v2 := range val {
			val[k] = normalizeYAMLMap(v2)
		}
		return val
	case []interface{}:
		for i, v2 := range val {
			val[i] = normalizeYAMLMap(v2)
		}
		return val
	default:
		return v
	}
}

// buildPeriodicReader constructs an sdkmetric.Reader from an otelconf PeriodicMetricReader config.
// Only the otlp exporter is supported; pull readers are handled separately via otelprom.
func buildPeriodicReader(ctx context.Context, cfg *otelconf.PeriodicMetricReader) (sdkmetric.Reader, error) {
	if cfg.Exporter.OTLP == nil {
		return nil, fmt.Errorf("only otlp exporter is supported in periodic readers")
	}
	otlpCfg := cfg.Exporter.OTLP

	// Build headers map.
	headers := make(map[string]string, len(otlpCfg.Headers))
	for _, h := range otlpCfg.Headers {
		if h.Value != nil {
			headers[h.Name] = *h.Value
		}
	}

	// Temporality selector.
	temporality := sdkmetric.DefaultTemporalitySelector
	if otlpCfg.TemporalityPreference != nil {
		switch *otlpCfg.TemporalityPreference {
		case "delta":
			temporality = func(sdkmetric.InstrumentKind) metricdata.Temporality {
				return metricdata.DeltaTemporality
			}
		case "lowmemory":
			temporality = sdkmetric.LowMemoryTemporalitySelector
		case "cumulative":
			temporality = func(sdkmetric.InstrumentKind) metricdata.Temporality {
				return metricdata.CumulativeTemporality
			}
		}
	}

	// Build the exporter — HTTP or gRPC.
	protocol := "grpc"
	if otlpCfg.Protocol != nil {
		protocol = *otlpCfg.Protocol
	}

	var exp sdkmetric.Exporter
	var err error
	switch protocol {
	case "http/protobuf", "http":
		opts := []otlpmetrichttp.Option{
			otlpmetrichttp.WithHeaders(headers),
			otlpmetrichttp.WithTemporalitySelector(temporality),
		}
		if otlpCfg.Endpoint != nil {
			opts = append(opts, otlpmetrichttp.WithEndpointURL(*otlpCfg.Endpoint))
		}
		if otlpCfg.Timeout != nil {
			opts = append(opts, otlpmetrichttp.WithTimeout(time.Duration(*otlpCfg.Timeout)*time.Millisecond))
		}
		exp, err = otlpmetrichttp.New(ctx, opts...)
	default: // grpc
		opts := []otlpmetricgrpc.Option{
			otlpmetricgrpc.WithHeaders(headers),
			otlpmetricgrpc.WithTemporalitySelector(temporality),
		}
		if otlpCfg.Endpoint != nil {
			opts = append(opts, otlpmetricgrpc.WithEndpoint(*otlpCfg.Endpoint))
		}
		if otlpCfg.Timeout != nil {
			opts = append(opts, otlpmetricgrpc.WithTimeout(time.Duration(*otlpCfg.Timeout)*time.Millisecond))
		}
		exp, err = otlpmetricgrpc.New(ctx, opts...)
	}
	if err != nil {
		return nil, err
	}

	var readerOpts []sdkmetric.PeriodicReaderOption
	if cfg.Interval != nil {
		readerOpts = append(readerOpts, sdkmetric.WithInterval(time.Duration(*cfg.Interval)*time.Millisecond))
	}
	if cfg.Timeout != nil {
		readerOpts = append(readerOpts, sdkmetric.WithTimeout(time.Duration(*cfg.Timeout)*time.Millisecond))
	}
	return sdkmetric.NewPeriodicReader(exp, readerOpts...), nil
}
