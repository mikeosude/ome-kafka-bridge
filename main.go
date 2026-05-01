package main

import (
	"context"
	"encoding/json"
	"flag"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/sirupsen/logrus"

	"github.com/ifesi/ome-kafka-bridge/internal/broker"
	"github.com/ifesi/ome-kafka-bridge/internal/cache"
	cfg "github.com/ifesi/ome-kafka-bridge/internal/config"
	"github.com/ifesi/ome-kafka-bridge/internal/forwarder"
	"github.com/ifesi/ome-kafka-bridge/internal/parser"
)

// ─── Self-observability metrics ───────────────────────────────────────────────

var (
	msgsReceived = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "ome_bridge_messages_received_total",
		Help: "Total Kafka messages received from OME by topic.",
	}, []string{"topic", "kind"})

	msgsDropped = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "ome_bridge_messages_dropped_total",
		Help: "Total messages dropped (parse error or relabeling).",
	}, []string{"topic", "reason"})

	forwardLatency = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "ome_bridge_forward_duration_seconds",
		Help:    "Time spent forwarding a batch.",
		Buckets: prometheus.DefBuckets,
	}, []string{"dest"})

	cacheEntries = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "ome_bridge_cache_entries",
		Help: "Number of cached (unsent) entries on disk.",
	})
)

func main() {
	configFile := flag.String("config", "config.yaml", "Path to config.yaml")
	flag.Parse()

	// ─── Config ───────────────────────────────────────────────────────────────
	conf, err := cfg.Load(*configFile)
	if err != nil {
		logrus.WithError(err).Fatal("load config")
	}

	// ─── Logger ───────────────────────────────────────────────────────────────
	log := logrus.New()
	if conf.Log.Format == "json" {
		log.SetFormatter(&logrus.JSONFormatter{})
	}
	lvl, err := logrus.ParseLevel(conf.Log.Level)
	if err != nil {
		lvl = logrus.InfoLevel
	}
	log.SetLevel(lvl)

	log.WithFields(logrus.Fields{
		"listen":       conf.Kafka.ListenAddr,
		"remote_write": conf.RemoteWrite.URL,
		"loki":         conf.Loki.URL,
	}).Info("ome-kafka-bridge starting")

	// ─── Cache ────────────────────────────────────────────────────────────────
	diskCache, err := cache.New(conf.Cache, log)
	if err != nil {
		log.WithError(err).Fatal("init cache")
	}

	// ─── Forwarders ───────────────────────────────────────────────────────────
	metricsFilter := buildTopicSet(conf.TopicRouting.MetricsTopics)
	logFilter := buildTopicSet(conf.TopicRouting.LogTopics)

	metricsFwd, err := forwarder.NewMetricsForwarder(
		conf.RemoteWrite,
		conf.ExtraLabels,
		conf.RelabelConfigs,
		log,
	)
	if err != nil {
		log.WithError(err).Fatal("init metrics forwarder")
	}

	logFwd, err := forwarder.NewLogForwarder(
		conf.Loki,
		conf.ExtraLabels,
		conf.LogRelabelCfgs,
		log,
	)
	if err != nil {
		log.WithError(err).Fatal("init log forwarder")
	}

	// Wire cache retry callbacks
	if diskCache != nil {
		diskCache.OnRetryMetrics = func(samples []parser.MetricSample) {
			t := time.Now()
			metricsFwd.Submit(samples)
			forwardLatency.WithLabelValues("remote_write_retry").
				Observe(time.Since(t).Seconds())
		}
		diskCache.OnRetryLogs = func(entries []parser.LogEntry) {
			for _, e := range entries {
				logFwd.Submit(e)
			}
		}
	}

	// ─── Kafka Broker ─────────────────────────────────────────────────────────
	kb := broker.New(conf.Kafka, log)
	if err := kb.Start(); err != nil {
		log.WithError(err).Fatal("start kafka broker")
	}

	// ─── Message pipeline ─────────────────────────────────────────────────────
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case msg, ok := <-kb.Messages():
				if !ok {
					return
				}
				processMessage(msg, metricsFilter, logFilter, metricsFwd, logFwd, diskCache, log)
			}
		}
	}()

	// ─── HTTP API ─────────────────────────────────────────────────────────────
	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.Handler())
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	})
	mux.HandleFunc("/cache/stats", func(w http.ResponseWriter, r *http.Request) {
		stats := diskCache.Stats()
		cacheEntries.Set(float64(stats.EntryCount))
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(stats)
	})

	srv := &http.Server{
		Addr:    conf.API.ListenAddr,
		Handler: mux,
	}
	go func() {
		log.WithField("addr", conf.API.ListenAddr).Info("HTTP API listening")
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.WithError(err).Error("HTTP API server")
		}
	}()

	// ─── Graceful shutdown ────────────────────────────────────────────────────
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGTERM, syscall.SIGINT)
	<-sigCh

	log.Info("shutting down...")
	cancel()
	kb.Close()
	metricsFwd.Close()
	logFwd.Close()
	diskCache.Close()

	shutCtx, shutCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer shutCancel()
	_ = srv.Shutdown(shutCtx)

	log.Info("bye")
}

// processMessage decodes and routes a single Kafka message.
func processMessage(
	msg broker.Message,
	metricsTopics, logTopics map[string]bool,
	mFwd *forwarder.MetricsForwarder,
	lFwd *forwarder.LogForwarder,
	dCache *cache.Cache,
	log *logrus.Logger,
) {
	parsed, err := parser.Parse(msg.Topic, msg.Value)
	if err != nil {
		log.WithError(err).WithField("topic", msg.Topic).Warn("parse message")
		msgsDropped.WithLabelValues(msg.Topic, "parse_error").Inc()
		return
	}

	msgsReceived.WithLabelValues(msg.Topic, string(parsed.Kind)).Inc()

	if metricsTopics[msg.Topic] && len(parsed.Metrics) > 0 {
		t := time.Now()
		mFwd.Submit(parsed.Metrics)
		forwardLatency.WithLabelValues("remote_write").Observe(time.Since(t).Seconds())
	}

	if logTopics[msg.Topic] && parsed.LogEntry != nil {
		lFwd.Submit(*parsed.LogEntry)
	}
}

func buildTopicSet(topics []string) map[string]bool {
	m := make(map[string]bool, len(topics))
	for _, t := range topics {
		m[strings.TrimSpace(t)] = true
	}
	return m
}
