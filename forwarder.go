// Package forwarder handles sending parsed metrics to a Prometheus remote_write
// endpoint and log entries to Loki.
package forwarder

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/base64"
	"fmt"
	"net/http"
	"os"
	"sync"
	"time"

	"github.com/golang/snappy"
	"github.com/sirupsen/logrus"

	cfg "github.com/mikeosude/ome-kafka-bridge/internal/config"
	"github.com/mikeosude/ome-kafka-bridge/internal/parser"
	"github.com/mikeosude/ome-kafka-bridge/internal/relabel"
	"github.com/mikeosude/ome-kafka-bridge/pkg/lokiwriter"
	prompb "github.com/mikeosude/ome-kafka-bridge/pkg/prompb"
)

// MetricsForwarder batches MetricSamples and ships them via Prometheus remote_write.
type MetricsForwarder struct {
	cfg         cfg.RemoteWriteConfig
	extraLabels map[string]string
	relabelEng  *relabel.Engine
	client      *http.Client
	log         *logrus.Logger

	mu      sync.Mutex
	pending []parser.MetricSample

	flushTicker *time.Ticker
	done        chan struct{}
}

func NewMetricsForwarder(
	rwCfg cfg.RemoteWriteConfig,
	extraLabels map[string]string,
	relabelCfgs []cfg.RelabelConfig,
	log *logrus.Logger,
) (*MetricsForwarder, error) {
	eng, err := relabel.New(relabelCfgs)
	if err != nil {
		return nil, fmt.Errorf("compiling relabel rules: %w", err)
	}

	httpClient, err := buildHTTPClient(rwCfg.TLS, rwCfg.Timeout.Duration)
	if err != nil {
		return nil, fmt.Errorf("building HTTP client: %w", err)
	}

	f := &MetricsForwarder{
		cfg:         rwCfg,
		extraLabels: extraLabels,
		relabelEng:  eng,
		client:      httpClient,
		log:         log,
		done:        make(chan struct{}),
	}
	f.flushTicker = time.NewTicker(rwCfg.FlushInterval.Duration)
	go f.flushLoop()
	return f, nil
}

func (f *MetricsForwarder) Submit(samples []parser.MetricSample) {
	f.mu.Lock()
	f.pending = append(f.pending, samples...)
	sz := len(f.pending)
	f.mu.Unlock()

	if sz >= f.cfg.BatchSize {
		f.flush()
	}
}

func (f *MetricsForwarder) flushLoop() {
	for {
		select {
		case <-f.flushTicker.C:
			f.flush()
		case <-f.done:
			f.flush()
			return
		}
	}
}

func (f *MetricsForwarder) flush() {
	f.mu.Lock()
	if len(f.pending) == 0 {
		f.mu.Unlock()
		return
	}
	batch := f.pending
	f.pending = nil
	f.mu.Unlock()

	// Apply relabeling + extra labels
	ts := make([]prompb.TimeSeries, 0, len(batch))
	for _, s := range batch {
		// Merge extra labels
		lbls := make(map[string]string, len(s.Labels)+len(f.extraLabels))
		for k, v := range f.extraLabels {
			lbls[k] = v
		}
		for k, v := range s.Labels {
			lbls[k] = v
		}
		lbls["__name__"] = s.Name

		if !f.relabelEng.Apply(lbls) {
			continue // dropped
		}

		pbLbls := make([]prompb.Label, 0, len(lbls))
		for k, v := range lbls {
			pbLbls = append(pbLbls, prompb.Label{Name: k, Value: v})
		}

		ts = append(ts, prompb.TimeSeries{
			Labels: pbLbls,
			Samples: []prompb.Sample{{
				Value:     s.Value,
				Timestamp: s.Timestamp.UnixMilli(),
			}},
		})
	}

	if len(ts) == 0 {
		return
	}

	req := &prompb.WriteRequest{Timeseries: ts}
	data, err := req.Marshal()
	if err != nil {
		f.log.WithError(err).Error("marshal remote_write protobuf")
		return
	}

	compressed := snappy.Encode(nil, data)
	if err := f.send(compressed); err != nil {
		f.log.WithError(err).WithField("series_count", len(ts)).
			Error("remote_write send failed")
		// TODO: hand off to cache
		return
	}
	f.log.WithField("series_count", len(ts)).Debug("remote_write flushed")
}

func (f *MetricsForwarder) send(body []byte) error {
	ctx, cancel := context.WithTimeout(context.Background(), f.cfg.Timeout.Duration)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, f.cfg.URL, bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/x-protobuf")
	req.Header.Set("Content-Encoding", "snappy")
	req.Header.Set("X-Prometheus-Remote-Write-Version", "0.1.0")

	if f.cfg.Username != "" {
		creds := base64.StdEncoding.EncodeToString([]byte(f.cfg.Username + ":" + f.cfg.Password))
		req.Header.Set("Authorization", "Basic "+creds)
	}

	resp, err := f.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode/100 != 2 {
		return fmt.Errorf("remote_write returned HTTP %d", resp.StatusCode)
	}
	return nil
}

func (f *MetricsForwarder) Close() {
	f.flushTicker.Stop()
	close(f.done)
}

// ─── Loki Forwarder ───────────────────────────────────────────────────────────

// LogForwarder batches LogEntries and ships them to Loki.
type LogForwarder struct {
	cfg         cfg.LokiConfig
	extraLabels map[string]string
	relabelEng  *relabel.Engine
	writer      *lokiwriter.Writer
	log         *logrus.Logger

	mu      sync.Mutex
	pending []parser.LogEntry

	flushTicker *time.Ticker
	done        chan struct{}
}

func NewLogForwarder(
	lokiCfg cfg.LokiConfig,
	extraLabels map[string]string,
	relabelCfgs []cfg.RelabelConfig,
	log *logrus.Logger,
) (*LogForwarder, error) {
	eng, err := relabel.New(relabelCfgs)
	if err != nil {
		return nil, fmt.Errorf("compiling log relabel rules: %w", err)
	}

	httpClient, err := buildHTTPClient(lokiCfg.TLS, lokiCfg.Timeout.Duration)
	if err != nil {
		return nil, fmt.Errorf("building Loki HTTP client: %w", err)
	}

	w := lokiwriter.New(lokiCfg.URL, httpClient, log)
	f := &LogForwarder{
		cfg:         lokiCfg,
		extraLabels: extraLabels,
		relabelEng:  eng,
		writer:      w,
		log:         log,
		done:        make(chan struct{}),
	}
	f.flushTicker = time.NewTicker(lokiCfg.FlushInterval.Duration)
	go f.flushLoop()
	return f, nil
}

func (f *LogForwarder) Submit(entry parser.LogEntry) {
	f.mu.Lock()
	f.pending = append(f.pending, entry)
	sz := len(f.pending)
	f.mu.Unlock()

	if sz >= f.cfg.BatchSize {
		f.flush()
	}
}

func (f *LogForwarder) flushLoop() {
	for {
		select {
		case <-f.flushTicker.C:
			f.flush()
		case <-f.done:
			f.flush()
			return
		}
	}
}

func (f *LogForwarder) flush() {
	f.mu.Lock()
	if len(f.pending) == 0 {
		f.mu.Unlock()
		return
	}
	batch := f.pending
	f.pending = nil
	f.mu.Unlock()

	entries := make([]lokiwriter.Entry, 0, len(batch))
	for _, e := range batch {
		streamLabels := make(map[string]string, len(e.StreamLabels)+len(f.extraLabels))
		for k, v := range f.extraLabels {
			streamLabels[k] = v
		}
		for k, v := range e.StreamLabels {
			streamLabels[k] = v
		}
		if !f.relabelEng.Apply(streamLabels) {
			continue
		}
		entries = append(entries, lokiwriter.Entry{
			StreamLabels: streamLabels,
			Timestamp:    e.Timestamp,
			Line:         e.Line,
		})
	}

	if len(entries) == 0 {
		return
	}

	if err := f.writer.Push(entries); err != nil {
		f.log.WithError(err).WithField("entry_count", len(entries)).
			Error("loki push failed")
		// TODO: hand off to cache
		return
	}
	f.log.WithField("entry_count", len(entries)).Debug("loki flushed")
}

func (f *LogForwarder) Close() {
	f.flushTicker.Stop()
	close(f.done)
}

// ─── TLS helper ──────────────────────────────────────────────────────────────

func buildHTTPClient(tlsCfg cfg.TLSConfig, timeout time.Duration) (*http.Client, error) {
	tlsConfig := &tls.Config{
		InsecureSkipVerify: tlsCfg.InsecureSkipVerify, //nolint:gosec
	}

	if tlsCfg.CAFile != "" {
		ca, err := os.ReadFile(tlsCfg.CAFile)
		if err != nil {
			return nil, fmt.Errorf("reading CA file: %w", err)
		}
		pool := x509.NewCertPool()
		pool.AppendCertsFromPEM(ca)
		tlsConfig.RootCAs = pool
	}

	if tlsCfg.CertFile != "" && tlsCfg.KeyFile != "" {
		cert, err := tls.LoadX509KeyPair(tlsCfg.CertFile, tlsCfg.KeyFile)
		if err != nil {
			return nil, fmt.Errorf("loading client cert/key: %w", err)
		}
		tlsConfig.Certificates = []tls.Certificate{cert}
	}

	return &http.Client{
		Timeout: timeout,
		Transport: &http.Transport{
			TLSClientConfig: tlsConfig,
		},
	}, nil
}
