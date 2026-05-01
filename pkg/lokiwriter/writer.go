// Package lokiwriter implements the Loki push API (POST /loki/api/v1/push).
// It serializes log entries as JSON using the Loki HTTP push format.
package lokiwriter

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"sort"
	"strings"
	"time"

	"github.com/sirupsen/logrus"
)

// Entry is a single log line with its stream labels and timestamp.
type Entry struct {
	StreamLabels map[string]string
	Timestamp    time.Time
	Line         string
}

// Writer pushes log entries to a Loki endpoint.
type Writer struct {
	url    string
	client *http.Client
	log    *logrus.Logger
}

func New(url string, client *http.Client, log *logrus.Logger) *Writer {
	return &Writer{url: url, client: client, log: log}
}

// ─── Loki push payload structs ────────────────────────────────────────────────

type pushRequest struct {
	Streams []stream `json:"streams"`
}

type stream struct {
	Stream map[string]string `json:"stream"`
	Values [][]string        `json:"values"` // [timestamp_ns_string, log_line]
}

// Push sends a batch of log entries to Loki.
// Entries with identical stream labels are grouped into a single stream.
func (w *Writer) Push(entries []Entry) error {
	// Group entries by stream fingerprint
	grouped := make(map[string]*stream)

	for _, e := range entries {
		fp := fingerprint(e.StreamLabels)
		if _, ok := grouped[fp]; !ok {
			grouped[fp] = &stream{Stream: e.StreamLabels}
		}
		tsNs := fmt.Sprintf("%d", e.Timestamp.UnixNano())
		grouped[fp].Values = append(grouped[fp].Values, []string{tsNs, e.Line})
	}

	streams := make([]stream, 0, len(grouped))
	for _, s := range grouped {
		streams = append(streams, *s)
	}

	req := pushRequest{Streams: streams}
	body, err := json.Marshal(req)
	if err != nil {
		return fmt.Errorf("marshal loki push request: %w", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, w.url, bytes.NewReader(body))
	if err != nil {
		return err
	}
	httpReq.Header.Set("Content-Type", "application/json")

	resp, err := w.client.Do(httpReq)
	if err != nil {
		return fmt.Errorf("loki HTTP request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode/100 != 2 {
		return fmt.Errorf("loki returned HTTP %d", resp.StatusCode)
	}
	return nil
}

// fingerprint produces a stable string key from a label set.
func fingerprint(labels map[string]string) string {
	keys := make([]string, 0, len(labels))
	for k := range labels {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	var sb strings.Builder
	for _, k := range keys {
		sb.WriteString(k)
		sb.WriteByte('=')
		sb.WriteString(labels[k])
		sb.WriteByte(',')
	}
	return sb.String()
}
