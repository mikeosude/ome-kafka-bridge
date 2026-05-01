// Package parser decodes OME Kafka messages into a unified internal model.
// OME publishes JSON on each topic. The structures below reflect the known
// OME 4.x payload schemas for telemetry, health, alerts, auditlogs, and inventory.
package parser

import (
	"encoding/json"
	"fmt"
	"strings"
	"time"
)

// MessageKind identifies which OME data category a message belongs to.
type MessageKind string

const (
	KindTelemetry  MessageKind = "telemetry"
	KindHealth     MessageKind = "health"
	KindAlerts     MessageKind = "alerts"
	KindAuditLogs  MessageKind = "auditlogs"
	KindInventory  MessageKind = "inventory"
	KindUnknown    MessageKind = "unknown"
)

// ParsedMessage is the unified output of parsing any OME topic message.
type ParsedMessage struct {
	Kind      MessageKind
	Topic     string
	Timestamp time.Time

	// Metrics carries time-series data (for telemetry + health topics)
	Metrics []MetricSample

	// LogEntry carries structured log data (for alerts, auditlogs, inventory)
	LogEntry *LogEntry
}

// MetricSample is a single Prometheus-style metric data point.
type MetricSample struct {
	Name      string
	Labels    map[string]string
	Value     float64
	Timestamp time.Time
}

// LogEntry is a structured log record destined for Loki.
type LogEntry struct {
	Timestamp  time.Time
	Line       string
	StreamLabels map[string]string
}

// ─── OME Telemetry payload ────────────────────────────────────────────────────

type OMETelemetryMessage struct {
	DeviceID      int64                    `json:"DeviceId"`
	DeviceName    string                   `json:"DeviceName"`
	ServiceTag    string                   `json:"ServiceTag"`
	Model         string                   `json:"Model"`
	DeviceType    string                   `json:"DeviceType"`
	IPAddress     string                   `json:"IPAddress"`
	Timestamp     string                   `json:"Timestamp"`
	MetricList    []OMETelemetryMetric     `json:"MetricList"`
}

type OMETelemetryMetric struct {
	MetricName  string      `json:"MetricName"`
	MetricValue interface{} `json:"MetricValue"`
	Unit        string      `json:"Unit,omitempty"`
}

// ─── OME Health payload ───────────────────────────────────────────────────────

type OMEHealthMessage struct {
	DeviceID   int64  `json:"DeviceId"`
	DeviceName string `json:"DeviceName"`
	ServiceTag string `json:"ServiceTag"`
	Model      string `json:"Model"`
	DeviceType string `json:"DeviceType"`
	IPAddress  string `json:"IPAddress"`
	Timestamp  string `json:"Timestamp"`
	// RollupStatus: OK=1000, Warning=2000, Critical=3000, Unknown=4000
	RollupStatus        int    `json:"RollupStatus"`
	ConnectionStatus    string `json:"ConnectionStatus"`
	PowerState          int    `json:"PowerState"`
	// Component health map: component name → status code
	ComponentHealthMap map[string]int `json:"ComponentHealthMap,omitempty"`
}

// ─── OME Alerts payload ───────────────────────────────────────────────────────

type OMEAlertMessage struct {
	EventID         int64  `json:"EventId"`
	EventCategory   string `json:"EventCategory"`
	EventSource     string `json:"EventSource"`
	Severity        string `json:"Severity"`    // Critical, Warning, Info, Normal
	SubCategory     string `json:"SubCategory"`
	Message         string `json:"Message"`
	RecommendedAction string `json:"RecommendedAction,omitempty"`
	AcknowledgedBy  string `json:"AcknowledgedBy,omitempty"`
	Timestamp       string `json:"Timestamp"`
	DeviceID        int64  `json:"DeviceId,omitempty"`
	DeviceName      string `json:"DeviceName,omitempty"`
	ServiceTag      string `json:"ServiceTag,omitempty"`
	Model           string `json:"Model,omitempty"`
	IPAddress       string `json:"IpAddress,omitempty"`
}

// ─── OME AuditLog payload ─────────────────────────────────────────────────────

type OMEAuditLogMessage struct {
	LogID       int64  `json:"LogId"`
	Category    string `json:"Category"`
	SubCategory string `json:"SubCategory"`
	Severity    string `json:"Severity"`
	Message     string `json:"Message"`
	UserName    string `json:"UserName"`
	IPAddress   string `json:"IpAddress"`
	Timestamp   string `json:"Timestamp"`
}

// ─── OME Inventory payload ────────────────────────────────────────────────────

type OMEInventoryMessage struct {
	DeviceID    int64             `json:"DeviceId"`
	DeviceName  string            `json:"DeviceName"`
	ServiceTag  string            `json:"ServiceTag"`
	Model       string            `json:"Model"`
	DeviceType  string            `json:"DeviceType"`
	IPAddress   string            `json:"IPAddress"`
	Timestamp   string            `json:"Timestamp"`
	Inventory   map[string]interface{} `json:"Inventory"`
}

// ─── Parser ───────────────────────────────────────────────────────────────────

// Parse decodes a raw Kafka message from a given OME topic.
func Parse(topic string, value []byte) (*ParsedMessage, error) {
	kind := topicKind(topic)
	switch kind {
	case KindTelemetry:
		return parseTelemetry(topic, value)
	case KindHealth:
		return parseHealth(topic, value)
	case KindAlerts:
		return parseAlert(topic, value)
	case KindAuditLogs:
		return parseAuditLog(topic, value)
	case KindInventory:
		return parseInventory(topic, value)
	default:
		return nil, fmt.Errorf("unknown topic kind for topic %q", topic)
	}
}

func topicKind(topic string) MessageKind {
	parts := strings.SplitN(topic, ".", 2)
	if len(parts) < 2 {
		return KindUnknown
	}
	switch parts[1] {
	case "telemetry":
		return KindTelemetry
	case "health":
		return KindHealth
	case "alerts":
		return KindAlerts
	case "auditlogs":
		return KindAuditLogs
	case "inventory":
		return KindInventory
	default:
		return KindUnknown
	}
}

func parseTelemetry(topic string, value []byte) (*ParsedMessage, error) {
	var msg OMETelemetryMessage
	if err := json.Unmarshal(value, &msg); err != nil {
		return nil, fmt.Errorf("unmarshal telemetry: %w", err)
	}

	ts, _ := parseOMETime(msg.Timestamp)
	baseLabels := map[string]string{
		"device_id":   fmt.Sprintf("%d", msg.DeviceID),
		"device_name": msg.DeviceName,
		"service_tag": msg.ServiceTag,
		"model":       msg.Model,
		"device_type": msg.DeviceType,
		"ip_address":  msg.IPAddress,
		"topic":       topic,
	}

	out := &ParsedMessage{Kind: KindTelemetry, Topic: topic, Timestamp: ts}
	for _, m := range msg.MetricList {
		val, err := toFloat64(m.MetricValue)
		if err != nil {
			continue // skip non-numeric metrics
		}
		lbls := copyLabels(baseLabels)
		if m.Unit != "" {
			lbls["unit"] = m.Unit
		}
		out.Metrics = append(out.Metrics, MetricSample{
			Name:      sanitizeMetricName("ome_telemetry_" + m.MetricName),
			Labels:    lbls,
			Value:     val,
			Timestamp: ts,
		})
	}
	return out, nil
}

func parseHealth(topic string, value []byte) (*ParsedMessage, error) {
	var msg OMEHealthMessage
	if err := json.Unmarshal(value, &msg); err != nil {
		return nil, fmt.Errorf("unmarshal health: %w", err)
	}

	ts, _ := parseOMETime(msg.Timestamp)
	baseLabels := map[string]string{
		"device_id":   fmt.Sprintf("%d", msg.DeviceID),
		"device_name": msg.DeviceName,
		"service_tag": msg.ServiceTag,
		"model":       msg.Model,
		"device_type": msg.DeviceType,
		"ip_address":  msg.IPAddress,
		"topic":       topic,
	}

	out := &ParsedMessage{Kind: KindHealth, Topic: topic, Timestamp: ts}

	// Rollup health status
	out.Metrics = append(out.Metrics, MetricSample{
		Name:      "ome_health_rollup_status",
		Labels:    copyLabels(baseLabels),
		Value:     float64(msg.RollupStatus),
		Timestamp: ts,
	})

	// Power state
	out.Metrics = append(out.Metrics, MetricSample{
		Name:      "ome_health_power_state",
		Labels:    copyLabels(baseLabels),
		Value:     float64(msg.PowerState),
		Timestamp: ts,
	})

	// Per-component health
	for component, status := range msg.ComponentHealthMap {
		lbls := copyLabels(baseLabels)
		lbls["component"] = component
		out.Metrics = append(out.Metrics, MetricSample{
			Name:      "ome_health_component_status",
			Labels:    lbls,
			Value:     float64(status),
			Timestamp: ts,
		})
	}

	return out, nil
}

func parseAlert(topic string, value []byte) (*ParsedMessage, error) {
	var msg OMEAlertMessage
	if err := json.Unmarshal(value, &msg); err != nil {
		return nil, fmt.Errorf("unmarshal alert: %w", err)
	}

	ts, _ := parseOMETime(msg.Timestamp)
	line, _ := json.Marshal(msg)

	return &ParsedMessage{
		Kind:      KindAlerts,
		Topic:     topic,
		Timestamp: ts,
		LogEntry: &LogEntry{
			Timestamp: ts,
			Line:      string(line),
			StreamLabels: map[string]string{
				"topic":        topic,
				"job":          "ome_alerts",
				"severity":     strings.ToLower(msg.Severity),
				"category":     msg.EventCategory,
				"sub_category": msg.SubCategory,
				"device_name":  msg.DeviceName,
				"service_tag":  msg.ServiceTag,
				"device_type":  "server",
			},
		},
	}, nil
}

func parseAuditLog(topic string, value []byte) (*ParsedMessage, error) {
	var msg OMEAuditLogMessage
	if err := json.Unmarshal(value, &msg); err != nil {
		return nil, fmt.Errorf("unmarshal auditlog: %w", err)
	}

	ts, _ := parseOMETime(msg.Timestamp)
	line, _ := json.Marshal(msg)

	return &ParsedMessage{
		Kind:      KindAuditLogs,
		Topic:     topic,
		Timestamp: ts,
		LogEntry: &LogEntry{
			Timestamp: ts,
			Line:      string(line),
			StreamLabels: map[string]string{
				"topic":        topic,
				"job":          "ome_auditlogs",
				"severity":     strings.ToLower(msg.Severity),
				"category":     msg.Category,
				"sub_category": msg.SubCategory,
				"username":     msg.UserName,
				"ip_address":   msg.IPAddress,
			},
		},
	}, nil
}

func parseInventory(topic string, value []byte) (*ParsedMessage, error) {
	var msg OMEInventoryMessage
	if err := json.Unmarshal(value, &msg); err != nil {
		return nil, fmt.Errorf("unmarshal inventory: %w", err)
	}

	ts, _ := parseOMETime(msg.Timestamp)
	line, _ := json.Marshal(msg)

	return &ParsedMessage{
		Kind:      KindInventory,
		Topic:     topic,
		Timestamp: ts,
		LogEntry: &LogEntry{
			Timestamp: ts,
			Line:      string(line),
			StreamLabels: map[string]string{
				"topic":       topic,
				"job":         "ome_inventory",
				"device_name": msg.DeviceName,
				"service_tag": msg.ServiceTag,
				"model":       msg.Model,
				"device_type": msg.DeviceType,
				"ip_address":  msg.IPAddress,
			},
		},
	}, nil
}

// ─── Helpers ──────────────────────────────────────────────────────────────────

// OME uses ISO8601 / RFC3339. Try several common formats.
var omeTimeFormats = []string{
	time.RFC3339,
	time.RFC3339Nano,
	"2006-01-02T15:04:05",
	"2006-01-02T15:04:05Z",
	"2006-01-02 15:04:05",
}

func parseOMETime(s string) (time.Time, error) {
	for _, fmt := range omeTimeFormats {
		if t, err := time.Parse(fmt, s); err == nil {
			return t, nil
		}
	}
	return time.Now(), fmt.Errorf("could not parse time %q", s)
}

func toFloat64(v interface{}) (float64, error) {
	switch val := v.(type) {
	case float64:
		return val, nil
	case float32:
		return float64(val), nil
	case int:
		return float64(val), nil
	case int64:
		return float64(val), nil
	case json.Number:
		return val.Float64()
	case string:
		// Try to strip units like "42 W" → 42
		fields := strings.Fields(val)
		if len(fields) > 0 {
			var f float64
			_, err := fmt.Sscanf(fields[0], "%f", &f)
			if err == nil {
				return f, nil
			}
		}
		return 0, fmt.Errorf("non-numeric string: %q", val)
	default:
		return 0, fmt.Errorf("unsupported type %T", v)
	}
}

func copyLabels(src map[string]string) map[string]string {
	out := make(map[string]string, len(src))
	for k, v := range src {
		out[k] = v
	}
	return out
}

// sanitizeMetricName replaces characters invalid in Prometheus metric names.
func sanitizeMetricName(s string) string {
	var b strings.Builder
	for i, r := range s {
		switch {
		case r >= 'a' && r <= 'z', r >= 'A' && r <= 'Z', r == '_':
			b.WriteRune(r)
		case r >= '0' && r <= '9' && i > 0:
			b.WriteRune(r)
		default:
			b.WriteRune('_')
		}
	}
	return strings.ToLower(b.String())
}
