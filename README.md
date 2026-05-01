# ome-kafka-bridge

A Go service that impersonates a Kafka broker so that Dell OpenManage Enterprise
(OME) can push its telemetry, health, alerts, audit logs, and inventory data
directly to it — and then forwards everything to your existing observability
stack: **VictoriaMetrics** (or Mimir) via Prometheus `remote_write`, and
**Loki** for logs and events.

```
┌─────────────────┐   Kafka        ┌──────────────────────┐
│  Dell OME 4.x   │  PLAINTEXT     │   ome-kafka-bridge    │
│                 │ ─────────────► │   :9092 (fake broker) │
│ Remote Telemetry│  (port 9092)   │                       │
│ Configuration   │                │  parser → relabeler   │
└─────────────────┘                │      ↙         ↘      │
                                   │ remote_write   loki   │
                                   │ (metrics)    (logs)   │
                                   └────────┬──────────────┘
                                            │    ↓ disk cache
                                    ┌───────┴──────────┐
                                    │  VictoriaMetrics  │
                                    │  (metrics.mikeosude.com)
                                    │  Loki             │
                                    │  (metrics.mikeosude.com:3100)
                                    └──────────────────┘
```

## What OME sends and where it goes

| OME Topic         | Kind       | Destination        |
|-------------------|------------|--------------------|
| `ome.telemetry`   | metrics    | remote_write       |
| `ome.health`      | metrics    | remote_write       |
| `ome.alerts`      | log        | Loki               |
| `ome.auditlogs`   | log        | Loki               |
| `ome.inventory`   | log        | Loki               |

Routing is configurable via `topic_routing` in `config.yaml`.

## Metrics emitted to VictoriaMetrics

From `ome.telemetry`:
- `ome_telemetry_<MetricName>` — raw sensor readings (watts, celsius, RPM, etc.)

From `ome.health`:
- `ome_health_rollup_status` — overall device health (1000=OK, 2000=Warn, 3000=Critical)
- `ome_health_power_state` — device power state
- `ome_health_component_status{component="..."}` — per-component health

All metrics carry labels: `device_name`, `service_tag`, `model`, `device_type`,
`ip_address`, plus any configured `extra_labels`.

## Quick start

### 1. Build

```bash
go build -o ome-kafka-bridge ./cmd/ome-kafka-bridge
```

Or with Podman:

```bash
podman build -f Containerfile -t ome-kafka-bridge:latest .
```

### 2. Configure

Edit `config.yaml`. Key settings:

```yaml
kafka:
  listen_addr: "0.0.0.0:9092"
  advertised_host: ""   # what OME reconnects to
  ome_identifier: "ome"

remote_write:
  url: "[remote write url]"

loki:
  url: "[loki url]"
```

### 3. Run

```bash
./ome-kafka-bridge -config config.yaml
```

### 4. Configure OME

In OME → Configuration → Remote Connectivity → Kafka Connectivity:

| Field                    | Value                                      |
|--------------------------|--------------------------------------------|
| OME Identifier           | `ome`                                      |
| Kafka Bootstrap Server   | `[the fqdn]:9092`      |
| Authentication Mode      | `None`                                     |

Click through to Data Configuration and select the desired data types.

### 5. Verify

```bash
# Health check
curl http://localhost:8080/healthz

# Self-metrics (Prometheus format)
curl http://localhost:8080/metrics | grep ome_bridge

# Cache stats
curl http://localhost:8080/cache/stats
```

## Relabeling

The bridge supports Prometheus-compatible relabeling for both metrics and log
stream labels. Rules live in `relabel_configs` and `log_relabel_configs` in
`config.yaml`.

### Examples

Rename the `ServiceTag` label to `instance`:
```yaml
relabel_configs:
  - source_labels: ["ServiceTag"]
    target_label: "instance"
    action: "replace"
```

Drop all metrics for Lab devices:
```yaml
relabel_configs:
  - source_labels: ["DeviceGroup"]
    regex: "Lab.*"
    action: "drop"
```

Keep only Critical alerts in Loki:
```yaml
log_relabel_configs:
  - source_labels: ["severity"]
    regex: "critical"
    action: "keep"
```

## Disk cache

When VictoriaMetrics or Loki are unreachable, messages are spooled to
`/var/lib/ome-kafka-bridge/cache/` as JSON files. On reconnect, they are
replayed in order. Configure retention via:

```yaml
cache:
  enabled: true
  dir: "/var/lib/ome-kafka-bridge/cache"
  max_size_mb: 512
  retry_interval: "30s"
  max_age: "24h"
```

## Deployment

### systemd (bare metal / VM)

```bash
sudo install -o root -g root -m 755 ome-kafka-bridge /usr/local/bin/
sudo install -o root -g root -m 644 config.yaml /etc/ome-kafka-bridge/
sudo install -o root -g root -m 644 deploy/ome-kafka-bridge.service \
    /etc/systemd/system/
sudo useradd -r -s /sbin/nologin ome-bridge
sudo mkdir -p /var/lib/ome-kafka-bridge && \
    sudo chown ome-bridge:ome-bridge /var/lib/ome-kafka-bridge
sudo systemctl daemon-reload
sudo systemctl enable --now ome-kafka-bridge
```

### Ansible (homelab pattern)

Drop into your existing `bootstrap.yml` pattern:

```yaml
- name: Deploy ome-kafka-bridge
  hosts: ome_bridge
  roles:
    - role: ome_kafka_bridge
```

### Podman / Kubernetes

```bash
podman run -d \
  --name ome-kafka-bridge \
  -p 9092:9092 \
  -p 8080:8080 \
  -v /etc/ome-kafka-bridge/config.yaml:/etc/ome-kafka-bridge/config.yaml:ro \
  -v /var/lib/ome-kafka-bridge:/var/lib/ome-kafka-bridge \
  ome-kafka-bridge:latest
```

## Limitations & roadmap

- **SASL/SSL**: The broker currently accepts PLAINTEXT only. For secure
  environments, front it with HAProxy/stunnel for TLS termination, or add
  SASL_PLAINTEXT support (next milestone).
- **Multi-partition**: Currently exposes one partition per topic, which is
  sufficient for OME's producer.
- **Compression**: LZ4/GZIP decompression of RecordBatch payloads is not yet
  implemented; OME defaults to no compression.

## License

Apache 2.0 — consistent with the Dell OpenManage-Enterprise repository.
