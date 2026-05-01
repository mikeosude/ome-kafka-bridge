package config

import (
	"fmt"
	"os"
	"time"

	"gopkg.in/yaml.v3"
)

type Config struct {
	Kafka            KafkaConfig            `yaml:"kafka"`
	RemoteWrite      RemoteWriteConfig      `yaml:"remote_write"`
	Loki             LokiConfig             `yaml:"loki"`
	Cache            CacheConfig            `yaml:"cache"`
	ExtraLabels      map[string]string      `yaml:"extra_labels"`
	RelabelConfigs   []RelabelConfig        `yaml:"relabel_configs"`
	LogRelabelCfgs   []RelabelConfig        `yaml:"log_relabel_configs"`
	TopicRouting     TopicRoutingConfig     `yaml:"topic_routing"`
	API              APIConfig              `yaml:"api"`
	Log              LogConfig              `yaml:"log"`
}

type KafkaConfig struct {
	ListenAddr        string   `yaml:"listen_addr"`
	OMEIdentifier     string   `yaml:"ome_identifier"`
	AdvertisedHost    string   `yaml:"advertised_host"`
	AdvertisedPort    int      `yaml:"advertised_port"`
	Topics            []string `yaml:"topics"`
	AutoCreateTopics  bool     `yaml:"auto_create_topics"`
	NumPartitions     int      `yaml:"num_partitions"`
	ReplicationFactor int      `yaml:"replication_factor"`
}

type TLSConfig struct {
	InsecureSkipVerify bool   `yaml:"insecure_skip_verify"`
	CAFile             string `yaml:"ca_file"`
	CertFile           string `yaml:"cert_file"`
	KeyFile            string `yaml:"key_file"`
}

type RemoteWriteConfig struct {
	URL           string        `yaml:"url"`
	Timeout       Duration      `yaml:"timeout"`
	Username      string        `yaml:"username"`
	Password      string        `yaml:"password"`
	TLS           TLSConfig     `yaml:"tls"`
	BatchSize     int           `yaml:"batch_size"`
	FlushInterval Duration      `yaml:"flush_interval"`
}

type LokiConfig struct {
	URL           string    `yaml:"url"`
	Timeout       Duration  `yaml:"timeout"`
	TLS           TLSConfig `yaml:"tls"`
	BatchSize     int       `yaml:"batch_size"`
	FlushInterval Duration  `yaml:"flush_interval"`
}

type CacheConfig struct {
	Enabled       bool     `yaml:"enabled"`
	Dir           string   `yaml:"dir"`
	MaxSizeMB     int64    `yaml:"max_size_mb"`
	RetryInterval Duration `yaml:"retry_interval"`
	MaxAge        Duration `yaml:"max_age"`
}

type RelabelConfig struct {
	SourceLabels []string `yaml:"source_labels"`
	Separator    string   `yaml:"separator"`
	TargetLabel  string   `yaml:"target_label"`
	Regex        string   `yaml:"regex"`
	Replacement  string   `yaml:"replacement"`
	Action       string   `yaml:"action"` // keep, drop, replace, labeldrop, labelkeep, labelmap
}

type TopicRoutingConfig struct {
	MetricsTopics []string `yaml:"metrics_topics"`
	LogTopics     []string `yaml:"log_topics"`
}

type APIConfig struct {
	ListenAddr string `yaml:"listen_addr"`
}

type LogConfig struct {
	Level  string `yaml:"level"`
	Format string `yaml:"format"`
}

// Duration wraps time.Duration for YAML unmarshalling ("30s", "5m", etc.)
type Duration struct {
	time.Duration
}

func (d *Duration) UnmarshalYAML(value *yaml.Node) error {
	dur, err := time.ParseDuration(value.Value)
	if err != nil {
		return fmt.Errorf("invalid duration %q: %w", value.Value, err)
	}
	d.Duration = dur
	return nil
}

func Load(path string) (*Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("reading config file: %w", err)
	}

	cfg := &Config{}
	if err := yaml.Unmarshal(data, cfg); err != nil {
		return nil, fmt.Errorf("parsing config: %w", err)
	}

	cfg.applyDefaults()
	return cfg, cfg.validate()
}

func (c *Config) applyDefaults() {
	if c.Kafka.ListenAddr == "" {
		c.Kafka.ListenAddr = "0.0.0.0:9092"
	}
	if c.Kafka.OMEIdentifier == "" {
		c.Kafka.OMEIdentifier = "ome"
	}
	if c.Kafka.NumPartitions == 0 {
		c.Kafka.NumPartitions = 1
	}
	if c.Kafka.ReplicationFactor == 0 {
		c.Kafka.ReplicationFactor = 1
	}
	if c.RemoteWrite.BatchSize == 0 {
		c.RemoteWrite.BatchSize = 500
	}
	if c.RemoteWrite.FlushInterval.Duration == 0 {
		c.RemoteWrite.FlushInterval.Duration = 10 * time.Second
	}
	if c.Loki.BatchSize == 0 {
		c.Loki.BatchSize = 100
	}
	if c.Loki.FlushInterval.Duration == 0 {
		c.Loki.FlushInterval.Duration = 5 * time.Second
	}
	if c.Cache.Dir == "" {
		c.Cache.Dir = "/var/lib/ome-kafka-bridge/cache"
	}
	if c.Cache.MaxSizeMB == 0 {
		c.Cache.MaxSizeMB = 512
	}
	if c.Cache.RetryInterval.Duration == 0 {
		c.Cache.RetryInterval.Duration = 30 * time.Second
	}
	if c.Cache.MaxAge.Duration == 0 {
		c.Cache.MaxAge.Duration = 24 * time.Hour
	}
	if c.API.ListenAddr == "" {
		c.API.ListenAddr = "0.0.0.0:8080"
	}
	if c.Log.Level == "" {
		c.Log.Level = "info"
	}
	if c.Log.Format == "" {
		c.Log.Format = "json"
	}
	if len(c.TopicRouting.MetricsTopics) == 0 {
		c.TopicRouting.MetricsTopics = []string{
			c.Kafka.OMEIdentifier + ".telemetry",
			c.Kafka.OMEIdentifier + ".health",
		}
	}
	if len(c.TopicRouting.LogTopics) == 0 {
		c.TopicRouting.LogTopics = []string{
			c.Kafka.OMEIdentifier + ".alerts",
			c.Kafka.OMEIdentifier + ".auditlogs",
			c.Kafka.OMEIdentifier + ".inventory",
		}
	}
}

func (c *Config) validate() error {
	if c.RemoteWrite.URL == "" {
		return fmt.Errorf("remote_write.url is required")
	}
	if c.Loki.URL == "" {
		return fmt.Errorf("loki.url is required")
	}
	return nil
}
