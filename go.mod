module github.com/mikeosude/ome-kafka-bridge

go 1.22

require (
	github.com/golang/snappy v0.0.4
	github.com/prometheus/client_golang v1.19.1
	github.com/sirupsen/logrus v1.9.3
	gopkg.in/yaml.v3 v3.0.1
)

require (
	github.com/beorn7/perks v1.0.1 // indirect
	github.com/cespare/xxhash/v2 v2.3.0 // indirect
	github.com/prometheus/client_model v0.6.1 // indirect
	github.com/prometheus/common v0.53.0 // indirect
	github.com/prometheus/procfs v0.15.0 // indirect
	golang.org/x/sys v0.20.0 // indirect
	google.golang.org/protobuf v1.34.1 // indirect
)

replace (
	golang.org/x/sys => github.com/golang/sys v0.20.0
	google.golang.org/protobuf => github.com/protocolbuffers/protobuf-go v1.34.1
	gopkg.in/check.v1 => github.com/go-check/check v0.0.0-20161208181325-20d25e280405
	gopkg.in/yaml.v3 => github.com/go-yaml/yaml/v3 v3.0.1
)
