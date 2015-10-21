package config

import (
	"encoding/json"
	"io"
	"os"
)

type Config struct {
	Syslog     string
	Deployment string
	Zone       string
	Job        string
	Index      uint

	LegacyIncomingMessagesPort    int
	DropsondeIncomingMessagesPort int

	EtcdUrls                      []string
	EtcdMaxConcurrentRequests     int
	EtcdQueryIntervalMilliseconds int

	LoggregatorDropsondePort int
	SharedSecret             string

	MetricBatchIntervalSeconds uint

	PreferredProtocol string
}

func ParseConfig(configFile string) (*Config, error) {
	file, err := os.Open(configFile)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	return Parse(file)
}

func Parse(reader io.Reader) (*Config, error) {
	config := &Config{}
	err := json.NewDecoder(reader).Decode(config)
	if err != nil {
		return nil, err
	}

	if config.MetricBatchIntervalSeconds == 0 {
		config.MetricBatchIntervalSeconds = 15
	}

	if config.PreferredProtocol == "" {
		config.PreferredProtocol = "tls"
	}

	return config, nil
}
