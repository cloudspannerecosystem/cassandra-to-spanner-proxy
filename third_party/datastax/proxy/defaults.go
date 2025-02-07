package proxy

import (
	"fmt"
	"regexp"
)

// Defaults for Spanner Settings.
const (
	DefaultSpannerGrpcChannels = 4
	DefaultSpannerMinSession   = 100
	DefaultSpannerMaxSession   = 400
	DefaultConfigTableName     = "TableConfigurations"
)

// ApplyDefaults applies default values to the configuration after it is loaded
func ValidateAndApplyDefaults(cfg *UserConfig) error {
	if len(cfg.Listeners) == 0 {
		return fmt.Errorf("listener configuration is missing in `config.yaml`")
	}
	if cfg.Otel != nil && cfg.Otel.Enabled {
		if cfg.Otel.Metrics.Enabled && (cfg.Otel.Metrics.Endpoint == "" || cfg.Otel.ServiceName == "") {
			return fmt.Errorf("define all of these parameters in config - otel.metrics.endpoint, otel.serviceName")
		}

		if cfg.Otel.Traces.Enabled && (cfg.Otel.Traces.Endpoint == "" || cfg.Otel.ServiceName == "") {
			return fmt.Errorf("define all of these parameters in config - otel.traces.endpoint, otel.serviceName")
		}

		// assign default value for SamplingRatio
		if cfg.Otel.Traces.SamplingRatio == 0 {
			cfg.Otel.Traces.SamplingRatio = 0.05
		}
	}

	for i := range cfg.Listeners {
		if cfg.Listeners[i].Spanner.Session.Min == 0 {
			cfg.Listeners[i].Spanner.Session.Min = uint64(DefaultSpannerMinSession)
		}
		if cfg.Listeners[i].Spanner.Session.Max == 0 {
			cfg.Listeners[i].Spanner.Session.Max = uint64(DefaultSpannerMaxSession)
		}
		if cfg.Listeners[i].Spanner.Session.GrpcChannels == 0 {
			cfg.Listeners[i].Spanner.Session.GrpcChannels = DefaultSpannerGrpcChannels
		}
		if cfg.Listeners[i].Spanner.ConfigTableName == "" {
			if cfg.CassandraToSpannerConfigs.ConfigTableName == "" {
				cfg.Listeners[i].Spanner.ConfigTableName = DefaultConfigTableName
			} else {
				cfg.Listeners[i].Spanner.ConfigTableName = cfg.CassandraToSpannerConfigs.ConfigTableName
			}
		}
		if cfg.Listeners[i].Spanner.DatabaseID == "" {
			return fmt.Errorf("database id is not defined for listener %s %d", cfg.Listeners[i].Name, cfg.Listeners[i].Port)
		}
		if cfg.CassandraToSpannerConfigs.Endpoint != "" && !regexp.MustCompile(`.*\.googleapis\.com.*`).MatchString(cfg.CassandraToSpannerConfigs.Endpoint) {
			cfg.CassandraToSpannerConfigs.ProjectID = "default"
			cfg.Listeners[i].Spanner.InstanceID = "default"
		}
		if cfg.Listeners[i].Spanner.ProjectID == "" {
			if cfg.CassandraToSpannerConfigs.ProjectID == "" {
				return fmt.Errorf("project id is not defined for listener %s %d", cfg.Listeners[i].Name, cfg.Listeners[i].Port)
			}
			cfg.Listeners[i].Spanner.ProjectID = cfg.CassandraToSpannerConfigs.ProjectID
		}
		if cfg.Listeners[i].Spanner.InstanceID == "" {
			return fmt.Errorf("instance id is not defined for listener %s %d", cfg.Listeners[i].Name, cfg.Listeners[i].Port)
		}
		if cfg.Listeners[i].Spanner.Operation.MaxCommitDelay > 500 {
			return fmt.Errorf("the max commit delay value should be between 0 and 500 ms")
		}
	}
	return nil
}
