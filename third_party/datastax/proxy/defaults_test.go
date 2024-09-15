package proxy

import (
	"testing"
)

func TestValidateAndApplyDefaultsNoListeners(t *testing.T) {
	cfg := &UserConfig{}
	err := ValidateAndApplyDefaults(cfg)
	if err == nil || err.Error() != "listener configuration is missing in `config.yaml`" {
		t.Errorf("Expected error for missing listeners, got: %v", err)
	}
}

func TestValidateAndApplyDefaultsMissingOtelEndpoints(t *testing.T) {
	cfg := &UserConfig{
		Listeners: []Listener{
			{
				Name: "Listener1",
				Port: 8080,
				Spanner: Spanner{
					DatabaseID: "db-1",
				},
			},
		},
		Otel: &OtelConfig{
			Enabled: true,
			Metrics: struct {
				Endpoint string `yaml:"endpoint"`
			}{Endpoint: ""},
			Traces: struct {
				Endpoint      string  `yaml:"endpoint"`
				SamplingRatio float64 `yaml:"samplingRatio"`
			}{Endpoint: "", SamplingRatio: 0},
		},
	}
	err := ValidateAndApplyDefaults(cfg)
	expectedError := "define all of these parameters in config - otel.metrics.endpoint, otel.traces.endpoint, otel.serviceName"
	if err == nil || err.Error() != expectedError {
		t.Errorf("Expected error for missing Otel endpoints, got: %v", err)
	}
}

func TestValidateAndApplyOverrideMaxCommitDelayAndReplayProtection(t *testing.T) {
	cfg := &UserConfig{
		Listeners: []Listener{
			{
				Name: "Listener1",
				Port: 8080,
				Spanner: Spanner{
					DatabaseID: "db-1",
					InstanceID: "inst-1",
					ProjectID:  "proj-1",
					Operation:  Operation{MaxCommitDelay: 100, ReplayProtection: true},
				},
			},
		},
	}
	err := ValidateAndApplyDefaults(cfg)
	if err != nil {
		t.Errorf("Did not expect an error, got: %v", err)
	}
	l := cfg.Listeners[0]
	if l.Spanner.Operation.MaxCommitDelay != 100 {
		t.Errorf("Override to MaxCommitDelay failed: %d", l.Spanner.Operation.MaxCommitDelay)
	}

	if l.Spanner.Operation.ReplayProtection != true {
		t.Errorf("Override to ReplayProtection failed: %t", l.Spanner.Operation.ReplayProtection)
	}
}

func TestValidateAndApplyDefaultsLargeMaxCommitDelay(t *testing.T) {
	cfg := &UserConfig{
		Listeners: []Listener{
			{
				Name: "Listener1",
				Port: 8080,
				Spanner: Spanner{
					DatabaseID: "db-1",
					InstanceID: "inst-1",
					ProjectID:  "proj-1",
					Operation:  Operation{MaxCommitDelay: 1000},
				},
			},
		},
	}
	err := ValidateAndApplyDefaults(cfg)
	expectedError := "the max commit delay value should be between 0 and 500 ms"
	if err == nil || err.Error() != expectedError {
		t.Errorf("Expected error for missing Otel endpoints, got: %v", err)
	}
}

func TestValidateAndApplyDefaultsDefaultsApplied(t *testing.T) {
	cfg := &UserConfig{
		Listeners: []Listener{
			{
				Name: "Listener1",
				Port: 8080,
				Spanner: Spanner{
					DatabaseID: "db-1",
					InstanceID: "inst-1",
					ProjectID:  "", // This should take default from CassandraToSpannerConfigs
					Session:    Session{Min: 0, Max: 0, GrpcChannels: 0},
				},
			},
		},
		Otel: &OtelConfig{
			Enabled:     true,
			ServiceName: "SomeService", // Add this to satisfy the required condition
			Metrics: struct {
				Endpoint string `yaml:"endpoint"`
			}{Endpoint: "metrics.endpoint"},
			Traces: struct {
				Endpoint      string  `yaml:"endpoint"`
				SamplingRatio float64 `yaml:"samplingRatio"`
			}{Endpoint: "traces.endpoint", SamplingRatio: 0},
		},
		CassandraToSpannerConfigs: CassandraToSpannerConfigs{
			ProjectID:       "default-project",
			ConfigTableName: "",
		},
	}
	err := ValidateAndApplyDefaults(cfg)
	if err != nil {
		t.Errorf("Did not expect an error, got: %v", err)
	}

	l := cfg.Listeners[0]
	if l.Spanner.Session.Min != 100 || l.Spanner.Session.Max != 400 || l.Spanner.Session.GrpcChannels != 4 {
		t.Errorf("Defaults not applied correctly to Spanner session: %+v", l.Spanner.Session)
	}
	if l.Spanner.ConfigTableName != "TableConfigurations" {
		t.Errorf("Defaults not applied correctly to ConfigTableName: %s", l.Spanner.ConfigTableName)
	}
	if l.Spanner.ProjectID != "default-project" {
		t.Errorf("Defaults not applied correctly to ProjectID: %s", l.Spanner.ProjectID)
	}
	if cfg.Otel.Traces.SamplingRatio != 0.05 {
		t.Errorf("Default not applied to SamplingRatio: %f", cfg.Otel.Traces.SamplingRatio)
	}
	if l.Spanner.Operation.MaxCommitDelay != 0 {
		t.Errorf("Default not applied to MaxCommitDelay: %d", l.Spanner.Operation.MaxCommitDelay)
	}
}

func TestValidateAndApplyDefaultsMissingDatabaseID(t *testing.T) {
	cfg := &UserConfig{
		Listeners: []Listener{
			{
				Name: "Listener1",
				Port: 8080,
				Spanner: Spanner{
					DatabaseID: "",
					InstanceID: "inst-1",
					ProjectID:  "proj-1",
					Session:    Session{Min: 100, Max: 400, GrpcChannels: 4},
				},
			},
		},
	}
	err := ValidateAndApplyDefaults(cfg)
	if err == nil || err.Error() != "database id is not defined for listener Listener1 8080" {
		t.Errorf("Expected error for missing DatabaseID, got: %v", err)
	}
}
