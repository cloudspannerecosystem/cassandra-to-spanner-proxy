// Copyright (c) DataStax, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package proxy

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"io/ioutil"
	"log"
	"net"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/alecthomas/kong"
	"github.com/cloudspannerecosystem/cassandra-to-spanner-proxy/third_party/datastax/proxycore"
	"github.com/cloudspannerecosystem/cassandra-to-spanner-proxy/utilities"
	"github.com/datastax/go-cassandra-native-protocol/primitive"
	"go.uber.org/zap"
	"gopkg.in/yaml.v2"
)

var (
	clusterPartitioner    = "org.apache.cassandra.dht.Murmur3Partitioner"
	clusterReleaseversion = "4.0.0.6816"
	defaultCqlVersion     = "3.4.5"
	TCP_BIND_PORT         = "0.0.0.0:%s"
	proxyReleaseVersion   = "v1.0.9"
)
var readFile = os.ReadFile
var atomicStartedListenersCounter atomic.Int32
var expectedListeners int // to be set in Run function

const defaultConfigFile = "config.yaml"

// Config holds all the configuration data
type UserConfig struct {
	CassandraToSpannerConfigs CassandraToSpannerConfigs `yaml:"cassandra_to_spanner_configs"`
	Listeners                 []Listener                `yaml:"listeners"`
	Otel                      *OtelConfig               `yaml:"otel"`
	LoggerConfig              *utilities.LoggerConfig   `yaml:"loggerConfig"`
}

// CassandraToSpannerConfigs contains configurations for Cassandra to Spanner
type CassandraToSpannerConfigs struct {
	KeyspaceFlatter        bool   `yaml:"keyspaceFlatter"`
	ProjectID              string `yaml:"projectId"`
	ConfigTableName        string `yaml:"configTableName"`
	UseRowTTL              bool   `yaml:"useRowTTL"`
	UseRowTimestamp        bool   `yaml:"useRowTimestamp"`
	Endpoint               string `yaml:"endpoint"`
	CaCertificate          string `yaml:"caCertificate"`
	ClientCertificate      string `yaml:"clientCertificate"`
	ClientKey              string `yaml:"clientKey"`
	UsePlainText           bool   `yaml:"usePlainText"`
	ReadinessCheckEndpoint string `yaml:"readinessCheckEndpoint"`
}

// OtelConfig defines the structure of the YAML configuration
type OtelConfig struct {
	Enabled                  bool   `yaml:"enabled"`
	EnabledClientSideMetrics bool   `yaml:"enabledClientSideMetrics"`
	ServiceName              string `yaml:"serviceName"`
	HealthCheck              struct {
		Enabled  bool   `yaml:"enabled"`
		Endpoint string `yaml:"endpoint"`
	} `yaml:"healthcheck"`
	Metrics struct {
		Enabled  bool   `yaml:"enabled"`
		Endpoint string `yaml:"endpoint"`
	} `yaml:"metrics"`
	Traces struct {
		Enabled       bool    `yaml:"enabled"`
		Endpoint      string  `yaml:"endpoint"`
		SamplingRatio float64 `yaml:"samplingRatio"`
	} `yaml:"traces"`
	DisableRandomServiceInstanceIDKey bool `yaml:"disableRandomServiceInstanceIDKey"`
}

// Listener represents each listener configuration
type Listener struct {
	Name    string  `yaml:"name"`
	Port    int     `yaml:"port"`
	Spanner Spanner `yaml:"spanner"`
	Otel    Otel    `yaml:"otel"`
}

// Spanner holds the Spanner database configuration
type Spanner struct {
	ProjectID       string    `yaml:"projectId"`
	InstanceID      string    `yaml:"instanceId"`
	DatabaseID      string    `yaml:"databaseId"`
	ConfigTableName string    `yaml:"configTableName"`
	Session         Session   `yaml:"Session"`
	Operation       Operation `yaml:"Operation"`
}

// Session describes the settings for Spanner sessions
type Session struct {
	Min          uint64 `yaml:"min"`
	Max          uint64 `yaml:"max"`
	GrpcChannels int    `yaml:"grpcChannels"`
}

// Spanner read/write operation settings.
type Operation struct {
	MaxCommitDelay   uint64 `yaml:"maxCommitDelay"`
	ReplayProtection bool   `yaml:"replayProtection"`
}

// Otel configures OpenTelemetry features
type Otel struct {
	Disabled bool `yaml:"disabled"`
}

type runConfig struct {
	Version            bool          `yaml:"version" help:"Show current proxy version" short:"v" default:"false" env:"PROXY_VERSION"`
	Username           string        `yaml:"username" help:"Username to use for authentication" short:"u" env:"USERNAME"`
	Password           string        `yaml:"password" help:"Password to use for authentication" short:"p" env:"PASSWORD"`
	ProtocolVersion    string        `yaml:"protocol-version" help:"Initial protocol version to use when connecting to the backend cluster (default: v4, options: v3, v4, v5, DSEv1, DSEv2)" default:"v4" short:"n" env:"PROTOCOL_VERSION"`
	MaxProtocolVersion string        `yaml:"max-protocol-version" help:"Max protocol version supported by the backend cluster (default: v4, options: v3, v4, v5, DSEv1, DSEv2)" default:"v4" short:"m" env:"MAX_PROTOCOL_VERSION"`
	Bind               string        `yaml:"bind" help:"Address to use to bind server" short:"a" default:":9042" env:"BIND"`
	Config             *os.File      `yaml:"-" help:"YAML configuration file" short:"f" env:"CONFIG_FILE"` // Not available in the configuration file
	Debug              bool          `yaml:"debug" help:"Show debug logging" default:"false" env:"DEBUG"`
	HeartbeatInterval  time.Duration `yaml:"heartbeat-interval" help:"Interval between performing heartbeats to the cluster" default:"30s" env:"HEARTBEAT_INTERVAL"`
	ConnectTimeout     time.Duration `yaml:"connect-timeout" help:"Duration before an attempt to connect to a cluster is considered timed out" default:"10s" env:"CONNECT_TIMEOUT"`
	IdleTimeout        time.Duration `yaml:"idle-timeout" help:"Duration between successful heartbeats before a connection to the cluster is considered unresponsive and closed" default:"60s" env:"IDLE_TIMEOUT"`
	ReadinessTimeout   time.Duration `yaml:"readiness-timeout" help:"Duration the proxy is unable to connect to the backend cluster before it is considered not ready" default:"30s" env:"READINESS_TIMEOUT"`
	ProxyCertFile      string        `yaml:"proxy-cert-file" help:"Path to a PEM encoded certificate file with its intermediate certificate chain. This is used to encrypt traffic for proxy clients" env:"PROXY_CERT_FILE"`
	ProxyKeyFile       string        `yaml:"proxy-key-file" help:"Path to a PEM encoded private key file. This is used to encrypt traffic for proxy clients" env:"PROXY_KEY_FILE"`
	DataCenter         string        `yaml:"data-center" help:"Data center to use in system tables" default:"datacenter1"  env:"DATA_CENTER"`
	ReleaseVersion     string        `yaml:"release-version" help:"Cluster Release version" default:"4.0.0.6816"  env:"RELEASE_VERSION"`
	Partitioner        string        `yaml:"partitioner" help:"Partitioner partitioner" default:"org.apache.cassandra.dht.Murmur3Partitioner"  env:"PARTITIONER"`
	Tokens             []string      `yaml:"tokens" help:"Tokens to use in the system tables. It's not recommended" env:"TOKENS"`
	CQLVersion         string        `yaml:"cql-version" help:"CQL version" default:"3.4.5"  env:"CQLVERSION"`
	LogLevel           string        `yaml:"log-level" help:"Log level configuration." default:"info" env:"LOG_LEVEL"`
}

func readinessCheckHandler(w http.ResponseWriter, r *http.Request) {
	started := int(atomicStartedListenersCounter.Load()) == expectedListeners

	if started {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("All listeners are up and running"))
	} else {
		w.WriteHeader(http.StatusServiceUnavailable)
		w.Write([]byte("Some listeners are still not ready"))
	}
}

// Run starts the proxy command. 'args' shouldn't include the executable (i.e. os.Args[1:]). It returns the exit code
// for the proxy.
func Run(ctx context.Context, args []string) int {
	var cfg runConfig
	var err error

	configFile := defaultConfigFile
	if configFileEnv := os.Getenv("CONFIG_FILE"); len(configFileEnv) != 0 {
		configFile = configFileEnv
	}

	UserConfig, err := LoadConfig(configFile)
	if err != nil {
		log.Fatalf("could not read configuration file %s: %v", configFile, err)
	}

	atomicStartedListenersCounter.Store(0)
	expectedListeners = len(UserConfig.Listeners)

	// Start HTTP server for readiness check if user has specified an endpoint for it.
	if UserConfig.CassandraToSpannerConfigs.ReadinessCheckEndpoint != "" {
		http.HandleFunc("/debug/health", readinessCheckHandler)
		httpServer := &http.Server{Addr: UserConfig.CassandraToSpannerConfigs.ReadinessCheckEndpoint}
		go func() {
			if err := httpServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
				log.Printf("Error when registering http health check endpoint:%v", err)
			}
		}()
		defer httpServer.Shutdown(ctx)
	}

	parser, err := kong.New(&cfg)
	if err != nil {
		panic(err)
	}

	var cliCtx *kong.Context
	if cliCtx, err = parser.Parse(args); err != nil {
		parser.Errorf("error parsing flags: %v", err)
		return 1
	}

	if cfg.Config != nil {
		bytes, err := ioutil.ReadAll(cfg.Config)
		if err != nil {
			cliCtx.Errorf("unable to read contents of configuration file '%s': %v", cfg.Config.Name(), err)
			return 1
		}
		err = yaml.Unmarshal(bytes, &cfg)
		if err != nil {
			cliCtx.Errorf("invalid YAML in configuration file '%s': %v", cfg.Config.Name(), err)
		}
	}

	var resolver proxycore.EndpointResolver
	if cfg.HeartbeatInterval >= cfg.IdleTimeout {
		cliCtx.Errorf("idle-timeout must be greater than heartbeat-interval (heartbeat interval: %s, idle timeout: %s)",
			cfg.HeartbeatInterval, cfg.IdleTimeout)
		return 1
	}

	var ok bool
	var version primitive.ProtocolVersion
	if version, ok = parseProtocolVersion(cfg.ProtocolVersion); !ok {
		cliCtx.Errorf("unsupported protocol version: %s", cfg.ProtocolVersion)
		return 1
	}

	var maxVersion primitive.ProtocolVersion
	if maxVersion, ok = parseProtocolVersion(cfg.MaxProtocolVersion); !ok {
		cliCtx.Errorf("unsupported max protocol version: %s", cfg.ProtocolVersion)
		return 1
	}

	if version > maxVersion {
		cliCtx.Errorf("default protocol version is greater than max protocol version")
		return 1
	}

	var partitioner string
	if cfg.Partitioner != "" {
		partitioner = cfg.Partitioner
	} else {
		partitioner = clusterPartitioner
	}

	var releaseVersion string
	if cfg.ReleaseVersion != "" {
		releaseVersion = cfg.ReleaseVersion
	} else {
		releaseVersion = clusterReleaseversion
	}

	var cqlVersion string
	if cfg.CQLVersion != "" {
		cqlVersion = cfg.CQLVersion
	} else {
		cqlVersion = defaultCqlVersion
	}

	if cfg.Debug {
		cfg.LogLevel = "debug"
	} else {
		if cfg.LogLevel == "debug" {
			cfg.Debug = true
		}

		flag := false
		supportedLogLevels := []string{"info", "debug", "error", "warn"}
		for _, level := range supportedLogLevels {
			if cfg.LogLevel == level {
				flag = true
			}
		}
		if !flag {
			cliCtx.Errorf("Invalid log-level should be [info/debug/error/warn]")
			return 1
		}
	}

	logger, err := utilities.SetupLogger(cfg.LogLevel, UserConfig.LoggerConfig)
	if err != nil {
		cliCtx.Errorf("unable to create logger")
		return 1
	}
	defer logger.Sync()
	if cfg.Version {
		cliCtx.Printf("Version - %s", proxyReleaseVersion)
		return 0
	}

	var auth proxycore.Authenticator

	if len(cfg.Username) > 0 || len(cfg.Password) > 0 {
		auth = proxycore.NewPasswordAuth(cfg.Username, cfg.Password)
	}
	if UserConfig.Otel == nil {
		UserConfig.Otel = &OtelConfig{
			Enabled: false,
		}
	} else {
		if UserConfig.Otel.Enabled {
			if UserConfig.Otel.Traces.SamplingRatio < 0 || UserConfig.Otel.Traces.SamplingRatio > 1 {
				cliCtx.Errorf("Sampling Ratio for Otel Traces should be between 0 and 1]")
				return 1
			}
		}
	}

	// config logs.
	logger.Info("Protocol Version:" + version.String())
	logger.Info("CQL Version:" + cqlVersion)
	logger.Info("Release Version:" + releaseVersion)
	logger.Info("Cassandra to Cloud Spanner Proxy Adapter Version:" + proxyReleaseVersion)
	logger.Info("Partitioner:" + partitioner)
	logger.Info("Data Center:" + cfg.DataCenter)
	logger.Info("Configured keyspace name flattening status", zap.Bool("isKeyspaceFlatteningEnabled", UserConfig.CassandraToSpannerConfigs.KeyspaceFlatter))
	enableDirectPath, err := strconv.ParseBool(strings.TrimSpace(os.Getenv("GOOGLE_SPANNER_ENABLE_DIRECT_ACCESS")))
	if err == nil && enableDirectPath {
		logger.Info("Google Spanner Direct Path Feature Enabled")
	} else {
		logger.Info("Google Spanner Direct Path Feature Disabled")
	}
	logger.Debug("Configuration - ", zap.Any("UserConfig", UserConfig))
	var wg sync.WaitGroup

	for _, listener := range UserConfig.Listeners {
		p, err1 := NewProxy(ctx, Config{
			Version:           version,
			MaxVersion:        maxVersion,
			Resolver:          resolver,
			Auth:              auth,
			Logger:            logger,
			HeartBeatInterval: cfg.HeartbeatInterval,
			ConnectTimeout:    cfg.ConnectTimeout,
			IdleTimeout:       cfg.IdleTimeout,
			DC:                cfg.DataCenter,
			Tokens:            cfg.Tokens,
			SpannerConfig: SpannerConfig{
				NumOfChannels:    listener.Spanner.Session.GrpcChannels,
				ConfigTableName:  listener.Spanner.ConfigTableName,
				InstanceName:     listener.Spanner.InstanceID,
				GCPProjectID:     listener.Spanner.ProjectID,
				DatabaseName:     listener.Spanner.DatabaseID,
				MaxSessions:      uint64(listener.Spanner.Session.Max),
				MinSessions:      uint64(listener.Spanner.Session.Min),
				MaxCommitDelay:   uint64(listener.Spanner.Operation.MaxCommitDelay),
				ReplayProtection: listener.Spanner.Operation.ReplayProtection,
			},
			Partitioner:       partitioner,
			ReleaseVersion:    releaseVersion,
			CQLVersion:        cqlVersion,
			OtelConfig:        UserConfig.Otel,
			KeyspaceFlatter:   UserConfig.CassandraToSpannerConfigs.KeyspaceFlatter,
			UseRowTimestamp:   UserConfig.CassandraToSpannerConfigs.UseRowTimestamp,
			UseRowTTL:         UserConfig.CassandraToSpannerConfigs.UseRowTTL,
			Debug:             cfg.Debug,
			UserAgent:         "cassandra-adapter/" + proxyReleaseVersion,
			Endpoint:          UserConfig.CassandraToSpannerConfigs.Endpoint,
			CaCertificate:     UserConfig.CassandraToSpannerConfigs.CaCertificate,
			ClientCertificate: UserConfig.CassandraToSpannerConfigs.ClientCertificate,
			ClientKey:         UserConfig.CassandraToSpannerConfigs.ClientKey,
			UsePlainText:      UserConfig.CassandraToSpannerConfigs.UsePlainText,
		})

		if err1 != nil {
			logger.Error(err1.Error())
			return 1
		}
		cfgloop := cfg
		cfgloop.Bind = fmt.Sprintf(TCP_BIND_PORT, strconv.Itoa(listener.Port))
		cfgloop.Bind = maybeAddPort(cfgloop.Bind, "9042")

		wg.Add(1)
		go func(cfg runConfig, p *Proxy) {
			defer wg.Done()
			err := cfg.listenAndServe(p, ctx, logger) // Use cfg2 or other instances as needed
			if err != nil {
				logger.Fatal("Error while serving - ", zap.Error(err))
			}
		}(cfgloop, p)

	}
	wg.Wait() // Wait for all servers to finish
	logger.Debug("\n>>>>>>>>>>>>> Closed All listeners <<<<<<<<<\n")

	return 0
}

// LoadConfig reads and parses the configuration from a YAML file
func LoadConfig(filename string) (*UserConfig, error) {
	data, err := readFile(filename)
	if err != nil {
		return nil, fmt.Errorf("failed to read config file: %w", err)
	}

	var config UserConfig
	if err = yaml.Unmarshal(data, &config); err != nil {
		return nil, fmt.Errorf("failed to unmarshal config: %w", err)
	}
	if err = ValidateAndApplyDefaults(&config); err != nil {
		return nil, err
	}
	return &config, nil
}

func parseProtocolVersion(s string) (version primitive.ProtocolVersion, ok bool) {
	ok = true
	lowered := strings.ToLower(s)
	if lowered == "3" || lowered == "v3" {
		version = primitive.ProtocolVersion3
	} else if lowered == "4" || lowered == "v4" {
		version = primitive.ProtocolVersion4
	} else if lowered == "5" || lowered == "v5" {
		version = primitive.ProtocolVersion5
	} else if lowered == "65" || lowered == "dsev1" {
		version = primitive.ProtocolVersionDse1
	} else if lowered == "66" || lowered == "dsev2" {
		version = primitive.ProtocolVersionDse1
	} else {
		ok = false
	}
	return version, ok
}

// maybeAddPort adds the default port to an IP; otherwise, it returns the original address.
func maybeAddPort(addr string, defaultPort string) string {
	if net.ParseIP(addr) != nil {
		return net.JoinHostPort(addr, defaultPort)
	}
	return addr
}

// listenAndServe correctly handles serving both the proxy and an HTTP server simultaneously.
func (c *runConfig) listenAndServe(p *Proxy, ctx context.Context, logger *zap.Logger) (err error) {
	var wg sync.WaitGroup

	ch := make(chan error)
	numServers := 1 // Without the HTTP server

	// Connect and listen is called first to set up the listening server connection and establish initial client
	// connections to the backend cluster so that when the readiness check is hit the proxy is actually ready.

	err = p.Connect()
	if err != nil {
		return err
	}

	proxyListener, err := resolveAndListen(c.Bind, c.ProxyCertFile, c.ProxyKeyFile)
	if err != nil {
		return err
	}

	logger.Info("proxy is listening", zap.Stringer("address", proxyListener.Addr()))
	atomicStartedListenersCounter.Add(1)

	wg.Add(numServers)

	go func() {
		wg.Wait()
		close(ch)
	}()

	go func() {
		select {
		case <-ctx.Done():
			logger.Debug("proxy interrupted/killed")
			_ = p.Close()
		}
	}()

	go func() {
		defer wg.Done()
		err := p.Serve(proxyListener)
		if err != nil && err != ErrProxyClosed {
			ch <- err
		}
	}()

	for err = range ch {
		if err != nil {
			return err
		}
	}

	return err
}

func resolveAndListen(address, cert, key string) (net.Listener, error) {
	if len(cert) > 0 || len(key) > 0 {
		if len(cert) == 0 || len(key) == 0 {
			return nil, errors.New("both certificate and private key are required for TLS")
		}
		cert, err := tls.LoadX509KeyPair(cert, key)
		if err != nil {
			return nil, fmt.Errorf("unable to load TLS certificate pair: %v", err)
		}
		return tls.Listen("tcp", address, &tls.Config{Certificates: []tls.Certificate{cert}})
	} else {
		return net.Listen("tcp", address)
	}
}
