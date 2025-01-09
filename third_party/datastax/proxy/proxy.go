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
	"bytes"
	"context"
	"crypto"
	"crypto/md5"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"reflect"
	"sync"
	"time"

	"cloud.google.com/go/spanner"
	otelgo "github.com/cloudspannerecosystem/cassandra-to-spanner-proxy/otel"
	"github.com/cloudspannerecosystem/cassandra-to-spanner-proxy/responsehandler"
	spannerModule "github.com/cloudspannerecosystem/cassandra-to-spanner-proxy/spanner"
	"github.com/cloudspannerecosystem/cassandra-to-spanner-proxy/tableConfig"
	"github.com/cloudspannerecosystem/cassandra-to-spanner-proxy/third_party/datastax/parser"
	"github.com/cloudspannerecosystem/cassandra-to-spanner-proxy/third_party/datastax/proxy/iface"
	"github.com/cloudspannerecosystem/cassandra-to-spanner-proxy/third_party/datastax/proxycore"
	"github.com/cloudspannerecosystem/cassandra-to-spanner-proxy/translator"
	"github.com/cloudspannerecosystem/cassandra-to-spanner-proxy/utilities"
	"github.com/datastax/go-cassandra-native-protocol/datatype"
	"github.com/datastax/go-cassandra-native-protocol/frame"
	"github.com/datastax/go-cassandra-native-protocol/message"
	"github.com/datastax/go-cassandra-native-protocol/primitive"
	lru "github.com/hashicorp/golang-lru"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.uber.org/zap"
	"google.golang.org/api/option"
	"google.golang.org/grpc"
)

const (
	commitTsFn              = "PENDING_COMMIT_TIMESTAMP()"
	SpannerConnectionString = "projects/%s/instances/%s/databases/%s"
)

const (
	handleQuery            = "handleQuery"
	handleBatch            = "Batch"
	handleExecuteForInsert = "handleExecuteForInsert"
	handleExecuteForDelete = "handleExecuteForDelete"
	handleExecuteForUpdate = "handleExecuteForUpdate"
	handleExecuteForSelect = "handleExecuteForSelect"
	cassandraQuery         = "Cassandra Query"
	spannerQuery           = "Spanner Query"
)

// Events
const (
	executingSpannerRequestEvent = "Executing Spanner Request"
	spannerExecutionDoneEvent    = "Spanner Execution Done"
)

var encodedOneValue, _ = proxycore.EncodeType(datatype.Int, primitive.ProtocolVersion4, 1)
var ErrProxyClosed = errors.New("proxy closed")
var ErrProxyAlreadyConnected = errors.New("proxy already connected")
var ErrProxyNotConnected = errors.New("proxy not connected")

var (
	system_schema         = "system_schema"
	keyspaces             = "keyspaces"
	tables                = "tables"
	columns               = "columns"
	system_virtual_schema = "system_virtual_schema"
	local                 = "local"
	spannerMaxStaleness   = "spanner_maxstaleness"
)

const selectType = "select"
const updateType = "update"
const insertType = "insert"
const deleteType = "delete"

const ttl_column = "spanner_ttl_ts"
const ts_column = "last_commit_ts"
const preparedIdSize = 16
const limitValue = "limitValue"
const Query = "Query"

const translatorErrorMessage = "Error occurred at translator"
const metadataFetchError = "Error while fetching table Metadata - "
const errorAtSpanner = "Error occurred at spanner - "
const errorWhileDecoding = "Error while decoding bytes - "
const errorWhileCasting = "Error while casting - "
const errUnhandledPrepare = "error while preparing query"
const errUnhandledPrepareExecute = "unhandled prepare execute scenario"
const errQueryNotPrepared = "query is not prepared"
const formateMsg = "%s -> %s"
const systemQueryMetadataNotFoundError = "data not found %s[%v]"
const errorStaleReadDecoding = "invalid type for stale read - expected bytes encoded string containing int64 value with suffix s(second), m(minute), h(hour)"

type PeerConfig struct {
	RPCAddr string   `yaml:"rpc-address"`
	DC      string   `yaml:"data-center,omitempty"`
	Tokens  []string `yaml:"tokens,omitempty"`
}

type Config struct {
	Version           primitive.ProtocolVersion
	MaxVersion        primitive.ProtocolVersion
	Auth              proxycore.Authenticator
	Resolver          proxycore.EndpointResolver
	RetryPolicy       RetryPolicy
	Logger            *zap.Logger
	HeartBeatInterval time.Duration
	ConnectTimeout    time.Duration
	IdleTimeout       time.Duration
	RPCAddr           string
	DC                string
	Tokens            []string
	SpannerConfig     SpannerConfig
	OtelConfig        *OtelConfig
	ReleaseVersion    string
	Partitioner       string
	CQLVersion        string
	// PreparedCache a cache that stores prepared queries. If not set it uses the default implementation with a max
	// capacity of ~100MB.
	PreparedCache   proxycore.PreparedCache
	KeyspaceFlatter bool
	UseRowTimestamp bool
	UseRowTTL       bool
	Debug           bool
	UserAgent       string
}

type SpannerConfig struct {
	DatabaseName     string
	ConfigTableName  string
	NumOfChannels    int
	InstanceName     string
	GCPProjectID     string
	MaxSessions      uint64
	MinSessions      uint64
	MaxCommitDelay   uint64
	ReplayProtection bool
	KeyspaceFlatter  bool
}

type Proxy struct {
	ctx                      context.Context
	config                   Config
	logger                   *zap.Logger
	cluster                  *proxycore.Cluster
	sessions                 [primitive.ProtocolVersionDse2 + 1]sync.Map // Cache sessions per protocol version
	mu                       sync.Mutex
	isConnected              bool
	isClosing                bool
	clients                  map[*client]struct{}
	listeners                map[*net.Listener]struct{}
	eventClients             sync.Map
	preparedCache            proxycore.PreparedCache
	preparedIdempotence      sync.Map
	preparedQueriesCache     sync.Map
	systemLocalValues        map[string]message.Column
	closed                   chan struct{}
	localNode                *node
	nodes                    []*node
	sClient                  spannerModule.SpannerClientIface
	translator               *translator.Translator
	tableConfig              *tableConfig.TableConfig
	otelInst                 *otelgo.OpenTelemetry
	otelShutdown             func(context.Context) error
	systemQueryMetadataCache *spannerModule.SystemQueryMetadataCache
}

type node struct {
	addr   *net.IPAddr
	dc     string
	tokens []string
}

func (p *Proxy) OnEvent(event proxycore.Event) {
	switch evt := event.(type) {
	case *proxycore.SchemaChangeEvent:
		p.logger.Debug("Schema change event detected", zap.String("SchemaChangeEvent", evt.Message.String()))
	}
}

func NewProxy(ctx context.Context, config Config) (*Proxy, error) {
	if config.Version == 0 {
		config.Version = primitive.ProtocolVersion4
	}
	if config.MaxVersion == 0 {
		config.MaxVersion = primitive.ProtocolVersion4
	}
	if config.RetryPolicy == nil {
		config.RetryPolicy = NewDefaultRetryPolicy()
	}

	logger := proxycore.GetOrCreateNopLogger(config.Logger)

	// Enable OpenTelemetry traces by setting environment variable GOOGLE_API_GO_EXPERIMENTAL_TELEMETRY_PLATFORM_TRACING to the case-insensitive value "opentelemetry" before loading the client library.
	otelInit := &otelgo.OTelConfig{}
	otelInst := &otelgo.OpenTelemetry{Config: &otelgo.OTelConfig{OTELEnabled: false}}
	var shutdownOTel func(context.Context) error

	var err error
	var spanCl iface.SpannerClientInterface

	// Initialize OpenTelemetry
	if config.OtelConfig.Enabled {
		otelInit = &otelgo.OTelConfig{
			TraceEnabled:       config.OtelConfig.Traces.Enabled,
			MetricEnabled:      config.OtelConfig.Metrics.Enabled,
			TracerEndpoint:     config.OtelConfig.Traces.Endpoint,
			MetricEndpoint:     config.OtelConfig.Metrics.Endpoint,
			ServiceName:        config.OtelConfig.ServiceName,
			OTELEnabled:        config.OtelConfig.Enabled,
			TraceSampleRatio:   config.OtelConfig.Traces.SamplingRatio,
			Database:           config.SpannerConfig.DatabaseName,
			Instance:           config.SpannerConfig.InstanceName,
			HealthCheckEnabled: config.OtelConfig.HealthCheck.Enabled,
			HealthCheckEp:      config.OtelConfig.HealthCheck.Endpoint,
			ServiceVersion:     config.Version.String(),
		}

		otelInst, shutdownOTel, err = otelgo.NewOpenTelemetry(ctx, otelInit, config.Logger)
		if err != nil {
			config.Logger.Error("Failed to enable the OTEL for the database: " + config.SpannerConfig.DatabaseName)
			return nil, err
		}
		config.Logger.Info("OTEL enabled at the application start for the database: " + config.SpannerConfig.DatabaseName)
	} else {
		otelInit = &otelgo.OTelConfig{
			OTELEnabled: false,
		}
		otelInst, shutdownOTel, err = otelgo.NewOpenTelemetry(ctx, otelInit, config.Logger)
		if err != nil {
			config.Logger.Error("Failed to enable the OTEL for the database: " + config.SpannerConfig.DatabaseName)
			return nil, err
		}
	}

	spanCl = NewSpannerClient(ctx, config, otelInst)
	spannerClient, err := spanCl.GetClient(ctx)
	if err != nil {
		return nil, err
	}
	var tableConfigurations *tableConfig.TableConfig

	spannerModuleClient := spannerModule.NewSpannerClient(spannerClient, config.Logger, &responsehandler.TypeHandler{
		Logger: logger,
	},
		config.KeyspaceFlatter,
		config.SpannerConfig.MaxCommitDelay,
		config.SpannerConfig.ReplayProtection)

	spannerQuery := fmt.Sprintf("SELECT * FROM %v ORDER BY TableName, IsPrimaryKey, PK_Precedence;", config.SpannerConfig.ConfigTableName)
	query := responsehandler.QueryMetadata{
		Query:        spannerQuery,
		TableName:    config.SpannerConfig.ConfigTableName,
		ProtocalV:    config.Version,
		KeyspaceName: config.SpannerConfig.DatabaseName,
	}

	tableMetadata, pkMetadata, systemQueryMetadataCache, err := spannerModuleClient.GetTableConfigs(ctx, query)
	if err != nil {
		return nil, err
	}

	tableConfigurations = &tableConfig.TableConfig{
		Logger:          config.Logger,
		TablesMetaData:  tableMetadata,
		PkMetadataCache: pkMetadata,
	}
	proxyTranslator := &translator.Translator{
		Logger:          config.Logger,
		TableConfig:     tableConfigurations,
		KeyspaceFlatter: config.KeyspaceFlatter,
		UseRowTimestamp: config.UseRowTimestamp,
		UseRowTTL:       config.UseRowTTL,
		Debug:           config.Debug,
	}

	proxy := Proxy{
		ctx:                      ctx,
		config:                   config,
		logger:                   logger,
		clients:                  make(map[*client]struct{}),
		listeners:                make(map[*net.Listener]struct{}),
		closed:                   make(chan struct{}),
		sClient:                  spannerModuleClient,
		translator:               proxyTranslator,
		tableConfig:              tableConfigurations,
		systemQueryMetadataCache: systemQueryMetadataCache,
	}

	proxy.otelInst = otelInst
	proxy.otelShutdown = shutdownOTel

	return &proxy, nil
}

func (p *Proxy) Connect() error {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.isConnected {
		return ErrProxyAlreadyConnected
	}

	var err error
	p.preparedCache, err = getOrCreateDefaultPreparedCache(p.config.PreparedCache)
	if err != nil {
		return fmt.Errorf("unable to create prepared cache %w", err)
	}

	// Connecting to cassandra cluster
	p.cluster, err = proxycore.ConnectCluster(p.ctx, proxycore.ClusterConfig{
		Version:           p.config.Version,
		Auth:              p.config.Auth,
		Resolver:          p.config.Resolver,
		HeartBeatInterval: p.config.HeartBeatInterval,
		ConnectTimeout:    p.config.ConnectTimeout,
		IdleTimeout:       p.config.IdleTimeout,
		Logger:            p.logger,
	})
	if err != nil {
		return fmt.Errorf("unable to connect to cluster %w", err)
	}

	err = p.buildNodes()
	if err != nil {
		return fmt.Errorf("unable to build node information: %w", err)
	}

	p.buildLocalRow()

	// Create cassandra session
	sess, err := proxycore.ConnectSession(p.ctx, p.cluster, proxycore.SessionConfig{
		Version:           p.cluster.NegotiatedVersion,
		Auth:              p.config.Auth,
		HeartBeatInterval: p.config.HeartBeatInterval,
		ConnectTimeout:    p.config.ConnectTimeout,
		IdleTimeout:       p.config.IdleTimeout,
		PreparedCache:     p.preparedCache,
		Logger:            p.logger,
	})

	if err != nil {
		return fmt.Errorf("unable to connect session %w", err)
	}

	p.sessions[p.cluster.NegotiatedVersion].Store("", sess) // No keyspace

	p.isConnected = true
	return nil
}

// Serve the proxy using the specified listener. It can be called multiple times with different listeners allowing
// them to share the same backend clusters.
func (p *Proxy) Serve(l net.Listener) (err error) {
	l = &closeOnceListener{Listener: l}
	defer l.Close()

	if err = p.addListener(&l); err != nil {
		return err
	}
	defer p.removeListener(&l)

	for {
		conn, err := l.Accept()
		if err != nil {
			select {
			case <-p.closed:
				return ErrProxyClosed
			default:
				return err
			}
		}
		p.handle(conn)
	}
}

func (p *Proxy) addListener(l *net.Listener) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.isClosing {
		return ErrProxyClosed
	}
	if !p.isConnected {
		return ErrProxyNotConnected
	}
	p.listeners[l] = struct{}{}
	return nil
}

func (p *Proxy) removeListener(l *net.Listener) {
	p.mu.Lock()
	defer p.mu.Unlock()
	delete(p.listeners, l)
}

func (p *Proxy) Close() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	select {
	case <-p.closed:
	default:
		close(p.closed)
	}
	var err error
	for l := range p.listeners {
		if closeErr := (*l).Close(); closeErr != nil && err == nil {
			err = closeErr
		}
	}
	for cl := range p.clients {
		_ = cl.conn.Close()
		p.eventClients.Delete(cl)
		delete(p.clients, cl)
	}

	p.sClient.Close()
	if p.otelShutdown != nil {
		err = p.otelShutdown(p.ctx)
	}

	return err
}

func (p *Proxy) Ready() bool {
	return true
}

func (p *Proxy) handle(conn net.Conn) {
	if tcpConn, ok := conn.(*net.TCPConn); ok {
		if err := tcpConn.SetKeepAlive(false); err != nil {
			p.logger.Warn("failed to disable keepalive on connection", zap.Error(err))
		}
		if err := tcpConn.SetNoDelay(true); err != nil {
			p.logger.Warn("failed to set TCP_NODELAY on connection", zap.Error(err))
		}
	}

	cl := &client{
		ctx:                 p.ctx,
		proxy:               p,
		preparedSystemQuery: make(map[[preparedIdSize]byte]interface{}),
	}
	cl.sender = cl
	p.addClient(cl)
	cl.conn = proxycore.NewConn(conn, cl)
	cl.conn.Start()
}

var (
	schemaVersion, _ = primitive.ParseUuid("4f2b29e6-59b5-4e2d-8fd6-01e32e67f0d7")
)

func (p *Proxy) buildNodes() (err error) {

	localDC := p.config.DC
	if len(localDC) == 0 {
		localDC = p.cluster.Info.LocalDC
		p.logger.Info("no local DC configured using DC from the first successful contact point",
			zap.String("dc", localDC))
	}

	p.localNode = &node{
		dc: localDC,
	}

	return nil
}

func (p *Proxy) buildLocalRow() {
	p.systemLocalValues = map[string]message.Column{
		"key":                     p.encodeTypeFatal(datatype.Varchar, "local"),
		"data_center":             p.encodeTypeFatal(datatype.Varchar, p.localNode.dc),
		"rack":                    p.encodeTypeFatal(datatype.Varchar, "rack1"),
		"tokens":                  p.encodeTypeFatal(datatype.NewListType(datatype.Varchar), [1]string{"-9223372036854775808"}),
		"release_version":         p.encodeTypeFatal(datatype.Varchar, p.config.ReleaseVersion),
		"partitioner":             p.encodeTypeFatal(datatype.Varchar, p.config.Partitioner),
		"cluster_name":            p.encodeTypeFatal(datatype.Varchar, "cql-proxy"),
		"cql_version":             p.encodeTypeFatal(datatype.Varchar, p.config.CQLVersion),
		"schema_version":          p.encodeTypeFatal(datatype.Uuid, schemaVersion), // TODO: Make this match the downstream cluster(s)
		"native_protocol_version": p.encodeTypeFatal(datatype.Varchar, p.config.Version.String()),
		"dse_version":             p.encodeTypeFatal(datatype.Varchar, p.cluster.Info.DSEVersion),
	}
}

func (p *Proxy) encodeTypeFatal(dt datatype.DataType, val interface{}) []byte {
	encoded, err := proxycore.EncodeType(dt, p.config.Version, val)
	if err != nil {
		p.logger.Fatal("unable to encode type", zap.Error(err))
	}
	return encoded
}

// isIdempotent checks whether a prepared ID is idempotent.
// If the proxy receives a query that it's never prepared then this will also return false.
func (p *Proxy) IsIdempotent(id []byte) bool {
	if val, ok := p.preparedIdempotence.Load(preparedIdKey(id)); !ok {
		// This should only happen if the proxy has never had a "PREPARE" request for this query ID.
		p.logger.Error("unable to determine if prepared statement is idempotent",
			zap.String("preparedID", hex.EncodeToString(id)))
		return false
	} else {
		return val.(bool)
	}
}

// MaybeStorePreparedIdempotence stores the idempotence of a "PREPARE" request's query.
// This information is used by future "EXECUTE" requests when they need to be retried.
func (p *Proxy) MaybeStorePreparedIdempotence(raw *frame.RawFrame, msg message.Message) {
	if prepareMsg, ok := msg.(*message.Prepare); ok && raw.Header.OpCode == primitive.OpCodeResult { // Prepared result
		frm, err := codec.ConvertFromRawFrame(raw)
		if err != nil {
			p.logger.Error("error attempting to decode prepared result message")
		} else if _, ok = frm.Body.Message.(*message.PreparedResult); !ok { // TODO: Use prepared type data to disambiguate idempotency
			p.logger.Error("expected prepared result message, but got something else")
		} else {
			idempotent, err := parser.IsQueryIdempotent(prepareMsg.Query)
			if err != nil {
				p.logger.Error("error parsing query for idempotence", zap.Error(err))
			} else if result, ok := frm.Body.Message.(*message.PreparedResult); ok {
				p.preparedIdempotence.Store(preparedIdKey(result.PreparedQueryId), idempotent)
			} else {
				p.logger.Error("expected prepared result, but got some other type of message",
					zap.Stringer("type", reflect.TypeOf(frm.Body.Message)))
			}
		}
	}
}

func (p *Proxy) addClient(cl *client) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.clients[cl] = struct{}{}
}

func (p *Proxy) registerForEvents(cl *client) {
	p.eventClients.Store(cl, struct{}{})
}

func (p *Proxy) removeClient(cl *client) {
	p.eventClients.Delete(cl)

	p.mu.Lock()
	defer p.mu.Unlock()
	delete(p.clients, cl)

}

type Sender interface {
	Send(hdr *frame.Header, msg message.Message)
}

type client struct {
	ctx                 context.Context
	proxy               *Proxy
	conn                *proxycore.Conn
	keyspace            string
	preparedSystemQuery map[[16]byte]interface{}
	sender              iface.Sender
}

type PreparedQuery struct {
	Query           string
	SelectedColumns []string
	PreparedColumns []string
}

func (c *client) AddQueryToCache(id [16]byte, parsedQueryMeta interface{}) {
	c.proxy.preparedQueriesCache.Store(id, parsedQueryMeta)
}

func (c *client) GetQueryFromCache(id [16]byte) (interface{}, bool) {
	return c.proxy.preparedQueriesCache.Load(id)
}

func (c *client) Receive(reader io.Reader) error {
	raw, err := codec.DecodeRawFrame(reader)
	if err != nil {
		if !errors.Is(err, io.EOF) {
			c.proxy.logger.Error("unable to decode frame", zap.Error(err))
		}
		return err
	}

	if raw.Header.Version > c.proxy.config.MaxVersion || raw.Header.Version < primitive.ProtocolVersion3 {
		c.sender.Send(raw.Header, &message.ProtocolError{
			ErrorMessage: fmt.Sprintf("Invalid or unsupported protocol version %d", raw.Header.Version),
		})
		return nil
	}

	body, err := codec.DecodeBody(raw.Header, bytes.NewReader(raw.Body))
	if err != nil {
		c.proxy.logger.Error("unable to decode body", zap.Error(err))
		return err
	}
	switch msg := body.Message.(type) {
	case *message.Options:
		// CC - responding with status READY
		c.sender.Send(raw.Header, &message.Supported{Options: map[string][]string{
			"CQL_VERSION": {c.proxy.config.CQLVersion},
			"COMPRESSION": {},
		}})
	case *message.Startup:
		// CC -  register for Event types and respond READY
		c.sender.Send(raw.Header, &message.Ready{})
	case *message.Register:
		for _, t := range msg.EventTypes {
			if t == primitive.EventTypeSchemaChange {
				c.proxy.registerForEvents(c)
			}
		}
		c.sender.Send(raw.Header, &message.Ready{})
	case *message.Prepare:
		c.proxy.logger.Debug("Prepare block -", zap.String(Query, msg.Query))
		c.handlePrepare(raw, msg)
	case *partialExecute:
		if bytes, found := body.CustomPayload[spannerMaxStaleness]; found {
			maxStaleness, err := utilities.ParseDurationToSeconds(string(bytes))
			if err != nil {
				c.sender.Send(raw.Header, &message.Invalid{ErrorMessage: errorStaleReadDecoding})
				return nil
			}
			c.handleExecute(raw, msg, utilities.ExecuteOptions{MaxStaleness: maxStaleness})
		} else {
			c.handleExecute(raw, msg, utilities.ExecuteOptions{MaxStaleness: 0})
		}
	case *partialQuery:
		c.handleQuery(raw, msg)
	case *partialBatch:
		c.handleBatch(raw, msg)
	default:
		c.sender.Send(raw.Header, &message.ProtocolError{ErrorMessage: "Unsupported operation"})
	}
	return nil
}

// function to execute query on cassandra
func (c *client) handlePrepare(raw *frame.RawFrame, msg *message.Prepare) {
	c.proxy.logger.Debug("handling prepare", zap.String(Query, msg.Query), zap.Int16("stream", raw.Header.StreamId))

	keyspace := c.keyspace
	if len(msg.Keyspace) != 0 {
		keyspace = msg.Keyspace
	}

	handled, stmt, queryType, err := parser.IsQueryHandledWithQueryType(parser.IdentifierFromString(keyspace), msg.Query)
	if handled {
		if err != nil {
			c.proxy.logger.Error("error parsing query to see if it's handled", zap.String(Query, msg.Query), zap.Error(err))
			c.sender.Send(raw.Header, &message.Invalid{ErrorMessage: err.Error()})
		} else {
			switch s := stmt.(type) {
			// handling select statement
			case *parser.SelectStatement:
				if systemColumns, ok := parser.SystemColumnsByName[s.Table]; ok {
					if columns, err := parser.FilterColumns(s, systemColumns); err != nil {
						c.sender.Send(raw.Header, &message.Invalid{ErrorMessage: err.Error()})
					} else {
						id := md5.Sum([]byte(msg.Query + keyspace))
						c.sender.Send(raw.Header, &message.PreparedResult{
							PreparedQueryId: id[:],
							ResultMetadata: &message.RowsMetadata{
								ColumnCount: int32(len(columns)),
								Columns:     columns,
							},
						})
						c.preparedSystemQuery[id] = stmt
					}
				} else {
					c.sender.Send(raw.Header, &message.Invalid{ErrorMessage: "system columns doesn't exist"})
				}
				// Prepare Use statement
			case *parser.UseStatement:
				id := md5.Sum([]byte(msg.Query))
				c.preparedSystemQuery[id] = stmt
				c.sender.Send(raw.Header, &message.PreparedResult{
					PreparedQueryId: id[:],
				})
			default:
				c.sender.Send(raw.Header, &message.ServerError{ErrorMessage: "Proxy attempted to intercept an unhandled query"})
			}
		}
	} else {
		c.handleServerPreparedQuery(raw, msg, queryType)
	}
}

// Check if query is already prepared and return response accordingly without re-processing
// Params: id [16]byte - Unique identifier for the prepared statement.
// Returns: []*message.ColumnMetadata - Variable metadata, []*message.ColumnMetadata - Column metadata, bool - Exists in cache.
// Retrieves and returns metadata for Select, Insert, Delete, and Update query maps from the cache, or nil and false if not found.
func (c *client) getMetadataFromCache(id [16]byte) ([]*message.ColumnMetadata, []*message.ColumnMetadata, bool) {
	preparedStmt, found := c.GetQueryFromCache(id)
	if !found {
		return nil, nil, false
	}
	switch st := preparedStmt.(type) {
	case *translator.SelectQueryMap:
		return st.VariableMetadata, st.ReturnMetadata, true
	case *translator.InsertQueryMap:
		return st.VariableMetadata, st.ReturnMetadata, true
	case *translator.DeleteQueryMap:
		return st.VariableMetadata, st.ReturnMetadata, true
	case *translator.UpdateQueryMap:
		return st.VariableMetadata, st.ReturnMetadata, true
	default:
		return nil, nil, false
	}

}

// handleServerPreparedQuery handle prepared query that was supposed to run on cassandra server
// This method will keep track of prepared query in a map and send hashed query_id with result
// metadata and variable column metadata to the client
//
// Parameters:
//   - raw: *frame.RawFrame
//   - msg: *message.Prepare
//
// Returns: nil
func (c *client) handleServerPreparedQuery(raw *frame.RawFrame, msg *message.Prepare, queryType string) {
	var PkIndices []uint16
	var err error
	var columns, variableColumnMetadata []*message.ColumnMetadata

	// Generating unique prepared query_id
	id := md5.Sum([]byte(msg.Query + c.keyspace))
	variableColumnMetadata, columns, found := c.getMetadataFromCache(id)
	if !found {
		switch queryType {
		case selectType:
			columns, variableColumnMetadata, err = c.prepareSelectType(raw, msg, id)
		case insertType:
			columns, variableColumnMetadata, err = c.prepareInsertType(raw, msg, id)
		case deleteType:
			columns, variableColumnMetadata, err = c.prepareDeleteType(raw, msg, id)
		case updateType:
			columns, variableColumnMetadata, err = c.prepareUpdateType(raw, msg, id)
		default:
			c.proxy.logger.Error(errUnhandledPrepare, zap.String(Query, msg.Query))
			c.sender.Send(raw.Header, &message.Invalid{ErrorMessage: errUnhandledPrepare})
			return
		}
		if err != nil {
			return
		}
	}
	// Generating Index array of size of variableMetadata
	for i := range variableColumnMetadata {
		PkIndices = append(PkIndices, uint16(i))
	}

	c.sender.Send(raw.Header, &message.PreparedResult{
		PreparedQueryId: id[:],
		ResultMetadata: &message.RowsMetadata{
			ColumnCount: int32(len(columns)),
			Columns:     columns,
		},
		VariablesMetadata: &message.VariablesMetadata{
			PkIndices: PkIndices,
			Columns:   variableColumnMetadata,
		},
	})
}

// function to handle and delete query of prepared type
func (c *client) prepareDeleteType(raw *frame.RawFrame, msg *message.Prepare, id [16]byte) ([]*message.ColumnMetadata, []*message.ColumnMetadata, error) {
	var returnColumns, variableColumns, columnsWithInOp []string
	deleteQueryMetadata, err := c.proxy.translator.ToSpannerDelete(c.keyspace, msg.Query)
	if err != nil {
		c.proxy.logger.Error(translatorErrorMessage, zap.String(Query, msg.Query), zap.Error(err))
		c.sender.Send(raw.Header, &message.Invalid{ErrorMessage: err.Error()})
		return nil, nil, err
	}

	deleteQueryMetadata.ExecuteByMutation = true

	if utilities.KeyExistsInList(ts_column, deleteQueryMetadata.ParamKeys) {
		variableColumns = append(variableColumns, ts_column)
	}

	// Capturing variable columns name assuming all columns are parameterized
	for _, clause := range deleteQueryMetadata.Clauses {
		variableColumns = append(variableColumns, clause.Column)
		// Capture columns with IN operator
		if clause.Operator == "IN" {
			columnsWithInOp = append(columnsWithInOp, clause.Column)
		}
		if deleteQueryMetadata.ExecuteByMutation {
			deleteQueryMetadata.ExecuteByMutation = !((clause.IsPrimaryKey && clause.Operator != "=") || !clause.IsPrimaryKey)
		}
	}

	if len(variableColumns) > 0 {
		// Get column metadata for variable fields
		deleteQueryMetadata.VariableMetadata, err = c.proxy.tableConfig.GetMetadataForColumns(deleteQueryMetadata.Table, variableColumns)
		if err != nil {
			c.proxy.logger.Error(metadataFetchError, zap.String(Query, msg.Query), zap.Error(err))
			c.sender.Send(raw.Header, &message.ConfigError{ErrorMessage: err.Error()})
			return nil, nil, err
		}
	}

	// Modify type to list type as being used with in operator
	for _, columnMeta := range deleteQueryMetadata.VariableMetadata {
		if utilities.KeyExistsInList(columnMeta.Name, columnsWithInOp) {
			columnMeta.Type = datatype.NewListType(columnMeta.Type)
		}
	}

	deleteQueryMetadata.ReturnMetadata, err = c.proxy.tableConfig.GetMetadataForColumns(deleteQueryMetadata.Table, returnColumns)
	if err != nil {
		c.sender.Send(raw.Header, &message.ConfigError{ErrorMessage: err.Error()})
		return nil, nil, err
	}

	// Caching Query info
	c.AddQueryToCache(id, deleteQueryMetadata)
	return deleteQueryMetadata.ReturnMetadata, deleteQueryMetadata.VariableMetadata, err
}

// function to handle and insert query of prepared type
func (c *client) prepareInsertType(raw *frame.RawFrame, msg *message.Prepare, id [16]byte) ([]*message.ColumnMetadata, []*message.ColumnMetadata, error) {
	var returnColumns []string

	insertQueryMetadata, err := c.proxy.translator.ToSpannerUpsert(c.keyspace, msg.Query)
	if err != nil {
		c.proxy.logger.Error(translatorErrorMessage, zap.String(Query, msg.Query), zap.Error(err))
		c.sender.Send(raw.Header, &message.Invalid{ErrorMessage: err.Error()})
		return nil, nil, err
	}

	insertQueryMetadata.VariableMetadata, err = c.proxy.tableConfig.GetMetadataForColumns(insertQueryMetadata.Table, insertQueryMetadata.ParamKeys)
	if err != nil {
		c.proxy.logger.Error(metadataFetchError, zap.String(Query, msg.Query), zap.Error(err))
		c.sender.Send(raw.Header, &message.ConfigError{ErrorMessage: err.Error()})
		return nil, nil, err
	}

	insertQueryMetadata.ReturnMetadata, err = c.proxy.tableConfig.GetMetadataForColumns(insertQueryMetadata.Table, returnColumns)
	if err != nil {
		c.proxy.logger.Error("error getting column metadata", zap.Error(err))
		c.sender.Send(raw.Header, &message.ConfigError{ErrorMessage: err.Error()})
		return nil, nil, err
	}

	// Caching pre-calculated values
	c.AddQueryToCache(id, insertQueryMetadata)
	return insertQueryMetadata.ReturnMetadata, insertQueryMetadata.VariableMetadata, err
}

// function to handle and select query of prepared type
func (c *client) prepareSelectType(raw *frame.RawFrame, msg *message.Prepare, id [16]byte) ([]*message.ColumnMetadata, []*message.ColumnMetadata, error) {
	var variableColumns, columnsWithInOp []string
	queryMetadata, err := c.proxy.translator.ToSpannerSelect(c.keyspace, msg.Query)
	if err != nil {
		c.proxy.logger.Error(translatorErrorMessage, zap.String(Query, msg.Query), zap.Error(err))
		c.sender.Send(raw.Header, &message.Invalid{ErrorMessage: err.Error()})
		return nil, nil, err
	}
	// Get Column metadata for the table or selected field
	queryMetadata.ReturnMetadata, err = c.proxy.tableConfig.GetMetadataForSelectedColumns(queryMetadata.Table, queryMetadata.ColumnMeta.Column, queryMetadata.AliasMap)
	if err != nil {
		c.proxy.logger.Error(metadataFetchError, zap.String(Query, msg.Query), zap.Error(err))
		c.sender.Send(raw.Header, &message.ConfigError{ErrorMessage: err.Error()})
		return nil, nil, err
	}

	// Capturing variable columns name assuming all columns are parameterized
	for _, clause := range queryMetadata.Clauses {
		variableColumns = append(variableColumns, clause.Column)
		if clause.Operator == "IN" {
			columnsWithInOp = append(columnsWithInOp, clause.Column)
		}
	}

	if utilities.KeyExistsInList(limitValue, queryMetadata.ParamKeys) {
		variableColumns = append(variableColumns, limitValue)
	}

	c.proxy.logger.Debug("Prepare Select Query ", zap.Strings("variableColumns", variableColumns))

	if len(variableColumns) > 0 {
		// Get column metadata for variable fields
		queryMetadata.VariableMetadata, err = c.proxy.tableConfig.GetMetadataForColumns(queryMetadata.Table, variableColumns)
		if err != nil {
			c.sender.Send(raw.Header, &message.ConfigError{ErrorMessage: err.Error()})
			c.proxy.logger.Error(metadataFetchError, zap.String(Query, msg.Query), zap.Error(err))
			return nil, nil, err
		}
	}

	// Modify type to list type as being used with in operator
	for _, columnMeta := range queryMetadata.VariableMetadata {
		if utilities.KeyExistsInList(columnMeta.Name, columnsWithInOp) {
			columnMeta.Type = datatype.NewListType(columnMeta.Type)
		}
	}

	// Caching Query info
	c.AddQueryToCache(id, queryMetadata)
	return queryMetadata.ReturnMetadata, queryMetadata.VariableMetadata, err
}

// function to handle update query of prepared type
func (c *client) prepareUpdateType(raw *frame.RawFrame, msg *message.Prepare, id [16]byte) ([]*message.ColumnMetadata, []*message.ColumnMetadata, error) {
	var returnColumns, variableColumns, columnsWithInOp []string
	updateQueryMetadata, err := c.proxy.translator.ToSpannerUpdate(c.keyspace, msg.Query)
	if err != nil {
		c.proxy.logger.Error(translatorErrorMessage, zap.String(Query, msg.Query), zap.Error(err))
		c.sender.Send(raw.Header, &message.Invalid{ErrorMessage: err.Error()})
		return nil, nil, err
	}

	// capturing variable columns name assuming all columns are parameterized
	for _, sets := range updateQueryMetadata.UpdateSetValues {
		if sets.Value == commitTsFn {
			continue
		}
		variableColumns = append(variableColumns, sets.Column)
	}

	// Capturing variable columns name assuming all columns are parameterized
	for _, clause := range updateQueryMetadata.Clauses {
		if ts_column == clause.Column {
			continue
		}
		variableColumns = append(variableColumns, clause.Column)
		if clause.Operator == "IN" {
			columnsWithInOp = append(columnsWithInOp, clause.Column)
		}
	}

	if len(variableColumns) > 0 {
		// Get column metadata for variable fields
		updateQueryMetadata.VariableMetadata, err = c.proxy.tableConfig.GetMetadataForColumns(updateQueryMetadata.Table, variableColumns)
		if err != nil {
			c.proxy.logger.Error(metadataFetchError, zap.String(Query, msg.Query), zap.Error(err))
			c.sender.Send(raw.Header, &message.ConfigError{ErrorMessage: err.Error()})
			return nil, nil, err
		}

		if updateQueryMetadata.SelectQueryMapUpdate != "" {
			for index, column := range updateQueryMetadata.VariableMetadata {
				if column.Name == updateQueryMetadata.MapUpdateColumn {
					updateQueryMetadata.VariableMetadata[index].Type = datatype.Varchar
				}
			}
		}
	}

	// Modify type to list type as being used with in operator
	for _, columnMeta := range updateQueryMetadata.VariableMetadata {
		if utilities.KeyExistsInList(columnMeta.Name, columnsWithInOp) {
			columnMeta.Type = datatype.NewListType(columnMeta.Type)
		}
	}

	updateQueryMetadata.ReturnMetadata, err = c.proxy.tableConfig.GetMetadataForColumns(updateQueryMetadata.Table, returnColumns)
	if err != nil {
		c.sender.Send(raw.Header, &message.ConfigError{ErrorMessage: err.Error()})
		c.proxy.logger.Error(metadataFetchError, zap.String(Query, msg.Query), zap.Error(err))
		return nil, nil, err
	}

	// Caching Query info
	c.AddQueryToCache(id, updateQueryMetadata)
	return updateQueryMetadata.ReturnMetadata, updateQueryMetadata.VariableMetadata, err
}

// handleExecute for prepared query
func (c *client) handleExecute(raw *frame.RawFrame, msg *partialExecute, executeOptions utilities.ExecuteOptions) {
	ctx := context.Background()
	id := preparedIdKey(msg.queryId)
	if preparedStmt, ok := c.GetQueryFromCache(id); ok {
		switch st := preparedStmt.(type) {
		case *translator.SelectQueryMap:
			c.handleExecuteForSelect(raw, msg, st, ctx, executeOptions)
		case *translator.InsertQueryMap:
			c.handleExecuteForInsert(raw, msg, st, ctx)
		case *translator.DeleteQueryMap:
			c.handleExecuteForDelete(raw, msg, st, ctx)
		case *translator.UpdateQueryMap:
			c.handleExecuteForUpdate(raw, msg, st, ctx)
		default:
			c.proxy.logger.Error(errUnhandledPrepareExecute)
			c.sender.Send(raw.Header, &message.ServerError{ErrorMessage: errUnhandledPrepareExecute})
		}
	} else if stmt, ok := c.preparedSystemQuery[id]; ok {
		c.interceptSystemQuery(raw.Header, stmt)
	} else {
		c.sender.Send(raw.Header, &message.Unprepared{ErrorMessage: errQueryNotPrepared, Id: id[:]})
		c.proxy.logger.Error(errQueryNotPrepared)
	}
}

// handle batch queries

func (c *client) handleBatch(raw *frame.RawFrame, msg *partialBatch) {
	startTime := time.Now()
	batchSize := len(msg.queryOrIds)
	batchQueries := make([]*responsehandler.QueryMetadata, batchSize)
	otelCtx, span := c.proxy.otelInst.StartSpan(c.proxy.ctx, handleBatch, []attribute.KeyValue{
		attribute.Int("Batch Size", batchSize),
	})
	defer c.proxy.otelInst.EndSpan(span)
	var otelErr error
	defer recordMetrics(c.proxy.ctx, c.proxy.otelInst, handleBatch, startTime, handleBatch, otelErr)

	for index, queryId := range msg.queryOrIds {
		queryOrId, ok := queryId.([]byte)
		if !ok {
			otelErr = fmt.Errorf("item is not of type [16]byte")
			c.proxy.otelInst.RecordError(span, otelErr)
			c.proxy.logger.Error("Item is not of type [16]byte")
			continue
		}
		id := preparedIdKey(queryOrId)
		var (
			queryMetadata *responsehandler.QueryMetadata
			err           error
		)

		if preparedStmt, ok := c.GetQueryFromCache(id); ok {
			switch st := preparedStmt.(type) {
			case *translator.InsertQueryMap:
				queryMetadata, err = c.prepareInsertQueryMetadata(raw, msg.BatchPositionalValues[index], st)
				if err != nil {
					c.proxy.logger.Error("Error preparing insert batch query metadata", zap.String(Query, st.CassandraQuery), zap.Error(err))
					c.sender.Send(raw.Header, &message.ConfigError{ErrorMessage: err.Error()})
					otelErr = err
					c.proxy.otelInst.RecordError(span, otelErr)
					return
				}
				batchQueries[index] = queryMetadata
			case *translator.DeleteQueryMap:
				queryMetadata, _, err = c.prepareDeleteQueryMetadata(raw, msg.BatchPositionalValues[index], st)
				if err != nil {
					c.proxy.logger.Error("Error preparing delete batch query metadata", zap.String(Query, st.CassandraQuery), zap.Error(err))
					c.sender.Send(raw.Header, &message.ConfigError{ErrorMessage: err.Error()})
					otelErr = err
					c.proxy.otelInst.RecordError(span, otelErr)
					return
				}

				batchQueries[index] = queryMetadata
			case *translator.UpdateQueryMap:
				queryMetadata, err = c.prepareUpdateQueryMetadata(raw, msg.BatchPositionalValues[index], st)
				if err != nil {
					c.proxy.logger.Error("Error preparing update batch query metadata", zap.String(Query, st.CassandraQuery), zap.Error(err))
					c.sender.Send(raw.Header, &message.ConfigError{ErrorMessage: err.Error()})
					otelErr = err
					c.proxy.otelInst.RecordError(span, otelErr)
					return
				}

				if st.SelectQueryMapUpdate != "" {
					queryMetadata.SelectQueryMapUpdate = st.SelectQueryMapUpdate
					queryMetadata.UpdateSetValues = st.UpdateSetValues
				}
				batchQueries[index] = queryMetadata
			default:
				otelErr = fmt.Errorf("unhandled prepared batch query object")
				c.sender.Send(raw.Header, &message.ServerError{ErrorMessage: otelErr.Error()})
				c.proxy.otelInst.RecordError(span, otelErr)
				c.proxy.logger.Error("Unhandled Prepare Batch Scenario")
				return
			}
		} else {
			c.sender.Send(raw.Header, &message.ServerError{ErrorMessage: otelErr.Error()})
			c.proxy.otelInst.RecordError(span, fmt.Errorf(errQueryNotPrepared))
			c.proxy.logger.Error(otelErr.Error())
		}
	}
	result, err := c.proxy.sClient.FilterAndExecuteBatch(otelCtx, batchQueries)
	if err != nil {
		c.proxy.logger.Error(errorAtSpanner, zap.Error(err))
		c.sender.Send(raw.Header, &message.Invalid{ErrorMessage: err.Error()})
		otelErr = err
		c.proxy.otelInst.RecordError(span, otelErr)
		return
	}

	c.sender.Send(raw.Header, result)
}

// handleExecute for update prepared query
func (c *client) handleExecuteForUpdate(raw *frame.RawFrame, msg *partialExecute, st *translator.UpdateQueryMap, ctx context.Context) {
	startTime := time.Now()
	var result *message.RowsResult
	var otelErr error
	ctx, span := c.proxy.otelInst.StartSpan(ctx, updateType, []attribute.KeyValue{
		attribute.String(cassandraQuery, st.CassandraQuery),
		attribute.String(spannerQuery, st.SpannerQuery),
	})
	defer c.proxy.otelInst.EndSpan(span)
	defer recordMetrics(ctx, c.proxy.otelInst, handleExecuteForUpdate, startTime, updateType, otelErr)

	queryMetadata, err := c.prepareUpdateQueryMetadata(raw, msg.PositionalValues, st)
	if err != nil {
		c.proxy.logger.Error("Error preparing update query metadata", zap.String(Query, st.CassandraQuery), zap.Error(err))
		c.sender.Send(raw.Header, &message.ConfigError{ErrorMessage: err.Error()})
		otelErr = err
		c.proxy.otelInst.SetError(ctx, otelErr)
		return
	}
	otelgo.AddAnnotation(ctx, executingSpannerRequestEvent)
	// Handle - Special Scenario for update query of pattern column[?] = true/false
	if st.SelectQueryMapUpdate != "" {

		queryMetadata.SelectQueryMapUpdate = st.SelectQueryMapUpdate
		queryMetadata.UpdateSetValues = st.UpdateSetValues

		result, err = c.proxy.sClient.UpdateMapByKey(ctx, *queryMetadata)
		if err != nil {
			c.proxy.logger.Error(fmt.Sprintf("Error while performing map update by key - Query -> %s <-", st.CassandraQuery), zap.Error(err))
			c.sender.Send(raw.Header, &message.Invalid{ErrorMessage: err.Error()})
			otelErr = err
			c.proxy.otelInst.SetError(ctx, otelErr)
			return
		}
	} else {
		result, err = c.proxy.sClient.InsertUpdateOrDeleteStatement(ctx, *queryMetadata)
	}
	otelgo.AddAnnotation(ctx, spannerExecutionDoneEvent)
	if err != nil {
		c.proxy.logger.Error(errorAtSpanner, zap.String(Query, st.CassandraQuery), zap.Error(err))
		c.sender.Send(raw.Header, &message.Invalid{ErrorMessage: err.Error()})
		otelErr = err
		c.proxy.otelInst.SetError(ctx, otelErr)
		return
	}
	c.sender.Send(raw.Header, result)
}

// handleExecute for delete prepared query
func (c *client) handleExecuteForDelete(raw *frame.RawFrame, msg *partialExecute, st *translator.DeleteQueryMap, ctx context.Context) {
	start := time.Now()
	var otelErr error
	ctx, span := c.proxy.otelInst.StartSpan(ctx, deleteType, []attribute.KeyValue{
		attribute.String(cassandraQuery, st.CassandraQuery),
		attribute.String(spannerQuery, st.SpannerQuery),
	})
	defer c.proxy.otelInst.EndSpan(span)
	defer recordMetrics(ctx, c.proxy.otelInst, handleExecuteForDelete, start, deleteType, otelErr)

	queryMetadata, executeByMutation, err := c.prepareDeleteQueryMetadata(raw, msg.PositionalValues, st)
	if err != nil {
		c.proxy.logger.Error("Error preparing Delete query metadata", zap.String(Query, st.CassandraQuery), zap.Error(err))
		c.sender.Send(raw.Header, &message.ConfigError{ErrorMessage: err.Error()})
		otelErr = err
		c.proxy.otelInst.SetError(ctx, otelErr)
		return
	}
	otelgo.AddAnnotation(ctx, executingSpannerRequestEvent)
	var result *message.RowsResult
	if !executeByMutation {
		result, err = c.proxy.sClient.InsertUpdateOrDeleteStatement(ctx, *queryMetadata)
	} else {
		result, err = c.proxy.sClient.DeleteUsingMutations(ctx, *queryMetadata)
	}
	otelgo.AddAnnotation(ctx, spannerExecutionDoneEvent)

	if err != nil {
		c.proxy.logger.Error(errorAtSpanner, zap.String(Query, st.CassandraQuery), zap.Error(err))
		c.sender.Send(raw.Header, &message.Invalid{ErrorMessage: err.Error()})
		otelErr = err
		c.proxy.otelInst.SetError(ctx, otelErr)
		return
	}
	c.sender.Send(raw.Header, result)
}

// handleExecute for insert prepared query
func (c *client) handleExecuteForInsert(raw *frame.RawFrame, msg *partialExecute, st *translator.InsertQueryMap, ctx context.Context) {
	startTime := time.Now()
	var otelErr error
	ctx, span := c.proxy.otelInst.StartSpan(ctx, insertType, []attribute.KeyValue{
		attribute.String(cassandraQuery, st.CassandraQuery),
		attribute.String(spannerQuery, st.SpannerQuery),
	})
	defer c.proxy.otelInst.EndSpan(span)
	defer recordMetrics(ctx, c.proxy.otelInst, handleExecuteForInsert, startTime, insertType, otelErr)

	queryMetadata, err := c.prepareInsertQueryMetadata(raw, msg.PositionalValues, st)
	if err != nil {
		c.proxy.logger.Error("Error preparing insert query metadata", zap.String(Query, st.CassandraQuery), zap.Error(err))
		c.sender.Send(raw.Header, &message.ConfigError{ErrorMessage: err.Error()})
		otelErr = err
		c.proxy.otelInst.SetError(ctx, otelErr)
		return
	}
	otelgo.AddAnnotation(ctx, executingSpannerRequestEvent)
	result, err := c.proxy.sClient.InsertOrUpdateMutation(ctx, *queryMetadata)
	otelgo.AddAnnotation(ctx, spannerExecutionDoneEvent)

	if err != nil {
		c.proxy.logger.Error(errorAtSpanner, zap.String(Query, st.CassandraQuery), zap.Error(err))
		c.sender.Send(raw.Header, &message.Invalid{ErrorMessage: err.Error()})
		otelErr = err
		c.proxy.otelInst.SetError(ctx, otelErr)
		return
	}
	c.sender.Send(raw.Header, result)
}

// Prepare delete query metadata
func (c *client) prepareUpdateQueryMetadata(raw *frame.RawFrame, paramValue []*primitive.Value, st *translator.UpdateQueryMap) (*responsehandler.QueryMetadata, error) {
	var query responsehandler.QueryMetadata
	params := make(map[string]interface{})

	// Get Decoded parameters
	for index, columnMetadata := range st.VariableMetadata {

		if st.SelectQueryMapUpdate != "" && columnMetadata.Name == st.MapUpdateColumn {
			columnMetadata.Type = datatype.Varchar
		}

		decodedValue, err := utilities.DecodeBytesToSpannerColumnType(paramValue[index].Contents, columnMetadata.Type, raw.Header.Version)
		if err != nil {
			c.proxy.logger.Error(errorWhileDecoding, zap.String(Query, st.CassandraQuery), zap.String("Column", columnMetadata.Name), zap.Error(err))
			return nil, fmt.Errorf(formateMsg, errorWhileDecoding, err.Error())
		}

		// Handle calculation of TTL
		if columnMetadata.Name == ttl_column {
			if castedToInt64, ok := decodedValue.(int64); ok {
				if castedToInt64 == 0 {
					decodedValue = nil
				} else {
					decodedValue = utilities.AddSecondsToCurrentTimestamp(castedToInt64)
				}
			} else {
				c.proxy.logger.Error(errorWhileCasting+ttl_column, zap.String(Query, st.CassandraQuery), zap.String("Column", columnMetadata.Name))
				return nil, fmt.Errorf(formateMsg, errorWhileCasting, ttl_column)
			}
		}

		if columnMetadata.Name == ts_column {
			if castedToInt64, ok := decodedValue.(int64); ok {
				decodedValue, err = utilities.FormatTimestamp(castedToInt64)
				if err != nil {
					c.proxy.logger.Error(errorWhileDecoding, zap.String(Query, st.CassandraQuery), zap.String("Column", columnMetadata.Name), zap.Error(err))
					return nil, fmt.Errorf(formateMsg, errorWhileDecoding, err.Error())
				}
			} else {
				c.proxy.logger.Error(errorWhileCasting+ts_column, zap.String(Query, st.CassandraQuery), zap.String("Column", columnMetadata.Name))
				return nil, fmt.Errorf("%s %s", errorWhileCasting, ts_column)
			}
		}
		params[st.ParamKeys[index]] = decodedValue
	}

	c.proxy.logger.Debug("Update PreparedExecute Query", zap.String("SpannerQuery", st.SpannerQuery))
	query = responsehandler.QueryMetadata{
		Query:        st.SpannerQuery,
		QueryType:    updateType,
		TableName:    st.Table,
		KeyspaceName: st.Keyspace,
		ProtocalV:    raw.Header.Version,
		Params:       params,
		PrimaryKeys:  st.PrimaryKeys,
	}
	return &query, nil
}

// Prepare delete query metadata
func (c *client) prepareDeleteQueryMetadata(raw *frame.RawFrame, paramValue []*primitive.Value, st *translator.DeleteQueryMap) (*responsehandler.QueryMetadata, bool, error) {
	var query responsehandler.QueryMetadata
	params := make(map[string]interface{})
	colVal := make(map[string]interface{})
	var mutationKeyRange []interface{}

	// Get Decoded parameters
	for index, columnMetadata := range st.VariableMetadata {
		decodedValue, err := utilities.DecodeBytesToSpannerColumnType(paramValue[index].Contents, columnMetadata.Type, raw.Header.Version)
		if err != nil {
			c.proxy.logger.Error(errorWhileDecoding, zap.String(Query, st.CassandraQuery), zap.String("Column", columnMetadata.Name), zap.Error(err))
			return nil, st.ExecuteByMutation, fmt.Errorf(formateMsg, errorWhileDecoding, err.Error())
		}

		if columnMetadata.Name == ts_column {
			if castedToInt64, ok := decodedValue.(int64); ok {

				decodedValue, err = utilities.FormatTimestamp(castedToInt64)
				if err != nil {
					c.proxy.logger.Error(errorWhileDecoding, zap.String(Query, st.CassandraQuery), zap.String("Column", columnMetadata.Name), zap.Error(err))
					return nil, st.ExecuteByMutation, fmt.Errorf(formateMsg, errorWhileDecoding, err.Error())
				}
			} else {
				c.proxy.logger.Error(errorWhileCasting+ts_column, zap.String(Query, st.CassandraQuery), zap.String("Column", columnMetadata.Name))
				return nil, st.ExecuteByMutation, fmt.Errorf("%s- %s", errorWhileCasting, ts_column)
			}
		}
		params[st.ParamKeys[index]] = decodedValue
		colVal[columnMetadata.Name] = decodedValue
	}

	if st.ExecuteByMutation {
		for _, col := range st.PrimaryKeys {
			val, ok := colVal[col]
			if ok {
				mutationKeyRange = append(mutationKeyRange, val)
			}
		}
	}

	c.proxy.logger.Debug("Delete PreparedExecute Query", zap.String("SpannerQuery", st.SpannerQuery))
	query = responsehandler.QueryMetadata{
		Query:            st.SpannerQuery,
		QueryType:        deleteType,
		TableName:        st.Table,
		KeyspaceName:     st.Keyspace,
		ProtocalV:        raw.Header.Version,
		Params:           params,
		PrimaryKeys:      st.PrimaryKeys,
		MutationKeyRange: mutationKeyRange,
	}
	return &query, st.ExecuteByMutation, nil
}

// Prepare insert query metadata
func (c *client) prepareInsertQueryMetadata(raw *frame.RawFrame, paramValue []*primitive.Value, st *translator.InsertQueryMap) (*responsehandler.QueryMetadata, error) {
	var query responsehandler.QueryMetadata
	params := make(map[string]interface{})
	paramValues := []interface{}{}
	paramKeys := []string{}

	// Get Decoded parameters
	for index, columnMetadata := range st.VariableMetadata {
		decodedValue, err := utilities.DecodeBytesToSpannerColumnType(paramValue[index].Contents, columnMetadata.Type, raw.Header.Version)
		if err != nil {
			c.proxy.logger.Error(errorWhileDecoding, zap.String(Query, st.CassandraQuery), zap.String("Column", columnMetadata.Name), zap.Error(err))
			return nil, fmt.Errorf(formateMsg, errorWhileDecoding, err.Error())
		}
		// Handle calculation of TTL
		if columnMetadata.Name == ttl_column {
			if castedToInt64, ok := decodedValue.(int64); ok {
				if castedToInt64 == 0 {
					decodedValue = nil
				} else {
					decodedValue = utilities.AddSecondsToCurrentTimestamp(castedToInt64)
				}
			} else {
				c.proxy.logger.Error(errorWhileCasting+ttl_column, zap.String(Query, st.CassandraQuery), zap.String("Column", columnMetadata.Name))
				return nil, fmt.Errorf(formateMsg, errorWhileCasting, ttl_column)

			}
		}

		if columnMetadata.Name == ts_column && st.UsingTSCheck != "" {
			if castedToInt64, ok := decodedValue.(int64); ok {
				decodedValue, err = utilities.FormatTimestamp(castedToInt64)
				if err != nil {
					c.proxy.logger.Error(errorWhileDecoding, zap.String(Query, st.CassandraQuery), zap.String("Column", columnMetadata.Name), zap.Error(err))
					return nil, fmt.Errorf("%s - %s", errorWhileDecoding, ts_column)
				}
			} else {
				c.proxy.logger.Error(errorWhileCasting+ts_column, zap.String(Query, st.CassandraQuery), zap.String("Column", columnMetadata.Name))
				return nil, fmt.Errorf("%s - %s", errorWhileCasting, ts_column)
			}
		}

		paramValues = append(paramValues, decodedValue)
		paramKeys = append(paramKeys, st.ParamKeys[index])
		params[st.ParamKeys[index]] = decodedValue
	}

	if st.UsingTSCheck == "" && c.proxy.translator.UseRowTimestamp {
		paramValues = append(paramValues, spanner.CommitTimestamp)
		paramKeys = append(paramKeys, ts_column)
	}
	c.proxy.logger.Debug("Insert PreparedExecute Query", zap.String("SpannerQuery", "Insert Operation use mutation"))
	query = responsehandler.QueryMetadata{
		Query:          st.SpannerQuery,
		QueryType:      insertType,
		TableName:      st.Table,
		KeyspaceName:   st.Keyspace,
		ProtocalV:      raw.Header.Version,
		Params:         params,
		Paramkeys:      paramKeys,
		ParamValues:    paramValues,
		UsingTSCheck:   st.UsingTSCheck,
		HasIfNotExists: st.HasIfNotExists,
		PrimaryKeys:    st.PrimaryKeys,
	}
	return &query, nil
}

// handleExecute for Select prepared query
func (c *client) handleExecuteForSelect(raw *frame.RawFrame, msg *partialExecute, st *translator.SelectQueryMap, ctx context.Context, executeOptions utilities.ExecuteOptions) {
	startTime := time.Now()
	var query responsehandler.QueryMetadata
	var err error
	var result *message.RowsResult
	ctx, span := c.proxy.otelInst.StartSpan(ctx, selectType, []attribute.KeyValue{
		attribute.String(cassandraQuery, st.CassandraQuery),
		attribute.String(spannerQuery, st.SpannerQuery),
	})
	defer c.proxy.otelInst.EndSpan(span)
	defer recordMetrics(c.proxy.ctx, c.proxy.otelInst, handleExecuteForSelect, startTime, selectType, err)

	// Get Decoded parameters
	otelgo.AddAnnotation(ctx, "Decoding Bytes To Spanner Column Type")
	params := make(map[string]interface{})
	for index, columnMetadata := range st.VariableMetadata {
		decodedValue, err := utilities.DecodeBytesToSpannerColumnType(msg.PositionalValues[index].Contents, columnMetadata.Type, raw.Header.Version)
		if err != nil {
			c.proxy.logger.Error(errorWhileDecoding, zap.String(Query, st.CassandraQuery), zap.String("Column", columnMetadata.Name), zap.Error(err))
			c.sender.Send(raw.Header, &message.ConfigError{ErrorMessage: err.Error()})
			c.proxy.otelInst.SetError(ctx, err)
			return
		}
		params[st.ParamKeys[index]] = decodedValue
	}
	otelgo.AddAnnotation(ctx, "Decoding Done")

	c.proxy.logger.Debug("Select PreparedExecute Query", zap.String("SpannerQuery", st.SpannerQuery))
	query = responsehandler.QueryMetadata{
		Query:           st.SpannerQuery,
		QueryType:       selectType,
		TableName:       st.Table,
		KeyspaceName:    st.Keyspace,
		ProtocalV:       raw.Header.Version,
		Params:          params,
		SelectedColumns: st.ColumnMeta.Column,
		AliasMap:        st.AliasMap,
		PrimaryKeys:     st.PrimaryKeys,
		ExecuteOptions:  executeOptions,
	}

	otelgo.AddAnnotation(ctx, executingSpannerRequestEvent)
	result, err = c.proxy.sClient.SelectStatement(ctx, query)
	otelgo.AddAnnotation(ctx, spannerExecutionDoneEvent)

	if err != nil {
		c.proxy.logger.Error(errorAtSpanner, zap.String(Query, st.CassandraQuery), zap.Error(err))
		c.sender.Send(raw.Header, &message.Invalid{ErrorMessage: err.Error()})
		c.proxy.otelInst.SetError(ctx, err)
		return
	}
	c.sender.Send(raw.Header, result)
}

func (c *client) handleQuery(raw *frame.RawFrame, msg *partialQuery) {
	startTime := time.Now()
	c.proxy.logger.Debug("handling query", zap.String("encodedQuery", msg.query), zap.Int16("stream", raw.Header.StreamId))

	handled, stmt, queryType, err := parser.IsQueryHandledWithQueryType(parser.IdentifierFromString(c.keyspace), msg.query)
	otelCtx, span := c.proxy.otelInst.StartSpan(c.proxy.ctx, handleQuery, []attribute.KeyValue{
		attribute.String("Query", msg.query),
	})
	defer c.proxy.otelInst.EndSpan(span)
	if handled {
		if err != nil {
			c.proxy.logger.Error("error parsing query to see if it's handled", zap.String(Query, msg.query), zap.Error(err))
			c.sender.Send(raw.Header, &message.Invalid{ErrorMessage: err.Error()})
			c.proxy.otelInst.RecordError(span, err)
			return
		} else {
			c.interceptSystemQuery(raw.Header, stmt)
		}
	} else {
		var result *message.RowsResult
		var otelErr error
		defer recordMetrics(c.proxy.ctx, c.proxy.otelInst, handleQuery, startTime, queryType, otelErr)

		switch queryType {
		case selectType:
			queryMetadata, err := c.proxy.translator.ToSpannerSelect(c.keyspace, msg.query)
			if err != nil {
				c.proxy.logger.Error(translatorErrorMessage, zap.String(Query, msg.query), zap.Error(err))
				c.sender.Send(raw.Header, &message.Invalid{ErrorMessage: err.Error()})
				c.proxy.otelInst.RecordError(span, err)
				otelErr = err
				return
			}

			c.proxy.logger.Debug("Select raw Query", zap.String("SpannerQuery", queryMetadata.SpannerQuery))

			queryMeta := responsehandler.QueryMetadata{
				Query:           queryMetadata.SpannerQuery,
				TableName:       queryMetadata.Table,
				KeyspaceName:    queryMetadata.Keyspace,
				ProtocalV:       raw.Header.Version,
				Params:          queryMetadata.Params,
				SelectedColumns: queryMetadata.ColumnMeta.Column,
				PrimaryKeys:     queryMetadata.PrimaryKeys,
			}

			result, err = c.proxy.sClient.SelectStatement(otelCtx, queryMeta)

			if err != nil {
				c.proxy.logger.Error(errorAtSpanner, zap.String(Query, msg.query), zap.Error(err))
				c.sender.Send(raw.Header, &message.Invalid{ErrorMessage: err.Error()})
				c.proxy.otelInst.RecordError(span, err)
				otelErr = err
				return
			}
			c.sender.Send(raw.Header, result)
		case insertType:
			queryMetadata, err := c.proxy.translator.ToSpannerUpsert(c.keyspace, msg.query)
			if err != nil {
				c.proxy.logger.Error(translatorErrorMessage, zap.String(Query, msg.query), zap.Error(err))
				c.sender.Send(raw.Header, &message.Invalid{ErrorMessage: err.Error()})
				c.proxy.otelInst.RecordError(span, err)
				otelErr = err
				return
			}

			c.proxy.logger.Debug("Insert raw Query", zap.String("SpannerQuery", "Insert Operation use mutation"))

			queryMeta := responsehandler.QueryMetadata{
				Query:          queryMetadata.SpannerQuery,
				TableName:      queryMetadata.Table,
				KeyspaceName:   queryMetadata.Keyspace,
				ProtocalV:      raw.Header.Version,
				Params:         queryMetadata.Params,
				Paramkeys:      queryMetadata.ParamKeys,
				ParamValues:    queryMetadata.Values,
				UsingTSCheck:   queryMetadata.UsingTSCheck,
				HasIfNotExists: queryMetadata.HasIfNotExists,
				PrimaryKeys:    queryMetadata.PrimaryKeys,
			}

			VariableMetadata, err := c.proxy.tableConfig.GetMetadataForColumns(queryMetadata.Table, queryMetadata.ParamKeys)
			if err != nil {
				c.proxy.logger.Error(metadataFetchError, zap.String(Query, msg.query), zap.Error(err))
				c.sender.Send(raw.Header, &message.ServerError{ErrorMessage: err.Error()})
			}

			result, err := c.proxy.sClient.InsertOrUpdateMutation(otelCtx, queryMeta)

			if err != nil {
				c.proxy.logger.Error(errorAtSpanner, zap.String(Query, msg.query), zap.Error(err))
				c.sender.Send(raw.Header, &message.Invalid{ErrorMessage: err.Error()})
				c.proxy.otelInst.RecordError(span, err)
				otelErr = err
				return
			}

			result.Metadata.Columns = VariableMetadata
			result.Metadata.ColumnCount = int32(len(VariableMetadata))

			c.sender.Send(raw.Header, result)
		case deleteType:
			queryMetadata, err := c.proxy.translator.ToSpannerDelete(c.keyspace, msg.query)
			if err != nil {
				c.proxy.logger.Error(translatorErrorMessage, zap.String(Query, msg.query), zap.Error(err))
				c.sender.Send(raw.Header, &message.Invalid{ErrorMessage: err.Error()})
				c.proxy.otelInst.RecordError(span, err)
				otelErr = err
				return
			}

			c.proxy.logger.Debug("Delete raw Query", zap.String("SpannerQuery", queryMetadata.SpannerQuery))

			queryMeta := responsehandler.QueryMetadata{
				Query:        queryMetadata.SpannerQuery,
				TableName:    queryMetadata.Table,
				KeyspaceName: queryMetadata.Keyspace,
				ProtocalV:    raw.Header.Version,
				Params:       queryMetadata.Params,
				PrimaryKeys:  queryMetadata.PrimaryKeys,
			}

			VariableMetadata, err := c.proxy.tableConfig.GetMetadataForColumns(queryMetadata.Table, queryMetadata.PrimaryKeys)
			if err != nil {
				c.proxy.logger.Error(metadataFetchError, zap.String(Query, msg.query), zap.Error(err))
				c.sender.Send(raw.Header, &message.ServerError{ErrorMessage: err.Error()})
			}

			result, err := c.proxy.sClient.InsertUpdateOrDeleteStatement(otelCtx, queryMeta)
			if err != nil {
				c.proxy.logger.Error(errorAtSpanner, zap.String(Query, msg.query), zap.Error(err))
				c.sender.Send(raw.Header, &message.Invalid{ErrorMessage: err.Error()})
				c.proxy.otelInst.RecordError(span, err)
				otelErr = err
				return
			}

			result.Metadata.Columns = VariableMetadata
			result.Metadata.ColumnCount = int32(len(VariableMetadata))
			c.sender.Send(raw.Header, result)
		case updateType:
			queryMetadata, err := c.proxy.translator.ToSpannerUpdate(c.keyspace, msg.query)
			if err != nil {
				c.proxy.logger.Error(translatorErrorMessage, zap.String(Query, msg.query), zap.Error(err))
				c.sender.Send(raw.Header, &message.Invalid{ErrorMessage: err.Error()})
				c.proxy.otelInst.RecordError(span, err)
				otelErr = err
				return
			}

			c.proxy.logger.Debug("Update raw Query", zap.String("SpannerQuery", queryMetadata.SpannerQuery))

			queryMeta := responsehandler.QueryMetadata{
				Query:        queryMetadata.SpannerQuery,
				TableName:    queryMetadata.Table,
				KeyspaceName: queryMetadata.Keyspace,
				ProtocalV:    raw.Header.Version,
				Params:       queryMetadata.Params,
				PrimaryKeys:  queryMetadata.PrimaryKeys,
			}

			VariableMetadata, err := c.proxy.tableConfig.GetMetadataForColumns(queryMetadata.Table, queryMetadata.PrimaryKeys)
			if err != nil {
				c.proxy.logger.Error(metadataFetchError, zap.String(Query, msg.query), zap.Error(err))
				c.sender.Send(raw.Header, &message.ServerError{ErrorMessage: err.Error()})
			}

			result, err := c.proxy.sClient.InsertUpdateOrDeleteStatement(otelCtx, queryMeta)
			if err != nil {
				c.proxy.logger.Error(errorAtSpanner, zap.String(Query, msg.query), zap.Error(err))
				c.sender.Send(raw.Header, &message.Invalid{ErrorMessage: err.Error()})
				c.proxy.otelInst.RecordError(span, err)
				otelErr = err
				return
			}

			result.Metadata.Columns = VariableMetadata
			result.Metadata.ColumnCount = int32(len(VariableMetadata))
			c.sender.Send(raw.Header, result)
		default:
			otelErr = fmt.Errorf("invalid query type")
			c.proxy.otelInst.RecordError(span, otelErr)
			c.proxy.logger.Error(otelErr.Error(), zap.String(Query, msg.query))
			return
		}
	}
}

func (c *client) filterSystemLocalValues(stmt *parser.SelectStatement, filtered []*message.ColumnMetadata) (row []message.Column, err error) {
	return parser.FilterValues(stmt, filtered, func(name string) (value message.Column, err error) {
		if name == "rpc_address" {
			return proxycore.EncodeType(datatype.Inet, c.proxy.cluster.NegotiatedVersion, net.ParseIP("127.0.0.1"))
		} else if name == "host_id" {
			return proxycore.EncodeType(datatype.Uuid, c.proxy.cluster.NegotiatedVersion, nameBasedUUID("127.0.0.1"))
		} else if val, ok := c.proxy.systemLocalValues[name]; ok {
			return val, nil
		} else if name == parser.CountValueName {
			return encodedOneValue, nil
		} else {
			return nil, fmt.Errorf("no column value for %s", name)
		}
	})
}
func (c *client) filterSystemPeerValues(stmt *parser.SelectStatement, filtered []*message.ColumnMetadata, peer *node, peerCount int) (row []message.Column, err error) {
	return parser.FilterValues(stmt, filtered, func(name string) (value message.Column, err error) {
		if name == "data_center" {
			return proxycore.EncodeType(datatype.Varchar, c.proxy.cluster.NegotiatedVersion, peer.dc)
		} else if name == "host_id" {
			return proxycore.EncodeType(datatype.Uuid, c.proxy.cluster.NegotiatedVersion, nameBasedUUID(peer.addr.String()))
		} else if name == "tokens" {
			return proxycore.EncodeType(datatype.NewListType(datatype.Varchar), c.proxy.cluster.NegotiatedVersion, peer.tokens)
		} else if name == "peer" {
			return proxycore.EncodeType(datatype.Inet, c.proxy.cluster.NegotiatedVersion, peer.addr.IP)
		} else if name == "rpc_address" {
			return proxycore.EncodeType(datatype.Inet, c.proxy.cluster.NegotiatedVersion, peer.addr.IP)
		} else if val, ok := c.proxy.systemLocalValues[name]; ok {
			return val, nil
		} else if name == parser.CountValueName {
			return proxycore.EncodeType(datatype.Int, c.proxy.cluster.NegotiatedVersion, peerCount)
		} else {
			return nil, fmt.Errorf("no column value for %s", name)
		}
	})
}

// getSystemMetadata retrieves system metadata for `system_schema` keyspaces, tables, or columns.
//
// Parameters:
// - hdr: *frame.Header (request version info)
// - s: *parser.SelectStatement (keyspace and table info)
//
// Returns:
// - []message.Row: Metadata rows for the requested table; empty if keyspace/table is invalid.
func (c *client) getSystemMetadata(hdr *frame.Header, s *parser.SelectStatement) ([]message.Row, error) {
	if s.Keyspace != system_schema || (s.Table != keyspaces && s.Table != tables && s.Table != columns) {
		return nil, nil
	}

	var cache map[primitive.ProtocolVersion][]message.Row
	var errMsg error
	switch s.Table {
	case keyspaces:
		cache = c.proxy.systemQueryMetadataCache.KeyspaceSystemQueryMetadataCache
		errMsg = fmt.Errorf(systemQueryMetadataNotFoundError, "KeyspaceSystemQueryMetadataCache", hdr.Version)
	case tables:
		cache = c.proxy.systemQueryMetadataCache.TableSystemQueryMetadataCache
		errMsg = fmt.Errorf(systemQueryMetadataNotFoundError, "TableSystemQueryMetadataCache", hdr.Version)
	case columns:
		cache = c.proxy.systemQueryMetadataCache.ColumnsSystemQueryMetadataCache
		errMsg = fmt.Errorf(systemQueryMetadataNotFoundError, "ColumnsSystemQueryMetadataCache", hdr.Version)
	}

	if data, exist := cache[hdr.Version]; !exist {
		return nil, errMsg
	} else {
		return data, nil
	}
}

// Intercept and handle system query
func (c *client) interceptSystemQuery(hdr *frame.Header, stmt interface{}) {
	switch s := stmt.(type) {
	case *parser.SelectStatement:
		if s.Keyspace == system_schema || s.Keyspace == system_virtual_schema {
			var localColumns []*message.ColumnMetadata
			var isFound bool
			if s.Keyspace == system_schema {
				localColumns, isFound = parser.SystemSchematablesColumn[s.Table]
				if isFound {
					tableMetadata := &message.RowsMetadata{
						ColumnCount: int32(len(localColumns)),
						Columns:     localColumns,
					}

					data, err := c.getSystemMetadata(hdr, s)
					if err != nil {
						c.sender.Send(hdr, &message.Invalid{ErrorMessage: err.Error()})
						return
					}

					c.sender.Send(hdr, &message.RowsResult{
						Metadata: tableMetadata,
						Data:     data,
					})
					return
				}
			} else {
				// get Table metadata for system_virtual_schema schema
				localColumns, isFound = parser.SystemVirtualSchemaColumn[s.Table]
				if isFound {
					c.sender.Send(hdr, &message.RowsResult{
						Metadata: &message.RowsMetadata{
							ColumnCount: int32(len(localColumns)),
							Columns:     localColumns,
						},
					})
					return
				}
			}
			if !isFound {
				c.sender.Send(hdr, &message.Invalid{ErrorMessage: "Error while fetching mocked table info"})
				return
			}
		} else if s.Table == local {
			localColumns := parser.SystemLocalColumns
			if len(c.proxy.cluster.Info.DSEVersion) > 0 {
				localColumns = parser.DseSystemLocalColumns
			}
			if columns, err := parser.FilterColumns(s, localColumns); err != nil {
				c.sender.Send(hdr, &message.Invalid{ErrorMessage: err.Error()})
			} else if row, err := c.filterSystemLocalValues(s, columns); err != nil {
				c.sender.Send(hdr, &message.Invalid{ErrorMessage: err.Error()})
			} else {
				c.sender.Send(hdr, &message.RowsResult{
					Metadata: &message.RowsMetadata{
						ColumnCount: int32(len(columns)),
						Columns:     columns,
					},
					Data: []message.Row{row},
				})
			}
		} else if s.Table == "peers" {
			peersColumns := parser.SystemPeersColumns
			if len(c.proxy.cluster.Info.DSEVersion) > 0 {
				peersColumns = parser.DseSystemPeersColumns
			}
			if columns, err := parser.FilterColumns(s, peersColumns); err != nil {
				c.sender.Send(hdr, &message.Invalid{ErrorMessage: err.Error()})
			} else {
				var data []message.Row
				for _, n := range c.proxy.nodes {
					if n != c.proxy.localNode {
						var row message.Row
						row, err = c.filterSystemPeerValues(s, columns, n, len(c.proxy.nodes)-1)
						if err != nil {
							break
						}
						data = append(data, row)
					}
				}
				if err != nil {
					c.sender.Send(hdr, &message.Invalid{ErrorMessage: err.Error()})
				} else {
					c.sender.Send(hdr, &message.RowsResult{
						Metadata: &message.RowsMetadata{
							ColumnCount: int32(len(columns)),
							Columns:     columns,
						},
						Data: data,
					})
				}
			}
			// CC- metadata is mocked here as well for system queries
		} else if columns, ok := parser.SystemColumnsByName[s.Table]; ok {
			c.sender.Send(hdr, &message.RowsResult{
				Metadata: &message.RowsMetadata{
					ColumnCount: int32(len(columns)),
					Columns:     columns,
				},
			})
		} else {
			c.sender.Send(hdr, &message.Invalid{ErrorMessage: "Doesn't exist"})
		}
	case *parser.UseStatement:
		c.keyspace = s.Keyspace
		c.sender.Send(hdr, &message.SetKeyspaceResult{Keyspace: s.Keyspace})
	default:
		c.sender.Send(hdr, &message.ServerError{ErrorMessage: "Proxy attempted to intercept an unhandled query"})
	}
}

func (c *client) Send(hdr *frame.Header, msg message.Message) {
	_ = c.conn.Write(proxycore.SenderFunc(func(writer io.Writer) error {
		return codec.EncodeFrame(frame.NewFrame(hdr.Version, hdr.StreamId, msg), writer)
	}))
}

func (c *client) Closing(_ error) {
	c.proxy.removeClient(c)
}

func getOrCreateDefaultPreparedCache(cache proxycore.PreparedCache) (proxycore.PreparedCache, error) {
	if cache == nil {
		return NewDefaultPreparedCache(1e8 / 256) // ~100MB with an average query size of 256 bytes
	}
	return cache, nil
}

// NewDefaultPreparedCache creates a new default prepared cache capping the max item capacity to `size`.
func NewDefaultPreparedCache(size int) (proxycore.PreparedCache, error) {
	cache, err := lru.New(size)
	if err != nil {
		return nil, err
	}
	return &defaultPreparedCache{cache}, nil
}

type defaultPreparedCache struct {
	cache *lru.Cache
}

func (d defaultPreparedCache) Store(id string, entry *proxycore.PreparedEntry) {
	d.cache.Add(id, entry)
}

func (d defaultPreparedCache) Load(id string) (entry *proxycore.PreparedEntry, ok bool) {
	if val, ok := d.cache.Get(id); ok {
		return val.(*proxycore.PreparedEntry), true
	}
	return nil, false
}

func preparedIdKey(bytes []byte) [preparedIdSize]byte {
	var buf [preparedIdSize]byte
	copy(buf[:], bytes)
	return buf
}

func nameBasedUUID(name string) primitive.UUID {
	var uuid primitive.UUID
	m := crypto.MD5.New()
	_, _ = io.WriteString(m, name)
	hash := m.Sum(nil)
	for i := 0; i < len(uuid); i++ {
		uuid[i] = hash[i]
	}
	uuid[6] &= 0x0F
	uuid[6] |= 0x30
	uuid[8] &= 0x3F
	uuid[8] |= 0x80
	return uuid
}

// Wrap the listener so that if it's closed in the serve loop it doesn't race with proxy Close()
type closeOnceListener struct {
	net.Listener
	once     sync.Once
	closeErr error
}

func (oc *closeOnceListener) Close() error {
	oc.once.Do(oc.close)
	return oc.closeErr
}

func (oc *closeOnceListener) close() { oc.closeErr = oc.Listener.Close() }

type SpannerClient struct {
	Client          *spanner.Client
	Logger          interface{}
	KeyspaceFlatter interface{}
}

func (sc *SpannerClient) GetClient(ctx context.Context) (*spanner.Client, error) {
	return sc.Client, nil
}

// NewSpannerClient creates a new instance of SpannerClient
var NewSpannerClient = func(ctx context.Context, config Config, ot *otelgo.OpenTelemetry) iface.SpannerClientInterface {
	// Check the environment variables for the Spanner emulator.
	// If not set, enable multiplexed session and direct access features.
	if os.Getenv("SPANNER_EMULATOR_HOST") == "" {
		// Enable multiplexed sessions
		os.Setenv("GOOGLE_CLOUD_SPANNER_MULTIPLEXED_SESSIONS", "true")
	}
	// Implementation
	// Configure gRPC connection pool with minimum connection timeout
	pool := grpc.WithConnectParams(grpc.ConnectParams{
		MinConnectTimeout: 10 * time.Second,
	})

	spc := spanner.DefaultSessionPoolConfig

	if config.SpannerConfig.MinSessions != 0 {
		spc.MinOpened = config.SpannerConfig.MinSessions
	}

	if config.SpannerConfig.MaxSessions != 0 {
		spc.MaxOpened = config.SpannerConfig.MaxSessions
	}

	spc.InactiveTransactionRemovalOptions = spanner.InactiveTransactionRemovalOptions{
		ActionOnInactiveTransaction: spanner.WarnAndClose,
	}

	cfg := spanner.ClientConfig{SessionPoolConfig: spc, UserAgent: config.UserAgent}

	// If OpenTelemetry is provided, configure instrumentation
	if config.OtelConfig.Enabled && ot != nil {
		if config.OtelConfig.Metrics.Enabled {
			if config.OtelConfig.EnabledClientSideMetrics {
				// Enable OpenTelemetry metrics before injecting meter provider.
				spanner.EnableOpenTelemetryMetrics()
			}
			// Add OpenTelemetry instrumentation to Spanner client configuration
			cfg.OpenTelemetryMeterProvider = ot.MeterProvider
		}

		if config.OtelConfig.Traces.Enabled {
			// Setting the GOOGLE_API_GO_EXPERIMENTAL_TELEMETRY_PLATFORM_TRACING env varibale to 'opentelemetry' will enable traces on spanner client library.
			os.Setenv("GOOGLE_API_GO_EXPERIMENTAL_TELEMETRY_PLATFORM_TRACING", "opentelemetry")

			// Set up OpenTelemetry traces and metrics
			otel.SetTracerProvider(ot.TracerProvider)
		}
	}

	database := fmt.Sprintf(SpannerConnectionString, config.SpannerConfig.GCPProjectID, config.SpannerConfig.InstanceName, config.SpannerConfig.DatabaseName)
	client, err := spanner.NewClientWithConfig(ctx, database,
		cfg,
		option.WithGRPCConnectionPool(config.SpannerConfig.NumOfChannels),
		option.WithGRPCDialOption(pool))
	// Create the Spanner client
	if err != nil {
		config.Logger.Error("Failed to create client" + err.Error())
		return nil
	}
	// return client, nil
	return &SpannerClient{
		Client:          client,
		KeyspaceFlatter: config.KeyspaceFlatter,
	}
}

// Function to records otel metrics on otel
// Metrics: 1. request count; 2. latency
func recordMetrics(ctx context.Context, o *otelgo.OpenTelemetry, method string, start time.Time, queryType string, err error) {
	status := "OK"
	if err != nil {
		status = "failure"
	}
	o.RecordRequestCountMetric(ctx, otelgo.Attributes{
		Method:    method,
		Status:    status,
		QueryType: queryType,
	})
	o.RecordLatencyMetric(ctx, start, otelgo.Attributes{
		Method:    method,
		QueryType: queryType,
	})
}
