// Copyright (c) DataStax, Inc.

// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at

//      http://www.apache.org/licenses/LICENSE-2.0

// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package proxy

import (
	"context"
	"errors"
	"fmt"
	"log"
	"math/big"
	"net"
	"net/http"
	"reflect"

	sdktrace "go.opentelemetry.io/otel/sdk/trace"

	frame "github.com/datastax/go-cassandra-native-protocol/frame"

	"strconv"
	"sync"
	"testing"
	"time"

	message "github.com/datastax/go-cassandra-native-protocol/message"

	"cloud.google.com/go/spanner"
	otelMock "github.com/cloudspannerecosystem/cassandra-to-spanner-proxy/mocks/otel"
	proxyIfaceMock "github.com/cloudspannerecosystem/cassandra-to-spanner-proxy/mocks/proxy/iface"
	spannerMock "github.com/cloudspannerecosystem/cassandra-to-spanner-proxy/mocks/spanner"
	otelgo "github.com/cloudspannerecosystem/cassandra-to-spanner-proxy/otel"
	"github.com/cloudspannerecosystem/cassandra-to-spanner-proxy/third_party/datastax/parser"
	"github.com/cloudspannerecosystem/cassandra-to-spanner-proxy/third_party/datastax/proxy/iface"
	"github.com/cloudspannerecosystem/cassandra-to-spanner-proxy/translator"
	"github.com/cloudspannerecosystem/cassandra-to-spanner-proxy/utilities"
	"github.com/datastax/go-cassandra-native-protocol/datatype"
	"github.com/datastax/go-cassandra-native-protocol/primitive"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"

	// "github.com/cloudspannerecosystem/cassandra-to-spanner-proxy/third_party/datastax/proxy"
	"github.com/cloudspannerecosystem/cassandra-to-spanner-proxy/responsehandler"
	spannerModule "github.com/cloudspannerecosystem/cassandra-to-spanner-proxy/spanner"
	"github.com/cloudspannerecosystem/cassandra-to-spanner-proxy/tableConfig"
	"github.com/cloudspannerecosystem/cassandra-to-spanner-proxy/third_party/datastax/proxycore"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

const (
	testAddr      = "127.0.0.1"
	testStartAddr = "127.0.0.0"
)

func generateTestPort() int {
	addr, err := net.ResolveTCPAddr("tcp", "localhost:0")
	if err != nil {
		log.Panicf("failed to resolve for local port: %v", err)
	}

	l, err := net.ListenTCP("tcp", addr)
	if err != nil {
		log.Panicf("failed to listen for local port: %v", err)
	}
	defer l.Close()
	return l.Addr().(*net.TCPAddr).Port
}

func generateTestAddr(baseAddress string, n int) string {
	ip := make(net.IP, net.IPv6len)
	new(big.Int).Add(new(big.Int).SetBytes(net.ParseIP(baseAddress)), big.NewInt(int64(n))).FillBytes(ip)
	return ip.String()
}

func generateTestAddrs(host string) (clusterPort int, clusterAddr, proxyAddr, httpAddr string) {
	clusterPort = generateTestPort()
	clusterAddr = net.JoinHostPort(host, strconv.Itoa(clusterPort))
	proxyPort := generateTestPort()
	proxyAddr = net.JoinHostPort(host, strconv.Itoa(proxyPort))
	httpPort := generateTestPort()
	httpAddr = net.JoinHostPort(host, strconv.Itoa(httpPort))
	return clusterPort, clusterAddr, proxyAddr, httpAddr
}

func TestNewProxyWithOtel(t *testing.T) {
	ctx := context.Background()

	// Initialize a logger
	logger := proxycore.GetOrCreateNopLogger(nil)

	// Mock for SpannerClientInterface
	mockSpannerClient := new(proxyIfaceMock.SpannerClientInterface)
	mockSpannerClient.On("GetClient", ctx).Return(&spanner.Client{}, nil)
	originalNewProxySpannerClient := NewSpannerClient
	NewSpannerClient = func(ctx context.Context, config Config, ot *otelgo.OpenTelemetry) iface.SpannerClientInterface {
		return mockSpannerClient
	}
	defer func() { NewSpannerClient = originalNewProxySpannerClient }()

	// Initialize the columnMap and columnListMap
	columnMap := make(map[string]map[string]*tableConfig.Column)
	columnMap["table1"] = make(map[string]*tableConfig.Column)
	columnMap["table1"]["column1"] = &tableConfig.Column{
		Name:    "column1",
		CQLType: "string",
	}

	columnListMap := make(map[string][]tableConfig.Column)
	columnListMap["table1"] = []tableConfig.Column{
		{
			Name:    "column1",
			CQLType: "string",
		},
		{
			Name:    "column2",
			CQLType: "int",
		},
	}
	proxyConfig := Config{
		OtelConfig: &OtelConfig{
			Enabled: true,
		},
		Version: primitive.ProtocolVersion4,
		Logger:  logger,
		SpannerConfig: SpannerConfig{
			GCPProjectID:    "cassandra-to-spanner",
			InstanceName:    "test-instance-v1",
			DatabaseName:    "test-database",
			ConfigTableName: "test",
		},
		KeyspaceFlatter: true,
	}

	// Mock for SpannerClientIface
	spanmockface := new(spannerMock.SpannerClientIface)
	spannerQuery := fmt.Sprintf("SELECT * FROM %v ORDER BY TableName, IsPrimaryKey, PK_Precedence;", proxyConfig.SpannerConfig.ConfigTableName)
	query := responsehandler.QueryMetadata{
		Query:        spannerQuery,
		TableName:    proxyConfig.SpannerConfig.ConfigTableName,
		ProtocalV:    proxyConfig.Version,
		KeyspaceName: proxyConfig.SpannerConfig.DatabaseName,
	}
	spanmockface.On("GetTableConfigs", ctx, query).Return(columnMap, columnListMap, &spannerModule.SystemQueryMetadataCache{}, nil)

	// Override the factory function to return the mock
	originalNewSpannerClient := spannerModule.NewSpannerClient
	spannerModule.NewSpannerClient = func(client *spanner.Client, logger *zap.Logger, responseHandler *responsehandler.TypeHandler, keyspaceFlatter bool, maxCommitDelay uint64, replayProtection bool) spannerModule.SpannerClientIface {
		return spanmockface
	}
	defer func() { spannerModule.NewSpannerClient = originalNewSpannerClient }()

	mterIFaceface := new(otelMock.MeterProvider)
	mterIFaceface.On("InitMeterProvider", ctx).Return(&sdkmetric.MeterProvider{}, nil)

	tracerIFaceface := new(otelMock.TracerProvider)
	tracerIFaceface.On("InitTracerProvider", ctx).Return(&sdktrace.TracerProvider{}, nil)

	// Call the function under test
	proxyConfig.Version = 0
	proxyConfig.OtelConfig.HealthCheck.Endpoint = "localhost:7062/TestNewOpenTelemetry"
	proxyConfig.OtelConfig.Traces.Endpoint = "http://localhost:7060"
	proxyConfig.OtelConfig.Metrics.Endpoint = "http://localhost:7061"
	proxyConfig.OtelConfig.ServiceName = "test"
	proxyConfig.OtelConfig.Traces.SamplingRatio = 1.0

	srv1 := setupTestEndpoint(":7060", "/trace")
	srv2 := setupTestEndpoint(":7061", "/metric")
	srv := setupTestEndpoint(":7062", "/TestNewOpenTelemetry")
	time.Sleep(3 * time.Second)
	defer func() {
		assert.NoError(t, srv.Shutdown(ctx), "failed to shutdown srv")
		assert.NoError(t, srv1.Shutdown(ctx), "failed to shutdown srv1")
		assert.NoError(t, srv2.Shutdown(ctx), "failed to shutdown srv2")
	}()
	prox, err := NewProxy(ctx, proxyConfig)
	assert.NotNil(t, prox, "should not be nil")
	assert.NoError(t, err, "should not throw an error")
}

func TestConnect(t *testing.T) {
	var logger *zap.Logger
	logger = proxycore.GetOrCreateNopLogger(logger)
	mp := make(map[*client]struct{})
	prox := &Proxy{
		clients: mp,
		logger:  logger,
	}
	err := prox.Connect()
	assert.NoError(t, err, "function should return no error")
}

func TestNameBasedUUID(t *testing.T) {
	uuid := nameBasedUUID("test")
	assert.NotNilf(t, uuid, "should not be nil")
}

func Test_addclient(t *testing.T) {

	mp := make(map[*client]struct{})
	prox := &Proxy{
		clients: mp,
	}

	cl := &client{
		ctx:                 prox.ctx,
		proxy:               prox,
		preparedSystemQuery: make(map[[preparedIdSize]byte]interface{}),
	}
	prox.addClient(cl)
	prox.registerForEvents(cl)
}

func Test_OnEvent(t *testing.T) {
	var logger *zap.Logger

	logger = proxycore.GetOrCreateNopLogger(logger)
	mp := make(map[*client]struct{})
	prox := &Proxy{
		clients: mp,
		logger:  logger,
	}
	prox.OnEvent(&proxycore.SchemaChangeEvent{Message: &message.SchemaChangeEvent{ChangeType: primitive.SchemaChangeType(primitive.EventTypeSchemaChange)}})

}

func Test_Serve(t *testing.T) {
	var logger *zap.Logger
	logger = proxycore.GetOrCreateNopLogger(logger)

	logger = proxycore.GetOrCreateNopLogger(logger)
	mp := make(map[*client]struct{})
	ls := make(map[*net.Listener]struct{})
	prox := &Proxy{
		clients:   mp,
		logger:    logger,
		listeners: ls,
	}

	var httpListener net.Listener
	httpListener, err := resolveAndListen(":7777", "", "")
	assert.NoErrorf(t, err, "no error expected")

	prox.isConnected = true
	go func() {
		time.Sleep(5 * time.Second)
		prox.mu.Lock()
		prox.isClosing = true
		httpListener.Close()
		prox.mu.Unlock()
	}()
	err = prox.Serve(httpListener)
	assert.Errorf(t, err, "error expected")

}

func Test_ServeScenario2(t *testing.T) {
	var logger *zap.Logger

	logger = proxycore.GetOrCreateNopLogger(logger)
	mp := make(map[*client]struct{})
	ls := make(map[*net.Listener]struct{})
	cl := make(chan struct{})
	prox := &Proxy{
		clients:   mp,
		logger:    logger,
		listeners: ls,
		closed:    cl,
	}

	var httpListener net.Listener
	httpListener, err := resolveAndListen(":7777", "", "")
	assert.NoErrorf(t, err, "no error expected")

	prox.isConnected = true
	go func() {
		time.Sleep(5 * time.Second)
		close(prox.closed)
		httpListener.Close()
	}()
	err = prox.Serve(httpListener)
	assert.Errorf(t, err, "error expected")

}

func TestProxy_UseKeyspace(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	_, proxyContactPoint, err := setupProxyTest(ctx, 1, nil)
	defer func() {
		cancel()
	}()
	require.NoError(t, err)

	cl := connectTestClient(t, ctx, proxyContactPoint)

	resp, err := cl.SendAndReceive(ctx, frame.NewFrame(primitive.ProtocolVersion4, 0, &message.Query{Query: "USE system"}))
	require.NoError(t, err)

	assert.Equal(t, primitive.OpCodeResult, resp.Header.OpCode)
	res, ok := resp.Body.Message.(*message.SetKeyspaceResult)
	require.True(t, ok, "expected set keyspace result")
	assert.Equal(t, "system", res.Keyspace)
}

func TestProxy_NegotiateProtocolV5(t *testing.T) {

	ctx, cancel := context.WithCancel(context.Background())

	_, proxyContactPoint, err := setupProxyTest(ctx, 1, nil)
	defer func() {
		cancel()
	}()
	require.NoError(t, err)

	cl, err := proxycore.ConnectClient(ctx, proxycore.NewEndpoint(proxyContactPoint), proxycore.ClientConnConfig{})
	require.NoError(t, err)

	version, err := cl.Handshake(ctx, primitive.ProtocolVersion5, nil)
	require.NoError(t, err)
	assert.Equal(t, primitive.ProtocolVersion4, version) // Expected to be negotiated to v4
}

func queryTestHosts(ctx context.Context, cl *proxycore.ClientConn) (map[string]struct{}, error) {
	hosts := make(map[string]struct{})
	for i := 0; i < 3; i++ {
		rs, err := cl.Query(ctx, primitive.ProtocolVersion4, &message.Query{Query: "SELECT * FROM test.test"})
		if err != nil {
			return nil, err
		}
		if rs.RowCount() < 1 {
			return nil, errors.New("invalid row count")
		}
		val, err := rs.Row(0).ByName("host")
		if err != nil {
			return nil, err
		}
		hosts[val.(string)] = struct{}{}
	}
	return hosts, nil
}

type proxyTester struct {
	cluster *proxycore.MockCluster
	proxy   *Proxy
	wg      sync.WaitGroup
}

func (w *proxyTester) shutdown() {
	w.cluster.Shutdown()
	_ = w.proxy.Close()
	w.wg.Wait()
}

func setupProxyTest(ctx context.Context, numNodes int, handlers proxycore.MockRequestHandlers) (tester *proxyTester, proxyContactPoint string, err error) {
	return setupProxyTestWithConfig(ctx, numNodes, &proxyTestConfig{handlers: handlers})
}

type proxyTestConfig struct {
	handlers        proxycore.MockRequestHandlers
	dseVersion      string
	rpcAddr         string
	peers           []PeerConfig
	idempotentGraph bool
}

func setupProxyTestWithConfig(ctx context.Context, numNodes int, cfg *proxyTestConfig) (tester *proxyTester, proxyContactPoint string, err error) {
	tester = &proxyTester{
		wg: sync.WaitGroup{},
	}

	clusterPort, clusterAddr, proxyAddr, _ := generateTestAddrs(testAddr)

	tester.cluster = proxycore.NewMockCluster(net.ParseIP(testStartAddr), clusterPort)
	tester.cluster.DseVersion = cfg.dseVersion

	if cfg == nil {
		cfg = &proxyTestConfig{}
	}

	if cfg.handlers != nil {
		tester.cluster.Handlers = proxycore.NewMockRequestHandlers(cfg.handlers)
	}

	for i := 1; i <= numNodes; i++ {
		err = tester.cluster.Add(ctx, i)
		if err != nil {
			return tester, proxyAddr, err
		}
	}
	tester.proxy, err = newProxy(&Config{Resolver: proxycore.NewResolverWithDefaultPort([]string{clusterAddr}, clusterPort)})
	if err != nil {
		return tester, proxyAddr, err
	}

	err = tester.proxy.Connect()
	if err != nil {
		return tester, proxyAddr, err
	}

	l, err := resolveAndListen(proxyAddr, "", "")
	if err != nil {
		return tester, proxyAddr, err
	}

	tester.wg.Add(1)

	go func() {
		_ = tester.proxy.Serve(l)
		tester.wg.Done()
	}()

	return tester, proxyAddr, nil
}

func connectTestClient(t *testing.T, ctx context.Context, proxyContactPoint string) *proxycore.ClientConn {
	cl, err := proxycore.ConnectClient(ctx, proxycore.NewEndpoint(proxyContactPoint), proxycore.ClientConnConfig{})
	require.NoError(t, err)

	version, err := cl.Handshake(ctx, primitive.ProtocolVersion4, nil)
	require.NoError(t, err)
	assert.Equal(t, primitive.ProtocolVersion4, version)

	return cl
}

func waitUntil(d time.Duration, check func() bool) bool {
	iterations := int(d / (100 * time.Millisecond))
	for i := 0; i < iterations; i++ {
		if check() {
			return true
		}
		time.Sleep(100 * time.Millisecond)
	}
	return false
}

func Test_client_prepareDeleteType(t *testing.T) {

	mockRawFrame := &frame.RawFrame{
		Header: &frame.Header{
			Version:  primitive.ProtocolVersion4,
			Flags:    0,
			StreamId: 0,
			OpCode:   primitive.OpCodePrepare,
		},
		Body: []byte{},
	}
	mockColMeta := []*message.ColumnMetadata{
		{Name: "test_id", Type: datatype.Varchar, Index: 0},
		{Name: "test_hash", Type: datatype.Varchar, Index: 1},
	}

	mockProxy, err := newProxy(&Config{})
	assert.NoErrorf(t, err, "error occured while creating the proxy")
	mockProxy.translator = &translator.Translator{
		TableConfig:     mockProxy.tableConfig,
		Logger:          mockProxy.logger,
		KeyspaceFlatter: false,
	}
	mockSender1 := new(proxyIfaceMock.Sender)
	mockSender1.On("Send", mock.AnythingOfType("*frame.Header"), mock.AnythingOfType("*message.Invalid")).Return()
	type fields struct {
		ctx                 context.Context
		proxy               *Proxy
		conn                *proxycore.Conn
		keyspace            string
		preparedSystemQuery map[[16]byte]interface{}
	}

	tests := []struct {
		name    string
		fields  fields
		raw     *frame.RawFrame
		query   *message.Prepare
		id      [16]byte
		modify  func(*client)
		setup   func(*client)
		want    []*message.ColumnMetadata
		want1   []*message.ColumnMetadata
		wantErr bool
	}{

		{
			name: "Success Case",
			fields: fields{
				proxy: mockProxy,
				ctx:   mockProxy.ctx,
			},
			setup: func(c *client) {
				c.preparedSystemQuery = make(map[[16]byte]interface{})
				c.sender = mockSender1
			},
			id:  [16]byte{10},
			raw: mockRawFrame,
			query: &message.Prepare{
				Query: "DELETE FROM test_keyspace.test_table WHERE test_id = '?' AND test_hash = '?'",
			},
			want:    mockColMeta,
			want1:   mockColMeta,
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := &client{
				ctx:                 tt.fields.ctx,
				proxy:               mockProxy,
				conn:                tt.fields.conn,
				keyspace:            tt.fields.keyspace,
				preparedSystemQuery: tt.fields.preparedSystemQuery,
			}
			if tt.setup != nil {
				tt.setup(c)
			}
			if tt.modify != nil {
				tt.modify(c)
			}
			_, _, err := c.prepareDeleteType(tt.raw, tt.query, tt.id)
			if (err != nil) != tt.wantErr {
				t.Errorf("client.prepareDeleteType() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
		})
	}
}

func Test_client_prepareInsertType(t *testing.T) {
	mockRawFrame := &frame.RawFrame{
		Header: &frame.Header{
			Version:  primitive.ProtocolVersion4,
			Flags:    0,
			StreamId: 0,
			OpCode:   primitive.OpCodePrepare,
		},
		Body: []byte{},
	}
	mockColMeta := []*message.ColumnMetadata{
		{Name: "test_id", Type: datatype.Varchar, Index: 0},
		{Name: "test_hash", Type: datatype.Varchar, Index: 1},
	}
	ctx := context.Background()

	// Initialize a logger
	logger := proxycore.GetOrCreateNopLogger(nil)

	// Mock for SpannerClientInterface
	mockSpannerClient := new(proxyIfaceMock.SpannerClientInterface)
	mockSpannerClient.On("GetClient", ctx).Return(&spanner.Client{}, nil)
	originalNewProxySpannerClient := NewSpannerClient
	NewSpannerClient = func(ctx context.Context, config Config, ot *otelgo.OpenTelemetry) iface.SpannerClientInterface {
		return mockSpannerClient
	}
	defer func() { NewSpannerClient = originalNewProxySpannerClient }()

	// Initialize the columnMap and columnListMap
	columnMap := make(map[string]map[string]*tableConfig.Column)
	columnMap["test_table"] = make(map[string]*tableConfig.Column)
	columnMap["test_table"]["test_id"] = &tableConfig.Column{
		Name:         "test_id",
		CQLType:      "text",
		SpannerType:  "string",
		IsPrimaryKey: true,
		PkPrecedence: 0,
		Metadata: message.ColumnMetadata{
			Keyspace: "test_keyspace",
			Table:    "test_table",
			Index:    0,
			Type:     datatype.Varchar,
			Name:     "test_id",
		},
	}
	columnMap["test_table"]["test_hash"] = &tableConfig.Column{
		Name:         "test_hash",
		CQLType:      "text",
		SpannerType:  "string",
		IsPrimaryKey: false,
		PkPrecedence: 1,
		Metadata: message.ColumnMetadata{
			Keyspace: "test_keyspace",
			Table:    "test_table",
			Index:    1,
			Type:     datatype.Varchar,
			Name:     "test_hash",
		},
	}

	columnListMap := make(map[string][]tableConfig.Column)
	columnListMap["test_table"] = []tableConfig.Column{
		{
			Name:         "test_id",
			CQLType:      "text",
			IsPrimaryKey: true,
			Metadata: message.ColumnMetadata{
				Keyspace: "test_keyspace",
				Table:    "test_table",
				Index:    0,
				Type:     datatype.Varchar,
				Name:     "test_id",
			},
		},
		{
			Name:         "test_hash",
			CQLType:      "text",
			IsPrimaryKey: false,
			Metadata: message.ColumnMetadata{
				Keyspace: "test_keyspace",
				Table:    "test_table",
				Index:    1,
				Type:     datatype.Varchar,
				Name:     "test_hash",
			},
		},
	}
	proxyConfig := Config{
		OtelConfig: &OtelConfig{
			Enabled: false,
		},
		Version: primitive.ProtocolVersion4,
		Logger:  logger,
		SpannerConfig: SpannerConfig{
			GCPProjectID:    "cassandra-to-spanner",
			InstanceName:    "test-instance-v1",
			DatabaseName:    "test_database",
			ConfigTableName: "test_table_config",
		},
		KeyspaceFlatter: false,
	}

	// Mock for SpannerClientIface
	spanmockface := new(spannerMock.SpannerClientIface)
	spannerQuery := fmt.Sprintf("SELECT * FROM %v ORDER BY TableName, IsPrimaryKey, PK_Precedence;", proxyConfig.SpannerConfig.ConfigTableName)
	query := responsehandler.QueryMetadata{
		Query:        spannerQuery,
		TableName:    proxyConfig.SpannerConfig.ConfigTableName,
		ProtocalV:    proxyConfig.Version,
		KeyspaceName: "test_database",
	}
	queryForTest := responsehandler.QueryMetadata{
		Query:        "SELECT test_id, test_hash FROM test_keyspace.test_table WHERE test_id = '?' AND test_hash = '?'",
		TableName:    "test_table",
		ProtocalV:    primitive.ProtocolVersion4,
		Params:       make(map[string]interface{}),
		KeyspaceName: "test_keyspace",
	}
	// fmt.Println(queryMetadata)
	spanmockface.On("GetTableConfigs", ctx, query).Return(columnMap, columnListMap, &spannerModule.SystemQueryMetadataCache{}, nil)
	spanmockface.On("SelectStatement", ctx, queryForTest).Return(&message.RowsResult{}, nil)

	// Override the factory function to return the mock
	originalNewSpannerClient := spannerModule.NewSpannerClient
	spannerModule.NewSpannerClient = func(client *spanner.Client, logger *zap.Logger, responseHandler *responsehandler.TypeHandler, keysaceflatter bool, maxCommitDelay uint64, replayProtection bool) spannerModule.SpannerClientIface {
		return spanmockface
	}
	defer func() { spannerModule.NewSpannerClient = originalNewSpannerClient }()
	mockSender1 := new(proxyIfaceMock.Sender)
	mockSender1.On("Send", mock.AnythingOfType("*frame.Header"), mock.AnythingOfType("*message.Invalid")).Return()
	// Call the function under test
	proxyConfig.Version = 0
	prox, err := NewProxy(ctx, proxyConfig)
	prox.tableConfig = &tableConfig.TableConfig{
		TablesMetaData:  columnMap,
		PkMetadataCache: columnListMap,
	}
	if err != nil {
		assert.Errorf(t, err, "error occured while creating Proxy")
	}

	cl := &client{
		ctx:                 prox.ctx,
		proxy:               prox,
		preparedSystemQuery: make(map[[preparedIdSize]byte]interface{}),
		keyspace:            "test_keyspace",
		conn:                &proxycore.Conn{},
		sender:              mockSender1,
	}
	fmt.Println(mockSender1)
	fmt.Println("reached client initialization")
	prox.registerForEvents(cl)
	prox.addClient(cl)

	type fields struct {
		ctx                 context.Context
		proxy               *Proxy
		conn                *proxycore.Conn
		keyspace            string
		preparedSystemQuery map[[16]byte]interface{}
	}

	tests := []struct {
		name    string
		fields  fields
		raw     *frame.RawFrame
		query   *message.Prepare
		id      [16]byte
		setup   func(*client)
		want    []*message.ColumnMetadata
		want1   []*message.ColumnMetadata
		wantErr bool
	}{
		{
			name: "Success Case",
			fields: fields{
				proxy: prox,
				ctx:   prox.ctx,
			},
			setup: func(c *client) {
				c.preparedSystemQuery = make(map[[16]byte]interface{})
				c.sender = mockSender1
			},
			id:  [16]byte{10},
			raw: mockRawFrame,
			query: &message.Prepare{
				Query:    "INSERT INTO test_keyspace.test_table (test_id, test_hash) VALUES ('?', '?')",
				Keyspace: "key_space",
			},
			want:    mockColMeta,
			want1:   mockColMeta,
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := &client{
				ctx:                 prox.ctx,
				proxy:               prox,
				conn:                tt.fields.conn,
				keyspace:            tt.fields.keyspace,
				preparedSystemQuery: tt.fields.preparedSystemQuery,
			}
			if tt.setup != nil {
				tt.setup(c)
			}
			_, _, err = c.prepareInsertType(tt.raw, tt.query, tt.id)
			if (err != nil) != tt.wantErr {
				t.Errorf("client.prepareInsertType() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
		})
	}
}
func Test_client_prepareUpdateType(t *testing.T) {
	mockRawFrame := &frame.RawFrame{
		Header: &frame.Header{
			Version:  primitive.ProtocolVersion4,
			Flags:    0,
			StreamId: 0,
			OpCode:   primitive.OpCodePrepare,
		},
		Body: []byte{},
	}

	mockColMeta := []*message.ColumnMetadata{
		{Name: "test_id", Type: datatype.Varchar, Index: 0},
		{Name: "test_hash", Type: datatype.Varchar, Index: 1},
	}

	ctx := context.Background()

	// Initialize a logger
	logger := proxycore.GetOrCreateNopLogger(nil)

	// Mock for SpannerClientInterface
	mockSpannerClient := new(proxyIfaceMock.SpannerClientInterface)
	mockSpannerClient.On("GetClient", ctx).Return(&spanner.Client{}, nil)
	originalNewProxySpannerClient := NewSpannerClient
	NewSpannerClient = func(ctx context.Context, config Config, ot *otelgo.OpenTelemetry) iface.SpannerClientInterface {
		return mockSpannerClient
	}
	defer func() { NewSpannerClient = originalNewProxySpannerClient }()

	// Initialize the columnMap and columnListMap
	columnMap := make(map[string]map[string]*tableConfig.Column)
	columnMap["test_table"] = make(map[string]*tableConfig.Column)
	columnMap["test_table"]["test_id"] = &tableConfig.Column{
		Name:         "test_id",
		CQLType:      "text",
		SpannerType:  "string",
		IsPrimaryKey: true,
		PkPrecedence: 0,
		Metadata: message.ColumnMetadata{
			Keyspace: "test_keyspace",
			Table:    "test_table",
			Index:    0,
			Type:     datatype.Varchar,
			Name:     "test_id",
		},
	}
	columnMap["test_table"]["test_hash"] = &tableConfig.Column{
		Name:         "test_hash",
		CQLType:      "text",
		SpannerType:  "string",
		IsPrimaryKey: false,
		PkPrecedence: 1,
		Metadata: message.ColumnMetadata{
			Keyspace: "test_keyspace",
			Table:    "test_table",
			Index:    1,
			Type:     datatype.Varchar,
			Name:     "test_hash",
		},
	}

	columnListMap := make(map[string][]tableConfig.Column)
	columnListMap["test_table"] = []tableConfig.Column{
		{
			Name:         "test_id",
			CQLType:      "text",
			IsPrimaryKey: true,
			Metadata: message.ColumnMetadata{
				Keyspace: "test_keyspace",
				Table:    "test_table",
				Index:    0,
				Type:     datatype.Varchar,
				Name:     "test_id",
			},
		},
		{
			Name:         "test_hash",
			CQLType:      "text",
			IsPrimaryKey: false,
			Metadata: message.ColumnMetadata{
				Keyspace: "test_keyspace",
				Table:    "test_table",
				Index:    1,
				Type:     datatype.Varchar,
				Name:     "test_hash",
			},
		},
	}
	proxyConfig := Config{
		OtelConfig: &OtelConfig{
			Enabled: false,
		},
		Version: primitive.ProtocolVersion4,
		Logger:  logger,
		SpannerConfig: SpannerConfig{
			GCPProjectID:    "cassandra-to-spanner",
			InstanceName:    "test-instance-v1",
			DatabaseName:    "test_database",
			ConfigTableName: "test_table_config",
		},
		KeyspaceFlatter: false,
	}
	// Mock for SpannerClientIface
	spanmockface := new(spannerMock.SpannerClientIface)
	spannerQuery := fmt.Sprintf("SELECT * FROM %v ORDER BY TableName, IsPrimaryKey, PK_Precedence;", proxyConfig.SpannerConfig.ConfigTableName)
	query := responsehandler.QueryMetadata{
		Query:        spannerQuery,
		TableName:    proxyConfig.SpannerConfig.ConfigTableName,
		ProtocalV:    proxyConfig.Version,
		KeyspaceName: "test_database",
	}

	queryForTest := responsehandler.QueryMetadata{
		Query:        "SELECT test_id, test_hash FROM test_keyspace.test_table WHERE test_id = '?' AND test_hash = '?'",
		TableName:    "test_table",
		ProtocalV:    primitive.ProtocolVersion4,
		Params:       make(map[string]interface{}),
		KeyspaceName: "test_keyspace",
	}
	// fmt.Println(queryMetadata)
	spanmockface.On("GetTableConfigs", ctx, query).Return(columnMap, columnListMap, &spannerModule.SystemQueryMetadataCache{}, nil)
	spanmockface.On("SelectStatement", ctx, queryForTest).Return(&message.RowsResult{}, nil)

	// Override the factory function to return the mock
	originalNewSpannerClient := spannerModule.NewSpannerClient
	spannerModule.NewSpannerClient = func(client *spanner.Client, logger *zap.Logger, responseHandler *responsehandler.TypeHandler, keyspaceflatter bool, maxCommitDelay uint64, replayProtection bool) spannerModule.SpannerClientIface {
		return spanmockface
	}
	defer func() { spannerModule.NewSpannerClient = originalNewSpannerClient }()
	mockSender1 := new(proxyIfaceMock.Sender)
	mockSender1.On("Send", mock.AnythingOfType("*frame.Header"), mock.AnythingOfType("*message.Invalid")).Return()
	// Call the function under test
	proxyConfig.Version = 0
	prox, err := NewProxy(ctx, proxyConfig)
	prox.tableConfig = &tableConfig.TableConfig{
		TablesMetaData:  columnMap,
		PkMetadataCache: columnListMap,
	}
	if err != nil {
		assert.Errorf(t, err, "error occured while creating Proxy")
	}

	cl := &client{
		ctx:                 prox.ctx,
		proxy:               prox,
		preparedSystemQuery: make(map[[preparedIdSize]byte]interface{}),
		keyspace:            "test_keyspace",
		conn:                &proxycore.Conn{},
		sender:              mockSender1,
	}
	fmt.Println(mockSender1)
	fmt.Println("reached client initialization")
	prox.registerForEvents(cl)
	prox.addClient(cl)

	type fields struct {
		ctx                 context.Context
		proxy               *Proxy
		conn                *proxycore.Conn
		keyspace            string
		preparedSystemQuery map[[16]byte]interface{}
	}

	tests := []struct {
		name    string
		fields  fields
		raw     *frame.RawFrame
		query   *message.Prepare
		id      [16]byte
		setup   func(*client)
		want    []*message.ColumnMetadata
		want1   []*message.ColumnMetadata
		wantErr bool
	}{
		{
			name: "Success Case",
			fields: fields{
				proxy: prox,
				ctx:   prox.ctx,
			},
			setup: func(c *client) {
				c.preparedSystemQuery = make(map[[16]byte]interface{})
				c.sender = mockSender1
			},
			id:  [16]byte{10},
			raw: mockRawFrame,
			query: &message.Prepare{
				Query:    "UPDATE test_keyspace.test_table SET test_hash = ? WHERE test_id = ?",
				Keyspace: "key_space",
			},
			want:    mockColMeta,
			want1:   mockColMeta,
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := &client{
				ctx:                 prox.ctx,
				proxy:               prox,
				conn:                tt.fields.conn,
				keyspace:            tt.fields.keyspace,
				preparedSystemQuery: tt.fields.preparedSystemQuery,
			}
			if tt.setup != nil {
				tt.setup(c)
			}
			_, _, err = c.prepareUpdateType(tt.raw, tt.query, tt.id)
			if (err != nil) != tt.wantErr {
				t.Errorf("client.prepareInsertType() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
		})
	}
}

func Test_client_prepareSelectType(t *testing.T) {
	mockRawFrame := &frame.RawFrame{
		Header: &frame.Header{
			Version:  primitive.ProtocolVersion4,
			Flags:    0,
			StreamId: 0,
			OpCode:   primitive.OpCodePrepare,
		},
		Body: []byte{},
	}

	mockColMeta := []*message.ColumnMetadata{
		{Name: "test_id", Type: datatype.Varchar, Index: 0},
		{Name: "test_hash", Type: datatype.Varchar, Index: 1},
	}

	proxyConfig := &Config{
		OtelConfig: &OtelConfig{
			Enabled: false,
		},
		Version: primitive.ProtocolVersion4,
		Logger:  zap.NewNop(),
		SpannerConfig: SpannerConfig{
			GCPProjectID:    "cassandra-to-spanner",
			InstanceName:    "test-instance-v1",
			DatabaseName:    "test-database",
			ConfigTableName: "test_table_config",
		},
		KeyspaceFlatter: false,
	}
	mockProxy, err := newProxy(proxyConfig)
	assert.NoErrorf(t, err, "failed to create the proxy instance")
	mockProxy.translator = &translator.Translator{
		TableConfig:     mockProxy.tableConfig,
		Logger:          mockProxy.logger,
		KeyspaceFlatter: false,
	}

	type fields struct {
		ctx                 context.Context
		proxy               *Proxy
		conn                *proxycore.Conn
		keyspace            string
		preparedSystemQuery map[[16]byte]interface{}
	}
	mockSender1 := new(proxyIfaceMock.Sender)
	mockSender1.On("Send", mock.AnythingOfType("*frame.Header"), mock.AnythingOfType("*message.Invalid")).Return()
	tests := []struct {
		name    string
		fields  fields
		raw     *frame.RawFrame
		query   *message.Prepare
		id      [16]byte
		setup   func(*client)
		want    []*message.ColumnMetadata
		want1   []*message.ColumnMetadata
		wantErr bool
	}{
		{
			name: "Success Case",
			fields: fields{
				proxy: mockProxy,
				ctx:   mockProxy.ctx,
			},
			setup: func(c *client) {
				c.preparedSystemQuery = make(map[[16]byte]interface{})
				c.sender = mockSender1
			},
			id:  [16]byte{10},
			raw: mockRawFrame,
			query: &message.Prepare{
				Query:    "SELECT test_id, test_hash FROM test_keyspace.test_table WHERE test_id = '?' AND test_hash = '?'",
				Keyspace: "key_space",
			},
			want:    mockColMeta,
			want1:   mockColMeta,
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := &client{
				ctx:                 tt.fields.ctx,
				proxy:               mockProxy,
				conn:                tt.fields.conn,
				keyspace:            tt.fields.keyspace,
				preparedSystemQuery: tt.fields.preparedSystemQuery,
				// preparedQueriesCache: tt.fields.preparedQuerys,
			}
			if tt.setup != nil {
				tt.setup(c)
			}
			op := c.proxy.tableConfig.TablesMetaData["test_table"]
			fmt.Println("tablemetadata", op)

			op1 := c.proxy.tableConfig.PkMetadataCache["test_table"]
			fmt.Println("PkMetadataCache", op1)
			_, _, err := c.prepareSelectType(tt.raw, tt.query, tt.id)
			if (err != nil) != tt.wantErr {
				t.Errorf("client.prepareSelectType() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
		})
	}
}

type MockQueryPreparer struct {
	DeleteCalled bool
	DeleteParams struct {
		Raw *frame.RawFrame
		Msg *message.Prepare
		Id  [16]byte
	}
	InsertCalled bool
	InsertParams struct {
		Raw *frame.RawFrame
		Msg *message.Prepare
		Id  [16]byte
	}
	SelectCalled bool
	SelectParams struct {
		Raw *frame.RawFrame
		Msg *message.Prepare
		Id  [16]byte
	}
}

type mockSender struct {
	SendCalled bool
	SendParams struct {
		Hdr *frame.Header
		Msg message.Message
	}
}

func (m *mockSender) Send(hdr *frame.Header, msg message.Message) {
	m.SendCalled = true
	m.SendParams.Hdr = hdr
	m.SendParams.Msg = msg
}

var mockRawFrame = &frame.RawFrame{
	Header: &frame.Header{
		Version:  primitive.ProtocolVersion4,
		Flags:    0,
		StreamId: 0,
		OpCode:   primitive.OpCodePrepare,
	},
	Body: []byte{},
}

func Test_client_handleServerPreparedQuery(t *testing.T) {

	mockProxy, err := newProxy(&Config{})
	if err != nil {
		return
	}
	type fields struct {
		ctx                 context.Context
		proxy               *Proxy
		conn                *proxycore.Conn
		keyspace            string
		preparedSystemQuery map[[16]byte]interface{}
		preparedQuerys      map[[16]byte]interface{}
	}

	tests := []struct {
		name      string
		fields    fields
		raw       *frame.RawFrame
		query     *message.Prepare
		queryType string
		id        [16]byte
		setup     func(*client)
		want      []*message.ColumnMetadata
		want1     []*message.ColumnMetadata
		wantErr   bool
	}{
		{
			name: "Select Query Test",
			fields: fields{
				proxy: mockProxy,
			},
			raw: mockRawFrame,
			setup: func(c *client) {
				c.preparedSystemQuery = make(map[[16]byte]interface{})
			},
			query: &message.Prepare{Query: "SELECT test_id, test_hash FROM key_space.test_table WHERE test_id = '?'"},
		},
		{
			name: "Insert Query Test",
			fields: fields{
				proxy: mockProxy,
			},
			raw:   mockRawFrame,
			query: &message.Prepare{Query: "INSERT INTO key_space.test_table (test_id, test_hash) VALUES ('?', '?')"},
			setup: func(c *client) {
				c.preparedSystemQuery = make(map[[16]byte]interface{})
			},
		},
		{
			name: "Delete Query Test",
			fields: fields{
				proxy: mockProxy,
			},
			raw:   mockRawFrame,
			query: &message.Prepare{Query: "DELETE FROM key_space.test_table WHERE test_id = '?'"},
			setup: func(c *client) {
				c.preparedSystemQuery = make(map[[16]byte]interface{})
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mockQueryPreparer := &MockQueryPreparer{}
			mockSender := &mockSender{}
			c := &client{
				ctx:                 tt.fields.ctx,
				proxy:               mockProxy,
				conn:                tt.fields.conn,
				keyspace:            tt.fields.keyspace,
				preparedSystemQuery: tt.fields.preparedSystemQuery,
				sender:              mockSender,
			}
			if tt.setup != nil {
				tt.setup(c)
			}
			c.handleServerPreparedQuery(tt.raw, tt.query, tt.queryType)

			if mockQueryPreparer.DeleteCalled {
				if !reflect.DeepEqual(mockQueryPreparer.DeleteParams.Raw, tt.raw) {
					t.Errorf("PrepareDeleteType was not called with the correct RawFrame")
				}
				if !reflect.DeepEqual(mockQueryPreparer.DeleteParams.Msg, tt.query) {
					t.Errorf("PrepareDeleteType was not called with the correct Prepare message")
				}
			}

			if mockQueryPreparer.InsertCalled {
				if !reflect.DeepEqual(mockQueryPreparer.InsertParams.Raw, tt.raw) {
					t.Errorf("PrepareInsertType was not called with the correct RawFrame")
				}
				if !reflect.DeepEqual(mockQueryPreparer.InsertParams.Msg, tt.query) {
					t.Errorf("PrepareInsertType was not called with the correct Prepare message")
				}
			}

			if mockQueryPreparer.SelectCalled {
				if !reflect.DeepEqual(mockQueryPreparer.SelectParams.Raw, tt.raw) {
					t.Errorf("PrepareSelectType was not called with the correct RawFrame")
				}
				if !reflect.DeepEqual(mockQueryPreparer.SelectParams.Msg, tt.query) {
					t.Errorf("PrepareSelectType was not called with the correct Prepare message")
				}
			}
		})
	}
}

// Create mock for handleExecutionForDeletePreparedQuery/handleExecutionForSelectPreparedQuery/handleExecutionForInsertPreparedQuery functions.
type MockSpannerClient struct {
	SelectStatementFunc               func(ctx context.Context, query responsehandler.QueryMetadata) (*message.RowsResult, error)
	InsertUpdateOrDeleteStatementFunc func(ctx context.Context, query responsehandler.QueryMetadata) (*message.RowsResult, error)
	CloseFunc                         func() error
	InsertOrUpdateMutationFunc        func(ctx context.Context, query responsehandler.QueryMetadata) (*message.RowsResult, error)
	UpdateMapByKeyFunc                func(ctx context.Context, query responsehandler.QueryMetadata) (*message.RowsResult, error)
	DeleteUsingMutationsFunc          func(ctx context.Context, query responsehandler.QueryMetadata) (*message.RowsResult, error)
	FilterAndExecuteBatchFunc         func(ctx context.Context, queries []*responsehandler.QueryMetadata) (*message.RowsResult, error)
}

func (m *MockSpannerClient) DeleteUsingMutations(ctx context.Context, query responsehandler.QueryMetadata) (*message.RowsResult, error) {
	if m.DeleteUsingMutationsFunc != nil {
		return m.DeleteUsingMutationsFunc(ctx, query)
	}
	return nil, nil
}

var positionValues = []*primitive.Value{
	{Type: primitive.ValueTypeNull, Contents: []byte("value1")},
	{Type: primitive.ValueTypeNull, Contents: []byte("value2")},
}

var namedValues = map[string]*primitive.Value{
	"test_id":   {Type: primitive.ValueTypeNull, Contents: []byte("value1")},
	"test_hash": {Type: primitive.ValueTypeNull, Contents: []byte("value2")},
}

func Test_handleExecutionForDeletePreparedQuery(t *testing.T) {

	ctx := context.Background()
	// Mock for SpannerClientInterface
	mockSpannerClient := new(proxyIfaceMock.SpannerClientInterface)
	mockSpannerClient.On("GetClient", ctx).Return(&spanner.Client{}, nil)
	originalNewProxySpannerClient := NewSpannerClient
	NewSpannerClient = func(ctx context.Context, config Config, ot *otelgo.OpenTelemetry) iface.SpannerClientInterface {
		return mockSpannerClient
	}
	defer func() { NewSpannerClient = originalNewProxySpannerClient }()

	// Initialize the columnMap and columnListMap
	columnMap := make(map[string]map[string]*tableConfig.Column)
	columnMap["test_table"] = make(map[string]*tableConfig.Column)
	columnMap["test_table"]["test_id"] = &tableConfig.Column{
		Name:         "test_id",
		CQLType:      "text",
		SpannerType:  "string",
		IsPrimaryKey: true,
		PkPrecedence: 0,
		Metadata: message.ColumnMetadata{
			Keyspace: "test_keyspace",
			Table:    "test_table",
			Index:    0,
			Type:     datatype.Varchar,
			Name:     "test_id",
		},
	}
	columnMap["test_table"]["test_hash"] = &tableConfig.Column{
		Name:         "test_hash",
		CQLType:      "text",
		SpannerType:  "string",
		IsPrimaryKey: false,
		PkPrecedence: 1,
		Metadata: message.ColumnMetadata{
			Keyspace: "test_keyspace",
			Table:    "test_table",
			Index:    1,
			Type:     datatype.Varchar,
			Name:     "test_hash",
		},
	}

	columnListMap := make(map[string][]tableConfig.Column)
	columnListMap["test_table"] = []tableConfig.Column{
		{
			Name:         "test_id",
			CQLType:      "text",
			IsPrimaryKey: true,
			Metadata: message.ColumnMetadata{
				Keyspace: "test_keyspace",
				Table:    "test_table",
				Index:    0,
				Type:     datatype.Varchar,
				Name:     "test_id",
			},
		},
		{
			Name:         "test_hash",
			CQLType:      "text",
			IsPrimaryKey: false,
			Metadata: message.ColumnMetadata{
				Keyspace: "test_keyspace",
				Table:    "test_table",
				Index:    1,
				Type:     datatype.Varchar,
				Name:     "test_hash",
			},
		},
	}

	logger := proxycore.GetOrCreateNopLogger(nil)
	p, err := newProxy(&Config{})
	assert.NoErrorf(t, err, "error occured while creating the new proxy instance")
	proxyConfig := Config{
		OtelConfig: &OtelConfig{
			Enabled: false,
		},
		Version: primitive.ProtocolVersion4,
		Logger:  logger,
		SpannerConfig: SpannerConfig{
			GCPProjectID:    "cassandra-to-spanner",
			InstanceName:    "test-instance-v1",
			DatabaseName:    "test-database",
			ConfigTableName: "test",
			// StaleReads:      2,
		},
		KeyspaceFlatter: true,
	}

	queryForTest := responsehandler.QueryMetadata{
		Query:     "SELECT test_id, test_hash FROM key_space.test_table WHERE test_id = '?' AND test_hash = '?'",
		TableName: "test_table",
		ProtocalV: primitive.ProtocolVersion4,
		Params:    make(map[string]interface{}),
	}
	spannerQuery := fmt.Sprintf("SELECT * FROM %v ORDER BY TableName, IsPrimaryKey, PK_Precedence;", proxyConfig.SpannerConfig.ConfigTableName)

	// Mock for SpannerClientIface
	spanmockface := new(spannerMock.SpannerClientIface)
	query := responsehandler.QueryMetadata{
		Query:        spannerQuery,
		TableName:    proxyConfig.SpannerConfig.ConfigTableName,
		ProtocalV:    proxyConfig.Version,
		KeyspaceName: proxyConfig.SpannerConfig.DatabaseName,
	}
	spanmockface.On("GetTableConfigs", ctx, query).Return(columnMap, columnListMap, &spannerModule.SystemQueryMetadataCache{}, nil)
	spanmockface.On("SelectStatement", ctx, queryForTest).Return(&message.RowsResult{}, nil)
	spanmockface.On("InsertUpdateOrDeleteStatement", ctx, mock.AnythingOfType("responsehandler.QueryMetadata")).Return(&message.RowsResult{}, nil).Once()
	spanmockface.On("InsertUpdateOrDeleteStatement", ctx, mock.AnythingOfType("responsehandler.QueryMetadata")).Return(&message.RowsResult{}, errors.New("error occured")).Once()
	spanmockface.On("DeleteUsingMutations", ctx, mock.AnythingOfType("responsehandler.QueryMetadata")).Return(&message.RowsResult{}, errors.New("error occured"))
	p.sClient = spanmockface
	p.translator = &translator.Translator{TableConfig: &tableConfig.TableConfig{
		TablesMetaData:  columnMap,
		PkMetadataCache: columnListMap,
	}}
	p.tableConfig = &tableConfig.TableConfig{
		TablesMetaData:  columnMap,
		PkMetadataCache: columnListMap,
	}

	type fields struct {
		ctx                 context.Context
		proxy               *Proxy
		conn                *proxycore.Conn
		keyspace            string
		preparedSystemQuery map[[16]byte]interface{}
	}
	type args struct {
		raw           *frame.RawFrame
		msg           *partialExecute
		customPayload map[string][]byte
		st            *translator.DeleteQueryMap
	}
	tests := []struct {
		name   string
		fields fields
		args   args
	}{
		{
			name: "Test handleExecutionForDeletePreparedQuery Query",
			fields: fields{
				ctx:   context.Background(),
				proxy: p,
			},

			args: args{
				raw: mockRawFrame,
				msg: &partialExecute{
					PositionalValues: positionValues,
					NamedValues:      namedValues,
				},
				customPayload: map[string][]byte{
					"test_id":   []byte("1234"),
					"test_hash": []byte("1234-2345"),
				},
				st: &translator.DeleteQueryMap{
					Table:          "test_table",
					CassandraQuery: "DELETE FROM key_space.test_table WHERE test_id = '?'",
					ParamKeys:      []string{"test_id", "test_hash"},
					SpannerQuery:   "DELETE FROM key_space.test_table WHERE test_id = '?'",
				},
			},
		},
		{
			name: "Test handleExecutionForDeletePreparedQuery - InsertUpdateOrDeleteStatement failure",
			fields: fields{
				ctx:   context.Background(),
				proxy: p,
			},

			args: args{
				raw: mockRawFrame,
				msg: &partialExecute{
					PositionalValues: positionValues,
					NamedValues:      namedValues,
				},
				customPayload: map[string][]byte{
					"test_id":   []byte("1234"),
					"test_hash": []byte("1234-2345"),
				},
				st: &translator.DeleteQueryMap{
					Table:          "test_table",
					CassandraQuery: "DELETE FROM key_space.test_table WHERE test_id = '?'",
					ParamKeys:      []string{"test_id", "test_hash"},
					SpannerQuery:   "DELETE FROM key_space.test_table WHERE test_id = '?'",
				},
			},
		},
		{
			name: "Test handleExecutionForDeletePreparedQuery - execute by mutation false",
			fields: fields{
				ctx:   context.Background(),
				proxy: p,
			},

			args: args{
				raw: mockRawFrame,
				msg: &partialExecute{
					PositionalValues: positionValues,
					NamedValues:      namedValues,
				},
				customPayload: map[string][]byte{
					"test_id":   []byte("1234"),
					"test_hash": []byte("1234-2345"),
				},
				st: &translator.DeleteQueryMap{
					Table:             "test_table",
					CassandraQuery:    "DELETE FROM key_space.test_table WHERE test_id = '?'",
					ParamKeys:         []string{"test_id", "test_hash"},
					SpannerQuery:      "DELETE FROM key_space.test_table WHERE test_id = '?'",
					ExecuteByMutation: true,
				},
			},
		},
		{
			name: "Test handleExecutionForDeletePreparedQuery - prepareDeleteQueryMetadata query failure",
			fields: fields{
				ctx:   context.Background(),
				proxy: p,
			},

			args: args{
				raw: mockRawFrame,
				msg: &partialExecute{
					PositionalValues: positionValues,
					NamedValues:      namedValues,
				},
				customPayload: map[string][]byte{
					"test_id":   []byte("1234"),
					"test_hash": []byte("1234-2345"),
				},
				st: &translator.DeleteQueryMap{
					Table:          "test_table",
					CassandraQuery: "DELETE FROM key_space.test_table WHERE test_id = '?'",
					ParamKeys:      []string{"test_id", "test_hash"},
					SpannerQuery:   "DELETE FROM key_space.test_table WHERE test_id = '?'",
					VariableMetadata: []*message.ColumnMetadata{
						{
							Keyspace: "key_space",
							Table:    "test_table",
							Name:     "test_id",
							Index:    0,
							Type:     datatype.Int,
						},
					},
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mockSender := &mockSender{}
			c := &client{
				ctx:                 tt.fields.ctx,
				proxy:               tt.fields.proxy,
				conn:                tt.fields.conn,
				keyspace:            tt.fields.keyspace,
				preparedSystemQuery: tt.fields.preparedSystemQuery,
				sender:              mockSender,
			}
			c.handleExecuteForDelete(tt.args.raw, tt.args.msg, tt.args.st, c.proxy.ctx)
			assert.Nil(t, c.preparedSystemQuery)
		})
	}
}

func Test_client_handleExecutionForSelectPreparedQuery(t *testing.T) {
	proxyConfig := &Config{
		OtelConfig: &OtelConfig{
			Enabled: false,
		},
		Version: primitive.ProtocolVersion4,
		Logger:  zap.NewNop(),
		SpannerConfig: SpannerConfig{
			GCPProjectID:    "cassandra-to-spanner",
			InstanceName:    "test-instance-v1",
			DatabaseName:    "test-database",
			ConfigTableName: "test",
		},
		KeyspaceFlatter: false,
	}
	mockProxy, err := newProxy(proxyConfig)
	assert.NoErrorf(t, err, "failed to create the proxy instance")

	spanmockface := new(spannerMock.SpannerClientIface)
	spanmockface.On("SelectStatement", mockProxy.ctx, mock.AnythingOfType("responsehandler.QueryMetadata")).Return(&message.RowsResult{}, nil).Once()
	spanmockface.On("SelectStatement", mockProxy.ctx, mock.AnythingOfType("responsehandler.QueryMetadata")).Return(&message.RowsResult{}, errors.New("error occured")).Once()
	mockProxy.sClient = spanmockface
	type fields struct {
		ctx                 context.Context
		proxy               *Proxy
		conn                *proxycore.Conn
		keyspace            string
		preparedSystemQuery map[[16]byte]interface{}
	}
	type args struct {
		raw           *frame.RawFrame
		msg           *partialExecute
		customPayload map[string][]byte
		st            *translator.SelectQueryMap
	}
	tests := []struct {
		name   string
		fields fields
		args   args
	}{
		{
			name: "Test handleExecutionForSelectPreparedQuery Query",
			fields: fields{
				ctx:   context.Background(),
				proxy: mockProxy,
			},

			args: args{
				raw: mockRawFrame,
				msg: &partialExecute{
					PositionalValues: positionValues,
					NamedValues:      namedValues,
				},
				customPayload: map[string][]byte{
					"test_id":   []byte("1234"),
					"test_hash": []byte("1234-2345"),
				},
				st: &translator.SelectQueryMap{
					Table:          "test_table",
					CassandraQuery: "SELECT test_id, test_hash FROM key_space.test_table WHERE test_id = '?' AND test_hash = '?'",
					ParamKeys:      []string{"test_id", "test_hash"},
					SpannerQuery:   "SELECT test_id, test_hash FROM key_space.test_table WHERE test_id = '?' AND test_hash = '?'",
				},
			},
		},
		{
			name: "Test handleExecutionForSelectPreparedQuery Query Failure",
			fields: fields{
				ctx:   context.Background(),
				proxy: mockProxy,
			},

			args: args{
				raw: mockRawFrame,
				msg: &partialExecute{
					PositionalValues: positionValues,
					NamedValues:      namedValues,
				},
				customPayload: map[string][]byte{
					"test_id":   []byte("1234"),
					"test_hash": []byte("1234-2345"),
				},
				st: &translator.SelectQueryMap{
					Table:          "test_table",
					CassandraQuery: "SELECT test_id, test_hash FROM key_space.test_table WHERE test_id = '?' AND test_hash = '?'",
					ParamKeys:      []string{"test_id", "test_hash"},
					SpannerQuery:   "SELECT test_id, test_hash FROM key_space.test_table WHERE test_id = '?' AND test_hash = '?'",
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mockSender := &mockSender{}
			c := &client{
				ctx:                 tt.fields.ctx,
				proxy:               tt.fields.proxy,
				conn:                tt.fields.conn,
				keyspace:            tt.fields.keyspace,
				preparedSystemQuery: tt.fields.preparedSystemQuery,
				sender:              mockSender,
			}
			c.handleExecuteForSelect(tt.args.raw, tt.args.msg, tt.args.st, c.ctx, utilities.ExecuteOptions{MaxStaleness: 3})
			assert.Nil(t, c.preparedSystemQuery)
		})
	}
}

func Test_client_handleExecutionForInsertPreparedQuery(t *testing.T) {

	mockProxy, err := newProxy(&Config{})
	if err != nil {
		return
	}

	spanmockface := new(spannerMock.SpannerClientIface)
	spanmockface.On("InsertOrUpdateMutation", mockProxy.ctx, mock.AnythingOfType("responsehandler.QueryMetadata")).Return(&message.RowsResult{}, nil).Once()
	spanmockface.On("InsertOrUpdateMutation", mockProxy.ctx, mock.AnythingOfType("responsehandler.QueryMetadata")).Return(&message.RowsResult{}, errors.New("error occured")).Once()
	mockProxy.sClient = spanmockface
	type fields struct {
		ctx                 context.Context
		proxy               *Proxy
		conn                *proxycore.Conn
		keyspace            string
		preparedSystemQuery map[[16]byte]interface{}
	}
	type args struct {
		raw           *frame.RawFrame
		msg           *partialExecute
		customPayload map[string][]byte
		st            *translator.InsertQueryMap
	}
	tests := []struct {
		name   string
		fields fields
		args   args
	}{
		{
			name: "Test handleExecutionForDeletePreparedQuery Query",
			fields: fields{
				ctx:   context.Background(),
				proxy: mockProxy,
			},

			args: args{
				raw: mockRawFrame,
				msg: &partialExecute{
					PositionalValues: positionValues,
					NamedValues:      namedValues,
				},
				customPayload: map[string][]byte{
					"test_id":   []byte("1234"),
					"test_hash": []byte("1234-2345"),
				},
				st: &translator.InsertQueryMap{
					CassandraQuery: "INSERT INTO keyspace.table (col1, col2) VALUES (?, ?);",
					SpannerQuery:   "INSERT INTO keyspace.table (col1, col2) VALUES (@col1, @col2);",
					QueryType:      "INSERT",
					Table:          "table",
					Keyspace:       "keyspace",
					Columns: []translator.Column{
						{
							Name:        "col1",
							SpannerType: "STRING",
							CQLType:     "text",
						},
						{
							Name:        "col2",
							SpannerType: "INT64",
							CQLType:     "int",
						},
					},
					Values: []interface{}{"value1", 42},
					Params: map[string]interface{}{
						"col1": "value1",
						"col2": 42,
					},
					ParamKeys:    []string{"col1", "col2"},
					UsingTSCheck: "",
					PrimaryKeys:  []string{"col1"},
					ReturnMetadata: []*message.ColumnMetadata{
						{
							Keyspace: "keyspace",
							Table:    "table",
							Name:     "col1",
							Index:    0,
							Type:     datatype.Varchar,
						},
						{
							Keyspace: "keyspace",
							Table:    "table",
							Name:     "col2",
							Index:    1,
							Type:     datatype.Int,
						},
					},
					VariableMetadata: []*message.ColumnMetadata{
						{
							Keyspace: "keyspace",
							Table:    "table",
							Name:     "col1",
							Index:    0,
							Type:     datatype.Varchar,
						},
						{
							Keyspace: "keyspace",
							Table:    "table",
							Name:     "col2",
							Index:    1,
							Type:     datatype.Int,
						},
					},
				},
			},
		},
		{
			name: "Test handleExecutionForDeletePreparedQuery Query",
			fields: fields{
				ctx:   context.Background(),
				proxy: mockProxy,
			},

			args: args{
				raw: mockRawFrame,
				msg: &partialExecute{
					PositionalValues: []*primitive.Value{
						{Type: primitive.ValueTypeNull, Contents: []byte("value1")},
						{Type: primitive.ValueTypeNull, Contents: []byte{0, 0, 0, 42}},
					},
					NamedValues: map[string]*primitive.Value{
						"col1": {Type: primitive.ValueTypeNull, Contents: []byte("value1")},
						"col2": {Type: primitive.ValueTypeNull, Contents: []byte{0, 0, 0, 42}},
					},
				},
				st: &translator.InsertQueryMap{
					CassandraQuery: "INSERT INTO keyspace.table (col1, col2) VALUES (?, ?);",
					SpannerQuery:   "INSERT INTO keyspace.table (col1, col2) VALUES (@col1, @col2);",
					QueryType:      "INSERT",
					Table:          "table",
					Keyspace:       "keyspace",
					Columns: []translator.Column{
						{
							Name:        "col1",
							SpannerType: "STRING",
							CQLType:     "text",
						},
						{
							Name:        "col2",
							SpannerType: "INT64",
							CQLType:     "int",
						},
					},
					Values: []interface{}{"value1", []byte{0, 0, 0, 42}},
					Params: map[string]interface{}{
						"col1": "value1",
						"col2": []byte{0, 0, 0, 42},
					},
					ParamKeys:    []string{"col1", "col2"},
					UsingTSCheck: "",
					PrimaryKeys:  []string{"col1"},
					ReturnMetadata: []*message.ColumnMetadata{
						{
							Keyspace: "keyspace",
							Table:    "table",
							Name:     "col1",
							Index:    0,
							Type:     datatype.Varchar,
						},
						{
							Keyspace: "keyspace",
							Table:    "table",
							Name:     "col2",
							Index:    1,
							Type:     datatype.Int,
						},
					},
					VariableMetadata: []*message.ColumnMetadata{
						{
							Keyspace: "keyspace",
							Table:    "table",
							Name:     "col1",
							Index:    0,
							Type:     datatype.Varchar,
						},
						{
							Keyspace: "keyspace",
							Table:    "table",
							Name:     "col2",
							Index:    1,
							Type:     datatype.Int,
						},
					},
				},
			},
		},
		{
			name: "Test handleExecutionForDeletePreparedQuery Query",
			fields: fields{
				ctx:   context.Background(),
				proxy: mockProxy,
			},

			args: args{
				raw: mockRawFrame,
				msg: &partialExecute{
					PositionalValues: []*primitive.Value{
						{Type: primitive.ValueTypeNull, Contents: []byte("value1")},
						{Type: primitive.ValueTypeNull, Contents: []byte{0, 0, 0, 42}},
					},
					NamedValues: map[string]*primitive.Value{
						"col1": {Type: primitive.ValueTypeNull, Contents: []byte("value1")},
						"col2": {Type: primitive.ValueTypeNull, Contents: []byte{0, 0, 0, 42}},
					},
				},
				st: &translator.InsertQueryMap{
					CassandraQuery: "INSERT INTO keyspace.table (col1, col2) VALUES (?, ?);",
					SpannerQuery:   "INSERT INTO keyspace.table (col1, col2) VALUES (@col1, @col2);",
					QueryType:      "INSERT",
					Table:          "table",
					Keyspace:       "keyspace",
					Columns: []translator.Column{
						{
							Name:        "col1",
							SpannerType: "STRING",
							CQLType:     "text",
						},
						{
							Name:        "col2",
							SpannerType: "INT64",
							CQLType:     "int",
						},
					},
					Values: []interface{}{"value1", []byte{0, 0, 0, 42}},
					Params: map[string]interface{}{
						"col1": "value1",
						"col2": []byte{0, 0, 0, 42},
					},
					ParamKeys:    []string{"col1", "col2"},
					UsingTSCheck: "",
					PrimaryKeys:  []string{"col1"},
					ReturnMetadata: []*message.ColumnMetadata{
						{
							Keyspace: "keyspace",
							Table:    "table",
							Name:     "col1",
							Index:    0,
							Type:     datatype.Varchar,
						},
						{
							Keyspace: "keyspace",
							Table:    "table",
							Name:     "col2",
							Index:    1,
							Type:     datatype.Int,
						},
					},
					VariableMetadata: []*message.ColumnMetadata{
						{
							Keyspace: "keyspace",
							Table:    "table",
							Name:     "col1",
							Index:    0,
							Type:     datatype.Varchar,
						},
						{
							Keyspace: "keyspace",
							Table:    "table",
							Name:     "col2",
							Index:    1,
							Type:     datatype.Int,
						},
					},
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mockSender := &mockSender{}
			c := &client{
				ctx:                 tt.fields.ctx,
				proxy:               tt.fields.proxy,
				conn:                tt.fields.conn,
				keyspace:            tt.fields.keyspace,
				preparedSystemQuery: tt.fields.preparedSystemQuery,
				sender:              mockSender,
			}
			c.handleExecuteForInsert(tt.args.raw, tt.args.msg, tt.args.st, c.ctx)
			assert.Nil(t, c.preparedSystemQuery)
		})
	}
}

// TODO: Need to recheck this test as the input are incorrect.

// func Test_client_handleExecutionForUpdatePreparedQuery(t *testing.T) {

// 	mockProxy, err := newProxy(&Config{})
// 	if err != nil {
// 		return
// 	}
// 	ctx := context.Background()

// 	// Mock for SpannerClientIface
// 	spanmockface := new(spannerMock.SpannerClientIface)
// 	spanmockface.On("InsertUpdateOrDeleteStatement", ctx, mock.AnythingOfType("responsehandler.QueryMetadata")).Return(&message.RowsResult{}, nil).Once()
// 	spanmockface.On("InsertUpdateOrDeleteStatement", ctx, mock.AnythingOfType("responsehandler.QueryMetadata")).Return(&message.RowsResult{}, errors.New("error occured")).Once()
// 	spanmockface.On("UpdateMapByKey", ctx, mock.AnythingOfType("responsehandler.QueryMetadata")).Return(&message.RowsResult{}, nil).Once()
// 	spanmockface.On("UpdateMapByKey", ctx, mock.AnythingOfType("responsehandler.QueryMetadata")).Return(&message.RowsResult{}, errors.New("error occured")).Once()
// 	mockProxy.sClient = spanmockface

// 	type fields struct {
// 		ctx                 context.Context
// 		proxy               *Proxy
// 		conn                *proxycore.Conn
// 		keyspace            string
// 		preparedSystemQuery map[[16]byte]interface{}
// 		preparedQuerys      map[[16]byte]interface{}
// 	}
// 	type args struct {
// 		raw           *frame.RawFrame
// 		msg           *partialExecute
// 		customPayload map[string][]byte
// 		st            *translator.UpdateQueryMap
// 	}
// 	tests := []struct {
// 		name   string
// 		fields fields
// 		args   args
// 	}{
// 		{
// 			name: "Test handleExecutionForUpdatePreparedQuery Query",
// 			fields: fields{
// 				ctx:   context.Background(),
// 				proxy: mockProxy,
// 			},
// 			args: args{
// 				raw: mockRawFrame,
// 				msg: &partialExecute{
// 					PositionalValues: []*primitive.Value{
// 						{
// 							Type:     int32(primitive.DataTypeCodeText),
// 							Contents: []byte("hello"),
// 						}, {
// 							Type:     int32(primitive.DataTypeCodeInt),
// 							Contents: []byte("1234"),
// 						},
// 					},
// 					NamedValues: map[string]*primitive.Value{
// 						"col1": {Type: int32(primitive.DataTypeCodeVarint), Contents: []byte("value1")},
// 						"col2": {Type: int32(primitive.DataTypeCodeVarint), Contents: []byte("value2")},
// 					},
// 				},
// 				customPayload: map[string][]byte{
// 					"test_id":   []byte("1234"),
// 					"test_hash": []byte("1234"),
// 				},
// 				st: &translator.UpdateQueryMap{
// 					CassandraQuery: "INSERT INTO keyspace.table (col1, col2) VALUES (?, ?);",
// 					SpannerQuery:   "INSERT INTO keyspace.table (col1, col2) VALUES (@col1, @col2);",
// 					QueryType:      "INSERT",
// 					Table:          "table",
// 					Keyspace:       "keyspace",
// 					Params: map[string]interface{}{
// 						"col1": "we",
// 						"col2": int32(1234),
// 					},
// 					ParamKeys:   []string{"col1", "col2"},
// 					PrimaryKeys: []string{"col1"},
// 					ReturnMetadata: []*message.ColumnMetadata{
// 						{
// 							Keyspace: "keyspace",
// 							Table:    "table",
// 							Name:     "col1",
// 							Index:    0,
// 							Type:     datatype.Varchar,
// 						},
// 						{
// 							Keyspace: "keyspace",
// 							Table:    "table",
// 							Name:     "col2",
// 							Index:    1,
// 							Type:     datatype.Int,
// 						},
// 					},
// 					VariableMetadata: []*message.ColumnMetadata{
// 						{
// 							Keyspace: "keyspace",
// 							Table:    "table",
// 							Name:     "col1",
// 							Index:    0,
// 							Type:     datatype.Varchar,
// 						},
// 						{
// 							Keyspace: "keyspace",
// 							Table:    "table",
// 							Name:     "col2",
// 							Index:    1,
// 							Type:     datatype.Int,
// 						},
// 					},
// 				},
// 			},
// 		},
// 		{
// 			name: "Test handleExecutionForUpdatePreparedQuery Query - query error",
// 			fields: fields{
// 				ctx:   context.Background(),
// 				proxy: mockProxy,
// 			},
// 			args: args{
// 				raw: mockRawFrame,
// 				msg: &partialExecute{
// 					PositionalValues: []*primitive.Value{
// 						{
// 							Type:     int32(primitive.DataTypeCodeText),
// 							Contents: []byte("hello"),
// 						}, {
// 							Type:     int32(primitive.DataTypeCodeInt),
// 							Contents: []byte("1234"),
// 						},
// 					},
// 					NamedValues: map[string]*primitive.Value{
// 						"col1": {Type: int32(primitive.DataTypeCodeVarint), Contents: []byte("value1")},
// 						"col2": {Type: int32(primitive.DataTypeCodeVarint), Contents: []byte("value2")},
// 					},
// 				},
// 				customPayload: map[string][]byte{
// 					"test_id":   []byte("1234"),
// 					"test_hash": []byte("1234"),
// 				},
// 				st: &translator.UpdateQueryMap{
// 					CassandraQuery: "INSERT INTO keyspace.table (col1, col2) VALUES (?, ?);",
// 					SpannerQuery:   "INSERT INTO keyspace.table (col1, col2) VALUES (@col1, @col2);",
// 					QueryType:      "INSERT",
// 					Table:          "table",
// 					Keyspace:       "keyspace",
// 					Params: map[string]interface{}{
// 						"col1": "we",
// 						"col2": int32(1234),
// 					},
// 					ParamKeys:   []string{"col1", "col2"},
// 					PrimaryKeys: []string{"col1"},
// 					ReturnMetadata: []*message.ColumnMetadata{
// 						{
// 							Keyspace: "keyspace",
// 							Table:    "table",
// 							Name:     "col1",
// 							Index:    0,
// 							Type:     datatype.Varchar,
// 						},
// 						{
// 							Keyspace: "keyspace",
// 							Table:    "table",
// 							Name:     "col2",
// 							Index:    1,
// 							Type:     datatype.Int,
// 						},
// 					},
// 					VariableMetadata: []*message.ColumnMetadata{
// 						{
// 							Keyspace: "keyspace",
// 							Table:    "table",
// 							Name:     "col1",
// 							Index:    0,
// 							Type:     datatype.Varchar,
// 						},
// 						{
// 							Keyspace: "keyspace",
// 							Table:    "table",
// 							Name:     "col2",
// 							Index:    1,
// 							Type:     datatype.Int,
// 						},
// 					},
// 				},
// 			},
// 		},
// 		{
// 			name: "Test handleExecutionForUpdatePreparedQuery Query - Failure",
// 			fields: fields{
// 				ctx:   context.Background(),
// 				proxy: mockProxy,
// 			},
// 			args: args{
// 				raw: mockRawFrame,
// 				msg: &partialExecute{
// 					PositionalValues: []*primitive.Value{
// 						{
// 							Type:     int32(primitive.DataTypeCodeText),
// 							Contents: []byte("hello"),
// 						},
// 						{
// 							Type:     int32(primitive.DataTypeCodeInt),
// 							Contents: []byte("1234"),
// 						},
// 					},
// 					NamedValues: map[string]*primitive.Value{
// 						"col1": {Type: int32(primitive.DataTypeCodeVarint), Contents: []byte("value1")},
// 						"col2": {Type: int32(primitive.DataTypeCodeVarint), Contents: []byte("value2")},
// 					},
// 				},
// 				st: &translator.UpdateQueryMap{
// 					CassandraQuery: "INSERT INTO keyspace.table (col1, col2) VALUES (?, ?);",
// 					SpannerQuery:   "INSERT INTO keyspace.table (col1, col2) VALUES (@col1, @col2);",
// 					QueryType:      "INSERT",
// 					Table:          "table",
// 					Keyspace:       "keyspace",
// 					Params: map[string]interface{}{
// 						"col1": "we",
// 						"col2": int32(0),
// 					},
// 					ParamKeys: []string{"col1", "col2"},
// 					// UsingTSCheck: "timestamp_check",
// 					PrimaryKeys: []string{"col1"},
// 					ReturnMetadata: []*message.ColumnMetadata{
// 						{
// 							Keyspace: "keyspace",
// 							Table:    "table",
// 							Name:     "col1",
// 							Index:    0,
// 							Type:     datatype.Varchar,
// 						},
// 						{
// 							Keyspace: "keyspace",
// 							Table:    "table",
// 							Name:     "col2",
// 							Index:    1,
// 							Type:     datatype.Int,
// 						},
// 					},
// 					VariableMetadata: []*message.ColumnMetadata{
// 						{
// 							Keyspace: "keyspace",
// 							Table:    "table",
// 							Name:     "col1",
// 							Index:    0,
// 							Type:     datatype.Varchar,
// 						},
// 						{
// 							Keyspace: "keyspace",
// 							Table:    "table",
// 							Name:     "col2",
// 							Index:    1,
// 							Type:     datatype.Ascii,
// 						},
// 					},
// 				},
// 			},
// 		},
// 		{
// 			name: "Test handleExecutionForUpdatePreparedQuery Query - map update by key",
// 			fields: fields{
// 				ctx:   context.Background(),
// 				proxy: mockProxy,
// 			},
// 			args: args{
// 				raw: mockRawFrame,
// 				msg: &partialExecute{
// 					PositionalValues: []*primitive.Value{
// 						{
// 							Type:     int32(primitive.DataTypeCodeText),
// 							Contents: []byte("hello"),
// 						},
// 						{
// 							Type:     int32(primitive.DataTypeCodeInt),
// 							Contents: []byte("1234"),
// 						},
// 					},
// 					NamedValues: map[string]*primitive.Value{
// 						"col1": {Type: int32(primitive.DataTypeCodeVarint), Contents: []byte("value1")},
// 						"col2": {Type: int32(primitive.DataTypeCodeVarint), Contents: []byte("value2")},
// 					},
// 				},
// 				st: &translator.UpdateQueryMap{
// 					CassandraQuery: "UPDATE key_space.test_table SET column8[?] = true WHERE column1 = ?;",
// 					SpannerQuery:   "UPDATE test_table SET column8 = @map_key WHERE column1 = @value1;",
// 					QueryType:      "UPDATE",
// 					Table:          "table",
// 					Keyspace:       "keyspace",
// 					Params: map[string]interface{}{
// 						"map_key": "key",
// 						"value1":  "test",
// 					},
// 					ParamKeys:   []string{"map_key", "value1"},
// 					PrimaryKeys: []string{"column1"},
// 					ReturnMetadata: []*message.ColumnMetadata{
// 						{
// 							Keyspace: "keyspace",
// 							Table:    "table",
// 							Name:     "column8",
// 							Index:    0,
// 							Type:     datatype.Varchar,
// 						},
// 						{
// 							Keyspace: "keyspace",
// 							Table:    "table",
// 							Name:     "column1",
// 							Index:    1,
// 							Type:     datatype.Varchar,
// 						},
// 					},
// 					VariableMetadata: []*message.ColumnMetadata{
// 						{
// 							Keyspace: "keyspace",
// 							Table:    "table",
// 							Name:     "column8",
// 							Index:    0,
// 							Type:     datatype.Varchar,
// 						},
// 						{
// 							Keyspace: "keyspace",
// 							Table:    "table",
// 							Name:     "column1",
// 							Index:    1,
// 							Type:     datatype.Varchar,
// 						},
// 					},
// 					SelectQueryMapUpdate: "SELECT column8 FROM table WHERE column3 = @value1;",
// 				},
// 			},
// 		},
// 		{
// 			name: "Test handleExecutionForUpdatePreparedQuery",
// 			fields: fields{
// 				ctx:   context.Background(),
// 				proxy: mockProxy,
// 			},
// 			// decodedValue, err := DecodeBytesToSpannerColumnType(paramValue[index].Contents, columnMetada.Type, raw.Header.Version)
// 			args: args{
// 				raw: mockRawFrame,
// 				msg: &partialExecute{
// 					PositionalValues: []*primitive.Value{
// 						{
// 							Type:     int32(primitive.DataTypeCodeText),
// 							Contents: []byte("hello"),
// 						},
// 						{
// 							Type:     int32(primitive.DataTypeCodeInt),
// 							Contents: []byte("1234"),
// 						},
// 					},
// 					NamedValues: map[string]*primitive.Value{
// 						"col1": {Type: int32(primitive.DataTypeCodeVarint), Contents: []byte("value1")},
// 						"col2": {Type: int32(primitive.DataTypeCodeVarint), Contents: []byte("value2")},
// 					},
// 				},
// 				st: &translator.UpdateQueryMap{
// 					CassandraQuery: "UPDATE keyspace.table SET col2 = ? WHERE col1 = ?;",
// 					SpannerQuery:   "UPDATE keyspace.table SET col2 = @col2 WHERE col1 = @col1;",
// 					QueryType:      "UPDATE",
// 					Table:          "table",
// 					Keyspace:       "keyspace",
// 					Params: map[string]interface{}{
// 						"col1": "we",
// 						"col2": int32(0),
// 					},
// 					ParamKeys:   []string{"col1", "col2"},
// 					PrimaryKeys: []string{"col1"},
// 					ReturnMetadata: []*message.ColumnMetadata{
// 						{
// 							Keyspace: "keyspace",
// 							Table:    "table",
// 							Name:     "col1",
// 							Index:    0,
// 							Type:     datatype.Varchar,
// 						},
// 						{
// 							Keyspace: "keyspace",
// 							Table:    "table",
// 							Name:     "col2",
// 							Index:    1,
// 							Type:     datatype.Int,
// 						},
// 					},
// 					VariableMetadata: []*message.ColumnMetadata{
// 						{
// 							Keyspace: "keyspace",
// 							Table:    "table",
// 							Name:     "col1",
// 							Index:    0,
// 							Type:     datatype.Varchar,
// 						},
// 						{
// 							Keyspace: "keyspace",
// 							Table:    "table",
// 							Name:     "col2",
// 							Index:    1,
// 							Type:     datatype.Int,
// 						},
// 					},
// 				},
// 			},
// 		},
// 		{
// 			name: "Test handleExecutionForUpdatePreparedQuery Query - Update Map by key error",
// 			fields: fields{
// 				ctx:   context.Background(),
// 				proxy: mockProxy,
// 			},
// 			args: args{
// 				raw: mockRawFrame,
// 				msg: &partialExecute{
// 					PositionalValues: []*primitive.Value{
// 						{
// 							Type:     int32(primitive.DataTypeCodeText),
// 							Contents: []byte("hello"),
// 						},
// 						{
// 							Type:     int32(primitive.DataTypeCodeInt),
// 							Contents: []byte("1234"),
// 						},
// 					},
// 					NamedValues: map[string]*primitive.Value{
// 						"col1": {Type: int32(primitive.DataTypeCodeVarint), Contents: []byte("value1")},
// 						"col2": {Type: int32(primitive.DataTypeCodeVarint), Contents: []byte("value2")},
// 					},
// 				},
// 				st: &translator.UpdateQueryMap{
// 					CassandraQuery: "UPDATE keyspace.table SET col2 = ? WHERE col1 = ?;",
// 					SpannerQuery:   "UPDATE keyspace.table SET col2 = @col2 WHERE col1 = @col1;",
// 					QueryType:      "UPDATE",
// 					Table:          "table",
// 					Keyspace:       "keyspace",
// 					Params: map[string]interface{}{
// 						"col1": "we",
// 						"col2": int32(0),
// 					},
// 					ParamKeys: []string{"col1", "col2"},
// 					// UsingTSCheck: "timestamp_check",
// 					PrimaryKeys: []string{"col1"},
// 					ReturnMetadata: []*message.ColumnMetadata{
// 						{
// 							Keyspace: "keyspace",
// 							Table:    "table",
// 							Name:     "col1",
// 							Index:    0,
// 							Type:     datatype.Varchar,
// 						},
// 						{
// 							Keyspace: "keyspace",
// 							Table:    "table",
// 							Name:     "col2",
// 							Index:    1,
// 							Type:     datatype.Int,
// 						},
// 					},
// 					VariableMetadata: []*message.ColumnMetadata{
// 						{
// 							Keyspace: "keyspace",
// 							Table:    "table",
// 							Name:     "col1",
// 							Index:    0,
// 							Type:     datatype.Varchar,
// 						},
// 						{
// 							Keyspace: "keyspace",
// 							Table:    "table",
// 							Name:     "col2",
// 							Index:    1,
// 							Type:     datatype.Int,
// 						},
// 					},
// 				},
// 			},
// 		},
// 	}
// 	for _, tt := range tests {
// 		t.Run(tt.name, func(t *testing.T) {
// 			mockSender := &mockSender{}
// 			c := &client{
// 				ctx:                 tt.fields.ctx,
// 				proxy:               tt.fields.proxy,
// 				conn:                tt.fields.conn,
// 				keyspace:            tt.fields.keyspace,
// 				preparedSystemQuery: tt.fields.preparedSystemQuery,
// 				sender:              mockSender,
// 			}
// 			c.handleExecuteForUpdate(tt.args.raw, tt.args.msg, tt.args.st, c.ctx)
// 			assert.Nil(t, c.preparedSystemQuery)
// 		})
// 	}
// }

func Test_prepareDeleteQueryMetadata(t *testing.T) {
	prox, err := newProxy(&Config{})
	assert.NoErrorf(t, err, "error while creating a proxy instance")

	tests := []struct {
		name           string
		raw            *frame.RawFrame
		deleteQueryMap *translator.DeleteQueryMap
		paramValues    []*primitive.Value
		expectedErr    bool
	}{
		{
			name: "Delete Success",
			raw: &frame.RawFrame{
				Header: &frame.Header{
					Version:    primitive.ProtocolVersion4,
					IsResponse: false,
					Flags:      00000000,
					StreamId:   26,
					OpCode:     primitive.OpCodeExecute,
					BodyLength: 75,
				},
				Body: []byte{0, 16, 138, 79, 105, 55, 55, 35, 39, 40, 255, 33, 236, 245, 251, 56, 125, 120, 0, 6, 37, 0, 1, 0, 0, 0, 36, 48,
					52, 98, 101, 56, 57, 102, 55, 45, 53, 56, 55, 53, 45, 52, 102, 101, 99, 45, 57, 57, 50, 53, 45, 49, 98, 102, 55, 100, 101, 56, 55,
					98, 98, 57, 52, 0, 0, 19, 136, 0, 6, 27, 155, 194, 214, 88, 249},
			},
			deleteQueryMap: &translator.DeleteQueryMap{
				CassandraQuery:    "delete from xobni_derived.batch_endpoint_network where guid = ?;",
				SpannerQuery:      "DELETE FROM xobni_derived_batch_endpoint_network WHERE `guid` = @value1;",
				QueryType:         "delete",
				Table:             "xobni_derived_batch_endpoint_network",
				Keyspace:          "xobni_derived",
				Clauses:           []translator.Clause{{Column: "guid", Operator: "=", Value: "@value1", IsPrimaryKey: true}},
				ParamKeys:         []string{"value1"},
				VariableMetadata:  []*message.ColumnMetadata{{Keyspace: "xobni_derived", Table: "batch_endpoint_network", Name: "guid", Index: 0, Type: datatype.Varchar}},
				ExecuteByMutation: true,
				PrimaryKeys:       []string{"guid", "client"},
				ReturnMetadata:    []*message.ColumnMetadata{},
			},
			expectedErr: false,
			paramValues: []*primitive.Value{{Type: primitive.ValueTypeRegular, Contents: []byte{48, 52, 98, 101, 56, 57, 102, 55, 45, 53, 56, 55, 53,
				45, 52, 102, 101, 99, 45, 57, 57, 50, 53, 45, 49, 98, 102, 55, 100, 101, 56, 55, 98, 98, 57, 52}}},
		},
		{
			name: "Delete DecodeBytesToSpannerColumnType Failure",
			raw: &frame.RawFrame{
				Header: &frame.Header{
					Version:    primitive.ProtocolVersion4,
					IsResponse: false,
					Flags:      00000000,
					StreamId:   26,
					OpCode:     primitive.OpCodeExecute,
					BodyLength: 75,
				},
				Body: []byte{0, 16, 138, 79, 105, 55, 55, 35, 39, 40, 255, 33, 236, 245, 251, 56, 125, 120, 0, 6, 37, 0, 1, 0, 0, 0, 36, 48,
					52, 98, 101, 56, 57, 102, 55, 45, 53, 56, 55, 53, 45, 52, 102, 101, 99, 45, 57, 57, 50, 53, 45, 49, 98, 102, 55, 100, 101, 56, 55,
					98, 98, 57, 52, 0, 0, 19, 136, 0, 6, 27, 155, 194, 214, 88, 249},
			},
			deleteQueryMap: &translator.DeleteQueryMap{
				CassandraQuery:    "delete from xobni_derived.batch_endpoint_network where guid = ?;",
				SpannerQuery:      "DELETE FROM xobni_derived_batch_endpoint_network WHERE `guid` = @value1;",
				QueryType:         "delete",
				Table:             "xobni_derived_batch_endpoint_network",
				Keyspace:          "xobni_derived",
				Clauses:           []translator.Clause{{Column: "guid", Operator: "=", Value: "@value1", IsPrimaryKey: true}},
				ParamKeys:         []string{"value1"},
				VariableMetadata:  []*message.ColumnMetadata{{Keyspace: "xobni_derived", Table: "batch_endpoint_network", Name: "guid", Index: 0, Type: datatype.Ascii}},
				ExecuteByMutation: true,
				PrimaryKeys:       []string{"guid", "client"},
				ReturnMetadata:    []*message.ColumnMetadata{},
			},
			expectedErr: true,
			paramValues: []*primitive.Value{{Contents: []byte("hello world")}},
		},
		{
			name: "Delete Failure",
			raw: &frame.RawFrame{
				Header: &frame.Header{
					Version:    primitive.ProtocolVersion4,
					IsResponse: false,
					Flags:      00000000,
					StreamId:   26,
					OpCode:     primitive.OpCodeExecute,
					BodyLength: 75,
				},
				Body: []byte{0, 16, 138, 79, 105, 55, 55, 35, 39, 40, 255, 33, 236, 245, 251, 56, 125, 120, 0, 6, 37, 0, 1, 0, 0, 0, 36, 48,
					52, 98, 101, 56, 57, 102, 55, 45, 53, 56, 55, 53, 45, 52, 102, 101, 99, 45, 57, 57, 50, 53, 45, 49, 98, 102, 55, 100, 101, 56, 55,
					98, 98, 57, 52, 0, 0, 19, 136, 0, 6, 27, 155, 194, 214, 88, 249},
			},
			deleteQueryMap: &translator.DeleteQueryMap{
				CassandraQuery:    "delete from xobni_derived.batch_endpoint_network where guid = ?;",
				SpannerQuery:      "DELETE FROM xobni_derived_batch_endpoint_network WHERE `guid` = @value1;",
				QueryType:         "delete",
				Table:             "xobni_derived_batch_endpoint_network",
				Keyspace:          "xobni_derived",
				Clauses:           []translator.Clause{{Column: "guid", Operator: "=", Value: "@value1", IsPrimaryKey: true}},
				ParamKeys:         []string{"value1"},
				VariableMetadata:  []*message.ColumnMetadata{{Keyspace: "xobni_derived", Table: "batch_endpoint_network", Name: ts_column, Index: 0, Type: datatype.Varchar}},
				ExecuteByMutation: true,
				PrimaryKeys:       []string{"guid", "client"},
				ReturnMetadata:    []*message.ColumnMetadata{},
			},
			expectedErr: true,
			paramValues: []*primitive.Value{{Type: primitive.ValueTypeRegular, Contents: []byte{48, 52, 98, 101, 56, 57, 102, 55, 45, 53, 56, 55, 53,
				45, 52, 102, 101, 99, 45, 57, 57, 50, 53, 45, 49, 98, 102, 55, 100, 101, 56, 55, 98, 98, 57, 52}}},
		},
		{
			name: "Delete Success",
			raw: &frame.RawFrame{
				Header: &frame.Header{
					Version:    primitive.ProtocolVersion4,
					IsResponse: false,
					Flags:      00000000,
					StreamId:   26,
					OpCode:     primitive.OpCodeExecute,
					BodyLength: 75,
				},
				Body: []byte{0, 16, 138, 79, 105, 55, 55, 35, 39, 40, 255, 33, 236, 245, 251, 56, 125, 120, 0, 6, 37, 0, 1, 0, 0, 0, 36, 48,
					52, 98, 101, 56, 57, 102, 55, 45, 53, 56, 55, 53, 45, 52, 102, 101, 99, 45, 57, 57, 50, 53, 45, 49, 98, 102, 55, 100, 101, 56, 55,
					98, 98, 57, 52, 0, 0, 19, 136, 0, 6, 27, 155, 194, 214, 88, 249},
			},
			deleteQueryMap: &translator.DeleteQueryMap{
				CassandraQuery:    "delete from xobni_derived.batch_endpoint_network where last_commit_ts = ?;",
				SpannerQuery:      "DELETE FROM xobni_derived_batch_endpoint_network WHERE `last_commit_ts` = @value1;",
				QueryType:         "delete",
				Table:             "xobni_derived_batch_endpoint_network",
				Keyspace:          "xobni_derived",
				Clauses:           []translator.Clause{{Column: "last_commit_ts", Operator: "=", Value: "@value1", IsPrimaryKey: true}},
				ParamKeys:         []string{"value1"},
				VariableMetadata:  []*message.ColumnMetadata{{Keyspace: "xobni_derived", Table: "batch_endpoint_network", Name: ts_column, Index: 0, Type: datatype.Int}},
				ExecuteByMutation: true,
				PrimaryKeys:       []string{"last_commit_ts", "client"},
				ReturnMetadata:    []*message.ColumnMetadata{},
			},
			expectedErr: false,
			paramValues: []*primitive.Value{{Type: primitive.ValueTypeRegular, Contents: []byte{97, 53, 0, 0}}},
		},
		{
			name: "Delete Success",
			raw: &frame.RawFrame{
				Header: &frame.Header{
					Version:    primitive.ProtocolVersion4,
					IsResponse: false,
					Flags:      00000000,
					StreamId:   26,
					OpCode:     primitive.OpCodeExecute,
					BodyLength: 75,
				},
				Body: []byte{0, 16, 138, 79, 105, 55, 55, 35, 39, 40, 255, 33, 236, 245, 251, 56, 125, 120, 0, 6, 37, 0, 1, 0, 0, 0, 36, 48,
					52, 98, 101, 56, 57, 102, 55, 45, 53, 56, 55, 53, 45, 52, 102, 101, 99, 45, 57, 57, 50, 53, 45, 49, 98, 102, 55, 100, 101, 56, 55,
					98, 98, 57, 52, 0, 0, 19, 136, 0, 6, 27, 155, 194, 214, 88, 249},
			},
			deleteQueryMap: &translator.DeleteQueryMap{
				CassandraQuery:    "delete from xobni_derived.batch_endpoint_network where last_commit_ts = ?;",
				SpannerQuery:      "DELETE FROM xobni_derived_batch_endpoint_network WHERE `last_commit_ts` = @value1;",
				QueryType:         "delete",
				Table:             "xobni_derived_batch_endpoint_network",
				Keyspace:          "xobni_derived",
				Clauses:           []translator.Clause{{Column: "last_commit_ts", Operator: "=", Value: "@value1", IsPrimaryKey: true}},
				ParamKeys:         []string{"value1"},
				VariableMetadata:  []*message.ColumnMetadata{{Keyspace: "xobni_derived", Table: "batch_endpoint_network", Name: ts_column, Index: 0, Type: datatype.Int}},
				ExecuteByMutation: true,
				PrimaryKeys:       []string{"last_commit_ts", "client"},
				ReturnMetadata:    []*message.ColumnMetadata{},
			},
			expectedErr: true,
			paramValues: []*primitive.Value{{Type: primitive.ValueTypeRegular, Contents: []byte{0, 0, 0, 0}}},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			client := &client{
				proxy: prox,
			}

			_, _, err := client.prepareDeleteQueryMetadata(tt.raw, tt.paramValues, tt.deleteQueryMap)
			if (err != nil) != tt.expectedErr {
				t.Errorf("client.prepareDeleteQueryMetadata() error = %v, wantErr %v", err, tt.expectedErr)
				return
			}
		})
	}
}

func Test_HandleQuery(t *testing.T) {
	mockSender := &mockSender{}
	mockProxy, err := newProxy(&Config{})
	if err != nil {
		return
	}

	type fields struct {
		ctx                 context.Context
		proxy               *Proxy
		conn                *proxycore.Conn
		keyspace            string
		preparedSystemQuery map[[16]byte]interface{}
	}

	selectQuery := &partialQuery{
		query: "SELECT * FROM test_keyspace.test_table WHERE test_id = '123",
	}

	insertQuery := &partialQuery{
		query: "INSERT INTO test_keyspace.test_table (test_id, test_hash) VALUES ('asd', 'asd-asd-asd-asd')",
	}

	deleteQuery := &partialQuery{
		query: "DELETE FROM test_keyspace.test_table WHERE test_id = '123'",
	}

	tests := []struct {
		name    string
		fields  fields
		c       *client
		raw     *frame.RawFrame
		query   *partialQuery
		wantErr bool
		setup   func(*client)
	}{
		{
			name:    "Select Query - Success",
			raw:     mockRawFrame,
			query:   selectQuery,
			wantErr: false,
			fields: fields{
				proxy: mockProxy,
			},
			setup: func(c *client) {
				c.preparedSystemQuery = make(map[[16]byte]interface{})
			},
		},
		{
			name:    "Insert Query - Success",
			raw:     mockRawFrame,
			query:   insertQuery,
			wantErr: false,
			fields: fields{
				proxy: mockProxy,
			},
			setup: func(c *client) {
				c.preparedSystemQuery = make(map[[16]byte]interface{})
			},
		},
		{
			name:    "Delete Query - Success",
			raw:     mockRawFrame,
			query:   deleteQuery,
			wantErr: false,
			fields: fields{
				proxy: mockProxy,
			},
			setup: func(c *client) {
				c.preparedSystemQuery = make(map[[16]byte]interface{})
			},
		},
	}

	for _, tt := range tests {
		c := &client{
			ctx:                 tt.fields.ctx,
			proxy:               mockProxy,
			conn:                tt.fields.conn,
			keyspace:            tt.fields.keyspace,
			preparedSystemQuery: tt.fields.preparedSystemQuery,
			sender:              mockSender,
		}
		t.Run(tt.name, func(t *testing.T) {
			mockSender.SendCalled = false
			c.handleQuery(tt.raw, tt.query)

			if tt.wantErr {
				if mockSender.SendCalled {
					t.Errorf("Send should not have been called for test: %s", tt.name)
				}
			} else {
				if !mockSender.SendCalled {
					t.Errorf("Send was not called when expected for test: %s", tt.name)
				}
			}
		})
	}
}

type mockQuery struct{}

func TestHandleExecute(t *testing.T) {
	mockSender := &mockSender{}
	prox, err := newProxy(&Config{})
	if err != nil {
		return
	}
	mockPartialExecute := &partialExecute{
		PositionalValues: positionValues,
		NamedValues:      namedValues,
	}
	mockSystemQueryID := preparedIdKey([]byte("some_prepared_query_id"))

	tests := []struct {
		name           string
		client         *client
		raw            *frame.RawFrame
		msg            *partialExecute
		setupClient    func(c *client)
		wantSendCalled bool
		wantSendMsg    message.Message
	}{
		{
			name: "System Query Execution",
			raw:  mockRawFrame,
			msg:  &partialExecute{queryId: []byte("some_prepared_query_id")},
			setupClient: func(c *client) {
				c.preparedSystemQuery = map[[16]byte]interface{}{
					mockSystemQueryID: &mockQuery{},
				}
			},
			wantSendCalled: true,
		},
		{
			name:           "Prepared Query Execution",
			raw:            mockRawFrame,
			msg:            &partialExecute{queryId: []byte("some_other_prepared_query_id")},
			setupClient:    func(c *client) {},
			wantSendCalled: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := &client{
				proxy:               prox,
				preparedSystemQuery: make(map[[16]byte]interface{}),
				sender:              mockSender,
			}
			if tt.setupClient != nil {
				tt.setupClient(c)
			}

			c.handleExecute(mockRawFrame, mockPartialExecute, utilities.ExecuteOptions{MaxStaleness: 0})
			if tt.wantSendCalled {
				if !mockSender.SendCalled {
					t.Errorf("Send was not called when expected for test: %s", tt.name)
				}
			} else {
				if mockSender.SendCalled {
					t.Errorf("Send was called when not expected for test: %s", tt.name)
				}
			}
		})
	}
}

func Test_client_handlePrepare(t *testing.T) {
	mockSender := &mockSender{}
	prox, err := newProxy(&Config{})
	if err != nil {
		return
	}
	selectQuery := &message.Prepare{
		Query: "SELECT * FROM system.local WHERE key = 'local'",
	}
	useQuery := &message.Prepare{
		Query: "USE test_keyspace",
	}
	invalidQuery := &message.Prepare{
		Query: "INVALID QUERY",
	}

	tests := []struct {
		name           string
		client         *client
		raw            *frame.RawFrame
		msg            *message.Prepare
		setupClient    func(c *client)
		wantSendCalled bool
		wantSendMsg    message.Message
	}{
		{
			name: "Select Query",
			raw:  mockRawFrame,
			msg:  selectQuery,
			setupClient: func(c *client) {
				c.preparedSystemQuery = make(map[[16]byte]interface{})
			},
			wantSendCalled: true,
		},
		{
			name: "Use Query",
			raw:  mockRawFrame,
			msg:  useQuery,
			setupClient: func(c *client) {
				c.preparedSystemQuery = make(map[[16]byte]interface{})
			},
			wantSendCalled: true,
		},
		{
			name: "Invalid Query",
			raw:  mockRawFrame,
			msg:  invalidQuery,
			setupClient: func(c *client) {
				c.preparedSystemQuery = make(map[[16]byte]interface{})
			},
			wantSendCalled: true,
		},
		{
			name: "Select Statement with Invalid Table",
			raw:  mockRawFrame,
			msg:  selectQuery,
			setupClient: func(c *client) {
				c.preparedSystemQuery = make(map[[16]byte]interface{})
			},
			wantSendCalled: true,
			wantSendMsg:    &message.Invalid{},
		},
		{
			name: "Query Parsing Error",
			raw:  mockRawFrame,
			msg:  &message.Prepare{Query: "invalid query syntax"},
			setupClient: func(c *client) {
				c.preparedSystemQuery = make(map[[16]byte]interface{})
			},
			wantSendCalled: true,
			wantSendMsg:    nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := &client{
				proxy:               prox,
				preparedSystemQuery: make(map[[16]byte]interface{}),
				sender:              mockSender,
			}
			if tt.setupClient != nil {
				tt.setupClient(c)
			}

			c.handlePrepare(tt.raw, tt.msg)
			if tt.wantSendCalled {
				if !mockSender.SendCalled {
					t.Errorf("Send was not called when expected for test: %s", tt.name)
				}
			} else {
				if mockSender.SendCalled {
					t.Errorf("Send was called when not expected for test: %s", tt.name)
				}
			}
		})
	}
}

// testHandler handles requests to the test endpoint.
func testHandler(w http.ResponseWriter, r *http.Request) {
	fmt.Fprintln(w, "Hello, this is a test endpoint!")
}

func setupTestEndpoint(st string, typ string) *http.Server {
	// Create a new instance of a server
	server := &http.Server{Addr: st}

	// Register the test handler with the specific type
	http.HandleFunc(typ, testHandler)

	// Start the server in a goroutine
	go func() {
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			fmt.Printf("ListenAndServe error: %s\n", err)
		}
	}()

	return server
}

func newProxy(cfg *Config) (*Proxy, error) {
	ctx := context.Background()

	// Initialize a logger
	logger := proxycore.GetOrCreateNopLogger(nil)

	// Mock for SpannerClientInterface
	mockSpannerClient := new(proxyIfaceMock.SpannerClientInterface)
	mockSpannerClient.On("GetClient", ctx).Return(&spanner.Client{}, nil)
	originalNewProxySpannerClient := NewSpannerClient
	NewSpannerClient = func(ctx context.Context, config Config, ot *otelgo.OpenTelemetry) iface.SpannerClientInterface {
		return mockSpannerClient
	}
	defer func() { NewSpannerClient = originalNewProxySpannerClient }()

	// Initialize the columnMap and columnListMap
	columnMap := make(map[string]map[string]*tableConfig.Column)
	columnMap["test_table"] = make(map[string]*tableConfig.Column)
	columnMap["test_table"]["test_id"] = &tableConfig.Column{
		Name:         "test_id",
		CQLType:      "text",
		SpannerType:  "string",
		IsPrimaryKey: true,
		PkPrecedence: 0,
		Metadata: message.ColumnMetadata{
			Keyspace: "test_keyspace",
			Table:    "test_table",
			Index:    0,
			Type:     datatype.Varchar,
			Name:     "test_id",
		},
	}
	columnMap["test_table"]["test_hash"] = &tableConfig.Column{
		Name:         "test_hash",
		CQLType:      "text",
		SpannerType:  "string",
		IsPrimaryKey: false,
		PkPrecedence: 1,
		Metadata: message.ColumnMetadata{
			Keyspace: "test_keyspace",
			Table:    "test_table",
			Index:    1,
			Type:     datatype.Varchar,
			Name:     "test_hash",
		},
	}

	columnListMap := make(map[string][]tableConfig.Column)
	columnListMap["test_table"] = []tableConfig.Column{
		{
			Name:         "test_id",
			CQLType:      "text",
			IsPrimaryKey: true,
			Metadata: message.ColumnMetadata{
				Keyspace: "test_keyspace",
				Table:    "test_table",
				Index:    0,
				Type:     datatype.Varchar,
				Name:     "test_id",
			},
		},
		{
			Name:         "test_hash",
			CQLType:      "text",
			IsPrimaryKey: false,
			Metadata: message.ColumnMetadata{
				Keyspace: "test_keyspace",
				Table:    "test_table",
				Index:    1,
				Type:     datatype.Varchar,
				Name:     "test_hash",
			},
		},
	}
	proxyConfig := Config{
		OtelConfig: &OtelConfig{
			Enabled: false,
		},
		Version: primitive.ProtocolVersion4,
		Logger:  logger,
		SpannerConfig: SpannerConfig{
			GCPProjectID:    "cassandra-to-spanner",
			InstanceName:    "test-instance-v1",
			DatabaseName:    "test-database",
			ConfigTableName: "test",
		},
		KeyspaceFlatter: true,
	}
	if cfg != nil && cfg.Resolver != nil {
		proxyConfig.Resolver = cfg.Resolver
	}

	// Mock for SpannerClientIface
	spanmockface := new(spannerMock.SpannerClientIface)
	spannerQuery := fmt.Sprintf("SELECT * FROM %v ORDER BY TableName, IsPrimaryKey, PK_Precedence;", proxyConfig.SpannerConfig.ConfigTableName)
	query := responsehandler.QueryMetadata{
		Query:        spannerQuery,
		TableName:    proxyConfig.SpannerConfig.ConfigTableName,
		ProtocalV:    proxyConfig.Version,
		KeyspaceName: proxyConfig.SpannerConfig.DatabaseName,
	}

	queryForTest := responsehandler.QueryMetadata{
		Query:     "SELECT test_id, test_hash FROM key_space.test_table WHERE test_id = '?' AND test_hash = '?'",
		TableName: "test_table",
		ProtocalV: primitive.ProtocolVersion4,
		Params:    make(map[string]interface{}),
	}
	spanmockface.On("GetTableConfigs", ctx, query).Return(columnMap, columnListMap, &spannerModule.SystemQueryMetadataCache{}, nil)
	spanmockface.On("SelectStatement", ctx, queryForTest).Return(&message.RowsResult{}, nil)

	// Override the factory function to return the mock
	originalNewSpannerClient := spannerModule.NewSpannerClient
	spannerModule.NewSpannerClient = func(client *spanner.Client, logger *zap.Logger, responseHandler *responsehandler.TypeHandler, keyspaceflatter bool, maxCommitDelay uint64, replayProtection bool) spannerModule.SpannerClientIface {
		return spanmockface
	}
	defer func() { spannerModule.NewSpannerClient = originalNewSpannerClient }()
	mockSender := new(proxyIfaceMock.Sender)
	mockSender.On("Send", mock.AnythingOfType("*frame.Header"), mock.AnythingOfType("message.Message")).Return()

	// Call the function under test
	proxyConfig.Version = 0
	prox, err := NewProxy(ctx, proxyConfig)
	if err != nil {
		return nil, err
	}

	cl := &client{
		ctx:                 prox.ctx,
		proxy:               prox,
		preparedSystemQuery: make(map[[preparedIdSize]byte]interface{}),
		keyspace:            "test",
		conn:                &proxycore.Conn{},
		sender:              mockSender,
	}
	prox.registerForEvents(cl)
	prox.clients[cl] = struct{}{}
	prox.tableConfig = &tableConfig.TableConfig{
		TablesMetaData:  columnMap,
		PkMetadataCache: columnListMap,
		Logger:          logger,
		KeyspaceFlatter: false,
	}
	return prox, err
}

func TestIsIdempotent(t *testing.T) {
	// Create a zap logger for testing
	logger := proxycore.GetOrCreateNopLogger(nil)

	// Initialize the Proxy with a sync.Map and logger
	proxy := &Proxy{
		preparedIdempotence: sync.Map{},
		logger:              logger,
	}

	// Test data
	id := []byte{0x01, 0x02, 0x03, 0x04}

	// Scenario 1: preparedIdempotence has no entry for the given id
	assert.False(t, proxy.IsIdempotent(id), "Expected IsIdempotent to return false for missing idempotence entry")

	// Scenario 2: preparedIdempotence has an entry for the given id
	proxy.preparedIdempotence.Store(preparedIdKey(id), true)
	assert.True(t, proxy.IsIdempotent(id), "Expected IsIdempotent to return true for idempotent entry")

	// Scenario 3: preparedIdempotence has a non-idempotent entry for the given id
	proxy.preparedIdempotence.Store(preparedIdKey(id), false)
	assert.False(t, proxy.IsIdempotent(id), "Expected IsIdempotent to return false for non-idempotent entry")
}

func TestFilterSystemPeerValues(t *testing.T) {
	mockProxy, err := newProxy(&Config{})
	assert.NoErrorf(t, err, "error occured while creating the proxy instance")
	mockProxy.cluster = &proxycore.Cluster{
		NegotiatedVersion: primitive.ProtocolVersion4,
	}

	mockSender1 := new(proxyIfaceMock.Sender)
	mockSender1.On("Send", mock.AnythingOfType("*frame.Header"), mock.AnythingOfType("*message.Invalid")).Return()

	mockNode := &node{
		dc:     "example_dc",
		addr:   &net.IPAddr{IP: []byte("127.0.1")},
		tokens: []string{"token1", "token2"},
	}

	client := &client{
		ctx:    context.TODO(),
		proxy:  mockProxy,
		sender: mockSender1,
	}

	stmt := &parser.SelectStatement{
		Keyspace: "test_keyspace",
		Table:    "test_table",
		Selectors: []parser.Selector{
			&parser.CountStarSelector{Name: "data_center"},
			&parser.CountStarSelector{Name: "host_id"},
			&parser.CountStarSelector{Name: "tokens"},
			&parser.CountStarSelector{Name: "peer"},
			&parser.CountStarSelector{Name: "rpc_address"},
			&parser.CountStarSelector{Name: "example_column"},
			&parser.CountStarSelector{Name: parser.CountValueName},
		},
	}
	filtered := []*message.ColumnMetadata{
		{Name: "data_center"},
		{Name: "host_id"},
		{Name: "tokens"},
		{Name: "peer"},
		{Name: "rpc_address"},
		{Name: "example_column"},
		{Name: parser.CountValueName},
	}

	_, err = client.filterSystemPeerValues(stmt, filtered, mockNode, 5)
	assert.NoError(t, err)
}

func TestFilterSystemLocalValues(t *testing.T) {
	mockProxy, err := newProxy(&Config{})
	assert.NoErrorf(t, err, "error occured while creating the proxy instance")
	mockProxy.cluster = &proxycore.Cluster{
		NegotiatedVersion: primitive.ProtocolVersion4,
	}
	mockProxy.systemLocalValues = map[string]message.Column{
		"example_column": []byte(""),
	}

	mockSender := new(proxyIfaceMock.Sender)
	mockSender.On("Send", mock.AnythingOfType("*frame.Header"), mock.AnythingOfType("*message.Invalid")).Return()

	client := &client{
		ctx:    context.TODO(),
		proxy:  mockProxy,
		sender: mockSender,
	}

	stmt := &parser.SelectStatement{
		Keyspace: "test_keyspace",
		Table:    "test_table",
		Selectors: []parser.Selector{
			&parser.CountStarSelector{Name: "rpc_address"},
			&parser.CountStarSelector{Name: "host_id"},
			&parser.CountStarSelector{Name: "example_column"},
			&parser.CountStarSelector{Name: parser.CountValueName},
		},
	}

	filtered := []*message.ColumnMetadata{
		{Name: "rpc_address"},
		{Name: "host_id"},
		{Name: "example_column"},
		{Name: parser.CountValueName},
	}
	_, err = client.filterSystemLocalValues(stmt, filtered)
	assert.NoError(t, err)
}

func TestGetMetadataFromCache(t *testing.T) {
	testID := [16]byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16}

	columnMetadata := []*message.ColumnMetadata{
		{Name: "column1"},
		{Name: "column2"},
	}

	tests := []struct {
		name          string
		preparedQuery interface{}
		expectedFound bool
		expectedVarMD []*message.ColumnMetadata
		expectedRetMD []*message.ColumnMetadata
	}{
		{
			name: "SelectQueryMap found",
			preparedQuery: &translator.SelectQueryMap{
				VariableMetadata: columnMetadata,
				ReturnMetadata:   columnMetadata,
			},
			expectedFound: true,
			expectedVarMD: columnMetadata,
			expectedRetMD: columnMetadata,
		},
		{
			name: "InsertQueryMap found",
			preparedQuery: &translator.InsertQueryMap{
				VariableMetadata: columnMetadata,
				ReturnMetadata:   columnMetadata,
			},
			expectedFound: true,
			expectedVarMD: columnMetadata,
			expectedRetMD: columnMetadata,
		},
		{
			name: "DeleteQueryMap found",
			preparedQuery: &translator.DeleteQueryMap{
				VariableMetadata: columnMetadata,
				ReturnMetadata:   columnMetadata,
			},
			expectedFound: true,
			expectedVarMD: columnMetadata,
			expectedRetMD: columnMetadata,
		},
		{
			name: "UpdateQueryMap found",
			preparedQuery: &translator.UpdateQueryMap{
				VariableMetadata: columnMetadata,
				ReturnMetadata:   columnMetadata,
			},
			expectedFound: true,
			expectedVarMD: columnMetadata,
			expectedRetMD: columnMetadata,
		},
		{
			name:          "Query not found",
			preparedQuery: nil,
			expectedFound: false,
			expectedVarMD: nil,
			expectedRetMD: nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			prox, _ := newProxy(&Config{})
			client := &client{
				proxy: prox,
			}

			if tt.preparedQuery != nil {
				client.proxy.preparedQueriesCache.Store(testID, tt.preparedQuery)
			}

			varMD, retMD, found := client.getMetadataFromCache(testID)

			assert.Equal(t, tt.expectedFound, found)
			assert.Equal(t, tt.expectedVarMD, varMD)
			assert.Equal(t, tt.expectedRetMD, retMD)
		})
	}
}

func Test_removeClient(t *testing.T) {
	p, err := newProxy(&Config{})
	assert.NoErrorf(t, err, "error occured while creating new proxy instance")
	cl := &client{
		ctx:                 p.ctx,
		proxy:               p,
		preparedSystemQuery: make(map[[preparedIdSize]byte]interface{}),
	}
	cl.sender = cl
	p.addClient(cl)

	p.removeClient(cl)
}
