package iface

import (
	"context"

	"cloud.google.com/go/spanner"
	"github.com/datastax/go-cassandra-native-protocol/frame"
	"github.com/datastax/go-cassandra-native-protocol/message"
)

type SpannerClientInterface interface {
	GetClient(ctx context.Context) (*spanner.Client, error)
}

type Sender interface {
	Send(hdr *frame.Header, msg message.Message)
}
