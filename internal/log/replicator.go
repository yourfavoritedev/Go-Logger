package log

import (
	"context"
	"sync"

	api "github.com/yourfavoritedev/proglog/api/v1"
	"go.uber.org/zap"
	"google.golang.org/grpc"
)

// Replicator connects to other servers with the gRPC client
// The client needs to be configured to authenticate with the servers
type Replicator struct {
	DialOptions []grpc.DialOption
	LocalServer api.LogClient

	logger *zap.Logger

	mu      sync.Mutex
	servers map[string]chan struct{}
	closed  bool
	close   chan struct{}
}

// Join adds the given server name to the map of servers to replicate
// and kicks off the replicate goroutine to run the replication logic
func (r *Replicator) Join(name, addr string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.init()

	if r.closed {
		return nil
	}

	if _, ok := r.servers[name]; ok {
		// already replicating so skip
		return nil
	}
	r.servers[name] = make(chan struct{}) // equivalent to a done channel, used to signal that the channel has closed
	go r.replicate(addr, r.servers[name])
	return nil
}

// replicate will create a client and open up a stream to consume all logs on the server
func (r *Replicator) replicate(addr string, leave chan struct{}) {
	// setup connection with server address to client target
	cc, err := grpc.Dial(addr, r.DialOptions...)
	if err != nil {
		r.logError(err, "failed to dial", addr)
		return
	}
	defer cc.Close()

	// create new client using clientConnection
	client := api.NewLogClient(cc)

	ctx := context.Background()
	// create stream to consume client requests
	stream, err := client.ConsumeStream(ctx,
		&api.ConsumeRequest{
			Offset: 0,
		},
	)

	if err != nil {
		r.logError(err, "Failed to consume", addr)
		return
	}

	records := make(chan *api.Record)
	// use stream to consume responses from server and write them to the records channel
	go func() {
		for {
			recv, err := stream.Recv()
			if err != nil {
				r.logError(err, "failed to receive", addr)
				return
			}
			// write log records to records channel
			records <- recv.Record
		}
	}()

	// Consume logs from the discovered server and produce them to the local server
	// It will continue to replicate messages to the local server until
	// that server fails, leaves the cluster or the replicator closes the channel for that server.
	for {
		select {
		case <-r.close:
			return
		case <-leave:
			return
		// Produce a copy of the record to the local-server
		case record := <-records:
			_, err = r.LocalServer.Produce(ctx,
				&api.ProduceRequest{
					Record: record,
				},
			)
			if err != nil {
				r.logError(err, "failed to produce", addr)
				return
			}
		}
	}
}

// Leave accepts a server name and handles the server leaving the cluster.
// It removes the server from the map of servers and closes the server's associated channel.
// Closing the channel signals to the receiver in the replicate goroutine to stop replicating from that server.
func (r *Replicator) Leave(name string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.init()
	if _, ok := r.servers[name]; !ok {
		return nil
	}
	close(r.servers[name])
	delete(r.servers, name)
	return nil
}

// init will lazily initialize the server map and structs giving them a useful zero value.
// Having a zero value reduces the API size and complexity while maintaining the same functionality.
// Without a useful zero value, we'd either have to export a replicator constructor function for the user to call or
// export the servers field on the replicator struct for the user to set - making the API for the user more complex.
func (r *Replicator) init() {
	if r.logger == nil {
		r.logger = zap.L().Named("replicator")
	}
	if r.servers == nil {
		r.servers = make(map[string]chan struct{})
	}
	if r.close == nil {
		r.close = make(chan struct{})
	}
}

// Close will close the replicator's close channel.
// A signal of the closed channel is received by the replicate gorountine, via select/case, causing it to return.
// This stops replicating existing servers and stops replicating new servers that join the cluster.
func (r *Replicator) Close() error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.init()

	if r.closed {
		return nil
	}
	r.closed = true
	close(r.close)
	return nil
}

// logError will log the given error
func (r *Replicator) logError(err error, msg, addr string) {
	r.logger.Error(
		msg,
		zap.String("addr", addr),
		zap.Error(err),
	)
}
