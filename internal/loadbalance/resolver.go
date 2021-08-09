package loadbalance

import (
	"context"
	"fmt"
	"sync"

	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/attributes"
	"google.golang.org/grpc/resolver"
	"google.golang.org/grpc/serviceconfig"

	api "github.com/yourfavoritedev/proglog/api/v1"
)

const Name = "proglog"

// Resolver is implmented into gRPC's resolver.Builder and resolver.Resolver interfaces
// The clientConn connection is the user's client connection and gRPC passes it to the resolver,
// for the resolver to update with the servers it discovers.
// The resolverConn is the resolver's own client connection to the server os it can call GetServers() and get the servers.
type Resolver struct {
	mu            sync.Mutex
	clientConn    resolver.ClientConn
	resolverConn  *grpc.ClientConn
	serviceConfig *serviceconfig.ParseResult
	logger        *zap.Logger
}

// Starts implementing the resolver.Builder interface
var _ resolver.Builder = (*Resolver)(nil)

// Build received the data needed to build a resolver that can discover the servers,
// and the client connection the resolver will update with the servers it discovers.
// It sets up a client connection to our gRPC server so the resolver can call the GetServers() API.
func (r *Resolver) Build(
	target resolver.Target,
	cc resolver.ClientConn,
	opts resolver.BuildOptions,
) (
	resolver.Resolver,
	error,
) {
	r.logger = zap.L().Named("resolver")
	r.clientConn = cc
	var dialOpts []grpc.DialOption
	if opts.DialCreds != nil {
		dialOpts = append(
			dialOpts,
			grpc.WithTransportCredentials(opts.DialCreds),
		)
	}
	r.serviceConfig = r.clientConn.ParseServiceConfig(
		fmt.Sprintf(`{"loadBalancingConfig":[{"%s":{}}]}`, Name),
	)
	var err error
	r.resolverConn, err = grpc.Dial(target.Endpoint, dialOpts...)
	if err != nil {
		return nil, err
	}
	r.ResolveNow(resolver.ResolveNowOptions{})
	return r, nil
}

// Scheme returns the resolver's scheme identifier. When gRPC.Dial is called,
// gRPC parses out the scheme fro the targetAddress provided and tries to find a resolver that matches.
func (r *Resolver) Scheme() string {
	return Name
}

// register this resolver with gRPC so gRPC knows about this resolver when its looking for resolvers that
// match the target's scheme.
func init() {
	resolver.Register(&Resolver{})
}

// start implmenting the resolver.Resolver interface
var _ resolver.Resolver = (*Resolver)(nil)

// ResolveNow is called by gRPC to resolve the target, discover the servers and update the client connection with the servers.
// Finally it updates the state with a service config, specifying to use the "proglog" load balancer.
func (r *Resolver) ResolveNow(resolver.ResolveNowOptions) {
	r.mu.Lock()
	defer r.mu.Unlock()
	client := api.NewLogClient(r.resolverConn)
	// get cluster and then set on cc attributes
	ctx := context.Background()
	// use the gRPC client to get the cluster's servers
	res, err := client.GetServers(ctx, &api.GetServersRequest{})
	if err != nil {
		r.logger.Error(
			"failed to resolve error",
			zap.Error(err),
		)
		return
	}
	var addrs []resolver.Address
	for _, server := range res.Servers {
		addrs = append(addrs, resolver.Address{
			Addr: server.RpcAddr,
			Attributes: attributes.New(
				"is_leader",
				server.IsLeader,
			),
		})
	}
	r.clientConn.UpdateState(resolver.State{
		Addresses:     addrs,
		ServiceConfig: r.serviceConfig,
	})
}

// Close closes the resolver and the connection to our server
func (r *Resolver) Close() {
	if err := r.resolverConn.Close(); err != nil {
		r.logger.Error(
			"failed to close conn",
			zap.Error(err),
		)
	}
}
