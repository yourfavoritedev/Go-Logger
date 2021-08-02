package agent

import (
	"crypto/tls"
	"fmt"
	"net"
	"sync"

	"go.uber.org/zap"

	api "github.com/yourfavoritedev/proglog/api/v1"
	"github.com/yourfavoritedev/proglog/internal/auth"
	"github.com/yourfavoritedev/proglog/internal/discovery"
	"github.com/yourfavoritedev/proglog/internal/log"
	"github.com/yourfavoritedev/proglog/internal/server"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
)

// An Agent runs on every service instance, setting up and connecting all the different components.
// It references and manages each component (log, server, membership, replicator).
type Agent struct {
	Config
	log        *log.Log
	server     *grpc.Server
	membership *discovery.Membership
	replicator *log.Replicator

	shutdown     bool
	shutdowns    chan struct{}
	shutdownLock sync.Mutex
}

// Config comprises the components' parameters, passing them through to the Agent to setup the components
type Config struct {
	ServerTLSConfig *tls.Config
	PeerTLSConfig   *tls.Config
	DataDir         string
	BindAddr        string
	RPCPort         int
	NodeName        string
	StartJoinAddrs  []string
	ACLModelFile    string
	ACLPolicyFile   string
}

// RPCAddr constructs a host and port given an address
func (c Config) RPCAddr() (string, error) {
	host, _, err := net.SplitHostPort(c.BindAddr)
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("%s:%d", host, c.RPCPort), nil
}

// New creates an Agent and runs a set of methods to set up and run the agent's components.
// After run, we can expect to have a running, functioning service.
func New(config Config) (*Agent, error) {
	a := &Agent{
		Config:    config,
		shutdowns: make(chan struct{}),
	}
	setup := []func() error{
		a.setupLogger,
		a.setupLog,
		a.setupServer,
		a.setupMembership,
	}
	for _, fn := range setup {
		if err := fn(); err != nil {
			return nil, err
		}
	}
	return a, nil
}

// setupLogger creates a new logger for the Agent
func (a *Agent) setupLogger() error {
	logger, err := zap.NewDevelopment()
	if err != nil {
		return err
	}
	zap.ReplaceGlobals(logger)
	return nil
}

// setupLog establishes a new log for the Agent with the given directory
func (a *Agent) setupLog() error {
	var err error
	a.log, err = log.NewLog(
		a.Config.DataDir,
		log.Config{},
	)
	return err
}

// setupServer configures a new gRPC server for the Agent.
// It authorizes the agent using the Agent's Config and ACL.
// It constructs an rpcAddress and Listens to it
// The server is then served and accepts incoming connections to the listener
func (a *Agent) setupServer() error {
	authorizer := auth.New(
		a.Config.ACLModelFile,
		a.Config.ACLPolicyFile,
	)

	serverConfig := &server.Config{
		CommitLog:  a.log,
		Authorizer: authorizer,
	}

	var opts []grpc.ServerOption
	if a.Config.ServerTLSConfig != nil {
		creds := credentials.NewTLS(a.Config.ServerTLSConfig)
		opts = append(opts, grpc.Creds(creds))
	}
	var err error
	a.server, err = server.NewGRPCServer(serverConfig, opts...)
	if err != nil {
		return err
	}
	rpcAddr, err := a.RPCAddr()
	if err != nil {
		return err
	}
	ln, err := net.Listen("tcp", rpcAddr)
	if err != nil {
		return err
	}
	go func() {
		if err := a.server.Serve(ln); err != nil {
			_ = a.Shutdown()
		}
	}()
	return err
}

// setupMembership sets up a Replicator for the Agent with the gRPC dial options needed to connect to other servers and a client,
// enabling the replicator to connect to other servers, consume their data and produce a copy of the data to the local server.
// Then it creates Membership for the Agent using the Replicator and its handler to notify the replicator when servers join and leave the cluster.
func (a *Agent) setupMembership() error {
	rpcAddr, err := a.Config.RPCAddr()
	if err != nil {
		return err
	}
	var opts []grpc.DialOption
	if a.Config.PeerTLSConfig != nil {
		opts = append(opts, grpc.WithTransportCredentials(credentials.NewTLS(a.Config.PeerTLSConfig)))
	}
	conn, err := grpc.Dial(rpcAddr, opts...)
	if err != nil {
		return err
	}
	client := api.NewLogClient(conn)
	a.replicator = &log.Replicator{
		DialOptions: opts,
		LocalServer: client,
	}
	a.membership, err = discovery.New(a.replicator, discovery.Config{
		NodeName: a.Config.NodeName,
		BindAddr: a.Config.BindAddr,
		Tags: map[string]string{
			"rpc_addr": rpcAddr,
		},
		StartJoinAddrs: a.Config.StartJoinAddrs,
	})
	return err
}

// Shutdown ensures the agent will shut down once even if people call it multiple times.
// It shuts down the agent and its components. It leaves the membership so that other servers will see
// that this server has left the cluster - this server should not receive discovery events anymore
// Closes the replicator so it doesn't continue to replicate.
// Gracefully stops the server - stopping the server from accepting new connections and blocks until all the pending RPCs have finished.
// Closes the log.
func (a *Agent) Shutdown() error {
	a.shutdownLock.Lock()
	defer a.shutdownLock.Unlock()
	if a.shutdown {
		return nil
	}
	a.shutdown = true
	close(a.shutdowns)
	shutdown := []func() error{
		a.membership.Leave,
		a.replicator.Close,
		func() error {
			a.server.GracefulStop()
			return nil
		},
		a.log.Close,
	}
	for _, fn := range shutdown {
		if err := fn(); err != nil {
			return err
		}
	}
	return nil
}
