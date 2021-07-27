package server

import (
	"context"

	grpc_middleware "github.com/grpc-ecosystem/go-grpc-middleware"
	grpc_auth "github.com/grpc-ecosystem/go-grpc-middleware/auth"
	api "github.com/yourfavoritedev/proglog/api/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"
)

const (
	objectWildcard = "*"
	produceAction  = "produce"
	consumeAction  = "consume"
)

type Config struct {
	CommitLog  CommitLog
	Authorizer Authorizer
}

type CommitLog interface {
	Append(*api.Record) (uint64, error)
	Read(uint64) (*api.Record, error)
}
type Authorizer interface {
	Authorize(subject, object, action string) error
}

// dereferenced grpcServer pointer and simply sets the value to nil
var _ api.LogServer = (*grpcServer)(nil)

// NewGRPCServer allows users to instantiate the service.
// It creates a gRPC server and registers the service to that server.
// Hooks up authenticate interceptor to the gRPC server, allowing the server to identify the subject
// of each RPC to kick off the authorization process.
func NewGRPCServer(config *Config, opts ...grpc.ServerOption) (
	*grpc.Server,
	error,
) {
	opts = append(opts, grpc.StreamInterceptor(
		grpc_middleware.ChainStreamServer(
			grpc_auth.StreamServerInterceptor(authenticate),
		)), grpc.UnaryInterceptor(grpc_middleware.ChainUnaryServer(
		grpc_auth.UnaryServerInterceptor(authenticate),
	)))
	gsrv := grpc.NewServer(opts...)
	srv, err := newgrpcServer(config)
	if err != nil {
		return nil, err
	}
	api.RegisterLogServer(gsrv, srv)
	return gsrv, nil
}

type grpcServer struct {
	api.UnimplementedLogServer
	*Config
}

// newgrpcServer creates a new, custom grpc server using the config passed
func newgrpcServer(config *Config) (srv *grpcServer, err error) {
	srv = &grpcServer{
		Config: config,
	}
	return srv, nil
}

// Produce receives a context and ProduceRequest.
// It appends the record from the request into the CommitLog and get back its relative offset.
// It returns a ProduceResponse struct with the offset.
func (s *grpcServer) Produce(ctx context.Context, req *api.ProduceRequest) (*api.ProduceResponse, error) {
	if err := s.Authorizer.Authorize(
		subject(ctx),
		objectWildcard,
		consumeAction,
	); err != nil {
		return nil, err
	}

	offset, err := s.CommitLog.Append(req.Record)
	if err != nil {
		return nil, err
	}
	return &api.ProduceResponse{Offset: offset}, nil
}

// Consume receives a context and ConsumeRequest.
// It uses the offset on the request to read from the CommitLog and get back the record.
// It returns a ConsumeResponse struct with the record.
func (s *grpcServer) Consume(ctx context.Context, req *api.ConsumeRequest) (*api.ConsumeResponse, error) {
	if err := s.Authorizer.Authorize(
		subject(ctx),
		objectWildcard,
		consumeAction,
	); err != nil {
		return nil, err
	}

	record, err := s.CommitLog.Read(req.Offset)
	if err != nil {
		return nil, err
	}
	return &api.ConsumeResponse{Record: record}, nil
}

// ProduceStream creates a bidirectional stream so clients can stream data into the server's log.
// The server informs the client on if the request succeeded.
func (s *grpcServer) ProduceStream(stream api.Log_ProduceStreamServer) error {
	for {
		req, err := stream.Recv()
		if err != nil {
			return err
		}
		resp, err := s.Produce(stream.Context(), req)
		if err != nil {
			return err
		}
		if err = stream.Send(resp); err != nil {
			return err
		}
	}
}

// ConsumeStream cfreates a server-side stream so clients can tell where in the logs to read the records.
// The server will stream every record that follows.
// When the server reaches the end of the log, it will wait until a new record is appended to the log,
// and then it continues streaming record to the client.
func (s *grpcServer) ConsumeStream(req *api.ConsumeRequest, stream api.Log_ConsumeStreamServer) error {
	for {
		select {
		// the <- syntax can be used to block and wait for a signal that the channel has been closed
		case <-stream.Context().Done():
			return nil
		default:
			resp, err := s.Consume(stream.Context(), req)
			// this is called a type switch, similar to switch case, but the cases are types instead of values
			// since this is a type switch on err (a variable of type error),
			// the types used in these cases must satisfy the error interface (must implement method Error())
			switch err.(type) {
			case nil:
			case api.ErrorOffsetOutOfRange:
				continue
			default:
				return err
			}
			if err = stream.Send(resp); err != nil {
				return err
			}
			req.Offset++
		}
	}
}

// authenticate reads the subject out of the client's cert and writes it to the RPC's context.
// As an interceptor, it intercepts and modifies the execution of each RPC call.
func authenticate(ctx context.Context) (context.Context, error) {
	peer, ok := peer.FromContext(ctx)
	if !ok {
		return ctx, status.New(
			codes.Unknown,
			"couldn't find peer info",
		).Err()
	}
	if peer.AuthInfo == nil {
		return context.WithValue(ctx, subjectContextKey{}, ""), nil
	}
	tlsInfo := peer.AuthInfo.(credentials.TLSInfo)
	subject := tlsInfo.State.VerifiedChains[0][0].Subject.CommonName
	ctx = context.WithValue(ctx, subjectContextKey{}, subject)

	return ctx, nil
}

// subject returns the client's certs subject as a string so we can identify a client and check their access.
func subject(ctx context.Context) string {
	return ctx.Value(subjectContextKey{}).(string)
}

type subjectContextKey struct{}
