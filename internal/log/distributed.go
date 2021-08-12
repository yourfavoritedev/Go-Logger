package log

import (
	"bytes"
	"crypto/tls"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"time"

	"github.com/hashicorp/raft"
	raftboltdb "github.com/hashicorp/raft-boltdb"
	api "github.com/yourfavoritedev/proglog/api/v1"
	"google.golang.org/protobuf/proto"
)

type DistributedLog struct {
	config Config
	log    *Log
	raft   *raft.Raft
}

// NewDistributedLog creates a distributed log. It setups a new log and a new raft instance.
func NewDistributedLog(dataDir string, config Config) (*DistributedLog, error) {
	l := &DistributedLog{
		config: config,
	}
	if err := l.setupLog(dataDir); err != nil {
		return nil, err
	}
	if err := l.setupRaft(dataDir); err != nil {
		return nil, err
	}
	return l, nil
}

// setupLog creates the log for this server, where this server will store the user's records
func (l *DistributedLog) setupLog(dataDir string) error {
	logDir := filepath.Join(dataDir, "log")
	if err := os.MkdirAll(logDir, 0755); err != nil {
		return err
	}
	var err error
	// create a new log for the DistributedLog
	l.log, err = NewLog(logDir, l.config)
	return err
}

func (l *DistributedLog) setupRaft(dataDir string) error {
	// create a finite-state-machine
	fsm := &fsm{
		log: l.log,
	}
	logDir := filepath.Join(dataDir, "raft", "log")
	if err := os.MkdirAll(logDir, 0755); err != nil {
		return err
	}
	// create Raft's log store using our own log
	logConfig := l.config
	// Raft requires our log's initial offset to be 1
	logConfig.Segment.InitialOffset = 1
	logStore, err := newLogStore(logDir, logConfig)
	if err != nil {
		return err
	}

	// the stableStore is a key-value store where Raft stores important metadata, ie - the server's current term,
	// or the candidate the server voted for.
	stableStore, err := raftboltdb.NewBoltStore(
		filepath.Join(dataDir, "raft", "stable"),
	)
	if err != nil {
		return err
	}
	retain := 1
	// use Raft's snapshot store to recover and restore data efficiently, when necessary.
	snapshotStore, err := raft.NewFileSnapshotStore(
		filepath.Join(dataDir, "raft"),
		retain,
		os.Stderr,
	)
	if err != nil {
		return err
	}
	maxPool := 5
	timeout := 10 * time.Second
	// create transport that wraps a stream layer - a low level stream abstration.
	transport := raft.NewNetworkTransport(
		l.config.Raft.StreamLayer,
		maxPool,
		timeout,
		os.Stderr,
	)

	config := raft.DefaultConfig()
	// the config's LocalIOd is the unique ID for this server
	config.LocalID = l.config.Raft.LocalID
	// override timeout configs to speedup Raft ie - when a leader is shut down, we want the election to finish within a second
	if l.config.Raft.HeartbeatTimeout != 0 {
		config.HeartbeatTimeout = l.config.Raft.HeartbeatTimeout
	}
	if l.config.Raft.ElectionTimeout != 0 {
		config.ElectionTimeout = l.config.Raft.ElectionTimeout
	}
	if l.config.Raft.LeaderLeaseTimeout != 0 {
		config.LeaderLeaseTimeout = l.config.Raft.LeaderLeaseTimeout
	}
	if l.config.Raft.CommitTimeout != 0 {
		config.CommitTimeout = l.config.Raft.CommitTimeout
	}

	// create a new Raft instance for the DistributedLog
	l.raft, err = raft.NewRaft(
		config,
		fsm,
		logStore,
		stableStore,
		snapshotStore,
		transport,
	)
	if err != nil {
		return err
	}
	hasState, err := raft.HasExistingState(
		logStore,
		stableStore,
		snapshotStore,
	)
	if err != nil {
		return err
	}
	if l.config.Raft.Bootstrap && !hasState {
		config := raft.Configuration{
			Servers: []raft.Server{{
				ID:      config.LocalID,
				Address: raft.ServerAddress(l.config.Raft.BindAddr),
			}},
		}
		err = l.raft.BootstrapCluster(config).Error()
	}
	return err
}

// Append appends the record to the log. Raft applies the ProduceRequest method as a command,
// which tells the FSM to append the record to the log
func (l *DistributedLog) Append(record *api.Record) (uint64, error) {
	res, err := l.apply(
		AppendRequestType,
		&api.ProduceRequest{Record: record},
	)
	if err != nil {
		return 0, err
	}
	return res.(*api.ProduceResponse).Offset, nil
}

// apply wraps Raft's API to apply requests and return their responses
func (l *DistributedLog) apply(reqType RequestType, req proto.Message) (interface{}, error) {
	var buf bytes.Buffer
	_, err := buf.Write([]byte{byte(reqType)})
	if err != nil {
		return nil, err
	}
	// marshal the request type and request data into bytes
	b, err := proto.Marshal(req)
	if err != nil {
		return nil, err
	}
	_, err = buf.Write(b)
	if err != nil {
		return nil, err
	}
	timeout := 10 * time.Second
	// Raft uses the bytes of our record's data to replicate to the other servers.
	// the record is replicated and appended to the leader's log
	future := l.raft.Apply(buf.Bytes(), timeout)
	// returns an error when something went wrong with Raft's replication, ie - it took too long for Raft to process the command
	// or the server had to shut down
	if future.Error() != nil {
		return nil, future.Error()
	}
	// returns the FSM's Apply method's results
	res := future.Response()
	if err, ok := res.(error); ok {
		return nil, err
	}
	return res, nil
}

// Read reads the record for the given offset from the server's log.
func (l *DistributedLog) Read(offset uint64) (*api.Record, error) {
	return l.log.Read(offset)
}

// Join adds the server to the Raft cluster. Every server is added as a voter
func (l *DistributedLog) Join(id, addr string) error {
	configFuture := l.raft.GetConfiguration()
	if err := configFuture.Error(); err != nil {
		return err
	}
	serverID := raft.ServerID(id)
	serverAddr := raft.ServerAddress(addr)
	for _, srv := range configFuture.Configuration().Servers {
		if srv.ID == serverID || srv.Address == serverAddr {
			// verify if the server has already joined
			if srv.ID == serverID && srv.Address == serverAddr {
				return nil
			}
			// remove the existing server
			removeFuture := l.raft.RemoveServer(serverID, 0, 0)
			if err := removeFuture.Error(); err != nil {
				return err
			}
		}
	}
	addFuture := l.raft.AddVoter(serverID, serverAddr, 0, 0)
	if err := addFuture.Error(); err != nil {
		return err
	}
	return nil
}

// Leave removes the server from the cluster
func (l *DistributedLog) Leave(id string) error {
	removeFuture := l.raft.RemoveServer(raft.ServerID(id), 0, 0)
	return removeFuture.Error()
}

// WaitForLeader blocks until the cluster has elected a leader or times out
func (l *DistributedLog) WaitForLeader(timeout time.Duration) error {
	// timeoutChannel is returned after the specified duration of our timeout
	timeoutChannel := time.After(timeout)
	// ticker includes a channel (ticker.C) which has new values written to it.
	// A new value is written periodically on the time interval we specified
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-timeoutChannel:
			return fmt.Errorf("timed out")
		case <-ticker.C:
			if l := l.raft.Leader(); l != "" {
				return nil
			}
		}
	}
}

// Close shuts down the Raft instance and closes the local log
func (l *DistributedLog) Close() error {
	f := l.raft.Shutdown()
	if err := f.Error(); err != nil {
		return err
	}
	return nil
}

// GetServers uses Raft to retrieve the servers in the cluster.
// Each server is converted from a raft.Server to an *api.Server for our gRPC API to work with.
func (l *DistributedLog) GetServers() ([]*api.Server, error) {
	future := l.raft.GetConfiguration()
	if err := future.Error(); err != nil {
		return nil, err
	}
	var servers []*api.Server
	for _, server := range future.Configuration().Servers {
		servers = append(servers, &api.Server{
			Id:       string(server.ID),
			RpcAddr:  string(server.Address),
			IsLeader: l.raft.Leader() == server.Address,
		})
	}
	return servers, nil
}

var _ raft.FSM = (*fsm)(nil)

// our internal fsm struct to interface with Raft's FSM
type fsm struct {
	log *Log
}

type RequestType uint8

// defined a custom request type to be used in Raft
const (
	AppendRequestType RequestType = 0
)

// Apply identifies the type of request and determines how the FSM should handle it
func (l *fsm) Apply(record *raft.Log) interface{} {
	buf := record.Data
	reqType := RequestType(buf[0])
	switch reqType {
	case AppendRequestType:
		// Raft will consume the returned response, sending it back to where we called raft.Apply()
		return l.applyAppend(buf[1:])
	}
	return nil
}

// applyAppend will unmarshal the request and then append the record to the local log
func (l *fsm) applyAppend(b []byte) interface{} {
	var req api.ProduceRequest
	err := proto.Unmarshal(b, &req)
	if err != nil {
		return err
	}
	offset, err := l.log.Append(req.Record)
	if err != nil {
		return err
	}
	return &api.ProduceResponse{Offset: offset}
}

// Snapshot returns an FSMSnapshot that represents a point-in-time snapshot of the FSM's state.
// The state is specifically the FSM's log.
func (f *fsm) Snapshot() (raft.FSMSnapshot, error) {
	// will read all the log's data
	r := f.log.Reader()
	return &snapshot{reader: r}, nil
}

type snapshot struct {
	reader io.Reader
}

// Persist is called by Raft on the created FSMSnapshot to write its state to a sink (something to store the bytes in)
func (s *snapshot) Persist(sink raft.SnapshotSink) error {
	if _, err := io.Copy(sink, s.reader); err != nil {
		return err
	}
	return sink.Close()
}

// Release is called by Raft when its finished with the snapshot
func (s *snapshot) Release() {}

// Restore is called by Raft to restore an FSM from a snapshot.
func (f *fsm) Restore(r io.ReadCloser) error {
	b := make([]byte, lenWidth)
	var buf bytes.Buffer
	// read all existing records from snapshot
	for i := 0; ; i++ {
		_, err := io.ReadFull(r, b)
		if err == io.EOF {
			break
		} else if err != nil {
			return err
		}
		size := int64(enc.Uint64(b))
		// copy existing record into new buffer
		if _, err = io.CopyN(&buf, r, size); err != nil {
			return err
		}
		record := &api.Record{}
		// unmarshal buffer data into record
		if err = proto.Unmarshal(buf.Bytes(), record); err != nil {
			return err
		}
		// reset the log and configure its initial offset to the first record's offset we read from the snapshot so that the log's offsets match
		if i == 0 {
			f.log.Config.Segment.InitialOffset = record.Offset
			if err := f.log.Reset(); err != nil {
				return err
			}
		}
		// append record to the FSM's log
		if _, err = f.log.Append(record); err != nil {
			return err
		}
		// reset the buffer after each successful append
		buf.Reset()
	}
	return nil
}

var _ raft.LogStore = (*logStore)(nil)

type logStore struct {
	*Log
}

// newLogStore creates a log store to interface with Raft's log store
func newLogStore(dir string, c Config) (*logStore, error) {
	log, err := NewLog(dir, c)
	if err != nil {
		return nil, err
	}
	return &logStore{log}, nil
}

func (l *logStore) FirstIndex() (uint64, error) {
	return l.LowestOffset()
}

func (l *logStore) LastIndex() (uint64, error) {
	off, err := l.HighestOffset()
	return off, err
}

func (l *logStore) GetLog(index uint64, out *raft.Log) error {
	in, err := l.Read(index)
	if err != nil {
		return err
	}
	out.Data = in.Value
	out.Index = in.Offset
	out.Type = raft.LogType(in.Type)
	out.Term = in.Term
	return nil
}

// StoreLog is a wrapper that accepts a record to be appended
func (l *logStore) StoreLog(record *raft.Log) error {
	return l.StoreLogs([]*raft.Log{record})
}

// StoreLogs is called by Raft to append records to the log store
func (l *logStore) StoreLogs(records []*raft.Log) error {
	for _, record := range records {
		if _, err := l.Append(&api.Record{
			Value: record.Data,
			Term:  record.Term,
			Type:  uint32(record.Type),
		}); err != nil {
			return err
		}
	}
	return nil
}

// DeleteRange removes the records between the offsets provided - its to remove records that are old or stored in a snapshot
func (l *logStore) DeleteRange(min, max uint64) error {
	return l.Truncate(max)
}

var _ raft.StreamLayer = (*StreamLayer)(nil)

type StreamLayer struct {
	ln              net.Listener
	serverTLSConfig *tls.Config
	peerTLSConfig   *tls.Config
}

// NewStreamLayer creates a new StreamLayer to satisfy with Raft's StreamLayer interface
func NewStreamLayer(
	ln net.Listener,
	serverTLSConfig,
	peerTLSConfig *tls.Config,
) *StreamLayer {
	return &StreamLayer{
		ln:              ln,
		serverTLSConfig: serverTLSConfig,
		peerTLSConfig:   peerTLSConfig,
	}
}

const RaftRPC = 1

// Dial makes outgoing connections to other servers in the Raft cluster.
func (s *StreamLayer) Dial(
	addr raft.ServerAddress,
	timeout time.Duration,
) (net.Conn, error) {
	dialer := &net.Dialer{Timeout: timeout}
	var conn, err = dialer.Dial("tcp", string(addr))
	if err != nil {
		return nil, err
	}
	// identify to mux this is a raft rpc
	// When we connect to a server, we write the RaftRPC byte to identify the connection type,
	// allowing us to multiplex Raft on the same port as our Log gRPC requests
	_, err = conn.Write([]byte{byte(RaftRPC)})
	if err != nil {
		return nil, err
	}
	// configure the stream layer with a peer TLS config, making a TLS client-side connection
	if s.peerTLSConfig != nil {
		conn = tls.Client(conn, s.peerTLSConfig)
	}
	return conn, err
}

// Accept is the mirror of Dial. It accepts the incoming connection and reads the byte that identifies the connection.
// Then it creates a server-side TLS connection.
func (s *StreamLayer) Accept() (net.Conn, error) {
	conn, err := s.ln.Accept()
	if err != nil {
		return nil, err
	}
	b := make([]byte, 1)
	_, err = conn.Read(b)
	if err != nil {
		return nil, err
	}
	if bytes.Compare([]byte{byte(RaftRPC)}, b) != 0 {
		return nil, fmt.Errorf("not a raft rpc")
	}
	if s.serverTLSConfig != nil {
		return tls.Server(conn, s.serverTLSConfig), nil
	}
	return conn, nil
}

// Close closes the listener
func (s *StreamLayer) Close() error {
	return s.ln.Close()
}

// Addr returns the listener's address
func (s *StreamLayer) Addr() net.Addr {
	return s.ln.Addr()
}
