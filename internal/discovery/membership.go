package discovery

import (
	"net"

	"github.com/hashicorp/serf/serf"
	"go.uber.org/zap"
)

// Membership warps Serf to provide directory and cluster membership to our service
type Membership struct {
	Config
	handler Handler
	serf    *serf.Serf
	events  chan serf.Event
	logger  *zap.Logger
}

// New creates a Membership using the required event handler and configuration.
func New(handler Handler, config Config) (*Membership, error) {
	m := &Membership{
		Config:  config,
		handler: handler,
		logger:  zap.L().Named("membership"),
	}
	if err := m.setupSerf(); err != nil {
		return nil, err
	}
	return m, nil
}

// Config stores metadata for a configuration that will be used for a Membership
type Config struct {
	NodeName       string            // NodeName acts as the node's unique identifier across the Serf cluster.
	BindAddr       string            // BindAddr is listened on by Serf for gossiping
	Tags           map[string]string // Tags are shared by Serf to the other nodes in the cluster. They should be used for simple data that informs the cluster how to handle the node.
	StartJoinAddrs []string          // StartJoinAddrs configures new nodes to join an existing cluster.
}

// setupSerf creates and configures a Serf instance and starts the eventHandler goroutine to handle Serf's events
func (m *Membership) setupSerf() (err error) {
	addr, err := net.ResolveTCPAddr("tcp", m.BindAddr)
	if err != nil {
		return err
	}
	config := serf.DefaultConfig()
	config.Init()
	config.MemberlistConfig.BindAddr = addr.IP.String()
	config.MemberlistConfig.BindPort = addr.Port
	m.events = make(chan serf.Event)
	// the event channel receives Serf's events when a node joins or leaves the cluster.
	config.EventCh = m.events
	config.Tags = m.Tags
	config.NodeName = m.Config.NodeName
	m.serf, err = serf.Create(config)
	if err != nil {
		return err
	}

	// Using a goroutine to faciliate concurrent Serf events.
	// When a member joins or leaves, we will write them to the respective channels in the handler.
	// Therefore a goroutine is necessary to write (send) the data into those channels to avoid a deadlock.
	go m.eventHandler()
	if m.StartJoinAddrs != nil {
		_, err = m.serf.Join(m.StartJoinAddrs, true)
		if err != nil {
			return err
		}
	}
	return nil
}

// Handler represents some component in our service that needs to know when a server joins or leaves the cluster
type Handler interface {
	Join(name, add string) error
	Leave(name string) error
}

// eventHandler runs in a loop reading events sent by Serf into the events channel
func (m *Membership) eventHandler() {
	// when a node joins or leaves the cluster, Serf sends an event to all nodes
	for e := range m.events {
		switch e.EventType() {
		case serf.EventMemberJoin:
			for _, member := range e.(serf.MemberEvent).Members {
				if m.isLocal(member) {
					continue
				}
				m.handleJoin(member)
			}
		case serf.EventMemberLeave, serf.EventMemberFailed:
			for _, member := range e.(serf.MemberEvent).Members {
				if m.isLocal(member) {
					return
				}
				m.handleLeave(member)
			}
		}
	}
}

// handleJoin will call the handlers Join method
func (m *Membership) handleJoin(member serf.Member) {
	if err := m.handler.Join(member.Name, member.Tags["rpc_addr"]); err != nil {
		m.logError(err, "failed to join", member)
	}
}

// handleLeave will call the handlers Leave method
func (m *Membership) handleLeave(member serf.Member) {
	if err := m.handler.Leave(member.Name); err != nil {
		m.logError(err, "failed to leave", member)
	}
}

// isLocal returns whether a member is the local member
func (m *Membership) isLocal(member serf.Member) bool {
	return m.serf.LocalMember().Name == member.Name
}

// Members returns all members in the cluster
func (m *Membership) Members() []serf.Member {
	return m.serf.Members()
}

// Leave tells the member to leave the Serf cluster
func (m *Membership) Leave() error {
	return m.serf.Leave()
}

// logError logs the given error and message
func (m *Membership) logError(err error, msg string, member serf.Member) {
	m.logger.Error(
		msg,
		zap.Error(err),
		zap.String("name", member.Name),
		zap.String("rpc_addr", member.Tags["rpc_addr"]),
	)
}
