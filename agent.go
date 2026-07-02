package ngrok

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net"
	"net/url"
	"slices"
	"sync"
	"time"

	"golang.org/x/net/proxy"

	"github.com/billythach/ngrok-go/v2/internal/legacy"
	"github.com/billythach/ngrok-go/v2/internal/legacy/config"
	"github.com/billythach/ngrok-go/v2/rpc"
)

// Agent is the main interface for interacting with the ngrok service.
type Agent interface {
	// Connect begins a new Session by connecting and authenticating to the ngrok cloud service.
	Connect(context.Context) error

	// Disconnect terminates the current Session which disconnects it from the ngrok cloud service.
	Disconnect() error

	// Session returns an object describing the connection of the Agent to the ngrok cloud service.
	Session() (AgentSession, error)

	// Endpoints returns the list of endpoints created by this Agent from calls to either Listen or Forward.
	Endpoints() []Endpoint

	// Listen creates an Endpoint which returns received connections to the caller via an EndpointListener.
	Listen(context.Context, ...EndpointOption) (EndpointListener, error)

	// Forward creates an Endpoint which forwards received connections to a target upstream URL.
	Forward(context.Context, *Upstream, ...EndpointOption) (EndpointForwarder, error)
}

// Dialer is an interface that is satisfied by net.Dialer or you can specify your
// own implementation.
type Dialer interface {
	Dial(network, address string) (net.Conn, error)
	DialContext(ctx context.Context, network, address string) (net.Conn, error)
}

// agent implements the Agent interface.
type agent struct {
	mu           sync.RWMutex
	sess         legacy.Session
	agentSession *agentSession
	opts         *agentOpts
	endpoints    []Endpoint
	// Event handlers registered with this agent
	eventHandlers []EventHandler
	eventMutex    sync.RWMutex // Protects eventHandlers
}

// NewAgent creates a new Agent object.
func NewAgent(agentOpts ...AgentOption) (Agent, error) {
	opts := defaultAgentOpts()
	for _, opt := range agentOpts {
		opt(opts)
	}

	return &agent{
		opts:          opts,
		endpoints:     make([]Endpoint, 0),
		eventHandlers: opts.eventHandlers,
	}, nil
}

// Connect begins a new Session by connecting and authenticating to the ngrok
// cloud service.
func (a *agent) Connect(ctx context.Context) error {
	debugf := func(format string, args ...interface{}) {
		log.Printf("[DEBUG] agent.Connect: "+format, args...)
	}

	debugf("start (autoConnect=%v, proxyConfigured=%v, hasCustomDialer=%v, hasRPCHandler=%v)", a.opts.autoConnect, a.opts.proxyURL != "", a.opts.dialer != nil, a.opts.rpcHandler != nil)
	if deadline, ok := ctx.Deadline(); ok {
		debugf("context deadline set: %s (remaining=%s)", deadline.Format(time.RFC3339Nano), time.Until(deadline))
	} else {
		debugf("context has no deadline")
	}

	a.mu.Lock()
	defer a.mu.Unlock()
	debugf("agent mutex acquired")

	// If we're already connected, return an error
	if a.sess != nil && a.agentSession != nil {
		debugf("already connected (sessionID=%s) - aborting", a.agentSession.id)
		return errors.New("agent already connected")
	}
	debugf("agent is not connected, continuing setup")

	// Add legacy connect handlers for events
	legacyOpts := append([]legacy.ConnectOption{}, a.opts.sessionOpts...)
	debugf("copied %d session connect options", len(legacyOpts))

	// Process proxy URL if provided
	if a.opts.proxyURL != "" {
		debugf("proxy URL configured: %s", a.opts.proxyURL)
		parsedURL, err := url.Parse(a.opts.proxyURL)
		if err != nil {
			debugf("proxy URL parse failed: %v", err)
			return fmt.Errorf("invalid proxy URL: %w", err)
		}
		debugf("proxy URL parsed successfully (scheme=%s, host=%s)", parsedURL.Scheme, parsedURL.Host)

		// Determine the base dialer to use for connecting to the proxy
		baseDialer := a.opts.dialer
		if baseDialer == nil {
			// If no custom dialer is provided, use a standard net.Dialer
			debugf("no custom dialer set, using default net.Dialer")
			baseDialer = &net.Dialer{}
		} else {
			debugf("using existing custom dialer for proxy initialization")
		}

		// Create a proxy dialer using the base dialer
		proxyDialer, err := proxy.FromURL(parsedURL, baseDialer)
		if err != nil {
			debugf("proxy dialer initialization failed: %v", err)
			return fmt.Errorf("failed to initialize proxy: %w", err)
		}
		debugf("proxy dialer initialized successfully")

		// We know FromURL returns a Dialer-compatible type
		dialer, ok := proxyDialer.(Dialer)
		if !ok {
			debugf("proxy dialer type assertion to ngrok Dialer failed (type=%T)", proxyDialer)
			return fmt.Errorf("proxy dialer is not compatible with ngrok Dialer interface")
		}
		debugf("proxy dialer is compatible with ngrok Dialer")

		// Set the dialer in our options
		a.opts.dialer = dialer
		debugf("stored proxy dialer in agent options")
		// Pass it to the legacy package
		legacyOpts = append(legacyOpts, legacy.WithDialer(dialer))
		debugf("appended legacy.WithDialer option (total options=%d)", len(legacyOpts))
	} else {
		debugf("no proxy URL configured")
	}

	// Create our AgentSession wrapper early so we can capture it in closures
	agentSession := &agentSession{
		agent:     a,
		startedAt: time.Now(),
	}
	debugf("created agentSession wrapper (startedAt=%s)", agentSession.startedAt.Format(time.RFC3339Nano))

	// Hook up connect event
	legacyOpts = append(legacyOpts, legacy.WithConnectHandler(func(_ context.Context, sess legacy.Session) {
		debugf("connect handler invoked (agentSessionID=%s)", sess.AgentSessionID())
		a.emitEvent(newAgentConnectSucceeded(a, agentSession))
	}))
	debugf("registered connect handler (total options=%d)", len(legacyOpts))

	// Hook up disconnect event
	legacyOpts = append(legacyOpts, legacy.WithDisconnectHandler(func(_ context.Context, sess legacy.Session, err error) {
		debugf("disconnect handler invoked (agentSessionID=%s, err=%v)", sess.AgentSessionID(), err)
		a.emitEvent(newAgentDisconnected(a, agentSession, err))
	}))
	debugf("registered disconnect handler (total options=%d)", len(legacyOpts))

	// Hook up heartbeat event
	legacyOpts = append(legacyOpts, legacy.WithHeartbeatHandler(func(_ context.Context, sess legacy.Session, latency time.Duration) {
		debugf("heartbeat handler invoked (agentSessionID=%s, latency=%s)", sess.AgentSessionID(), latency)
		a.emitEvent(newAgentHeartbeatReceived(a, agentSession, latency))
	}))
	debugf("registered heartbeat handler (total options=%d)", len(legacyOpts))

	// If an RPC handler is registered, hook up the command handlers
	if a.opts.rpcHandler != nil {
		debugf("rpc handler configured, registering stop/restart/update command handlers")
		// Register the command handlers that delegate to the RPC handler
		legacyOpts = append(legacyOpts,
			legacy.WithStopHandler(a.createCommandHandler(rpc.StopAgentMethod)),
			legacy.WithRestartHandler(a.createCommandHandler(rpc.RestartAgentMethod)),
			legacy.WithUpdateHandler(a.createCommandHandler(rpc.UpdateAgentMethod)),
		)
		debugf("registered RPC command handlers (total options=%d)", len(legacyOpts))
	} else {
		debugf("no rpc handler configured")
	}

	// Create a new ngrok session
	debugf("calling legacy.Connect with %d options", len(legacyOpts))
	sess, err := legacy.Connect(ctx, legacyOpts...)
	if err != nil {
		debugf("legacy.Connect failed: %v", err)
		return wrapError(err)
	}
	debugf("legacy.Connect succeeded (agentSessionID=%s, warnings=%d)", sess.AgentSessionID(), len(sess.Warnings()))

	// Complete the AgentSession wrapper with session-specific data
	agentSession.id = sess.AgentSessionID()
	agentSession.warnings = sess.Warnings()
	debugf("agentSession populated (id=%s, warnings=%d)", agentSession.id, len(agentSession.warnings))

	// Store in agent
	a.sess = sess
	a.agentSession = agentSession
	debugf("agent state updated with active session")
	debugf("connect complete")

	return nil
}

// Disconnect terminates the current Session which disconnects it from the ngrok
// cloud service.
func (a *agent) Disconnect() error {
	// Get what we need under lock
	a.mu.Lock()
	sess := a.sess
	endpoints := a.endpoints
	a.sess = nil
	a.agentSession = nil
	a.endpoints = make([]Endpoint, 0)
	a.mu.Unlock()

	if sess == nil {
		return nil
	}

	// Signal done for all endpoints (not holding the lock)
	for _, endpoint := range endpoints {
		// Only signal done, don't remove (already cleared the list)
		if e, ok := endpoint.(interface{ signalDone() }); ok {
			e.signalDone()
		}
	}

	// Close session (not holding the lock)
	err := sess.Close()
	return wrapError(err)
}

// Session returns an object describing the connection of the Agent to the ngrok
// cloud service.
func (a *agent) Session() (AgentSession, error) {
	a.mu.RLock()
	defer a.mu.RUnlock()

	if a.sess == nil || a.agentSession == nil {
		return nil, errors.New("agent not connected")
	}

	return a.agentSession, nil
}

// Endpoints returns the list of endpoints created by this Agent.
func (a *agent) Endpoints() []Endpoint {
	a.mu.RLock()
	defer a.mu.RUnlock()

	// Return a copy to avoid race conditions
	return slices.Clone(a.endpoints)
}

// createListener creates an endpointListener for internal use
func (a *agent) createListener(ctx context.Context, endpointOpts *endpointOpts) (*endpointListener, error) {
	// Get the session
	a.mu.RLock()
	sess := a.sess
	tunnelSessionID := sess.AgentSessionID()
	a.mu.RUnlock()

	// Determine URL scheme and configure endpoint
	scheme, err := determineURLScheme(endpointOpts.url)
	if err != nil {
		return nil, err
	}
	tunnelConfig, err := configureEndpoint(scheme, endpointOpts)
	if err != nil {
		return nil, err
	}

	// Create tunnel and parse URL
	tunnel, err := sess.Listen(ctx, tunnelConfig)
	if err != nil {
		return nil, wrapError(err)
	}
	tunnelURL, err := url.Parse(tunnel.URL())
	if err != nil {
		return nil, fmt.Errorf("failed to parse tunnel URL: %w", err)
	}

	// Validate upstream URL format if provided
	if endpointOpts.upstreamURL != "" {
		_, err = url.Parse(endpointOpts.upstreamURL)
		if err != nil {
			return nil, fmt.Errorf("invalid upstream URL: %w", err)
		}
	}

	now := time.Now()

	// Create endpoint listener
	endpoint := &endpointListener{
		baseEndpoint: baseEndpoint{
			agent:           a,
			id:              tunnel.ID(),
			name:            tunnel.Name(),
			poolingEnabled:  endpointOpts.poolingEnabled,
			bindings:        endpointOpts.bindings,
			description:     endpointOpts.description,
			metadata:        endpointOpts.metadata,
			agentTLSConfig:  endpointOpts.agentTLSConfig,
			trafficPolicy:   endpointOpts.trafficPolicy,
			endpointURL:     *tunnelURL,
			doneChannel:     make(chan struct{}),
			doneOnce:        &sync.Once{},
			createdAt:       now,
			updatedAt:       now,
			tunnelSessionID: tunnelSessionID,
			tunnelID:        tunnel.TunnelID(),
		},
		tunnel: tunnel,
	}

	// Add the endpoint to our list
	a.mu.Lock()
	a.endpoints = append(a.endpoints, endpoint)
	a.mu.Unlock()

	return endpoint, nil
}

// Listen creates an EndpointListener.
func (a *agent) Listen(ctx context.Context, opts ...EndpointOption) (EndpointListener, error) {
	// Apply all options
	endpointOpts := defaultEndpointOpts()
	for _, opt := range opts {
		opt(endpointOpts)
	}

	// Ensure we're connected
	if err := a.ensureConnected(ctx); err != nil {
		return nil, err
	}

	// Create the listener using the helper method
	listener, err := a.createListener(ctx, endpointOpts)
	if err != nil {
		return nil, err
	}

	return listener, nil
}

// ensureConnected handles automatic connection and verifies connection state
func (a *agent) ensureConnected(ctx context.Context) error {
	// First check if we're already connected (with a read lock)
	a.mu.RLock()
	sessionExists := a.sess != nil
	a.mu.RUnlock()

	// Only try to connect if needed and auto-connect is enabled
	if !sessionExists && a.opts.autoConnect {
		if err := a.Connect(ctx); err != nil {
			return fmt.Errorf("failed to connect: %w", err)
		}
	}

	// Final verification that we're connected
	a.mu.RLock()
	defer a.mu.RUnlock()

	if a.sess == nil {
		return errors.New("agent not connected, call Connect() first")
	}

	return nil
}

func (a *agent) patchTunnelState(_ context.Context, tunnelID string, name, description, metadata *string, poolingEnabled *bool, trafficPolicy *string) error {
	a.mu.RLock()
	sess := a.sess
	a.mu.RUnlock()
	if sess == nil {
		return fmt.Errorf("agent not connected")
	}
	return sess.PatchTunnelState(tunnelID, name, description, metadata, poolingEnabled, trafficPolicy)
}

// removeEndpoint removes an endpoint from the agent's list
func (a *agent) removeEndpoint(endpoint Endpoint) {
	// Remove the endpoint from our list under lock
	a.mu.Lock()
	for i, e := range a.endpoints {
		if e == endpoint {
			a.endpoints = append(a.endpoints[:i], a.endpoints[i+1:]...)
			break
		}
	}
	a.mu.Unlock()
}

// emitEvent sends an event to all registered handlers
func (a *agent) emitEvent(evt Event) {
	a.eventMutex.RLock()
	handlers := make([]EventHandler, len(a.eventHandlers))
	copy(handlers, a.eventHandlers)
	a.eventMutex.RUnlock()

	for _, handler := range handlers {
		// Call the handler directly
		// Note: The handler is responsible for not blocking
		handler(evt)
	}
}

// createCommandHandler returns a legacy.ServerCommandHandler that delegates to the RPCHandler
// for the specified RPC method.
func (a *agent) createCommandHandler(method string) legacy.ServerCommandHandler {
	return func(ctx context.Context, sess legacy.Session) error {
		if a.opts.rpcHandler == nil {
			return nil
		}

		// Get the current agent session
		agentSession, err := a.Session()
		if err != nil {
			return err
		}

		// Create request object with the specified method
		req := &rpcRequest{
			method:  method,
			payload: nil, // No payload for now
		}

		// Call the RPC handler
		_, err = a.opts.rpcHandler(ctx, agentSession, req)
		// Ignore response payload for now
		return err
	}
}

// Forward creates an EndpointForwarder that forwards traffic to the specified upstream.
// The upstream parameter is required and must be created using WithUpstream().
// Additional endpoint options can be provided to configure the endpoint.
func (a *agent) Forward(ctx context.Context, upstream *Upstream, opts ...EndpointOption) (EndpointForwarder, error) {
	// Apply all base options first
	endpointOpts := defaultEndpointOpts()

	// Set upstream values directly from the Upstream object
	endpointOpts.upstreamURL = upstream.addr
	endpointOpts.upstreamProtocol = upstream.protocol
	endpointOpts.upstreamTLSClientConfig = upstream.tlsClientConfig

	// Convert the proxy protocol to config.ProxyProtoVersion
	if upstream.proxyProto != "" {
		var proxyVersion config.ProxyProtoVersion
		switch upstream.proxyProto {
		case ProxyProtoV1:
			proxyVersion = config.ProxyProtoV1
		case ProxyProtoV2:
			proxyVersion = config.ProxyProtoV2
		default:
			return nil, fmt.Errorf("unsupported proxy protocol: %s", upstream.proxyProto)
		}
		endpointOpts.proxyProtoVersion = proxyVersion
	}

	// Apply additional options
	for _, opt := range opts {
		opt(endpointOpts)
	}

	// Ensure we're connected
	if err := a.ensureConnected(ctx); err != nil {
		return nil, err
	}

	// Create the listener using the helper method
	listener, err := a.createListener(ctx, endpointOpts)
	if err != nil {
		return nil, err
	}

	// Parse upstream URL - we know it exists and is valid from createListener
	upstreamURL, _ := url.Parse(endpointOpts.upstreamURL)

	// Create the forwarder
	endpoint := &endpointForwarder{
		baseEndpoint:            listener.baseEndpoint, // reuse the baseEndpoint from listener
		listener:                listener,
		upstreamProtocol:        endpointOpts.upstreamProtocol,
		upstreamTLSClientConfig: endpointOpts.upstreamTLSClientConfig,
		proxyProtocol:           upstream.proxyProto,
		upstreamDialer:          upstream.dialer,
	}
	endpoint.upstreamURL.Store(upstreamURL)

	// Start the forwarding process
	endpoint.start(ctx)

	// Add the endpoint to our list
	a.mu.Lock()
	a.endpoints = append(a.endpoints, endpoint)
	a.mu.Unlock()

	return endpoint, nil
}
