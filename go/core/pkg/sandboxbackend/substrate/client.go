package substrate

import (
	"context"
	"crypto/tls"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/connectivity"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"
)

// Client wraps ate-api Control gRPC.
type Client struct {
	ateapipb.ControlClient
	conn *grpc.ClientConn
	cfg  Config
}

// Dial connects to the ate-api server.
func Dial(ctx context.Context, cfg Config) (*Client, error) {
	if cfg.AteAPIEndpoint == "" {
		return nil, fmt.Errorf("substrate: ate-api endpoint is required")
	}
	dialTimeout := cfg.DialTimeout
	if dialTimeout <= 0 {
		dialTimeout = 10 * time.Second
	}
	dialCtx, cancel := context.WithTimeout(ctx, dialTimeout)
	defer cancel()

	opts := []grpc.DialOption{
		grpc.WithTransportCredentials(credentials.NewTLS(ateAPITLSConfig(cfg.Insecure))),
	}
	if cfg.TokenFile != "" {
		opts = append(opts, grpc.WithPerRPCCredentials(bearerTokenFile{
			path:       cfg.TokenFile,
			requireTLS: !cfg.Insecure,
		}))
	}

	conn, err := grpc.NewClient(cfg.AteAPIEndpoint, opts...)
	if err != nil {
		return nil, fmt.Errorf("substrate: dial ate-api %q: %w", cfg.AteAPIEndpoint, err)
	}
	// NewClient stays idle until Connect() or an RPC; waitConnReady enforces DialTimeout.
	conn.Connect()
	if err := waitConnReady(dialCtx, conn); err != nil {
		_ = conn.Close()
		return nil, fmt.Errorf("substrate: dial ate-api %q: %w", cfg.AteAPIEndpoint, err)
	}

	return &Client{
		ControlClient: ateapipb.NewControlClient(conn),
		conn:          conn,
		cfg:           cfg,
	}, nil
}

type bearerTokenFile struct {
	path       string
	requireTLS bool
}

func (b bearerTokenFile) GetRequestMetadata(_ context.Context, _ ...string) (map[string]string, error) {
	raw, err := os.ReadFile(b.path)
	if err != nil {
		return nil, fmt.Errorf("read bearer token file %q: %w", b.path, err)
	}
	token := strings.TrimSpace(string(raw))
	if token == "" {
		return nil, fmt.Errorf("bearer token file %q is empty", b.path)
	}
	return map[string]string{"authorization": "Bearer " + token}, nil
}

func (b bearerTokenFile) RequireTransportSecurity() bool { return b.requireTLS }

func ateAPITLSConfig(insecure bool) *tls.Config {
	tlsCfg := &tls.Config{MinVersion: tls.VersionTLS12}
	if insecure {
		// Kind/local ate-api uses pod-issued certs; skip verification (same as grpcurl -insecure).
		tlsCfg.InsecureSkipVerify = true
	}
	return tlsCfg
}

func waitConnReady(ctx context.Context, conn *grpc.ClientConn) error {
	for {
		switch s := conn.GetState(); s {
		case connectivity.Ready:
			return nil
		case connectivity.Shutdown:
			return fmt.Errorf("connection shut down")
		default:
			if !conn.WaitForStateChange(ctx, s) {
				if err := ctx.Err(); err != nil {
					return err
				}
				return fmt.Errorf("connection closed before ready")
			}
		}
	}
}

func (c *Client) Close() error {
	if c.conn != nil {
		return c.conn.Close()
	}
	return nil
}

func (c *Client) callCtx(ctx context.Context) (context.Context, context.CancelFunc) {
	if c.cfg.CallTimeout <= 0 {
		return ctx, func() {}
	}
	return context.WithTimeout(ctx, c.cfg.CallTimeout)
}

// actorRef builds the (atespace, name) reference used by Get/Resume/Suspend/Delete.
// v0.0.9 renamed the ActorRef message to ObjectRef.
func actorRef(atespace, actorID string) *ateapipb.ObjectRef {
	return &ateapipb.ObjectRef{Atespace: atespace, Name: actorID}
}

// actorName returns the actor's identity within its atespace. v0.0.9 moved this
// out of the flat Actor.ActorId field into Metadata.
func actorName(a *ateapipb.Actor) string {
	return a.GetMetadata().GetName()
}

func (c *Client) GetActor(ctx context.Context, atespace, actorID string) (*ateapipb.Actor, error) {
	ctx, cancel := c.callCtx(ctx)
	defer cancel()
	resp, err := c.ControlClient.GetActor(ctx, &ateapipb.GetActorRequest{Actor: actorRef(atespace, actorID)})
	if err != nil {
		return nil, err
	}
	return resp, nil
}

func (c *Client) CreateActor(ctx context.Context, atespace, actorID, tmplNS, tmplName string) (*ateapipb.Actor, error) {
	ctx, cancel := c.callCtx(ctx)
	defer cancel()
	resp, err := c.ControlClient.CreateActor(ctx, &ateapipb.CreateActorRequest{
		Actor: &ateapipb.Actor{
			Metadata:      &ateapipb.ResourceMetadata{Atespace: atespace, Name: actorID},
			ActorTemplate: &ateapipb.ObjectRef{Atespace: tmplNS, Name: tmplName},
		},
	})
	if err != nil {
		return nil, err
	}
	// A newly created actor has no EgressPolicy, and atenet-egress denies every
	// outbound connection from an actor with none (confirmed via its own logs:
	// "egress denied: actor has no egress policy"). golang-adk callbacks to
	// kagent-controller (KAGENT_URL/KAGENT_API_URL, e.g. POST /api/tasks for
	// task-state persistence) and any LLM-API calls the agent makes both need
	// this. Allow-all mirrors internal/e2e/egresspolicy.go's EgressAllowAll,
	// used by this repo's own tests that are not specifically about egress
	// policy; a real deployment would scope this to the model backend's
	// hostname plus kagent-controller instead.
	if err := c.ensureActorEgressPolicy(ctx, atespace, actorID); err != nil {
		return nil, fmt.Errorf("ensure egress policy for actor %s/%s: %w", atespace, actorID, err)
	}
	return resp, nil
}

func (c *Client) ensureActorEgressPolicy(ctx context.Context, atespace, actorID string) error {
	_, err := c.ControlClient.CreateActorEgressPolicy(ctx, &ateapipb.CreateActorEgressPolicyRequest{
		Actor: actorRef(atespace, actorID),
		EgressPolicy: &ateapipb.EgressPolicy{
			Metadata: &ateapipb.ResourceMetadata{Atespace: atespace, Name: "default"},
			Rules:    []*ateapipb.EgressRule{{All: &emptypb.Empty{}}},
		},
	})
	if status.Code(err) == codes.AlreadyExists {
		return nil
	}
	return err
}

func (c *Client) ResumeActor(ctx context.Context, atespace, actorID string) (*ateapipb.Actor, error) {
	ctx, cancel := c.callCtx(ctx)
	defer cancel()
	resp, err := c.ControlClient.ResumeActor(ctx, &ateapipb.ResumeActorRequest{Actor: actorRef(atespace, actorID)})
	if err != nil {
		return nil, err
	}
	return resp.GetActor(), nil
}

func (c *Client) SuspendActor(ctx context.Context, atespace, actorID string) error {
	ctx, cancel := c.callCtx(ctx)
	defer cancel()
	_, err := c.ControlClient.SuspendActor(ctx, &ateapipb.SuspendActorRequest{Actor: actorRef(atespace, actorID)})
	return err
}

func (c *Client) DeleteActor(ctx context.Context, atespace, actorID string) error {
	ctx, cancel := c.callCtx(ctx)
	defer cancel()
	_, err := c.ControlClient.DeleteActor(ctx, &ateapipb.DeleteActorRequest{Actor: actorRef(atespace, actorID)})
	return err
}

// EnsureAtespace idempotently ensures the named atespace exists on the substrate side.
// Actors cannot be created into a nonexistent atespace (FailedPrecondition).
func (c *Client) EnsureAtespace(ctx context.Context, name string) error {
	ctx, cancel := c.callCtx(ctx)
	defer cancel()
	_, err := c.CreateAtespace(ctx, &ateapipb.CreateAtespaceRequest{
		Atespace: &ateapipb.Atespace{Metadata: &ateapipb.ResourceMetadata{Name: name}},
	})
	if err != nil && status.Code(err) == codes.AlreadyExists {
		return nil
	}
	return err
}

// GetActorTemplateByName fetches an ActorTemplate by atespace/name, or (nil, nil) if it does
// not exist -- callers that need to distinguish NotFound from other errors should call
// ControlClient.GetActorTemplate directly instead.
func (c *Client) GetActorTemplateByName(ctx context.Context, atespace, name string) (*ateapipb.ActorTemplate, error) {
	ctx, cancel := c.callCtx(ctx)
	defer cancel()
	tmpl, err := c.ControlClient.GetActorTemplate(ctx, &ateapipb.GetActorTemplateRequest{
		ActorTemplate: actorRef(atespace, name),
	})
	if status.Code(err) == codes.NotFound {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("get ActorTemplate %s/%s: %w", atespace, name, err)
	}
	return tmpl, nil
}

// CreateActorTemplate creates an ActorTemplate, treating AlreadyExists as success: ActorTemplates
// are immutable on ate-api, so an existing template under this name is already what a caller
// asking to (re)create it wants.
func (c *Client) CreateActorTemplate(ctx context.Context, tmpl *ateapipb.ActorTemplate) error {
	ctx, cancel := c.callCtx(ctx)
	defer cancel()
	_, err := c.ControlClient.CreateActorTemplate(ctx, &ateapipb.CreateActorTemplateRequest{ActorTemplate: tmpl})
	if err != nil && status.Code(err) == codes.AlreadyExists {
		return nil
	}
	return err
}
