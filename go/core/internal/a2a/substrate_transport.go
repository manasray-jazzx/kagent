package a2a

import (
	"net/http"
	"net/url"

	"github.com/kagent-dev/kagent/go/core/pkg/sandboxbackend/substrate"
)

// atenetTargetActorHeader mirrors github.com/agent-substrate/substrate's
// internal/atenet.TargetActorHeader ("ate-target-actor"), an internal package this module cannot
// import. github.com/kagent-dev/substrate's own atenet-router apparently derived the target actor
// from the Host header (the DNS-style name GatewayRouterTarget/ActorHost build); the real
// atenet-router's ingress ext_proc handler (cmd/atenet/internal/router/ingress/ingress.go) only
// ever reads this literal header (via atenet.ParseTargetActor), never the Host, so every request
// built with Host alone was rejected with "invalid actor reference". Value format is
// "<atespace>/<actorName>", enforced by atenet.ParseTargetActor.
const atenetTargetActorHeader = "ate-target-actor"

// substrateAgentRoundTripper proxies A2A HTTP to an agent actor via atenet-router.
type substrateAgentRoundTripper struct {
	router    *url.URL
	actorHost string
	atespace  string
	actorID   string
	base      http.RoundTripper
}

func newSubstrateAgentRoundTripper(routerURL, atespace, actorID string, base http.RoundTripper) (http.RoundTripper, error) {
	target, host, err := substrate.GatewayRouterTarget(routerURL, atespace, actorID)
	if err != nil {
		return nil, err
	}
	if base == nil {
		base = http.DefaultTransport
	}
	return &substrateAgentRoundTripper{router: target, actorHost: host, atespace: atespace, actorID: actorID, base: base}, nil
}

func (t *substrateAgentRoundTripper) RoundTrip(req *http.Request) (*http.Response, error) {
	if t == nil || t.router == nil {
		return nil, http.ErrSkipAltProtocol
	}
	req = req.Clone(req.Context())
	req.URL.Scheme = t.router.Scheme
	req.URL.Host = t.router.Host
	if t.actorHost != "" {
		req.Host = t.actorHost
	}
	req.Header.Set(atenetTargetActorHeader, t.atespace+"/"+t.actorID)
	return t.base.RoundTrip(req)
}
