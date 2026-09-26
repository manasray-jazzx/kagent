package handlers

import (
	"net/http"

	"github.com/kagent-dev/kagent/go/core/internal/httpserver/errors"
	"github.com/kagent-dev/kagent/go/core/pkg/auth"
	"github.com/kagent-dev/kagent/go/core/pkg/sandboxbackend/substrate"
)

// SubstrateHandler exposes Agent Substrate inventory for the UI.
//
// HandleGetSubstrateStatus is stubbed out on this branch: it read several
// Actor/WorkerPool/ActorTemplate fields (GetActorTemplateNamespace/Name,
// GetAteomPodIp/Name/Namespace, WorkerPoolSpec.AteomImage,
// ateapipb.SnapshotInfo, atev1alpha1.ActorTemplateList) that github.com/agent-substrate/substrate
// either never had or has since renamed/restructured relative to the
// github.com/kagent-dev/substrate fork kagent's go.mod used to point at -- see
// docs/dev/eks-aks-workaround.md in agent-substrate/substrate for the broader compatibility gap
// this branch's kagent patch found and fixed for the SandboxAgent path specifically. This is a
// debug/status UI endpoint, not on the SandboxAgent actor-creation path this patch is about, so
// it was stubbed rather than chased down field-by-field.
type SubstrateHandler struct {
	*Base
	AteClient *substrate.Client
}

// NewSubstrateHandler creates a SubstrateHandler.
func NewSubstrateHandler(base *Base, ateClient *substrate.Client) *SubstrateHandler {
	return &SubstrateHandler{Base: base, AteClient: ateClient}
}

// HandleGetSubstrateStatus handles GET /api/substrate/status?namespace=… -- see the type doc
// comment for why this always answers 501 on this branch.
func (h *SubstrateHandler) HandleGetSubstrateStatus(w ErrorResponseWriter, r *http.Request) {
	if err := Check(h.Authorizer, r, auth.Resource{Type: "Agent"}); err != nil {
		w.RespondWithError(err)
		return
	}
	w.RespondWithError(errors.NewInternalServerError(
		"substrate status inventory is unavailable on this branch (see SubstrateHandler's doc comment)",
		nil,
	))
}
