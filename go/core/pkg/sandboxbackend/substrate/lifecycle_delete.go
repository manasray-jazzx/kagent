package substrate

import (
	"context"
	"strings"

	"github.com/kagent-dev/kagent/go/api/v1alpha2"
)

// CleanupGeneratedTemplate is a no-op on this branch: github.com/agent-substrate/substrate's real
// ateapi.CreateActorTemplate RPC (see reconcileActorTemplate) creates and suspends the golden
// actor server-side and deletes it itself once its snapshot is taken -- confirmed live (see
// docs/dev/eks-aks-workaround.md in agent-substrate/substrate: "operation: delete" /
// "state: deleted" logged automatically right after the golden actor's snapshot completes).
// There is no leftover golden actor for this function to find and delete, unlike the
// kagent-dev/substrate fork's CRD-driven design, whose client-side ActorTemplate controller had
// to do that step itself.
func (p *Lifecycle) CleanupGeneratedTemplate(_ context.Context, _ *v1alpha2.AgentHarness) (bool, error) {
	return true, nil
}

// GoldenActorAtespace is the reserved substrate atespace that per-template
// golden actors live in. Mirrors substrate's internal/resources.GoldenActorAtespace,
// duplicated here because that package is internal to the substrate module.
const GoldenActorAtespace = "ate-golden"

func deleteGoldenActor(ctx context.Context, ateClient *Client, actorID string) (bool, error) {
	return deleteActor(ctx, ateClient, GoldenActorAtespace, actorID)
}

// HarnessLabelKey labels substrate lifecycle managed for an AgentHarness.
const HarnessLabelKey = "kagent.dev/agent-harness"

// HarnessNameFromLabels returns the AgentHarness name from generated lifecycle labels.
func HarnessNameFromLabels(labels map[string]string) string {
	if labels == nil {
		return ""
	}
	return strings.TrimSpace(labels[HarnessLabelKey])
}

// CleanupSandboxAgentTemplate is a no-op on this branch -- see CleanupGeneratedTemplate.
func (p *Lifecycle) CleanupSandboxAgentTemplate(_ context.Context, _ *v1alpha2.SandboxAgent) (bool, error) {
	return true, nil
}
