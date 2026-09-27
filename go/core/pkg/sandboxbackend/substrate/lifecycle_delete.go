package substrate

import (
	"context"
	"fmt"
	"strings"

	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	"github.com/kagent-dev/kagent/go/api/v1alpha2"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// cleanupActorTemplatesByPrefix deletes every ActorTemplate in atespace named exactly base or
// named base + "-" + <shape hash> (buildSandboxAgentActorTemplate's fan-out convention for a
// SandboxAgent that has changed shape across its lifetime; an AgentHarness only ever uses the bare
// base name, since buildActorTemplate reconciles in place against one fixed name). ate-api's
// CreateActorTemplate creates and suspends the golden actor server-side, but never deletes it --
// confirmed live on an eks-aks/kind verification cluster: a SandboxAgent recreated three times
// left three orphaned ActorTemplates (and their golden actors) behind indefinitely, each retried
// by ate-api's own background golden-snapshot reconciler forever, because this function was
// previously a no-op that never called the real DeleteActorTemplate RPC (which does the actual
// golden-actor + snapshot cleanup server-side).
//
// A template whose golden actor is mid-reconcile (resuming/checkpointing) holds a store lease
// DeleteActorTemplate cannot preempt, and returns Aborted -- that is not a failure, just "try
// again on the next reconcile", exactly like the not-yet-drained-actors check this is paired
// with in the caller.
func (p *Lifecycle) cleanupActorTemplatesByPrefix(ctx context.Context, atespace, base string) (bool, error) {
	if p == nil || p.AteClient == nil {
		return true, nil
	}
	done := true
	pageToken := ""
	for {
		resp, err := p.AteClient.ListActorTemplates(ctx, &ateapipb.ListActorTemplatesRequest{
			Atespace:  atespace,
			PageSize:  100,
			PageToken: pageToken,
		})
		if err != nil {
			return false, fmt.Errorf("list actor templates in %q: %w", atespace, err)
		}
		for _, tmpl := range resp.GetActorTemplates() {
			name := tmpl.GetMetadata().GetName()
			if name != base && !strings.HasPrefix(name, base+"-") {
				continue
			}
			_, err := p.AteClient.DeleteActorTemplate(ctx, &ateapipb.DeleteActorTemplateRequest{
				ActorTemplate: &ateapipb.ObjectRef{Atespace: atespace, Name: name},
			})
			switch status.Code(err) {
			case codes.OK, codes.NotFound:
			case codes.Aborted:
				done = false
			default:
				return false, fmt.Errorf("delete actor template %s/%s: %w", atespace, name, err)
			}
		}
		pageToken = resp.GetNextPageToken()
		if pageToken == "" {
			break
		}
	}
	return done, nil
}

// CleanupGeneratedTemplate deletes the AgentHarness's generated ActorTemplate (and, server-side,
// its golden actor and snapshot) via the real DeleteActorTemplate RPC. See
// cleanupActorTemplatesByPrefix's doc comment for why this can no longer be a no-op.
func (p *Lifecycle) CleanupGeneratedTemplate(ctx context.Context, ah *v1alpha2.AgentHarness) (bool, error) {
	if ah == nil {
		return true, nil
	}
	return p.cleanupActorTemplatesByPrefix(ctx, ah.Namespace, actorTemplateName(ah))
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

// CleanupSandboxAgentTemplate deletes every ActorTemplate this SandboxAgent generated across its
// whole lifetime (base name plus every shape-hashed fan-out) via the real DeleteActorTemplate RPC.
// See cleanupActorTemplatesByPrefix's doc comment for why this can no longer be a no-op.
func (p *Lifecycle) CleanupSandboxAgentTemplate(ctx context.Context, sa *v1alpha2.SandboxAgent) (bool, error) {
	if sa == nil {
		return true, nil
	}
	return p.cleanupActorTemplatesByPrefix(ctx, sa.Namespace, sandboxAgentActorTemplateBaseName(sa))
}
