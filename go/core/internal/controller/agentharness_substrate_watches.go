package controller

import (
	"context"

	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	"github.com/kagent-dev/kagent/go/core/pkg/sandboxbackend/substrate"
)

func (r *SubstrateAgentHarnessController) enqueueAgentHarnessForSubstrateResource(ctx context.Context, obj client.Object) []reconcile.Request {
	harnessName := substrate.HarnessNameFromLabels(obj.GetLabels())
	if harnessName == "" {
		return nil
	}
	return []reconcile.Request{{
		NamespacedName: types.NamespacedName{
			Namespace: obj.GetNamespace(),
			Name:      harnessName,
		},
	}}
}

// substrateWatches used to register a Watch() for atev1alpha1.ActorTemplate as a Kubernetes CRD.
// github.com/agent-substrate/substrate (unlike the kagent-dev/substrate fork this was built
// against) has no such CRD, and that Watch() failing its cache sync crashed the whole manager --
// see the identical fix and rationale in sandboxagent_controller.go's SetupWithManager.
func (r *SubstrateAgentHarnessController) substrateWatches(b *builder.Builder) *builder.Builder {
	return b
}
