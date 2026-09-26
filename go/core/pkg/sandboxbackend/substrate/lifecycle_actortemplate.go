package substrate

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"strings"

	"github.com/kagent-dev/kagent/go/api/v1alpha2"
	apiequality "k8s.io/apimachinery/pkg/api/equality"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
)

// ErrActorTemplateReconcilePending indicates ActorTemplate reconciliation started
// a multi-step recreate (e.g. golden-actor deletion) and callers should requeue.
var ErrActorTemplateReconcilePending = errors.New("actor template reconciliation pending")

// substrateActorTemplateAnnotation records, on the owning SandboxAgent/AgentHarness object, the
// name of the ate-api ActorTemplate currently serving it -- see reconcileActorTemplate and
// ResolveCurrentActorTemplate.
const substrateActorTemplateAnnotation = "kagent.dev/substrate-actor-template"

func (p *Lifecycle) ensureActorTemplate(ctx context.Context, ah *v1alpha2.AgentHarness, wpKey types.NamespacedName) (types.NamespacedName, error) {
	key := types.NamespacedName{Namespace: ah.Namespace, Name: actorTemplateName(ah)}
	desired, err := p.buildActorTemplate(ctx, ah, wpKey)
	if err != nil {
		return types.NamespacedName{}, err
	}
	if err := reconcileActorTemplate(ctx, p.Client, p.AteClient, desired); err != nil {
		return types.NamespacedName{}, fmt.Errorf("reconcile ActorTemplate %s: %w", key, err)
	}
	return key, nil
}

// actorTemplateSpecEqual reports whether two ActorTemplate specs are semantically equal.
func actorTemplateSpecEqual(a, b ActorTemplateSpec) bool {
	return apiequality.Semantic.DeepEqual(a, b)
}

// reconcileActorTemplate ensures desired exists as an ate-api ActorTemplate, created via
// ateapipb.ControlClient.CreateActorTemplate rather than as a Kubernetes object:
// github.com/agent-substrate/substrate (this branch) has no CRD for ActorTemplate at all --
// see convertActorTemplate's doc comment for why, and for what this patch trades away relative
// to the kagent-dev/substrate fork's CRD-based design it replaces.
//
// ActorTemplates are immutable on ate-api (as they were as a CRD), so unlike the design this
// replaces, a spec drift is NOT detected or reconciled here -- desired.Name already encodes the
// shape hash (see sandboxAgentActorTemplateName), so a real shape change produces a new template
// under a new name on the next call instead of mutating this one. An existing template under
// this exact name is therefore always left untouched.
//
// The current template's name is recorded as an annotation on the owning SandboxAgent/
// AgentHarness object (found via desired's own lifecycle labels) so ResolveCurrentActorTemplate
// can find it later without a CRD to list.
func reconcileActorTemplate(ctx context.Context, c client.Client, ate *Client, desired *ActorTemplate) error {
	atespace, name := desired.Namespace, desired.Name

	existing, err := ate.GetActorTemplateByName(ctx, atespace, name)
	if err != nil {
		return fmt.Errorf("get ActorTemplate %s/%s: %w", atespace, name, err)
	}
	if existing == nil {
		proto, err := convertActorTemplate(ctx, c, desired)
		if err != nil {
			return fmt.Errorf("convert ActorTemplate %s/%s: %w", atespace, name, err)
		}
		if err := ate.EnsureAtespace(ctx, atespace); err != nil {
			return fmt.Errorf("ensure atespace %q: %w", atespace, err)
		}
		if err := ate.CreateActorTemplate(ctx, proto); err != nil {
			return fmt.Errorf("create ActorTemplate %s/%s: %w", atespace, name, err)
		}
	}

	return recordCurrentActorTemplateName(ctx, c, desired)
}

// recordCurrentActorTemplateName annotates the owning SandboxAgent or AgentHarness (identified by
// desired's own lifecycle labels -- see sandboxAgentLifecycleLabels/lifecycleLabels) with desired's
// name, so ResolveCurrentActorTemplate can resolve "the template this agent currently uses"
// without a CRD to list or select from. A missing owner (deleted mid-reconcile) is not an error.
func recordCurrentActorTemplateName(ctx context.Context, c client.Client, desired *ActorTemplate) error {
	if saName := desired.Labels[SandboxAgentLabelKey]; saName != "" {
		var sa v1alpha2.SandboxAgent
		key := types.NamespacedName{Namespace: desired.Namespace, Name: saName}
		if err := c.Get(ctx, key, &sa); err != nil {
			if apierrors.IsNotFound(err) {
				return nil
			}
			return fmt.Errorf("get SandboxAgent %s: %w", key, err)
		}
		return patchActorTemplateAnnotation(ctx, c, &sa, desired.Name)
	}
	if ahName := desired.Labels["kagent.dev/agent-harness"]; ahName != "" {
		var ah v1alpha2.AgentHarness
		key := types.NamespacedName{Namespace: desired.Namespace, Name: ahName}
		if err := c.Get(ctx, key, &ah); err != nil {
			if apierrors.IsNotFound(err) {
				return nil
			}
			return fmt.Errorf("get AgentHarness %s: %w", key, err)
		}
		return patchActorTemplateAnnotation(ctx, c, &ah, desired.Name)
	}
	return nil
}

func patchActorTemplateAnnotation(ctx context.Context, c client.Client, obj client.Object, name string) error {
	if obj.GetAnnotations()[substrateActorTemplateAnnotation] == name {
		return nil
	}
	patch := client.MergeFrom(obj.DeepCopyObject().(client.Object))
	ann := obj.GetAnnotations()
	if ann == nil {
		ann = map[string]string{}
	}
	ann[substrateActorTemplateAnnotation] = name
	obj.SetAnnotations(ann)
	if err := c.Patch(ctx, obj, patch); err != nil {
		return fmt.Errorf("record current ActorTemplate on %s/%s: %w", obj.GetNamespace(), obj.GetName(), err)
	}
	return nil
}

func (p *Lifecycle) buildActorTemplate(ctx context.Context, ah *v1alpha2.AgentHarness, wpKey types.NamespacedName) (*ActorTemplate, error) {
	key := types.NamespacedName{Namespace: ah.Namespace, Name: actorTemplateName(ah)}

	var (
		startupScript  string
		containerEnv   []EnvVar
		defaultImageFn func(acpSandboxImageConfig) (string, error)
		containerName  string
		err            error
	)
	// clawBackend selects the OpenClaw startup path; the cluster-wide
	// DefaultWorkloadImage only applies to claw backends (it points at the
	// openclaw sandbox image), other backends fall back to their own image.
	clawBackend := false
	switch ah.Spec.Backend {
	case v1alpha2.AgentHarnessBackendOpenClaw:
		clawBackend = true
		defaultImageFn = acpSandboxOpenClawImage
		containerName = defaultOpenClawContainer
		startupScript, containerEnv, err = p.buildOpenClawActorStartup(ctx, ah)
		if err != nil {
			return nil, fmt.Errorf("build openclaw actor startup: %w", err)
		}
	default:
		spec, ok := acpAgentSpecs[ah.Spec.Backend]
		if !ok {
			return nil, fmt.Errorf("substrate runtime does not support backend %q", ah.Spec.Backend)
		}
		defaultImageFn = spec.DefaultImage
		containerName = string(ah.Spec.Backend)
		startupScript, containerEnv, err = p.buildAcpAgentActorStartup(ctx, ah, spec)
		if err != nil {
			return nil, fmt.Errorf("build %s actor startup: %w", ah.Spec.Backend, err)
		}
	}

	workloadImage := strings.TrimSpace(ah.Spec.Substrate.WorkloadImage)
	if workloadImage == "" && clawBackend {
		workloadImage = strings.TrimSpace(p.Defaults.DefaultWorkloadImage)
	}
	if workloadImage == "" {
		// Fall back to the backend's built-in default, which is always
		// digest-pinned (or errors if the link-time digest is missing).
		workloadImage, err = defaultImageFn(p.acpSandboxImageConfig())
		if err != nil {
			return nil, err
		}
	} else {
		workloadImage, err = pinImageRef(workloadImage)
		if err != nil {
			return nil, err
		}
	}

	desired := &ActorTemplate{
		ObjectMeta: metav1.ObjectMeta{
			Name:      key.Name,
			Namespace: key.Namespace,
			Labels:    lifecycleLabels(ah),
		},
		Spec: ActorTemplateSpec{
			PauseImage:   p.Defaults.PauseImage,
			SandboxClass: SandboxClassGvisor,
			Containers: []Container{
				{
					Name:  containerName,
					Image: workloadImage,
					Command: []string{
						"/bin/sh",
						"-c",
						startupScript,
					},
					Env: containerEnv,
				},
			},
			WorkerSelector: p.workerSelectorForPool(ctx, wpKey),
			SnapshotsConfig: SnapshotsConfig{
				Location: substrateSnapshotsLocation(ah),
				// Mirror substrate's CRD defaults so kagent's spec-drift check
				// (apiequality.Semantic.DeepEqual) treats them as equal to the
				// values the API server fills in on admission — otherwise kagent
				// re-creates the ActorTemplate every reconcile in a hot loop.
				OnPause:  SnapshotScopeFull,
				OnCommit: SnapshotScopeFull,
			},
		},
	}
	if err := controllerutil.SetControllerReference(ah, desired, p.Client.Scheme()); err != nil {
		return nil, fmt.Errorf("set ActorTemplate owner ref: %w", err)
	}
	return desired, nil
}

func mergeLabels(existing, desired map[string]string) map[string]string {
	if len(existing) == 0 && len(desired) == 0 {
		return nil
	}
	merged := make(map[string]string, len(existing)+len(desired))
	maps.Copy(merged, existing)
	maps.Copy(merged, desired)
	return merged
}

// ActorTemplateReady reports whether the ActorTemplate golden snapshot is ready.
func (p *Lifecycle) ActorTemplateReady(ctx context.Context, key types.NamespacedName) (bool, error) {
	return p.actorTemplateReady(ctx, key)
}

func (p *Lifecycle) actorTemplateReady(ctx context.Context, key types.NamespacedName) (bool, error) {
	tmpl, err := p.AteClient.GetActorTemplateByName(ctx, key.Namespace, key.Name)
	if err != nil {
		return false, fmt.Errorf("get ActorTemplate %s: %w", key, err)
	}
	return tmpl.GetStatus().GetGoldenSnapshotStatus().GetGoldenTag() != nil, nil
}
