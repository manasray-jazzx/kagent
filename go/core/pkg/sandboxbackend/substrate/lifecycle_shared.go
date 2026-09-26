package substrate

import (
	"context"
	"fmt"
	"strconv"
	"strings"

	atev1alpha1 "github.com/agent-substrate/substrate/pkg/api/v1alpha1"
	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	"github.com/kagent-dev/kagent/go/api/v1alpha2"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

const (
	defaultSnapshotsBucket   = "ate-snapshots"
	defaultOpenClawContainer = "openclaw"

	// Referenced by generated ActorTemplates to gate scheduling onto a WorkerPool.
	// github.com/kagent-dev/substrate's WorkerPool controller stamped
	// "kagent.dev/worker-pool" on the pods it managed; github.com/agent-substrate/substrate's
	// real atecontroller (cmd/atecontroller/internal/controllers/workerpool_apply.go) stamps
	// "ate.dev/worker-pool" instead, so an ActorTemplate's WorkerSelector using the old key
	// matches no worker at all -- ate-api-server's AssignWorker then reports "no free workers
	// available" even when the pool has idle capacity.
	WorkerPoolLabelKey = "ate.dev/worker-pool"
)

// LifecycleDefaults are cluster-wide defaults for generated ActorTemplate lifecycle.
type LifecycleDefaults struct {
	PauseImage           string
	DefaultWorkloadImage string
	DefaultWorkerPool    types.NamespacedName
	// ImageRegistry and ImageRepository are the runtime registry/repository used
	// to compose digest-pinned acp-sandbox workload image refs (from
	// --image-registry/--image-repository). ImageRepository is the agent app
	// repository (e.g. "kagent-dev/kagent/app"); see acpSandboxImageConfig.
	ImageRegistry   string
	ImageRepository string
}

// Lifecycle reconciles the Kubernetes lifecycle that kagent owns for a substrate AgentHarness.
// WorkerPools are externally owned; this helper only resolves the selected WorkerPool.
type Lifecycle struct {
	Client    client.Client
	Defaults  LifecycleDefaults
	AteClient *Client
}

// AgentHarnessLifecycle is the substrate lifecycle surface used by the
// AgentHarness controller.
type AgentHarnessLifecycle interface {
	EnsureGeneratedTemplate(ctx context.Context, ah *v1alpha2.AgentHarness) (LifecycleState, error)
	CleanupGeneratedTemplate(ctx context.Context, ah *v1alpha2.AgentHarness) (bool, error)
}

var _ AgentHarnessLifecycle = (*Lifecycle)(nil)

func NewLifecycle(kube client.Client, defaults LifecycleDefaults, ateClient *Client) *Lifecycle {
	return &Lifecycle{
		Client:    kube,
		Defaults:  defaults,
		AteClient: ateClient,
	}
}

// acpSandboxImageConfig returns the runtime registry/repository used to compose
// digest-pinned acp-sandbox workload image refs.
func (p *Lifecycle) acpSandboxImageConfig() acpSandboxImageConfig {
	return acpSandboxImageConfig{
		Registry:   p.Defaults.ImageRegistry,
		Repository: p.Defaults.ImageRepository,
	}
}

// LifecycleState describes the generated Substrate lifecycle for an AgentHarness.
type LifecycleState struct {
	ActorTemplateReady bool
}

// workerSelectorForPool builds the ActorTemplate.WorkerSelector that will actually match workers
// in wpKey's pool.
//
// github.com/agent-substrate/substrate's real ate-api scheduler (cmd/ateapi/internal/scheduling)
// matches an ActorTemplate's WorkerSelector against ateapipb.Worker.Labels, and
// cmd/atecontroller/internal/workersync/syncer.go's createOrUpdateWorker sets that field to
// exactly pool.GetLabels() -- the WorkerPool object's own, admin-defined metadata.labels (e.g.
// this cluster's "counter" pool carries {"workload": "counter"}, not any fixed-key convention).
// So the only way to build a selector that matches is to read the real WorkerPool and copy its
// labels; WorkerPoolLabelKey's fixed-key convention (kagent-dev/substrate's own WorkerPool
// controller invariant) matches nothing here and silently produces
// "no free workers available" on every resume.
//
// Falls back to the fixed-key convention when the pool can't be read (letting existing unit
// tests that build templates without a live cluster/fake WorkerPool object keep passing
// unchanged) or has no labels of its own.
func (p *Lifecycle) workerSelectorForPool(ctx context.Context, wpKey types.NamespacedName) *metav1.LabelSelector {
	if wpKey.Name == "" {
		return nil
	}
	if p != nil && p.Client != nil {
		var wp atev1alpha1.WorkerPool
		if err := p.Client.Get(ctx, wpKey, &wp); err == nil && len(wp.GetLabels()) > 0 {
			return &metav1.LabelSelector{MatchLabels: wp.GetLabels()}
		}
	}
	return &metav1.LabelSelector{
		MatchLabels: map[string]string{WorkerPoolLabelKey: wpKey.Name},
	}
}

func substrateSnapshotsLocation(ah *v1alpha2.AgentHarness) string {
	if ah == nil {
		return substrateSnapshotsLocationFor("", "", "")
	}
	loc := ""
	if sub := ah.Spec.Substrate; sub != nil && sub.SnapshotsConfig != nil {
		loc = sub.SnapshotsConfig.Location
	}
	return substrateSnapshotsLocationFor(ah.Namespace, ah.Name, loc)
}

func substrateSnapshotsLocationFor(namespace, name, explicitLocation string) string {
	if loc := strings.TrimSpace(explicitLocation); loc != "" {
		return loc
	}
	return defaultSubstrateSnapshotsLocation(namespace, name)
}

func (p *Lifecycle) resolveWorkerPoolRefFor(
	ctx context.Context,
	namespace string,
	explicit *v1alpha2.TypedLocalReference,
) (types.NamespacedName, error) {
	if p == nil || p.Client == nil {
		return types.NamespacedName{}, fmt.Errorf("substrate lifecycle kubernetes client is required")
	}
	key := p.Defaults.DefaultWorkerPool
	if explicit != nil {
		if name := strings.TrimSpace(explicit.Name); name != "" {
			key = types.NamespacedName{Namespace: namespace, Name: name}
		}
	}
	if key.Name == "" {
		return types.NamespacedName{}, fmt.Errorf("substrate workerPoolRef is required when no default WorkerPool is configured")
	}
	if key.Namespace == "" {
		key.Namespace = namespace
	}

	var wp atev1alpha1.WorkerPool
	if err := p.Client.Get(ctx, key, &wp); err != nil {
		return types.NamespacedName{}, fmt.Errorf("get WorkerPool %s: %w", key, err)
	}
	return key, nil
}

func defaultSubstrateSnapshotsLocation(namespace, name string) string {
	return fmt.Sprintf("gs://%s/%s/%s", defaultSnapshotsBucket, namespace, name)
}

func lifecycleLabels(ah *v1alpha2.AgentHarness) map[string]string {
	return map[string]string{
		"app.kubernetes.io/managed-by": "kagent",
		"kagent.dev/agent-harness":     ah.Name,
	}
}

func actorTemplateName(ah *v1alpha2.AgentHarness) string {
	return truncateDNS1123(ah.Name)
}

func truncateDNS1123(s string) string {
	return truncateDNS1123To(s, 63)
}

func truncateDNS1123To(s string, max int) string {
	s = strings.ToLower(strings.ReplaceAll(s, "_", "-"))
	if len(s) > max {
		s = strings.TrimRight(s[:max], "-")
	}
	return s
}

// ResolveCurrentActorTemplate returns the ate-api ActorTemplate a SandboxAgent should currently
// serve from.
//
// github.com/agent-substrate/substrate (this branch) has no CRD for ActorTemplate, so unlike the
// kagent-dev/substrate fork's design (a blue-green pivot across every retained, per-shape
// template, selected by a kagent.dev/desired-generation annotation on each candidate), this reads
// a single pointer -- the substrateActorTemplateAnnotation reconcileActorTemplate wrote on the
// SandboxAgent itself the last time it successfully ensured a template -- and resolves that one
// template via ate-api directly. A shape change still produces a new template under a new
// (hash-derived) name and updates this pointer to it, so config changes are still picked up; what
// is lost is keeping a superseded template's golden warm for in-flight sessions during a
// rollout -- acceptable for proving this integration works at all, not for production blue-green
// semantics. Returns (nil, nil) when no template has been generated yet.
func ResolveCurrentActorTemplate(ctx context.Context, kube client.Client, ate *Client, namespace, agentName string) (*ateapipb.ActorTemplate, error) {
	var sa v1alpha2.SandboxAgent
	if err := kube.Get(ctx, types.NamespacedName{Namespace: namespace, Name: agentName}, &sa); err != nil {
		if apierrors.IsNotFound(err) {
			return nil, nil
		}
		return nil, fmt.Errorf("get SandboxAgent %s/%s: %w", namespace, agentName, err)
	}
	name := sa.Annotations[substrateActorTemplateAnnotation]
	if name == "" {
		return nil, nil
	}
	return ate.GetActorTemplateByName(ctx, namespace, name)
}

// selectCurrentActorTemplate selects the current actor as defined by the
// highest-desired-generation template whose golden is Ready
func selectCurrentActorTemplate(templates []*ActorTemplate) *ActorTemplate {
	var desiredReady, desired *ActorTemplate
	for i := range templates {
		t := templates[i]
		if desired == nil || moreDesiredActorTemplate(t, desired) {
			desired = t
		}
		if t.Status.Phase == PhaseReady {
			if desiredReady == nil || moreDesiredActorTemplate(t, desiredReady) {
				desiredReady = t
			}
		}
	}
	if desiredReady != nil {
		return desiredReady
	}
	return desired
}

// moreDesiredActorTemplate reports whether a is "more desired" than b: a higher desired-generation
// wins (the template applied for the current config), with creationTimestamp as a tiebreaker for
// legacy templates that predate the annotation.
func moreDesiredActorTemplate(a, b *ActorTemplate) bool {
	ga, gb := actorTemplateDesiredGeneration(a), actorTemplateDesiredGeneration(b)
	if ga != gb {
		return ga > gb
	}
	return a.CreationTimestamp.After(b.CreationTimestamp.Time)
}

// actorTemplateDesiredGeneration parses the desired-generation annotation; absent/invalid is 0.
func actorTemplateDesiredGeneration(t *ActorTemplate) int64 {
	g, err := strconv.ParseInt(t.Annotations[desiredGenerationAnnotation], 10, 64)
	if err != nil {
		return 0
	}
	return g
}

// listSandboxAgentActorTemplates is a stub: github.com/agent-substrate/substrate has no
// ActorTemplate CRD to list (see ResolveCurrentActorTemplate). Its only remaining caller,
// DeleteAllSandboxAgentActors' cleanup sweep, falls back to id-prefix matching when this returns
// nothing, which is sufficient to find and delete a SandboxAgent's session actors -- it only loses
// the extra match against a specific retained template, moot since ActorTemplates are no longer
// retained as Kubernetes objects to enumerate in the first place.
func listSandboxAgentActorTemplates(_ context.Context, _ client.Client, _, _ string) ([]*ActorTemplate, error) {
	return nil, nil
}

// pinImageRef ensures image refs satisfy Substrate ActorTemplate validation (must contain "@").
func pinImageRef(image string) (string, error) {
	image = strings.TrimSpace(image)
	if image == "" {
		return "", fmt.Errorf("workload image is required")
	}
	if !strings.Contains(image, "@") {
		return "", fmt.Errorf("workload image %q must be pinned with a digest (@sha256:...)", image)
	}
	return image, nil
}

// actorTemplateEnvFromPodEnv converts pod env vars into ActorTemplate env vars.
// Substrate ActorTemplates only support literal values, secretKeyRef, and configMapKeyRef.
func actorTemplateEnvFromPodEnv(env []corev1.EnvVar) []EnvVar {
	out := make([]EnvVar, 0, len(env))
	seen := make(map[string]struct{}, len(env))
	for _, e := range env {
		if e.Name == "" {
			continue
		}
		sanitized := sanitizeActorTemplateEnvVar(e)
		if sanitized == nil {
			continue
		}
		if _, ok := seen[sanitized.Name]; ok {
			continue
		}
		seen[sanitized.Name] = struct{}{}
		out = append(out, *sanitized)
	}
	return out
}

func sanitizeActorTemplateEnvVar(e corev1.EnvVar) *EnvVar {
	if e.Value != "" {
		return &EnvVar{
			Name:      e.Name,
			ValueFrom: nil,
			Value:     &e.Value,
		}
	}
	if ref := e.ValueFrom.SecretKeyRef; ref != nil {
		return &EnvVar{
			Name: e.Name,
			ValueFrom: &EnvVarSource{
				SecretKeyRef: &SecretKeySelector{
					Name:     ref.Name,
					Key:      ref.Key,
					Optional: ref.Optional,
				},
			},
		}
	}
	return nil
}
