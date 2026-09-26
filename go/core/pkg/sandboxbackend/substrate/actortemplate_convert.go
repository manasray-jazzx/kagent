package substrate

import (
	"context"
	"fmt"

	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// sandboxConfigNameFor maps an ActorTemplateSpec's SandboxClass to the cluster-scoped
// SandboxConfig object name every ate-system install registers for it (see
// manifests/ate-install/ate-setup.yaml and demos/*/*.yaml.tmpl in agent-substrate/substrate,
// which use these same two names universally).
func sandboxConfigNameFor(class SandboxClass) (ateapipb.SandboxClass, string) {
	if class == SandboxClassMicroVM {
		return ateapipb.SandboxClass_SANDBOX_CLASS_MICROVM, "microvm"
	}
	return ateapipb.SandboxClass_SANDBOX_CLASS_GVISOR, "gvisor-default"
}

func snapshotScopeFor(scope SnapshotScope) ateapipb.SnapshotContentScope {
	if scope == SnapshotScopeData {
		return ateapipb.SnapshotContentScope_SNAPSHOT_CONTENT_SCOPE_DATA
	}
	return ateapipb.SnapshotContentScope_SNAPSHOT_CONTENT_SCOPE_FULL
}

// convertActorTemplate translates a kagent-built *ActorTemplate (this package's
// intermediate "desired" value, produced by buildSandboxAgentActorTemplate/buildActorTemplate)
// into the ateapipb.ActorTemplate proto ate-api's CreateActorTemplate RPC actually accepts.
//
// github.com/agent-substrate/substrate (this branch, unlike the kagent-dev/substrate fork
// kagent's go.mod replaces it with) has no Kubernetes CRD counterpart for ActorTemplate at all --
// it exists purely via ateapipb.ControlClient. This function, plus reconcileActorTemplate and
// ResolveCurrentActorTemplate in the same package, is what lets kagent's SandboxAgent backend
// work against it anyway: kagent still builds the same ActorTemplateSpec shape it
// always did (buildSandboxAgentActorTemplate is untouched), only the last mile -- persisting it
// -- goes through the RPC instead of a client.Create() against a CRD that does not exist here.
//
// Two things the proto cannot express drop out here, both acceptable for this branch's purposes:
//   - EnvVarSource.SecretKeyRef: ateapipb.EnvVar only carries a literal value, so a secretKeyRef
//     is resolved once, here, by reading the Secret directly. This trades away kagent's "soft
//     config rollout without recreating the actor" optimization (a session's actor no longer
//     re-reads the Secret on every resume, since the value is now baked into the immutable
//     template) for working at all.
//   - PauseImage: ateapipb.ActorTemplate has no such field; this branch's sandbox runtime does
//     not need one supplied per-template.
func convertActorTemplate(ctx context.Context, kube client.Client, desired *ActorTemplate) (*ateapipb.ActorTemplate, error) {
	namespace := desired.Namespace
	containers := make([]*ateapipb.Container, 0, len(desired.Spec.Containers))
	for _, c := range desired.Spec.Containers {
		env, err := convertEnv(ctx, kube, namespace, c.Env)
		if err != nil {
			return nil, fmt.Errorf("container %q: %w", c.Name, err)
		}
		containers = append(containers, &ateapipb.Container{
			Name:         c.Name,
			Image:        c.Image,
			Command:      c.Command,
			Env:          env,
			Readyz:       convertReadyz(c.Readyz),
			VolumeMounts: convertVolumeMounts(c.VolumeMounts),
		})
	}

	volumes := make([]*ateapipb.Volume, 0, len(desired.Spec.Volumes))
	for _, v := range desired.Spec.Volumes {
		if v.DurableDir == nil {
			// buildSandboxAgentActorTemplate/buildActorTemplate only ever emit DurableDir
			// volumes; anything else here would be a future addition to this package this
			// conversion has not caught up with yet.
			return nil, fmt.Errorf("volume %q: only durableDir volumes are supported", v.Name)
		}
		volumes = append(volumes, &ateapipb.Volume{
			Name:       v.Name,
			DurableDir: &ateapipb.DurableDirVolumeSource{},
		})
	}

	sandboxClass, configName := sandboxConfigNameFor(desired.Spec.SandboxClass)

	return &ateapipb.ActorTemplate{
		Metadata: &ateapipb.ResourceMetadata{
			Atespace: desired.Namespace,
			Name:     desired.Name,
		},
		WorkerSelector: convertSelector(desired.Spec.WorkerSelector),
		Containers:     containers,
		Volumes:        volumes,
		SnapshotsConfig: &ateapipb.SnapshotsConfig{
			OnPause:         snapshotScopeFor(desired.Spec.SnapshotsConfig.OnPause),
			OnCommit:        snapshotScopeFor(desired.Spec.SnapshotsConfig.OnCommit),
			StorageLocation: desired.Spec.SnapshotsConfig.Location,
		},
		SandboxConfig: &ateapipb.SandboxConfig{
			SandboxClass: sandboxClass,
			ConfigName:   configName,
		},
		Resources: &ateapipb.Resources{
			Limits: []*ateapipb.Limits{
				{Name: "cpu", Quantity: "1"},
				{Name: "memory", Quantity: "1Gi"},
			},
		},
	}, nil
}

func convertReadyz(r *ContainerReadyz) *ateapipb.ContainerReadyz {
	if r == nil || r.HTTPGet == nil {
		return nil
	}
	return &ateapipb.ContainerReadyz{
		HttpGet: &ateapipb.HTTPGetAction{
			Path: r.HTTPGet.Path,
			Port: r.HTTPGet.Port,
		},
		TimeoutSeconds: 30,
	}
}

func convertVolumeMounts(mounts []VolumeMount) []*ateapipb.VolumeMount {
	out := make([]*ateapipb.VolumeMount, 0, len(mounts))
	for _, m := range mounts {
		out = append(out, &ateapipb.VolumeMount{Name: m.Name, MountPath: m.MountPath})
	}
	return out
}

func convertSelector(sel *metav1.LabelSelector) *ateapipb.Selector {
	if sel == nil || len(sel.MatchLabels) == 0 {
		return nil
	}
	return &ateapipb.Selector{MatchLabels: sel.MatchLabels}
}

// convertEnv resolves every EnvVar into a literal value: ValueFrom.SecretKeyRef is read from the
// live Secret now, since ateapipb.EnvVar has no indirection to carry a reference through.
func convertEnv(ctx context.Context, kube client.Client, namespace string, env []EnvVar) ([]*ateapipb.EnvVar, error) {
	out := make([]*ateapipb.EnvVar, 0, len(env))
	secrets := map[string]*corev1.Secret{}
	for _, e := range env {
		if e.Value != nil {
			out = append(out, &ateapipb.EnvVar{Name: e.Name, Value: *e.Value})
			continue
		}
		if e.ValueFrom == nil || e.ValueFrom.SecretKeyRef == nil {
			return nil, fmt.Errorf("env %q: unsupported EnvVarSource", e.Name)
		}
		ref := e.ValueFrom.SecretKeyRef
		secret, ok := secrets[ref.Name]
		if !ok {
			secret = &corev1.Secret{}
			key := types.NamespacedName{Namespace: namespace, Name: ref.Name}
			if err := kube.Get(ctx, key, secret); err != nil {
				optional := ref.Optional != nil && *ref.Optional
				if optional {
					continue
				}
				return nil, fmt.Errorf("env %q: get Secret %s: %w", e.Name, key, err)
			}
			secrets[ref.Name] = secret
		}
		value, ok := secret.Data[ref.Key]
		if !ok {
			if ref.Optional != nil && *ref.Optional {
				continue
			}
			return nil, fmt.Errorf("env %q: Secret %s has no key %q", e.Name, ref.Name, ref.Key)
		}
		out = append(out, &ateapipb.EnvVar{Name: e.Name, Value: string(value)})
	}
	return out, nil
}
