package substrate

// This file is a local, in-package copy of the ActorTemplate-shaped Go types
// github.com/kagent-dev/substrate (the fork kagent's go.mod used to replace
// github.com/agent-substrate/substrate with) defines as a Kubernetes CRD
// (pkg/api/v1alpha1/actortemplate_types.go). github.com/agent-substrate/substrate
// (this branch, what kagent now actually depends on -- see go.mod) has no such CRD:
// ActorTemplate exists purely via ateapipb.ControlClient's RPCs. buildSandboxAgentActorTemplate/
// buildActorTemplate (agent_lifecycle.go/lifecycle_actortemplate.go) still build this exact shape
// as an in-memory intermediate value -- unchanged from the CRD-based design -- and
// convertActorTemplate (actortemplate_convert.go) translates it into the real
// ateapipb.ActorTemplate proto reconcileActorTemplate sends over the RPC. Keeping the same field
// shape here minimizes the diff against upstream kagent and lets every other file in this package
// stay as close to unmodified as possible.
//
// ActorTemplate/ActorTemplateList still embed metav1.TypeMeta/ObjectMeta and implement
// client.Object (via the hand-written DeepCopyObject below; GetObjectKind and every
// metav1.Object accessor come from the embedded types) purely because
// sandboxbackend.Backend's interface methods (BuildSandbox, ReconcileActorTemplate,
// GetOwnedResourceTypes) are shared across every backend and require it -- not because any
// Kubernetes API operation is ever actually performed against a value of this type anymore.

import (
	"k8s.io/apimachinery/pkg/runtime"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

type PhaseType string

const (
	PhaseInitial           PhaseType = ""
	PhaseResumeGoldenActor PhaseType = "ResumeGoldenActor"
	PhaseWaitGoldenActor   PhaseType = "WaitGoldenActor"
	PhaseReady             PhaseType = "Ready"
	PhaseFailed            PhaseType = "Failed"
)

type DurableDirVolumeSource struct{}

type VolumeSource struct {
	DurableDir *DurableDirVolumeSource `json:"durableDir,omitempty"`
}

type Volume struct {
	Name string `json:"name"`
	VolumeSource
}

type VolumeMount struct {
	Name      string `json:"name"`
	MountPath string `json:"mountPath"`
}

type Container struct {
	Name         string           `json:"name"`
	Image        string           `json:"image,omitempty"`
	Command      []string         `json:"command,omitempty"`
	Env          []EnvVar         `json:"env,omitempty"`
	Readyz       *ContainerReadyz `json:"readyz,omitempty"`
	VolumeMounts []VolumeMount    `json:"volumeMounts,omitempty"`
}

type ContainerReadyz struct {
	HTTPGet *HTTPGetAction `json:"httpGet"`
}

type HTTPGetAction struct {
	Path string `json:"path,omitempty"`
	Port int32  `json:"port"`
}

type EnvVar struct {
	Name      string         `json:"name"`
	Value     *string        `json:"value,omitempty"`
	ValueFrom *EnvVarSource  `json:"valueFrom,omitempty"`
}

type EnvVarSource struct {
	SecretKeyRef *SecretKeySelector `json:"secretKeyRef,omitempty"`
}

type SecretKeySelector struct {
	Name     string `json:"name"`
	Key      string `json:"key"`
	Optional *bool  `json:"optional,omitempty"`
}

type SnapshotScope string

const (
	SnapshotScopeFull SnapshotScope = "Full"
	SnapshotScopeData SnapshotScope = "Data"
)

type SnapshotsConfig struct {
	Location string        `json:"location"`
	OnPause  SnapshotScope `json:"onPause,omitempty"`
	OnCommit SnapshotScope `json:"onCommit,omitempty"`
}

type SandboxClass string

const (
	SandboxClassGvisor  SandboxClass = "gvisor"
	SandboxClassMicroVM SandboxClass = "microvm"
)

type ActorTemplateSpec struct {
	PauseImage      string                `json:"pauseImage,omitempty"`
	Containers      []Container           `json:"containers,omitempty"`
	SnapshotsConfig SnapshotsConfig       `json:"snapshotsConfig"`
	SandboxClass    SandboxClass          `json:"sandboxClass,omitempty"`
	WorkerSelector  *metav1.LabelSelector `json:"workerSelector,omitempty"`
	Volumes         []Volume              `json:"volumes,omitempty"`
}

type ActorTemplateStatus struct {
	Phase                PhaseType          `json:"phase,omitempty"`
	GoldenActorID        string             `json:"goldenActorID,omitempty"`
	TakeGoldenSnapshotAt metav1.Time        `json:"takeGoldenSnapshotAt,omitempty"`
	GoldenSnapshot       string             `json:"goldenSnapshot,omitempty"`
	Conditions           []metav1.Condition `json:"conditions,omitempty"`
}

type ActorTemplate struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   ActorTemplateSpec   `json:"spec"`
	Status ActorTemplateStatus `json:"status,omitempty"`
}

// DeepCopyObject implements runtime.Object, needed for ActorTemplate to satisfy client.Object --
// see this file's top-level doc comment for why that is still required.
func (in *ActorTemplate) DeepCopyObject() runtime.Object {
	if in == nil {
		return nil
	}
	out := new(ActorTemplate)
	out.TypeMeta = in.TypeMeta
	in.ObjectMeta.DeepCopyInto(&out.ObjectMeta)

	out.Spec.PauseImage = in.Spec.PauseImage
	out.Spec.SandboxClass = in.Spec.SandboxClass
	out.Spec.SnapshotsConfig = in.Spec.SnapshotsConfig
	if in.Spec.WorkerSelector != nil {
		out.Spec.WorkerSelector = in.Spec.WorkerSelector.DeepCopy()
	}
	if in.Spec.Containers != nil {
		out.Spec.Containers = make([]Container, len(in.Spec.Containers))
		copy(out.Spec.Containers, in.Spec.Containers)
	}
	if in.Spec.Volumes != nil {
		out.Spec.Volumes = make([]Volume, len(in.Spec.Volumes))
		copy(out.Spec.Volumes, in.Spec.Volumes)
	}
	out.Status = in.Status
	return out
}
