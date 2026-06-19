package cpu_test

import (
	"context"
	"testing"

	"github.com/go-logr/logr"
	"github.com/lablabs/pod-deletion-cost-controller/internal/controller"
	"github.com/lablabs/pod-deletion-cost-controller/internal/cpu"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	metricsv1beta1 "k8s.io/metrics/pkg/apis/metrics/v1beta1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

// mockMetricsFetcher is a test double for cpu.PodMetricsFetcher
type mockMetricsFetcher struct {
	items []metricsv1beta1.PodMetrics
	err   error
}

func (m *mockMetricsFetcher) ListPodMetrics(_ context.Context, _ string) ([]metricsv1beta1.PodMetrics, error) {
	return m.items, m.err
}

func TestHandler_AcceptType(t *testing.T) {
	h := cpu.NewHandlerWithFetcher(nil, nil)
	assert.Equal(t, []string{"cpu"}, h.AcceptType())
}

func TestHandler_Handle_SetsDeleteCostProportionalToCPU(t *testing.T) {
	rsUID := types.UID("rs-uid-1")

	pods := []corev1.Pod{
		makePod("pod-high-cpu", "default", rsUID, nil),
		makePod("pod-low-cpu", "default", rsUID, nil),
		makePod("pod-no-metrics", "default", rsUID, nil),
	}

	fetcher := &mockMetricsFetcher{
		items: []metricsv1beta1.PodMetrics{
			makePodMetrics("pod-high-cpu", "default", "800m"),
			makePodMetrics("pod-low-cpu", "default", "200m"),
			// pod-no-metrics intentionally omitted
		},
	}

	k8sClient := buildFakeClient(pods)
	h := cpu.NewHandlerWithFetcher(k8sClient, fetcher)
	dep := &appsv1.Deployment{}

	err := h.Handle(context.Background(), logr.Discard(), &pods[0], dep)
	require.NoError(t, err)

	updatedPods := &corev1.PodList{}
	require.NoError(t, k8sClient.List(context.Background(), updatedPods, client.InNamespace("default")))

	costByName := make(map[string]int)
	for _, p := range updatedPods.Items {
		if cost, ok := controller.GetPodDeletionCost(&p); ok {
			costByName[p.Name] = cost
		}
	}

	assert.Equal(t, 800, costByName["pod-high-cpu"], "high CPU pod should have cost 800")
	assert.Equal(t, 200, costByName["pod-low-cpu"], "low CPU pod should have cost 200")
	assert.Equal(t, 0, costByName["pod-no-metrics"], "pod without metrics should have cost 0")
}

func TestHandler_Handle_SkipsDeletingPod(t *testing.T) {
	rsUID := types.UID("rs-uid-2")

	// Build a live pod first; simulate deletion only in the local struct passed to Handle
	pod := makePod("pod-live", "default", rsUID, nil)
	k8sClient := buildFakeClient([]corev1.Pod{pod})

	now := metav1.Now()
	deletingPod := pod.DeepCopy()
	deletingPod.DeletionTimestamp = &now

	fetcher := &mockMetricsFetcher{
		items: []metricsv1beta1.PodMetrics{
			makePodMetrics("pod-live", "default", "500m"),
		},
	}

	h := cpu.NewHandlerWithFetcher(k8sClient, fetcher)
	dep := &appsv1.Deployment{}

	err := h.Handle(context.Background(), logr.Discard(), deletingPod, dep)
	require.NoError(t, err)

	var result corev1.Pod
	require.NoError(t, k8sClient.Get(context.Background(), types.NamespacedName{Namespace: "default", Name: "pod-live"}, &result))
	_, hasCost := controller.GetPodDeletionCost(&result)
	assert.False(t, hasCost, "pod should not be patched when reconciled pod is deleting")
}

func TestHandler_Handle_SkipsPatchWhenCostUnchanged(t *testing.T) {
	rsUID := types.UID("rs-uid-3")

	pod := makePod("pod-stable", "default", rsUID, map[string]string{
		controller.PodDeletionCostAnnotation: "500",
	})

	fetcher := &mockMetricsFetcher{
		items: []metricsv1beta1.PodMetrics{
			makePodMetrics("pod-stable", "default", "500m"),
		},
	}

	k8sClient := buildFakeClient([]corev1.Pod{pod})
	h := cpu.NewHandlerWithFetcher(k8sClient, fetcher)
	dep := &appsv1.Deployment{}

	err := h.Handle(context.Background(), logr.Discard(), &pod, dep)
	require.NoError(t, err)

	var result corev1.Pod
	require.NoError(t, k8sClient.Get(context.Background(), types.NamespacedName{Namespace: "default", Name: "pod-stable"}, &result))
	cost, ok := controller.GetPodDeletionCost(&result)
	require.True(t, ok)
	assert.Equal(t, 500, cost)
}

func TestHandler_Handle_MultiContainerPodSumsCPU(t *testing.T) {
	rsUID := types.UID("rs-uid-4")
	pod := makePod("pod-multi", "default", rsUID, nil)

	fetcher := &mockMetricsFetcher{
		items: []metricsv1beta1.PodMetrics{
			{
				ObjectMeta: metav1.ObjectMeta{Name: "pod-multi", Namespace: "default"},
				Containers: []metricsv1beta1.ContainerMetrics{
					{Name: "app", Usage: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("300m")}},
					{Name: "sidecar", Usage: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("150m")}},
				},
			},
		},
	}

	k8sClient := buildFakeClient([]corev1.Pod{pod})
	h := cpu.NewHandlerWithFetcher(k8sClient, fetcher)
	dep := &appsv1.Deployment{}

	err := h.Handle(context.Background(), logr.Discard(), &pod, dep)
	require.NoError(t, err)

	var result corev1.Pod
	require.NoError(t, k8sClient.Get(context.Background(), types.NamespacedName{Namespace: "default", Name: "pod-multi"}, &result))
	cost, ok := controller.GetPodDeletionCost(&result)
	require.True(t, ok)
	assert.Equal(t, 450, cost, "multi-container pod should sum CPU across containers (300+150=450)")
}

// --- helpers ---

func makePod(name, namespace string, rsUID types.UID, annotations map[string]string) corev1.Pod {
	return corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:        name,
			Namespace:   namespace,
			Annotations: annotations,
			OwnerReferences: []metav1.OwnerReference{
				{Kind: "ReplicaSet", UID: rsUID, Name: "rs-1"},
			},
		},
	}
}

func makePodMetrics(name, namespace, cpuStr string) metricsv1beta1.PodMetrics {
	return metricsv1beta1.PodMetrics{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: namespace},
		Containers: []metricsv1beta1.ContainerMetrics{
			{Name: "app", Usage: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse(cpuStr)}},
		},
	}
}

func buildFakeClient(pods []corev1.Pod) client.Client {
	scheme := runtime.NewScheme()
	_ = corev1.AddToScheme(scheme)
	_ = appsv1.AddToScheme(scheme)

	objs := make([]client.Object, len(pods))
	for i := range pods {
		objs[i] = &pods[i]
	}

	return fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(objs...).
		WithIndex(&corev1.Pod{}, controller.PodToRSIndex, func(obj client.Object) []string {
			pod, ok := obj.(*corev1.Pod)
			if !ok {
				return nil
			}
			for _, owner := range pod.OwnerReferences {
				if owner.Kind == "ReplicaSet" {
					return []string{string(owner.UID)}
				}
			}
			return nil
		}).
		Build()
}
