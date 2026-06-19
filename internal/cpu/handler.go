package cpu

import (
	"context"
	"fmt"

	"github.com/go-logr/logr"
	"github.com/lablabs/pod-deletion-cost-controller/internal/controller"
	v1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	metricsv1beta1 "k8s.io/metrics/pkg/apis/metrics/v1beta1"
	metricsclientset "k8s.io/metrics/pkg/client/clientset/versioned"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

const (
	// TypeAnnotation is the value of pod-deletion-cost.lablabs.io/type that enables this algorithm
	TypeAnnotation = "cpu"
)

// PodMetricsFetcher abstracts fetching pod metrics for a namespace.
// In production this wraps metricsclientset.Interface; in tests it can be mocked.
type PodMetricsFetcher interface {
	ListPodMetrics(ctx context.Context, namespace string) ([]metricsv1beta1.PodMetrics, error)
}

// metricsClientFetcher wraps the real metrics clientset
type metricsClientFetcher struct {
	client metricsclientset.Interface
}

func (f *metricsClientFetcher) ListPodMetrics(ctx context.Context, namespace string) ([]metricsv1beta1.PodMetrics, error) {
	list, err := f.client.MetricsV1beta1().PodMetricses(namespace).List(ctx, metav1.ListOptions{})
	if err != nil {
		return nil, err
	}
	return list.Items, nil
}

// NewHandler creates a new CPU Handler
func NewHandler(k8sClient client.Client, metricsClient metricsclientset.Interface) *Handler {
	return &Handler{
		client:        k8sClient,
		metricsFetcher: &metricsClientFetcher{client: metricsClient},
	}
}

// NewHandlerWithFetcher creates a Handler with a custom PodMetricsFetcher (useful for testing)
func NewHandlerWithFetcher(k8sClient client.Client, fetcher PodMetricsFetcher) *Handler {
	return &Handler{
		client:        k8sClient,
		metricsFetcher: fetcher,
	}
}

// Handler handles reconcile loop for Pod/Deployment using CPU usage
type Handler struct {
	client        client.Client
	metricsFetcher PodMetricsFetcher
}

// AcceptType returns the accepted algorithm type annotation values
func (h *Handler) AcceptType() []string {
	return []string{TypeAnnotation}
}

// Handle reconciles pod-deletion-cost for all pods in the same ReplicaSet based on CPU usage.
// Pods with higher CPU usage receive a higher deletion cost, making them less likely to be
// deleted when the deployment scales down.
func (h *Handler) Handle(ctx context.Context, log logr.Logger, pod *corev1.Pod, dep *v1.Deployment) error {
	if controller.IsDeleting(pod) {
		return nil
	}

	podList := &corev1.PodList{}
	if err := listPodsByOwnerRS(ctx, h.client, pod, podList); err != nil {
		return fmt.Errorf("unable to list pods by replicaset: %w", err)
	}

	costs, err := h.computeCosts(ctx, pod.Namespace, podList.Items)
	if err != nil {
		return fmt.Errorf("unable to compute CPU-based deletion costs: %w", err)
	}

	for i := range podList.Items {
		p := &podList.Items[i]
		if controller.IsDeleting(p) {
			continue
		}
		cost, ok := costs[p.Name]
		if !ok {
			continue
		}
		// Skip patching if already has the correct cost
		if existing, exists := controller.GetPodDeletionCost(p); exists && existing == cost {
			continue
		}
		patch := client.MergeFrom(p.DeepCopy())
		controller.ApplyPodDeletionCost(p, cost)
		if err := h.client.Patch(ctx, p, patch); err != nil {
			return fmt.Errorf("unable to patch pod %s: %w", p.Name, err)
		}
		log.WithValues("pod", p.Name, controller.PodDeletionCostAnnotation, cost).Info("updated")
	}
	return nil
}

// computeCosts fetches PodMetrics for the namespace and returns a map of pod name to deletion cost.
// The cost is the pod's total CPU usage in millicores; pods missing metrics receive a cost of 0.
func (h *Handler) computeCosts(ctx context.Context, namespace string, pods []corev1.Pod) (map[string]int, error) {
	metrics, err := h.metricsFetcher.ListPodMetrics(ctx, namespace)
	if err != nil {
		return nil, fmt.Errorf("unable to list pod metrics: %w", err)
	}

	cpuByPod := make(map[string]int64, len(metrics))
	for _, pm := range metrics {
		var total int64
		for _, c := range pm.Containers {
			total += c.Usage.Cpu().MilliValue()
		}
		cpuByPod[pm.Name] = total
	}

	costs := make(map[string]int, len(pods))
	for _, p := range pods {
		costs[p.Name] = int(cpuByPod[p.Name])
	}
	return costs, nil
}

func listPodsByOwnerRS(ctx context.Context, c client.Client, pod *corev1.Pod, list *corev1.PodList) error {
	var rsUID types.UID
	for _, owner := range pod.OwnerReferences {
		if owner.Kind == "ReplicaSet" {
			rsUID = owner.UID
			break
		}
	}
	if rsUID == "" {
		return nil
	}
	return c.List(ctx, list,
		client.InNamespace(pod.Namespace),
		client.MatchingFields{controller.PodToRSIndex: string(rsUID)},
	)
}
