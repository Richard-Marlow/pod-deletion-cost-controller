package cpu

import (
	"fmt"

	"github.com/go-logr/logr"
	"github.com/lablabs/pod-deletion-cost-controller/internal/module"
	metricsclientset "k8s.io/metrics/pkg/client/clientset/versioned"
	"k8s.io/utils/strings/slices"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

const (
	// Name of module
	Name = "cpu"
)

// Registrator defines the controller manager interface
type Registrator interface {
	AddModule(module module.Handler) error
}

// Register registers the cpu module
func Register(log logr.Logger, r Registrator, k8sClient client.Client, metricsClient metricsclientset.Interface, algoTypes []string) error {
	if slices.Contains(algoTypes, Name) || len(algoTypes) == 0 {
		h := NewHandler(k8sClient, metricsClient)
		err := r.AddModule(h)
		if err != nil {
			return fmt.Errorf("register cpu module failed: %w", err)
		}
		log.WithValues("module", Name).Info("registered")
		return nil
	}
	log.V(2).WithValues("module", Name).Info("NOT registered")
	return nil
}
