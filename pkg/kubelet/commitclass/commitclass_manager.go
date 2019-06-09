package commitclass

import (
	"encoding/json"
	"fmt"
	"sort"

	"k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/tools/cache"
	v1helper "k8s.io/kubernetes/pkg/apis/core/v1/helper"
)

// structs defined here because I couldn't figure out how to get codegen to
// cooperate. I think this will be easier with 1.14+, where I can jump into the
// 'node.k8s.io' API group.

// CommitClass defines the over- or under-commit level for a set of resources on a group of nodes.
type CommitClass struct {
	metav1.TypeMeta `json:",inline"`
	// Standard object's metadata.
	// More info: https://git.k8s.io/community/contributors/devel/api-conventions.md#metadata
	// +optional
	metav1.ObjectMeta `json:"metadata,omitempty" protobuf:"bytes,1,opt,name=metadata"`

	Spec CommitClassSpec `json:"spec" protobuf:"bytes,2,opt,name=spec"`
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// CommitClassList is a collection of commit classes.
type CommitClassList struct {
	metav1.TypeMeta `json:",inline"`
	// Standard list metadata
	// More info: https://git.k8s.io/community/contributors/devel/api-conventions.md#metadata
	// +optional
	metav1.ListMeta `json:"metadata,omitempty" protobuf:"bytes,1,opt,name=metadata"`

	// items is the list of CommitClasses
	Items []CommitClass `json:"items" protobuf:"bytes,2,rep,name=items"`
}

type CommitClassSpec struct {
	// The selector which defines the set of nodes this CommitClass applies to.
	Selector v1.NodeSelectorTerm `json:"selector" protobuf:"bytes,2,name=selector"`

	// A list of resources with associated commit factors
	Resources []ResourceCommitPercent `json:"resources" protobuf:"bytes,3,name=resources"`
}

type ResourceCommitPercent struct {
	Name    string `json:"name" protobuf:"bytes,1,name=name"`
	Percent int32  `json:"percent" protobuf:"bytes,2,name=percent"`
}

var (
	commitClassGVR = schema.GroupVersionResource{
		Group:    "node.k8s.io",
		Version:  "v1alpha1",
		Resource: "commitclasses",
	}
)

type Settings struct {
	scales map[string]float64
}

func NewSettings() *Settings {
	return &Settings{
		scales: map[string]float64{},
	}
}

func (s *Settings) Set(name string, percent int32) {
	scaleFactor := float64(percent) / 100
	s.scales[name] = scaleFactor
}

func (s *Settings) Scale(name v1.ResourceName, quantity resource.Quantity) resource.Quantity {
	commitLevel, ok := s.scales[string(name)]
	// an implicit 'scale to 100% of current'
	if !ok {
		return quantity
	}

	scaled := commitLevel * float64(quantity.Value())
	quantity.Set(int64(scaled))
	return quantity
}

type Manager struct {
	informer cache.SharedInformer
}

var resourceWhitelist []v1.ResourceName = []v1.ResourceName{
	v1.ResourceCPU,
	v1.ResourceMemory,
	v1.ResourceEphemeralStorage,
}

func NewManager(client dynamic.Interface) *Manager {
	rc := client.Resource(commitClassGVR)
	lw := &cache.ListWatch{
		ListFunc: func(options metav1.ListOptions) (runtime.Object, error) {
			return rc.List(options)
		},
		WatchFunc: rc.Watch,
	}
	informer := cache.NewSharedInformer(lw, &unstructured.Unstructured{}, 0)

	return &Manager{
		informer: informer,
	}
}

func (m *Manager) Run(stopCh <-chan struct{}) {
	m.informer.Run(stopCh)
}

func (m *Manager) GetCommitSettings(node *v1.Node) (*Settings, error) {
	// threadsafe as long as no items are mutated
	items := m.informer.GetStore().List()
	commitClasses := make([]CommitClass, 0, len(items))
	for _, item := range items {
		unstructuredCC, ok := item.(*unstructured.Unstructured)
		if !ok {
			return nil, fmt.Errorf("unexpected CommitClass type %T", item)
		}
		bytes, err := unstructuredCC.MarshalJSON()
		if err != nil {
			return nil, fmt.Errorf("invalid CommitClass JSON %+v", err)
		}
		var cc CommitClass
		err = json.Unmarshal(bytes, &cc)
		if err != nil {
			return nil, fmt.Errorf("invalid CommitClass JSON %+v", err)
		}
		commitClasses = append(commitClasses, cc)
	}

	sort.Slice(commitClasses, func(i, j int) bool {
		return commitClasses[i].Name < commitClasses[j].Name
	})

	settings := NewSettings()
	for _, cc := range commitClasses {
		selector := cc.Spec.Selector
		// TODO(btyler) should any node fields be included? how should they be represented?
		matches := v1helper.MatchNodeSelectorTerms([]v1.NodeSelectorTerm{selector}, node.Labels, nil)
		// take the first matching CommitClass in lexical order, per the sort above
		if matches {
			for _, resource := range cc.Spec.Resources {
				if validResource(resource.Name) {
					settings.Set(resource.Name, resource.Percent)
				}
			}
			break
		}
	}

	return settings, nil
}

func validResource(resourceName string) bool {
	for _, allowed := range resourceWhitelist {
		if resourceName == string(allowed) {
			return true
		}
	}
	return false
}
