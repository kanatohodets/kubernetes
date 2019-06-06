package commitclass

import (
	"encoding/json"
	"fmt"
	"sort"

	"k8s.io/api/core/v1"
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
	Resources []ResourceCommitFactor `json:"resources" protobuf:"bytes,3,name=resources"`
}

type ResourceCommitFactor struct {
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

type ResourceScales map[string]float64
type Manager struct {
	informer cache.SharedInformer
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

func (m *Manager) GetActiveCommitClass(node *v1.Node) (ResourceScales, error) {
	// threadsafe as long as no items are mutated
	items := m.informer.GetStore().List()
	scales := ResourceScales{}
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

	for _, cc := range commitClasses {
		selector := cc.Spec.Selector
		// TODO(btyler) should any node fields be included? how should they be represented?
		matches := v1helper.MatchNodeSelectorTerms([]v1.NodeSelectorTerm{selector}, node.Labels, nil)
		// take the first matching CommitClass in lexical order
		if matches {
			for _, resource := range cc.Spec.Resources {
				scaleFactor := float64(resource.Percent) / 100
				scales[resource.Name] = scaleFactor
			}
			break
		}
	}

	return scales, nil
}
