/*
Copyright The Volcano Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package workloadmanager

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	sandboxv1alpha1 "sigs.k8s.io/agent-sandbox/api/v1alpha1"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

// newTestReconciler creates a SandboxReconciler with a fake client for unit tests.
func newTestReconciler(objs ...runtime.Object) *SandboxReconciler {
	scheme := runtime.NewScheme()
	_ = sandboxv1alpha1.AddToScheme(scheme)
	builder := fake.NewClientBuilder().WithScheme(scheme)
	for _, obj := range objs {
		builder = builder.WithRuntimeObjects(obj)
	}
	return &SandboxReconciler{
		Client: builder.Build(),
		Scheme: scheme,
	}
}

// readySandboxCR returns a Sandbox CRD with the Ready condition set to True.
func readySandboxCR(namespace, name string) *sandboxv1alpha1.Sandbox {
	return &sandboxv1alpha1.Sandbox{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: namespace,
		},
		Status: sandboxv1alpha1.SandboxStatus{
			Conditions: []metav1.Condition{{
				Type:   string(sandboxv1alpha1.SandboxConditionReady),
				Status: metav1.ConditionTrue,
			}},
		},
	}
}

// notReadySandboxCR returns a Sandbox CRD that is not yet ready.
func notReadySandboxCR(namespace, name string) *sandboxv1alpha1.Sandbox {
	return &sandboxv1alpha1.Sandbox{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: namespace,
		},
		Status: sandboxv1alpha1.SandboxStatus{
			Conditions: []metav1.Condition{{
				Type:   string(sandboxv1alpha1.SandboxConditionReady),
				Status: metav1.ConditionFalse,
			}},
		},
	}
}

func TestReconcile_NotifiesWaiter(t *testing.T) {
	sb := readySandboxCR("default", "test-sandbox")
	r := newTestReconciler(sb)

	// Register a watcher before reconciling
	ch := r.WatchSandboxOnce(context.Background(), "default", "test-sandbox")

	// Reconcile should detect the Ready sandbox and notify the watcher
	result, err := r.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Namespace: "default", Name: "test-sandbox"},
	})
	require.NoError(t, err)
	assert.Equal(t, ctrl.Result{}, result)

	// The channel should receive the sandbox update
	select {
	case update := <-ch:
		assert.Equal(t, "test-sandbox", update.Sandbox.Name)
		assert.Equal(t, "default", update.Sandbox.Namespace)
	case <-time.After(time.Second):
		t.Fatal("expected sandbox status update on channel, but timed out")
	}

	// Watcher should be removed after notification — verify by checking that
	// a second Reconcile does not panic or send again
	result, err = r.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Namespace: "default", Name: "test-sandbox"},
	})
	require.NoError(t, err)
	assert.Equal(t, ctrl.Result{}, result)
}

func TestReconcile_NoWatcher(t *testing.T) {
	sb := readySandboxCR("default", "test-sandbox")
	r := newTestReconciler(sb)

	// Reconcile without a registered watcher — should not panic
	result, err := r.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Namespace: "default", Name: "test-sandbox"},
	})
	require.NoError(t, err)
	assert.Equal(t, ctrl.Result{}, result)
}

func TestReconcile_NotReady(t *testing.T) {
	sb := notReadySandboxCR("default", "test-sandbox")
	r := newTestReconciler(sb)

	// Register a watcher
	ch := r.WatchSandboxOnce(context.Background(), "default", "test-sandbox")

	// Reconcile with a non-Ready sandbox should not notify the watcher
	result, err := r.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Namespace: "default", Name: "test-sandbox"},
	})
	require.NoError(t, err)
	assert.Equal(t, ctrl.Result{}, result)

	// The channel should NOT receive any update
	select {
	case <-ch:
		t.Fatal("watcher should not have been notified for a non-ready sandbox")
	case <-time.After(50 * time.Millisecond):
		// Expected: no notification
	}
}

func TestReconcile_SandboxNotFound(t *testing.T) {
	// Create reconciler with no objects — sandbox doesn't exist
	r := newTestReconciler()

	// Reconcile a non-existent sandbox should be silently ignored (IgnoreNotFound)
	result, err := r.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Namespace: "default", Name: "nonexistent"},
	})
	require.NoError(t, err)
	assert.Equal(t, ctrl.Result{}, result)
}

func TestWatchAndUnWatch(t *testing.T) {
	sb := readySandboxCR("default", "test-sandbox")
	r := newTestReconciler(sb)

	// Register a watcher, then immediately unwatch
	ch := r.WatchSandboxOnce(context.Background(), "default", "test-sandbox")
	r.UnWatchSandbox("default", "test-sandbox")

	// Reconcile should NOT send because the watcher was removed
	result, err := r.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Namespace: "default", Name: "test-sandbox"},
	})
	require.NoError(t, err)
	assert.Equal(t, ctrl.Result{}, result)

	select {
	case <-ch:
		t.Fatal("watcher should not have been notified after UnWatchSandbox")
	case <-time.After(50 * time.Millisecond):
		// Expected: no notification
	}
}

func TestWatchSandboxOnce_InitializesMap(t *testing.T) {
	r := &SandboxReconciler{}
	assert.Nil(t, r.watchers)

	// WatchSandboxOnce should lazily initialize the watchers map
	ch := r.WatchSandboxOnce(context.Background(), "default", "test-sandbox")
	assert.NotNil(t, r.watchers)
	assert.NotNil(t, ch)
}

func TestWatchSandboxOnce_OverwritesPreviousWatcher(t *testing.T) {
	sb := readySandboxCR("default", "test-sandbox")
	r := newTestReconciler(sb)

	// Register first watcher
	ch1 := r.WatchSandboxOnce(context.Background(), "default", "test-sandbox")

	// Register second watcher for the same key — should overwrite
	ch2 := r.WatchSandboxOnce(context.Background(), "default", "test-sandbox")

	// Reconcile should notify ch2 (the latest watcher), not ch1
	_, err := r.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Namespace: "default", Name: "test-sandbox"},
	})
	require.NoError(t, err)

	select {
	case update := <-ch2:
		assert.Equal(t, "test-sandbox", update.Sandbox.Name)
	case <-time.After(time.Second):
		t.Fatal("expected notification on ch2")
	}

	// ch1 should NOT receive anything
	select {
	case <-ch1:
		t.Fatal("ch1 should not have been notified after being overwritten")
	case <-time.After(50 * time.Millisecond):
		// Expected
	}
}

func TestUnWatchSandbox_NonExistentKey(t *testing.T) {
	r := newTestReconciler()

	// UnWatchSandbox on a key that was never registered should not panic
	r.UnWatchSandbox("default", "nonexistent")
}

func TestConcurrentWatchAndReconcile(t *testing.T) {
	sb := readySandboxCR("default", "test-sandbox")
	r := newTestReconciler(sb)

	const goroutines = 20
	var wg sync.WaitGroup
	wg.Add(goroutines)

	// Run WatchSandboxOnce, Reconcile, and UnWatchSandbox concurrently
	// to verify no data races exist under the race detector.
	for i := 0; i < goroutines; i++ {
		go func(idx int) {
			defer wg.Done()
			switch idx % 3 {
			case 0:
				r.WatchSandboxOnce(context.Background(), "default", "test-sandbox")
			case 1:
				_, _ = r.Reconcile(context.Background(), ctrl.Request{
					NamespacedName: types.NamespacedName{Namespace: "default", Name: "test-sandbox"},
				})
			case 2:
				r.UnWatchSandbox("default", "test-sandbox")
			}
		}(i)
	}

	wg.Wait()
}

func TestReconcile_MultipleNamespaces(t *testing.T) {
	sb1 := readySandboxCR("ns-a", "sandbox-1")
	sb2 := readySandboxCR("ns-b", "sandbox-2")
	r := newTestReconciler(sb1, sb2)

	// Register watchers for both sandboxes
	ch1 := r.WatchSandboxOnce(context.Background(), "ns-a", "sandbox-1")
	ch2 := r.WatchSandboxOnce(context.Background(), "ns-b", "sandbox-2")

	// Reconcile only sandbox-1
	_, err := r.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Namespace: "ns-a", Name: "sandbox-1"},
	})
	require.NoError(t, err)

	// ch1 should receive the update
	select {
	case update := <-ch1:
		assert.Equal(t, "sandbox-1", update.Sandbox.Name)
		assert.Equal(t, "ns-a", update.Sandbox.Namespace)
	case <-time.After(time.Second):
		t.Fatal("expected notification on ch1")
	}

	// ch2 should NOT have been notified yet
	select {
	case <-ch2:
		t.Fatal("ch2 should not have been notified yet")
	case <-time.After(50 * time.Millisecond):
		// Expected
	}
}
