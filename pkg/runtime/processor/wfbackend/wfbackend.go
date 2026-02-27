/*
Copyright 2026 The Dapr Authors
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

package wfbackend

import (
	"context"
	"sync"

	"github.com/dapr/durabletask-go/backend"

	componentsapi "github.com/dapr/dapr/pkg/apis/components/v1alpha1"
	compwfbackend "github.com/dapr/dapr/pkg/components/wfbackend"
	"github.com/dapr/dapr/pkg/runtime/compstore"
	"github.com/dapr/dapr/pkg/runtime/meta"
	"github.com/dapr/kit/logger"
)

var log = logger.NewLogger("dapr.runtime.processor.wfbackend")

// Options configures the workflow backend manager.
type Options struct {
	Registry       *compwfbackend.Registry
	ComponentStore *compstore.ComponentStore
	Meta           *meta.Meta
}

// wfbackend implements processor.WorkflowBackendManager and the component
// manager interface for initializing workflow backend components.
type wfbackend struct {
	registry *compwfbackend.Registry
	store    *compstore.ComponentStore
	meta     *meta.Meta

	lock    sync.RWMutex
	backend backend.Backend
	readyCh chan struct{}
	once    sync.Once
}

// New creates a workflow backend manager.
func New(opts Options) *wfbackend {
	return &wfbackend{
		registry: opts.Registry,
		store:    opts.ComponentStore,
		meta:     opts.Meta,
		readyCh:  make(chan struct{}),
	}
}

// Init implements the manager interface. It creates and stores a workflow backend.
func (w *wfbackend) Init(ctx context.Context, comp componentsapi.Component) error {
	w.lock.Lock()
	defer w.lock.Unlock()

	// Extract metadata properties as a flat map.
	props := make(map[string]string)
	for _, item := range comp.Spec.Metadata {
		props[item.Name] = item.Value.String()
	}

	fName := comp.LogName()
	be, err := w.registry.Create(ctx, comp.Spec.Type, comp.Spec.Version, fName, props)
	if err != nil {
		return err
	}

	w.backend = be
	w.store.AddWorkflowBackend(comp.Name, be)

	// Signal that the backend is ready.
	w.once.Do(func() { close(w.readyCh) })

	log.Infof("Initialized workflow backend %s (%s)", comp.Name, comp.Spec.Type)
	return nil
}

// Close implements the manager interface.
func (w *wfbackend) Close(comp componentsapi.Component) error {
	w.lock.Lock()
	defer w.lock.Unlock()

	w.store.DeleteWorkflowBackend(comp.Name)
	w.backend = nil
	return nil
}

// Backend implements processor.WorkflowBackendManager.
func (w *wfbackend) Backend() (backend.Backend, bool) {
	w.lock.RLock()
	defer w.lock.RUnlock()
	return w.backend, w.backend != nil
}

// Ready implements processor.WorkflowBackendManager.
func (w *wfbackend) Ready() <-chan struct{} {
	return w.readyCh
}

// MarkReady signals that no more workflow backend components will be loaded.
// Call this after all initial components have been processed.
func (w *wfbackend) MarkReady() {
	w.once.Do(func() { close(w.readyCh) })
}
