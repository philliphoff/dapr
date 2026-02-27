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
	"fmt"
	"strings"

	"github.com/dapr/durabletask-go/backend"
	"github.com/dapr/kit/logger"
)

// Factory creates a workflow backend from metadata properties.
type Factory func(ctx context.Context, metadata map[string]string, log logger.Logger) (backend.Backend, error)

// Registry holds registered workflow backend component factories.
type Registry struct {
	Logger   logger.Logger
	backends map[string]Factory
}

// DefaultRegistry is the singleton with the registry.
var DefaultRegistry *Registry = NewRegistry()

// NewRegistry creates a new workflow backend registry.
func NewRegistry() *Registry {
	return &Registry{
		Logger:   logger.NewLogger("dapr.wfbackend.registry"),
		backends: make(map[string]Factory),
	}
}

// RegisterComponent adds a new workflow backend to the registry.
func (r *Registry) RegisterComponent(factory Factory, names ...string) {
	for _, name := range names {
		r.backends[createFullName(name)] = factory
	}
}

// Create instantiates a registered workflow backend by name.
func (r *Registry) Create(ctx context.Context, name, version, logName string, metadata map[string]string) (backend.Backend, error) {
	nameLower := strings.ToLower(name)
	if factory, ok := r.backends[nameLower]; ok {
		l := r.Logger
		if logName != "" && l != nil {
			l = l.WithFields(map[string]any{"component": logName})
		}
		return factory(ctx, metadata, l)
	}
	return nil, fmt.Errorf("couldn't find workflow backend %s/%s", name, version)
}

func createFullName(name string) string {
	return strings.ToLower("workflowbackend." + name)
}
