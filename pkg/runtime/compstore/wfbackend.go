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

package compstore

import (
	"github.com/dapr/durabletask-go/backend"
)

func (c *ComponentStore) AddWorkflowBackend(name string, be backend.Backend) {
	c.lock.Lock()
	defer c.lock.Unlock()
	c.workflowBackends[name] = be
}

func (c *ComponentStore) GetWorkflowBackend(name string) (backend.Backend, bool) {
	c.lock.RLock()
	defer c.lock.RUnlock()
	be, ok := c.workflowBackends[name]
	return be, ok
}

func (c *ComponentStore) DeleteWorkflowBackend(name string) {
	c.lock.Lock()
	defer c.lock.Unlock()
	delete(c.workflowBackends, name)
}
