//go:build allcomponents || stablecomponents

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

package components

import (
	"context"

	"github.com/dapr/durabletask-go/backend"

	wfbackendLoader "github.com/dapr/dapr/pkg/components/wfbackend"
	dtsbackend "github.com/dapr/dapr/pkg/runtime/wfengine/backends/dts"
	"github.com/dapr/kit/logger"
)

func init() {
	wfbackendLoader.DefaultRegistry.RegisterComponent(func(ctx context.Context, metadata map[string]string, log logger.Logger) (backend.Backend, error) {
		connStr := metadata[dtsbackend.MetadataKeyEndpoint]
		taskHub := metadata[dtsbackend.MetadataKeyTaskHub]

		opts, err := dtsbackend.ParseConnectionString(connStr, taskHub)
		if err != nil {
			return nil, err
		}

		return dtsbackend.New(opts)
	}, "durabletaskscheduler")
}
