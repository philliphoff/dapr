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

package dts

import (
	"context"
	"fmt"
	"strconv"
	"testing"

	"github.com/dapr/dapr/tests/integration/framework/process/docker"
	"github.com/dapr/dapr/tests/integration/framework/process/ports"
)

const (
	// DTS emulator Docker image
	emulatorImage = "mcr.microsoft.com/dts/dts-emulator:latest"

	// Default container ports
	grpcContainerPort      = 8080
	dashboardContainerPort = 8082
)

// DTS manages a Durable Task Scheduler emulator container for integration tests.
type DTS struct {
	container *docker.Container
	grpcPort  int
	taskHub   string
}

type Option func(*options)

type options struct {
	taskHub string
}

// WithTaskHub sets the task hub name (default: "default").
func WithTaskHub(name string) Option {
	return func(o *options) {
		o.taskHub = name
	}
}

// New creates a new DTS emulator process.
func New(t *testing.T, fopts ...Option) *DTS {
	t.Helper()

	opts := options{taskHub: "default"}
	for _, fopt := range fopts {
		fopt(&opts)
	}

	// Reserve 2 ports: gRPC and dashboard
	fp := ports.Reserve(t, 2)
	grpcPort := fp.Port(t)
	dashPort := fp.Port(t)

	c := docker.New(t, emulatorImage, 0,
		docker.WithPort(grpcPort, grpcContainerPort),
		docker.WithPort(dashPort, dashboardContainerPort),
		docker.WithEnv("DTS_TASK_HUB_NAMES", opts.taskHub),
		docker.WithWaitPort(grpcPort),
	)

	return &DTS{
		container: c,
		grpcPort:  grpcPort,
		taskHub:   opts.taskHub,
	}
}

func (d *DTS) Run(t *testing.T, ctx context.Context) {
	d.container.Run(t, ctx)
}

func (d *DTS) Cleanup(t *testing.T) {
	d.container.Cleanup(t)
}

func (d *DTS) WaitUntilRunning(t *testing.T, ctx context.Context) {
	d.container.WaitUntilRunning(t, ctx)
}

// GRPCPort returns the host port mapped to the DTS gRPC endpoint.
func (d *DTS) GRPCPort() int {
	return d.grpcPort
}

// Address returns the gRPC address in host:port format.
func (d *DTS) Address() string {
	return "localhost:" + strconv.Itoa(d.grpcPort)
}

// ConnectionString returns a DTS connection string suitable for component metadata.
func (d *DTS) ConnectionString() string {
	return fmt.Sprintf("Endpoint=http://localhost:%d;Authentication=None", d.grpcPort)
}

// TaskHub returns the configured task hub name.
func (d *DTS) TaskHub() string {
	return d.taskHub
}

// GetComponent returns the Dapr component YAML for this DTS backend.
func (d *DTS) GetComponent(t *testing.T) string {
	t.Helper()
	return fmt.Sprintf(`apiVersion: dapr.io/v1alpha1
kind: Component
metadata:
  name: dts
spec:
  type: workflowbackend.durabletaskscheduler
  version: v1
  metadata:
  - name: endpoint
    value: "%s"
  - name: taskhub
    value: "%s"
`, d.ConnectionString(), d.taskHub)
}
