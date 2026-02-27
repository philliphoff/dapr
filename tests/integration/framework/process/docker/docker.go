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

package docker

import (
	"context"
	"fmt"
	"net"
	"os/exec"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/dapr/dapr/tests/integration/framework/process/ports"
)

// Container manages a Docker container lifecycle for integration tests.
type Container struct {
	image       string
	name        string
	envVars     map[string]string
	portMap     map[int]int // host port -> container port
	waitPort    int         // host port to wait for TCP readiness
	waitTimeout time.Duration
	freePort    *ports.Ports
}

type Option func(*Container)

// WithEnv sets an environment variable on the container.
func WithEnv(key, value string) Option {
	return func(c *Container) {
		c.envVars[key] = value
	}
}

// WithPort maps a host port to a container port.
func WithPort(hostPort, containerPort int) Option {
	return func(c *Container) {
		c.portMap[hostPort] = containerPort
	}
}

// WithWaitPort sets which host port to probe for TCP readiness.
func WithWaitPort(hostPort int) Option {
	return func(c *Container) {
		c.waitPort = hostPort
	}
}

// WithWaitTimeout sets the readiness timeout (default 60s).
func WithWaitTimeout(d time.Duration) Option {
	return func(c *Container) {
		c.waitTimeout = d
	}
}

// New creates a new Docker container process. If numPorts > 0, it reserves
// that many ports from the test port pool.
func New(t *testing.T, image string, numPorts int, opts ...Option) *Container {
	t.Helper()

	var fp *ports.Ports
	if numPorts > 0 {
		fp = ports.Reserve(t, numPorts)
	}
	c := &Container{
		image:       image,
		name:        fmt.Sprintf("dapr-inttest-%d", time.Now().UnixNano()),
		envVars:     make(map[string]string),
		portMap:     make(map[int]int),
		waitTimeout: 60 * time.Second,
		freePort:    fp,
	}
	for _, opt := range opts {
		opt(c)
	}
	return c
}

// Ports returns the reserved port pool for this container.
func (c *Container) Ports() *ports.Ports {
	return c.freePort
}

func (c *Container) Run(t *testing.T, ctx context.Context) {
	t.Helper()

	if c.freePort != nil {
		c.freePort.Free(t)
	}

	args := []string{"run", "--rm", "-d", "--name", c.name}
	for k, v := range c.envVars {
		args = append(args, "-e", k+"="+v)
	}
	for hp, cp := range c.portMap {
		args = append(args, "-p", strconv.Itoa(hp)+":"+strconv.Itoa(cp))
	}
	args = append(args, c.image)

	cmd := exec.CommandContext(ctx, "docker", args...)
	out, err := cmd.CombinedOutput()
	require.NoError(t, err, "docker run failed: %s", string(out))

	t.Logf("Started Docker container %s (image: %s)", c.name, c.image)
}

func (c *Container) Cleanup(t *testing.T) {
	t.Helper()

	cmd := exec.Command("docker", "rm", "-f", c.name)
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Logf("docker rm failed (may already be stopped): %s: %s", err, string(out))
	}
}

// WaitUntilRunning waits for the container's wait port to accept TCP connections.
func (c *Container) WaitUntilRunning(t *testing.T, ctx context.Context) {
	t.Helper()

	if c.waitPort == 0 {
		return
	}

	addr := net.JoinHostPort("localhost", strconv.Itoa(c.waitPort))
	deadline := time.Now().Add(c.waitTimeout)

	for time.Now().Before(deadline) {
		select {
		case <-ctx.Done():
			require.Fail(t, "context cancelled waiting for container", c.name)
		default:
		}
		conn, err := net.DialTimeout("tcp", addr, time.Second)
		if err == nil {
			conn.Close()
			t.Logf("Docker container %s is ready on port %d", c.name, c.waitPort)
			return
		}
		time.Sleep(500 * time.Millisecond)
	}
	require.Fail(t, fmt.Sprintf("container %s did not become ready on port %d within %s", c.name, c.waitPort, c.waitTimeout))
}
