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
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	rtv1 "github.com/dapr/dapr/pkg/proto/runtime/v1"
	"github.com/dapr/dapr/tests/integration/framework"
	"github.com/dapr/dapr/tests/integration/framework/process/daprd"
	procdts "github.com/dapr/dapr/tests/integration/framework/process/dts"
	"github.com/dapr/dapr/tests/integration/framework/process/placement"
	"github.com/dapr/dapr/tests/integration/framework/process/scheduler"
	"github.com/dapr/dapr/tests/integration/suite"
	"github.com/dapr/durabletask-go/api"
	"github.com/dapr/durabletask-go/backend"
	"github.com/dapr/durabletask-go/client"
	"github.com/dapr/durabletask-go/task"
)

func init() {
	suite.Register(new(pauseresume))
}

// pauseresume tests DTS-backed workflow pause, resume, and completion via event.
type pauseresume struct {
	daprd *daprd.Daprd
	dts   *procdts.DTS
	sched *scheduler.Scheduler
	place *placement.Placement
}

func (p *pauseresume) Setup(t *testing.T) []framework.Option {
	return setupDTS(t, &p.dts, &p.sched, &p.place, &p.daprd)
}

func (p *pauseresume) Run(t *testing.T, ctx context.Context) {
	p.dts.WaitUntilRunning(t, ctx)
	p.sched.WaitUntilRunning(t, ctx)
	p.place.WaitUntilRunning(t, ctx)
	p.daprd.WaitUntilRunning(t, ctx)

	gclient := p.daprd.GRPCClient(t, ctx)
	backendClient := client.NewTaskHubGrpcClient(p.daprd.GRPCConn(t, ctx), backend.DefaultLogger())

	reg := task.NewTaskRegistry()
	require.NoError(t, reg.AddOrchestratorN("Pauser", func(ctx *task.OrchestrationContext) (any, error) {
		ctx.WaitForSingleEvent("continue", time.Minute).Await(nil)
		return "completed", nil
	}))

	taskhubCtx, cancelTaskhub := context.WithCancel(ctx)
	defer cancelTaskhub()
	require.NoError(t, backendClient.StartWorkItemListener(taskhubCtx, reg))

	// Start workflow via gRPC API
	resp, err := gclient.StartWorkflowBeta1(ctx, &rtv1.StartWorkflowRequest{
		WorkflowComponent: "dts",
		WorkflowName:      "Pauser",
		InstanceId:        "dts-pause-1",
	})
	require.NoError(t, err)
	assert.Equal(t, "dts-pause-1", resp.GetInstanceId())

	// Wait for it to be running
	assert.EventuallyWithT(t, func(c *assert.CollectT) {
		get, err := gclient.GetWorkflowBeta1(ctx, &rtv1.GetWorkflowRequest{
			InstanceId:        "dts-pause-1",
			WorkflowComponent: "dts",
		})
		assert.NoError(c, err)
		assert.Equal(c, "RUNNING", get.GetRuntimeStatus())
	}, time.Second*30, time.Millisecond*500)

	// Pause
	_, err = gclient.PauseWorkflowBeta1(ctx, &rtv1.PauseWorkflowRequest{
		InstanceId:        "dts-pause-1",
		WorkflowComponent: "dts",
	})
	require.NoError(t, err)

	assert.EventuallyWithT(t, func(c *assert.CollectT) {
		get, err := gclient.GetWorkflowBeta1(ctx, &rtv1.GetWorkflowRequest{
			InstanceId:        "dts-pause-1",
			WorkflowComponent: "dts",
		})
		assert.NoError(c, err)
		assert.Equal(c, "SUSPENDED", get.GetRuntimeStatus())
	}, time.Second*30, time.Millisecond*500)

	// Resume
	_, err = gclient.ResumeWorkflowBeta1(ctx, &rtv1.ResumeWorkflowRequest{
		InstanceId:        "dts-pause-1",
		WorkflowComponent: "dts",
	})
	require.NoError(t, err)

	assert.EventuallyWithT(t, func(c *assert.CollectT) {
		get, err := gclient.GetWorkflowBeta1(ctx, &rtv1.GetWorkflowRequest{
			InstanceId:        "dts-pause-1",
			WorkflowComponent: "dts",
		})
		assert.NoError(c, err)
		assert.Equal(c, "RUNNING", get.GetRuntimeStatus())
	}, time.Second*30, time.Millisecond*500)

	// Raise event to complete
	_, err = gclient.RaiseEventWorkflowBeta1(ctx, &rtv1.RaiseEventWorkflowRequest{
		InstanceId:        "dts-pause-1",
		WorkflowComponent: "dts",
		EventName:         "continue",
	})
	require.NoError(t, err)

	_, err = backendClient.WaitForOrchestrationCompletion(ctx, api.InstanceID("dts-pause-1"))
	require.NoError(t, err)
}
