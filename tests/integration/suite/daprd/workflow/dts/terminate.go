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
	"github.com/dapr/durabletask-go/backend"
	"github.com/dapr/durabletask-go/client"
	"github.com/dapr/durabletask-go/task"
)

func init() {
	suite.Register(new(terminate))
}

// terminate tests terminating a DTS-backed workflow via the Dapr API.
type terminate struct {
	daprd *daprd.Daprd
	dts   *procdts.DTS
	sched *scheduler.Scheduler
	place *placement.Placement
}

func (te *terminate) Setup(t *testing.T) []framework.Option {
	return setupDTS(t, &te.dts, &te.sched, &te.place, &te.daprd)
}

func (te *terminate) Run(t *testing.T, ctx context.Context) {
	te.dts.WaitUntilRunning(t, ctx)
	te.sched.WaitUntilRunning(t, ctx)
	te.place.WaitUntilRunning(t, ctx)
	te.daprd.WaitUntilRunning(t, ctx)

	gclient := te.daprd.GRPCClient(t, ctx)
	backendClient := client.NewTaskHubGrpcClient(te.daprd.GRPCConn(t, ctx), backend.DefaultLogger())

	reg := task.NewTaskRegistry()
	require.NoError(t, reg.AddOrchestratorN("LongRunning", func(ctx *task.OrchestrationContext) (any, error) {
		ctx.WaitForSingleEvent("never", time.Hour).Await(nil)
		return nil, nil
	}))

	taskhubCtx, cancelTaskhub := context.WithCancel(ctx)
	defer cancelTaskhub()
	require.NoError(t, backendClient.StartWorkItemListener(taskhubCtx, reg))

	resp, err := gclient.StartWorkflowBeta1(ctx, &rtv1.StartWorkflowRequest{
		WorkflowComponent: "dts",
		WorkflowName:      "LongRunning",
		InstanceId:        "dts-terminate-1",
	})
	require.NoError(t, err)
	assert.Equal(t, "dts-terminate-1", resp.GetInstanceId())

	// Wait for running
	assert.EventuallyWithT(t, func(c *assert.CollectT) {
		get, err := gclient.GetWorkflowBeta1(ctx, &rtv1.GetWorkflowRequest{
			InstanceId:        "dts-terminate-1",
			WorkflowComponent: "dts",
		})
		assert.NoError(c, err)
		assert.Equal(c, "RUNNING", get.GetRuntimeStatus())
	}, time.Second*30, time.Millisecond*500)

	// Terminate
	_, err = gclient.TerminateWorkflowBeta1(ctx, &rtv1.TerminateWorkflowRequest{
		InstanceId:        "dts-terminate-1",
		WorkflowComponent: "dts",
	})
	require.NoError(t, err)

	assert.EventuallyWithT(t, func(c *assert.CollectT) {
		get, err := gclient.GetWorkflowBeta1(ctx, &rtv1.GetWorkflowRequest{
			InstanceId:        "dts-terminate-1",
			WorkflowComponent: "dts",
		})
		assert.NoError(c, err)
		assert.Equal(c, "TERMINATED", get.GetRuntimeStatus())
	}, time.Second*30, time.Millisecond*500)
}
