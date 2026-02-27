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
	suite.Register(new(purge))
}

// purge tests purging a completed DTS-backed workflow instance.
type purge struct {
	daprd *daprd.Daprd
	dts   *procdts.DTS
	sched *scheduler.Scheduler
	place *placement.Placement
}

func (p *purge) Setup(t *testing.T) []framework.Option {
	return setupDTS(t, &p.dts, &p.sched, &p.place, &p.daprd)
}

func (p *purge) Run(t *testing.T, ctx context.Context) {
	p.dts.WaitUntilRunning(t, ctx)
	p.sched.WaitUntilRunning(t, ctx)
	p.place.WaitUntilRunning(t, ctx)
	p.daprd.WaitUntilRunning(t, ctx)

	gclient := p.daprd.GRPCClient(t, ctx)
	backendClient := client.NewTaskHubGrpcClient(p.daprd.GRPCConn(t, ctx), backend.DefaultLogger())

	reg := task.NewTaskRegistry()
	require.NoError(t, reg.AddOrchestratorN("PurgeOrch", func(ctx *task.OrchestrationContext) (any, error) {
		return "done", nil
	}))

	taskhubCtx, cancelTaskhub := context.WithCancel(ctx)
	defer cancelTaskhub()
	require.NoError(t, backendClient.StartWorkItemListener(taskhubCtx, reg))

	// Retry scheduling — the DTS emulator may need a moment to be fully ready.
	var id api.InstanceID
	require.EventuallyWithT(t, func(c *assert.CollectT) {
		var err error
		id, err = backendClient.ScheduleNewOrchestration(ctx, "PurgeOrch",
			api.WithInstanceID("dts-purge-1"),
		)
		assert.NoError(c, err)
	}, 30*time.Second, time.Second)

	_, err := backendClient.WaitForOrchestrationCompletion(ctx, id)
	require.NoError(t, err)

	// Purge
	_, err = gclient.PurgeWorkflowBeta1(ctx, &rtv1.PurgeWorkflowRequest{
		InstanceId:        "dts-purge-1",
		WorkflowComponent: "dts",
	})
	require.NoError(t, err)

	// Verify purged — the Dapr API returns an empty response (not error)
	// for instances that don't exist. Poll because DTS may process the
	// purge asynchronously.
	assert.EventuallyWithT(t, func(c *assert.CollectT) {
		get, err := gclient.GetWorkflowBeta1(ctx, &rtv1.GetWorkflowRequest{
			InstanceId:        "dts-purge-1",
			WorkflowComponent: "dts",
		})
		assert.NoError(c, err)
		assert.Empty(c, get.GetWorkflowName(), "expected empty workflow name after purge")
		assert.Empty(c, get.GetRuntimeStatus(), "expected empty runtime status after purge")
	}, 30*time.Second, time.Millisecond*500)
}
