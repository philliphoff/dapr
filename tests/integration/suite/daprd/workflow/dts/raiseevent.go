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
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

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
	suite.Register(new(raiseevent))
}

// raiseevent tests raising an external event to a DTS-backed workflow.
type raiseevent struct {
	daprd *daprd.Daprd
	dts   *procdts.DTS
	sched *scheduler.Scheduler
	place *placement.Placement
}

func (r *raiseevent) Setup(t *testing.T) []framework.Option {
	return setupDTS(t, &r.dts, &r.sched, &r.place, &r.daprd)
}

func (r *raiseevent) Run(t *testing.T, ctx context.Context) {
	r.dts.WaitUntilRunning(t, ctx)
	r.sched.WaitUntilRunning(t, ctx)
	r.place.WaitUntilRunning(t, ctx)
	r.daprd.WaitUntilRunning(t, ctx)

	backendClient := client.NewTaskHubGrpcClient(r.daprd.GRPCConn(t, ctx), backend.DefaultLogger())

	var stage atomic.Int64

	reg := task.NewTaskRegistry()
	require.NoError(t, reg.AddOrchestratorN("EventOrch", func(ctx *task.OrchestrationContext) (any, error) {
		var input int
		ctx.GetInput(&input)
		ctx.CallActivity("Counter", task.WithActivityInput(input)).Await(nil)
		ctx.WaitForSingleEvent("testEvent", time.Minute).Await(nil)
		ctx.CallActivity("Counter", task.WithActivityInput(input)).Await(nil)
		return nil, nil
	}))
	require.NoError(t, reg.AddActivityN("Counter", func(c task.ActivityContext) (any, error) {
		var input int
		c.GetInput(&input)
		stage.Add(int64(input))
		return nil, nil
	}))

	taskhubCtx, cancelTaskhub := context.WithCancel(ctx)
	defer cancelTaskhub()
	require.NoError(t, backendClient.StartWorkItemListener(taskhubCtx, reg))

	id, err := backendClient.ScheduleNewOrchestration(ctx, "EventOrch",
		api.WithInstanceID("dts-raise-1"),
		api.WithInput(1),
	)
	require.NoError(t, err)

	// Wait for first activity to complete
	assert.EventuallyWithT(t, func(c *assert.CollectT) {
		assert.Equal(c, int64(1), stage.Load())
	}, time.Second*30, time.Millisecond*100)

	// Raise event
	require.NoError(t, backendClient.RaiseEvent(ctx, id, "testEvent"))

	// Wait for second activity to complete
	assert.EventuallyWithT(t, func(c *assert.CollectT) {
		assert.Equal(c, int64(2), stage.Load())
	}, time.Second*30, time.Millisecond*100)

	_, err = backendClient.WaitForOrchestrationCompletion(ctx, id)
	require.NoError(t, err)
}
