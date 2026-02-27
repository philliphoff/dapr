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
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	rtv1 "github.com/dapr/dapr/pkg/proto/runtime/v1"
	"github.com/dapr/dapr/tests/integration/framework"
	"github.com/dapr/dapr/tests/integration/framework/process/daprd"
	procdts "github.com/dapr/dapr/tests/integration/framework/process/dts"
	"github.com/dapr/dapr/tests/integration/framework/process/http/app"
	"github.com/dapr/dapr/tests/integration/framework/process/scheduler"
	"github.com/dapr/dapr/tests/integration/suite"
	"github.com/dapr/durabletask-go/api"
	"github.com/dapr/durabletask-go/backend"
	"github.com/dapr/durabletask-go/client"
	"github.com/dapr/durabletask-go/task"
)

func init() {
	suite.Register(new(basic))
}

// basic tests DTS-backed workflows: basic execution, pause/resume, raise event, terminate, purge.
type basic struct {
	daprd *daprd.Daprd
	dts   *procdts.DTS
	sched *scheduler.Scheduler
}

func (b *basic) Setup(t *testing.T) []framework.Option {
	a := app.New(t)
	b.dts = procdts.New(t)
	b.sched = scheduler.New(t)

	b.daprd = daprd.New(t,
		daprd.WithAppPort(a.Port()),
		daprd.WithAppProtocol("http"),
		daprd.WithSchedulerAddresses(b.sched.Address()),
		daprd.WithResourceFiles(b.dts.GetComponent(t)),
	)

	return []framework.Option{
		framework.WithProcesses(b.dts, b.sched, a, b.daprd),
	}
}

func (b *basic) Run(t *testing.T, ctx context.Context) {
	b.dts.WaitUntilRunning(t, ctx)
	b.sched.WaitUntilRunning(t, ctx)
	b.daprd.WaitUntilRunning(t, ctx)

	gclient := b.daprd.GRPCClient(t, ctx)
	backendClient := client.NewTaskHubGrpcClient(b.daprd.GRPCConn(t, ctx), backend.DefaultLogger())

	t.Run("basic_single_activity", func(t *testing.T) {
		reg := task.NewTaskRegistry()
		require.NoError(t, reg.AddOrchestratorN("SingleActivity", func(ctx *task.OrchestrationContext) (any, error) {
			var input string
			if err := ctx.GetInput(&input); err != nil {
				return nil, err
			}
			var output string
			err := ctx.CallActivity("SayHello", task.WithActivityInput(input)).Await(&output)
			return output, err
		}))
		require.NoError(t, reg.AddActivityN("SayHello", func(ctx task.ActivityContext) (any, error) {
			var name string
			if err := ctx.GetInput(&name); err != nil {
				return nil, err
			}
			return fmt.Sprintf("Hello, %s!", name), nil
		}))

		taskhubCtx, cancelTaskhub := context.WithCancel(ctx)
		defer cancelTaskhub()
		require.NoError(t, backendClient.StartWorkItemListener(taskhubCtx, reg))

		id, err := backendClient.ScheduleNewOrchestration(ctx, "SingleActivity",
			api.WithInstanceID("dts-basic-1"),
			api.WithInput("Dapr"),
		)
		require.NoError(t, err)

		metadata, err := backendClient.WaitForOrchestrationCompletion(ctx, id, api.WithFetchPayloads(true))
		require.NoError(t, err)
		assert.True(t, api.OrchestrationMetadataIsComplete(metadata))
		assert.Equal(t, `"Hello, Dapr!"`, metadata.GetOutput().GetValue())
	})

	t.Run("pause_resume", func(t *testing.T) {
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
		}, time.Second*20, time.Millisecond*100)

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
		}, time.Second*10, time.Millisecond*100)

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
		}, time.Second*10, time.Millisecond*100)

		// Raise event to complete
		_, err = gclient.RaiseEventWorkflowBeta1(ctx, &rtv1.RaiseEventWorkflowRequest{
			InstanceId:        "dts-pause-1",
			WorkflowComponent: "dts",
			EventName:         "continue",
		})
		require.NoError(t, err)

		_, err = backendClient.WaitForOrchestrationCompletion(ctx, api.InstanceID("dts-pause-1"))
		require.NoError(t, err)
	})

	t.Run("raise_event", func(t *testing.T) {
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
		}, time.Second*20, time.Millisecond*100)

		// Raise event
		require.NoError(t, backendClient.RaiseEvent(ctx, id, "testEvent"))

		// Wait for second activity to complete
		assert.EventuallyWithT(t, func(c *assert.CollectT) {
			assert.Equal(c, int64(2), stage.Load())
		}, time.Second*20, time.Millisecond*100)

		_, err = backendClient.WaitForOrchestrationCompletion(ctx, id)
		require.NoError(t, err)
	})

	t.Run("terminate", func(t *testing.T) {
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
		}, time.Second*20, time.Millisecond*100)

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
		}, time.Second*10, time.Millisecond*100)
	})

	t.Run("purge", func(t *testing.T) {
		reg := task.NewTaskRegistry()
		require.NoError(t, reg.AddOrchestratorN("PurgeOrch", func(ctx *task.OrchestrationContext) (any, error) {
			return "done", nil
		}))

		taskhubCtx, cancelTaskhub := context.WithCancel(ctx)
		defer cancelTaskhub()
		require.NoError(t, backendClient.StartWorkItemListener(taskhubCtx, reg))

		id, err := backendClient.ScheduleNewOrchestration(ctx, "PurgeOrch",
			api.WithInstanceID("dts-purge-1"),
		)
		require.NoError(t, err)

		_, err = backendClient.WaitForOrchestrationCompletion(ctx, id)
		require.NoError(t, err)

		// Purge
		_, err = gclient.PurgeWorkflowBeta1(ctx, &rtv1.PurgeWorkflowRequest{
			InstanceId:        "dts-purge-1",
			WorkflowComponent: "dts",
		})
		require.NoError(t, err)

		// Verify purged - should get not found
		_, err = gclient.GetWorkflowBeta1(ctx, &rtv1.GetWorkflowRequest{
			InstanceId:        "dts-purge-1",
			WorkflowComponent: "dts",
		})
		require.Error(t, err)
	})
}
