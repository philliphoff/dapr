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

	"github.com/dapr/dapr/tests/integration/framework"
	"github.com/dapr/dapr/tests/integration/framework/process/daprd"
	procdts "github.com/dapr/dapr/tests/integration/framework/process/dts"
	"github.com/dapr/dapr/tests/integration/framework/process/http/app"
	"github.com/dapr/dapr/tests/integration/framework/process/placement"
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

// basic tests a DTS-backed workflow that executes a single activity.
type basic struct {
	daprd *daprd.Daprd
	dts   *procdts.DTS
	sched *scheduler.Scheduler
	place *placement.Placement
}

func (b *basic) Setup(t *testing.T) []framework.Option {
	return setupDTS(t, &b.dts, &b.sched, &b.place, &b.daprd)
}

func (b *basic) Run(t *testing.T, ctx context.Context) {
	b.dts.WaitUntilRunning(t, ctx)
	b.sched.WaitUntilRunning(t, ctx)
	b.place.WaitUntilRunning(t, ctx)
	b.daprd.WaitUntilRunning(t, ctx)

	backendClient := client.NewTaskHubGrpcClient(b.daprd.GRPCConn(t, ctx), backend.DefaultLogger())

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

	// Retry scheduling — the DTS emulator's gRPC server may need a
	// moment after the TCP port becomes reachable.
	var id api.InstanceID
	var attempt atomic.Int32
	require.EventuallyWithT(t, func(c *assert.CollectT) {
		n := attempt.Add(1)
		instID := api.InstanceID(fmt.Sprintf("dts-basic-%d", n))
		var err error
		id, err = backendClient.ScheduleNewOrchestration(ctx, "SingleActivity",
			api.WithInstanceID(instID),
			api.WithInput("Dapr"),
		)
		assert.NoError(c, err)
	}, 30*time.Second, time.Second, "timed out waiting to schedule orchestration")

	metadata, err := backendClient.WaitForOrchestrationCompletion(ctx, id, api.WithFetchPayloads(true))
	require.NoError(t, err)
	assert.True(t, api.OrchestrationMetadataIsComplete(metadata))
	assert.Equal(t, `"Hello, Dapr!"`, metadata.GetOutput().GetValue())
}

// setupDTS creates the standard DTS test infrastructure: DTS emulator,
// scheduler, placement, app, and daprd. It assigns the created objects
// to the provided pointer targets so the caller can reference them.
func setupDTS(t *testing.T, dts **procdts.DTS, sched **scheduler.Scheduler, place **placement.Placement, d **daprd.Daprd) []framework.Option {
	t.Helper()
	a := app.New(t)
	*dts = procdts.New(t)
	*sched = scheduler.New(t)
	*place = placement.New(t)

	*d = daprd.New(t,
		daprd.WithAppPort(a.Port()),
		daprd.WithAppProtocol("http"),
		daprd.WithSchedulerAddresses((*sched).Address()),
		daprd.WithPlacementAddresses((*place).Address()),
		daprd.WithResourceFiles((*dts).GetComponent(t)),
	)

	return []framework.Option{
		framework.WithProcesses(*dts, *sched, *place, a, *d),
	}
}
