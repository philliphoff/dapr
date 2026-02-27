/*
Copyright 2023 The Dapr Authors
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

package wfengine

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"google.golang.org/grpc"

	"github.com/dapr/components-contrib/workflows"
	"github.com/dapr/dapr/pkg/actors"
	"github.com/dapr/dapr/pkg/actors/targets/workflow/orchestrator"
	"github.com/dapr/dapr/pkg/config"
	runtimev1pb "github.com/dapr/dapr/pkg/proto/runtime/v1"
	"github.com/dapr/dapr/pkg/resiliency"
	"github.com/dapr/dapr/pkg/runtime/compstore"
	"github.com/dapr/dapr/pkg/runtime/processor"
	backendactors "github.com/dapr/dapr/pkg/runtime/wfengine/backends/actors"
	"github.com/dapr/durabletask-go/backend"
	"github.com/dapr/kit/logger"
)

var (
	log             = logger.NewLogger("dapr.runtime.wfengine")
	wfBackendLogger = logger.NewLogger("dapr.wfengine.durabletask.backend")
)

type Interface interface {
	Run(context.Context) error
	RegisterGrpcServer(*grpc.Server)
	Client() workflows.Workflow
	RuntimeMetadata() *runtimev1pb.MetadataWorkflows

	ActivityActorType() string
}

type Options struct {
	AppID          string
	Namespace      string
	Actors         actors.Interface
	Spec           *config.WorkflowSpec
	BackendManager processor.WorkflowBackendManager
	Resiliency     resiliency.Provider
	EventSink      orchestrator.EventSink
	ComponentStore *compstore.ComponentStore

	EnableClusteredDeployment       bool
	WorkflowsRemoteActivityReminder bool
}

type engine struct {
	appID             string
	namespace         string
	actors            actors.Interface
	getWorkItemsCount *atomic.Int32

	worker        backend.TaskHubWorker
	actorsBackend *backendactors.Actors // nil when using external backend
	client        workflows.Workflow

	registerGrpcServerFn func(grpcServer grpc.ServiceRegistrar)

	// backendManager is checked in Run() to see if an external backend (e.g.
	// DTS) was loaded by the component processor. If so, the engine is
	// re-initialized with the external backend before starting the worker.
	backendManager processor.WorkflowBackendManager
	opts           Options
}

func New(opts Options) Interface {
	var retPolicy *config.WorkflowStateRetentionPolicy
	if opts.Spec != nil {
		retPolicy = opts.Spec.StateRetentionPolicy
	}

	// Always create the actors-based engine first. The external backend (if
	// any) will be wired in during Run(), after the component processor has
	// had a chance to initialize workflow backend components.
	e := newWithActorsBackend(opts, retPolicy)
	e.backendManager = opts.BackendManager
	e.opts = opts
	return e
}

// newWithExternalBackend creates an engine using a pre-configured external backend (e.g. DTS).
func newWithExternalBackend(opts Options, be backend.Backend) *engine {
	var getWorkItemsCount atomic.Int32
	executor, registerGrpcServerFn := backend.NewGrpcExecutor(be, log,
		backend.WithOnGetWorkItemsConnectionCallback(func(ctx context.Context) error {
			getWorkItemsCount.Add(1)
			return nil
		}),
		backend.WithOnGetWorkItemsDisconnectCallback(func(ctx context.Context) error {
			getWorkItemsCount.Add(-1)
			return nil
		}),
		backend.WithStreamSendTimeout(time.Second*10),
	)

	var topts []backend.NewTaskWorkerOptions
	if opts.Spec.GetMaxConcurrentWorkflowInvocations() != nil {
		topts = []backend.NewTaskWorkerOptions{
			backend.WithMaxParallelism(*opts.Spec.GetMaxConcurrentWorkflowInvocations()),
		}
	}

	oworker := backend.NewOrchestrationWorker(backend.OrchestratorOptions{
		Backend:  be,
		Executor: executor,
		Logger:   wfBackendLogger,
		AppID:    opts.AppID,
	}, topts...)

	topts = nil
	if opts.Spec.GetMaxConcurrentActivityInvocations() != nil {
		topts = []backend.NewTaskWorkerOptions{
			backend.WithMaxParallelism(*opts.Spec.GetMaxConcurrentActivityInvocations()),
		}
	}
	aworker := backend.NewActivityTaskWorker(be, executor, wfBackendLogger, topts...)
	worker := backend.NewTaskHubWorker(be, oworker, aworker, wfBackendLogger)

	return &engine{
		appID:                opts.AppID,
		namespace:            opts.Namespace,
		actors:               opts.Actors,
		worker:               worker,
		actorsBackend:        nil,
		registerGrpcServerFn: registerGrpcServerFn,
		getWorkItemsCount:    &getWorkItemsCount,
		client: &client{
			logger: wfBackendLogger,
			client: backend.NewTaskHubClient(be),
		},
	}
}

// newWithActorsBackend creates an engine using the built-in actors-based backend.
func newWithActorsBackend(opts Options, retPolicy *config.WorkflowStateRetentionPolicy) *engine {
	abackend := backendactors.New(backendactors.Options{
		AppID:           opts.AppID,
		Namespace:       opts.Namespace,
		Actors:          opts.Actors,
		Resiliency:      opts.Resiliency,
		EventSink:       opts.EventSink,
		ComponentStore:  opts.ComponentStore,
		RetentionPolicy: retPolicy,

		EnableClusteredDeployment:       opts.EnableClusteredDeployment,
		WorkflowsRemoteActivityReminder: opts.WorkflowsRemoteActivityReminder,
	})

	var getWorkItemsCount atomic.Int32
	var lock sync.Mutex
	executor, registerGrpcServerFn := backend.NewGrpcExecutor(abackend, log,
		backend.WithOnGetWorkItemsConnectionCallback(func(ctx context.Context) error {
			lock.Lock()
			defer lock.Unlock()

			if getWorkItemsCount.Add(1) == 1 {
				log.Debug("Registering workflow actors")
				return abackend.RegisterActors(ctx)
			}

			return nil
		}),
		backend.WithOnGetWorkItemsDisconnectCallback(func(ctx context.Context) error {
			lock.Lock()
			defer lock.Unlock()

			if ctx.Err() != nil {
				ctx = context.Background()
			}

			if getWorkItemsCount.Add(-1) == 0 {
				log.Debug("Unregistering workflow actors")
				return abackend.UnRegisterActors(ctx)
			}

			return nil
		}),
		backend.WithStreamSendTimeout(time.Second*10),
	)

	var topts []backend.NewTaskWorkerOptions
	if opts.Spec.GetMaxConcurrentWorkflowInvocations() != nil {
		topts = []backend.NewTaskWorkerOptions{
			backend.WithMaxParallelism(*opts.Spec.GetMaxConcurrentWorkflowInvocations()),
		}
	}

	// There are separate "workers" for executing orchestrations (workflows) and activities
	oworker := backend.NewOrchestrationWorker(backend.OrchestratorOptions{
		Backend:  abackend,
		Executor: executor,
		Logger:   wfBackendLogger,
		AppID:    opts.AppID,
	}, topts...)

	topts = nil
	if opts.Spec.GetMaxConcurrentActivityInvocations() != nil {
		topts = []backend.NewTaskWorkerOptions{
			backend.WithMaxParallelism(*opts.Spec.GetMaxConcurrentActivityInvocations()),
		}
	}
	aworker := backend.NewActivityTaskWorker(
		abackend,
		executor,
		wfBackendLogger,
		topts...,
	)
	worker := backend.NewTaskHubWorker(abackend, oworker, aworker, wfBackendLogger)

	return &engine{
		appID:                opts.AppID,
		namespace:            opts.Namespace,
		actors:               opts.Actors,
		worker:               worker,
		actorsBackend:        abackend,
		registerGrpcServerFn: registerGrpcServerFn,
		getWorkItemsCount:    &getWorkItemsCount,
		client: &client{
			logger: wfBackendLogger,
			client: backend.NewTaskHubClient(abackend),
		},
	}
}

func (wfe *engine) RegisterGrpcServer(server *grpc.Server) {
	wfe.registerGrpcServerFn(server)
}

func (wfe *engine) Run(ctx context.Context) error {
	// Wait for the component processor to finish loading initial components.
	// This ensures any configured workflow backend (e.g. DTS) has been
	// initialized before we decide which backend to use.
	if wfe.backendManager != nil {
		select {
		case <-wfe.backendManager.Ready():
		case <-ctx.Done():
			return ctx.Err()
		}

		if externalBackend, ok := wfe.backendManager.Backend(); ok && externalBackend != nil {
			log.Info("Using external workflow backend instead of actors")
			ext := newWithExternalBackend(wfe.opts, externalBackend)
			wfe.worker = ext.worker
			wfe.actorsBackend = nil
			wfe.client = ext.client
			wfe.getWorkItemsCount = ext.getWorkItemsCount
			wfe.registerGrpcServerFn = ext.registerGrpcServerFn
		}
	}

	// For actor-based backends, wait for the actor router to be available.
	// External backends (e.g. DTS) don't need actors.
	if wfe.actorsBackend != nil {
		_, err := wfe.actors.Router(ctx)
		if err != nil {
			<-ctx.Done()
			return ctx.Err()
		}
	}

	// Start the Durable Task worker, which will allow workflows to be scheduled and execute.
	if err := wfe.worker.Start(ctx); err != nil {
		return fmt.Errorf("failed to start workflow engine: %w", err)
	}

	log.Info("Workflow engine started")
	<-ctx.Done()

	if err := wfe.worker.Shutdown(context.Background()); err != nil {
		return fmt.Errorf("failed to shutdown the workflow worker: %w", err)
	}

	log.Info("Workflow engine stopped")
	return nil
}

func (wfe *engine) Client() workflows.Workflow {
	return wfe.client
}

func (wfe *engine) ActivityActorType() string {
	if wfe.actorsBackend != nil {
		return wfe.actorsBackend.ActivityActorType()
	}
	return ""
}

func (wfe *engine) RuntimeMetadata() *runtimev1pb.MetadataWorkflows {
	return &runtimev1pb.MetadataWorkflows{
		ConnectedWorkers: wfe.getWorkItemsCount.Load(),
	}
}
