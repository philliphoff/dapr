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

package wfengine

import (
	"context"
	"sync"

	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/emptypb"

	"github.com/dapr/durabletask-go/api/protos"
)

// sidecarServiceProxy is a TaskHubSidecarServiceServer that delegates all
// calls to a swappable underlying implementation. This allows the gRPC
// service to be registered once at startup (with the actors-based executor)
// and then transparently switch to a DTS-based executor at runtime.
type sidecarServiceProxy struct {
	protos.UnimplementedTaskHubSidecarServiceServer
	mu     sync.RWMutex
	target protos.TaskHubSidecarServiceServer
}

func newSidecarServiceProxy(initial protos.TaskHubSidecarServiceServer) *sidecarServiceProxy {
	return &sidecarServiceProxy{target: initial}
}

func (p *sidecarServiceProxy) swap(next protos.TaskHubSidecarServiceServer) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.target = next
}

func (p *sidecarServiceProxy) get() protos.TaskHubSidecarServiceServer {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.target
}

func (p *sidecarServiceProxy) register(server grpc.ServiceRegistrar) {
	protos.RegisterTaskHubSidecarServiceServer(server, p)
}

// --- Forwarding methods ---

func (p *sidecarServiceProxy) Hello(ctx context.Context, req *emptypb.Empty) (*emptypb.Empty, error) {
	return p.get().Hello(ctx, req)
}

func (p *sidecarServiceProxy) StartInstance(ctx context.Context, req *protos.CreateInstanceRequest) (*protos.CreateInstanceResponse, error) {
	return p.get().StartInstance(ctx, req)
}

func (p *sidecarServiceProxy) GetInstance(ctx context.Context, req *protos.GetInstanceRequest) (*protos.GetInstanceResponse, error) {
	return p.get().GetInstance(ctx, req)
}

func (p *sidecarServiceProxy) RewindInstance(ctx context.Context, req *protos.RewindInstanceRequest) (*protos.RewindInstanceResponse, error) {
	return p.get().RewindInstance(ctx, req)
}

func (p *sidecarServiceProxy) WaitForInstanceStart(ctx context.Context, req *protos.GetInstanceRequest) (*protos.GetInstanceResponse, error) {
	return p.get().WaitForInstanceStart(ctx, req)
}

func (p *sidecarServiceProxy) WaitForInstanceCompletion(ctx context.Context, req *protos.GetInstanceRequest) (*protos.GetInstanceResponse, error) {
	return p.get().WaitForInstanceCompletion(ctx, req)
}

func (p *sidecarServiceProxy) RaiseEvent(ctx context.Context, req *protos.RaiseEventRequest) (*protos.RaiseEventResponse, error) {
	return p.get().RaiseEvent(ctx, req)
}

func (p *sidecarServiceProxy) TerminateInstance(ctx context.Context, req *protos.TerminateRequest) (*protos.TerminateResponse, error) {
	return p.get().TerminateInstance(ctx, req)
}

func (p *sidecarServiceProxy) SuspendInstance(ctx context.Context, req *protos.SuspendRequest) (*protos.SuspendResponse, error) {
	return p.get().SuspendInstance(ctx, req)
}

func (p *sidecarServiceProxy) ResumeInstance(ctx context.Context, req *protos.ResumeRequest) (*protos.ResumeResponse, error) {
	return p.get().ResumeInstance(ctx, req)
}

func (p *sidecarServiceProxy) QueryInstances(ctx context.Context, req *protos.QueryInstancesRequest) (*protos.QueryInstancesResponse, error) {
	return p.get().QueryInstances(ctx, req)
}

func (p *sidecarServiceProxy) PurgeInstances(ctx context.Context, req *protos.PurgeInstancesRequest) (*protos.PurgeInstancesResponse, error) {
	return p.get().PurgeInstances(ctx, req)
}

func (p *sidecarServiceProxy) GetWorkItems(req *protos.GetWorkItemsRequest, stream protos.TaskHubSidecarService_GetWorkItemsServer) error {
	return p.get().GetWorkItems(req, stream)
}

func (p *sidecarServiceProxy) CompleteActivityTask(ctx context.Context, req *protos.ActivityResponse) (*protos.CompleteTaskResponse, error) {
	return p.get().CompleteActivityTask(ctx, req)
}

func (p *sidecarServiceProxy) CompleteOrchestratorTask(ctx context.Context, req *protos.OrchestratorResponse) (*protos.CompleteTaskResponse, error) {
	return p.get().CompleteOrchestratorTask(ctx, req)
}

func (p *sidecarServiceProxy) CompleteEntityTask(ctx context.Context, req *protos.EntityBatchResult) (*protos.CompleteTaskResponse, error) {
	return p.get().CompleteEntityTask(ctx, req)
}

func (p *sidecarServiceProxy) StreamInstanceHistory(req *protos.StreamInstanceHistoryRequest, stream protos.TaskHubSidecarService_StreamInstanceHistoryServer) error {
	return p.get().StreamInstanceHistory(req, stream)
}

func (p *sidecarServiceProxy) CreateTaskHub(ctx context.Context, req *protos.CreateTaskHubRequest) (*protos.CreateTaskHubResponse, error) {
	return p.get().CreateTaskHub(ctx, req)
}

func (p *sidecarServiceProxy) DeleteTaskHub(ctx context.Context, req *protos.DeleteTaskHubRequest) (*protos.DeleteTaskHubResponse, error) {
	return p.get().DeleteTaskHub(ctx, req)
}

func (p *sidecarServiceProxy) SignalEntity(ctx context.Context, req *protos.SignalEntityRequest) (*protos.SignalEntityResponse, error) {
	return p.get().SignalEntity(ctx, req)
}

func (p *sidecarServiceProxy) GetEntity(ctx context.Context, req *protos.GetEntityRequest) (*protos.GetEntityResponse, error) {
	return p.get().GetEntity(ctx, req)
}

func (p *sidecarServiceProxy) QueryEntities(ctx context.Context, req *protos.QueryEntitiesRequest) (*protos.QueryEntitiesResponse, error) {
	return p.get().QueryEntities(ctx, req)
}

func (p *sidecarServiceProxy) CleanEntityStorage(ctx context.Context, req *protos.CleanEntityStorageRequest) (*protos.CleanEntityStorageResponse, error) {
	return p.get().CleanEntityStorage(ctx, req)
}
