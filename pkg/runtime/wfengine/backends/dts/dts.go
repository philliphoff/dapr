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
	"errors"
	"fmt"
	"net/url"
	"strings"
	"sync/atomic"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/types/known/wrapperspb"

	"github.com/dapr/durabletask-go/api"
	"github.com/dapr/durabletask-go/api/protos"
	"github.com/dapr/durabletask-go/backend"
	"github.com/dapr/durabletask-go/backend/local"
	"github.com/dapr/kit/logger"
)

var log = logger.NewLogger("dapr.wfengine.backend.dts")

const completionTokenKey = "dts.completionToken"

// Metadata keys for component configuration.
const (
	MetadataKeyEndpoint = "endpoint"
	MetadataKeyTaskHub  = "taskhub"
)

// Backend implements backend.Backend by wrapping the DTS BackendService gRPC client.
type Backend struct {
	client  protos.BackendServiceClient
	conn    *grpc.ClientConn
	taskHub string

	// localTasks handles local task synchronization (WaitForOrchestratorCompletion, etc.)
	localTasks *local.TasksBackend

	orchCh chan *backend.OrchestrationWorkItem
	actCh  chan *backend.ActivityWorkItem

	stopped atomic.Bool
}

// Options configures the DTS backend.
type Options struct {
	// Endpoint is the gRPC address of the DTS service (host:port).
	Endpoint string
	// TaskHub is the name of the task hub (default: "default").
	TaskHub string
	// TransportCredentials are the gRPC transport credentials. If nil, insecure is used.
	TransportCredentials credentials.TransportCredentials
	// ClientConn is an optional pre-established gRPC connection.
	// If provided, Endpoint and TransportCredentials are ignored.
	ClientConn *grpc.ClientConn
}

// ParseConnectionString parses a DTS connection string of the form
// "Endpoint=http://host:port;Authentication=None" into Options.
func ParseConnectionString(connStr string, taskHub string) (Options, error) {
	opts := Options{TaskHub: taskHub}
	parts := strings.Split(connStr, ";")
	for _, part := range parts {
		kv := strings.SplitN(strings.TrimSpace(part), "=", 2)
		if len(kv) != 2 {
			continue
		}
		key := strings.TrimSpace(kv[0])
		val := strings.TrimSpace(kv[1])
		switch strings.ToLower(key) {
		case "endpoint":
			u, err := url.Parse(val)
			if err != nil {
				return opts, fmt.Errorf("invalid endpoint URL %q: %w", val, err)
			}
			host := u.Hostname()
			port := u.Port()
			if port == "" {
				if u.Scheme == "https" {
					port = "443"
				} else {
					port = "80"
				}
			}
			opts.Endpoint = host + ":" + port
			if u.Scheme == "https" {
				// Default TLS credentials for HTTPS endpoints.
				opts.TransportCredentials = credentials.NewClientTLSFromCert(nil, "")
			}
		case "authentication":
			// "None" means no auth (emulator). Other values may require tokens.
		}
	}
	if opts.Endpoint == "" {
		return opts, errors.New("connection string must contain an Endpoint")
	}
	return opts, nil
}

// New creates a new DTS backend instance.
func New(opts Options) (*Backend, error) {
	var conn *grpc.ClientConn
	if opts.ClientConn != nil {
		conn = opts.ClientConn
	} else {
		if opts.Endpoint == "" {
			return nil, errors.New("DTS endpoint is required")
		}
		creds := opts.TransportCredentials
		if creds == nil {
			creds = insecure.NewCredentials()
		}
		var err error
		conn, err = grpc.NewClient(opts.Endpoint, grpc.WithTransportCredentials(creds))
		if err != nil {
			return nil, fmt.Errorf("failed to connect to DTS endpoint %q: %w", opts.Endpoint, err)
		}
	}

	taskHub := opts.TaskHub
	if taskHub == "" {
		taskHub = "default"
	}

	return &Backend{
		client:     protos.NewBackendServiceClient(conn),
		conn:       conn,
		taskHub:    taskHub,
		localTasks: local.NewTasksBackend(),
		orchCh:     make(chan *backend.OrchestrationWorkItem),
		actCh:      make(chan *backend.ActivityWorkItem),
	}, nil
}

// withTaskHub injects the task hub name as gRPC metadata.
func (b *Backend) withTaskHub(ctx context.Context) context.Context {
	return metadata.AppendToOutgoingContext(ctx, "taskhub", b.taskHub)
}

// CreateTaskHub implements backend.Backend.
func (b *Backend) CreateTaskHub(ctx context.Context) error {
	// DTS manages task hubs externally; this is a no-op.
	return nil
}

// DeleteTaskHub implements backend.Backend.
func (b *Backend) DeleteTaskHub(ctx context.Context) error {
	return errors.New("deleting DTS task hubs is not supported from the Dapr runtime")
}

// Start implements backend.Backend.
func (b *Backend) Start(ctx context.Context) error {
	b.stopped.Store(false)
	go b.receiveWorkItems(ctx)
	return nil
}

// Stop implements backend.Backend.
func (b *Backend) Stop(ctx context.Context) error {
	b.stopped.Store(true)
	return nil
}

// String implements fmt.Stringer.
func (b *Backend) String() string {
	return "durabletaskscheduler/v1"
}

// CreateOrchestrationInstance implements backend.Backend.
func (b *Backend) CreateOrchestrationInstance(ctx context.Context, e *backend.HistoryEvent, opts ...backend.OrchestrationIdReusePolicyOptions) error {
	es := e.GetExecutionStarted()
	if es == nil {
		return errors.New("the history event must be an ExecutionStartedEvent")
	}
	oi := es.GetOrchestrationInstance()
	if oi == nil {
		return errors.New("the ExecutionStartedEvent did not contain orchestration instance information")
	}

	policy := &api.OrchestrationIdReusePolicy{}
	for _, opt := range opts {
		opt(policy)
	}

	req := &protos.CreateInstanceRequest{
		InstanceId:                 oi.GetInstanceId(),
		Name:                      es.GetName(),
		Version:                   es.GetVersion(),
		Input:                     es.GetInput(),
		ScheduledStartTimestamp:   es.GetScheduledStartTimestamp(),
		OrchestrationIdReusePolicy: policy,
		ParentTraceContext:        es.GetParentTraceContext(),
	}

	_, err := b.client.CreateInstance(b.withTaskHub(ctx), req)
	if err != nil {
		return fmt.Errorf("DTS CreateInstance failed: %w", err)
	}
	return nil
}

// RerunWorkflowFromEvent implements backend.Backend.
// This operation is not supported by the BackendService gRPC protocol.
func (b *Backend) RerunWorkflowFromEvent(ctx context.Context, req *protos.RerunWorkflowFromEventRequest) (api.InstanceID, error) {
	return "", errors.New("RerunWorkflowFromEvent is not supported by the DTS backend")
}

// AddNewOrchestrationEvent implements backend.Backend.
func (b *Backend) AddNewOrchestrationEvent(ctx context.Context, id api.InstanceID, e *backend.HistoryEvent) error {
	_, err := b.client.AddEvent(b.withTaskHub(ctx), &protos.AddEventRequest{
		Instance: &protos.OrchestrationInstance{InstanceId: string(id)},
		Event:    e,
	})
	if err != nil {
		return fmt.Errorf("DTS AddEvent failed: %w", err)
	}
	return nil
}

// GetOrchestrationMetadata implements backend.Backend.
func (b *Backend) GetOrchestrationMetadata(ctx context.Context, id api.InstanceID) (*backend.OrchestrationMetadata, error) {
	resp, err := b.client.GetInstance(b.withTaskHub(ctx), &protos.GetInstanceRequest{
		InstanceId:          string(id),
		GetInputsAndOutputs: true,
	})
	if err != nil {
		return nil, fmt.Errorf("DTS GetInstance failed: %w", err)
	}
	if !resp.GetExists() {
		return nil, api.ErrInstanceNotFound
	}

	state := resp.GetOrchestrationState()
	return &backend.OrchestrationMetadata{
		InstanceId:     state.GetInstanceId(),
		Name:           state.GetName(),
		RuntimeStatus:  state.GetOrchestrationStatus(),
		CreatedAt:      state.GetCreatedTimestamp(),
		LastUpdatedAt:  state.GetLastUpdatedTimestamp(),
		Input:          state.GetInput(),
		Output:         state.GetOutput(),
		CustomStatus:   state.GetCustomStatus(),
		FailureDetails: state.GetFailureDetails(),
	}, nil
}

// WatchOrchestrationRuntimeStatus implements backend.Backend.
func (b *Backend) WatchOrchestrationRuntimeStatus(ctx context.Context, id api.InstanceID, condition func(*backend.OrchestrationMetadata) bool) error {
	// Use polling against DTS GetInstance since WaitForInstance may not be
	// available or may not support arbitrary conditions.
	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()

	for {
		meta, err := b.GetOrchestrationMetadata(ctx, id)
		if err != nil {
			if errors.Is(err, api.ErrInstanceNotFound) {
				// Instance not yet created; wait and retry.
				select {
				case <-ctx.Done():
					return ctx.Err()
				case <-ticker.C:
					continue
				}
			}
			return err
		}
		if condition(meta) {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
}

// GetOrchestrationRuntimeState implements backend.Backend.
func (b *Backend) GetOrchestrationRuntimeState(ctx context.Context, owi *backend.OrchestrationWorkItem) (*backend.OrchestrationRuntimeState, error) {
	if owi.State != nil {
		return owi.State, nil
	}

	resp, err := b.client.GetOrchestrationRuntimeState(b.withTaskHub(ctx), &protos.GetOrchestrationRuntimeStateRequest{
		Instance: &protos.OrchestrationInstance{InstanceId: string(owi.InstanceID)},
	})
	if err != nil {
		return nil, fmt.Errorf("DTS GetOrchestrationRuntimeState failed: %w", err)
	}
	return &protos.OrchestrationRuntimeState{
		OldEvents: resp.GetHistory(),
	}, nil
}

// receiveWorkItems opens a GetWorkItems stream from DTS and fans out to orchestration/activity channels.
func (b *Backend) receiveWorkItems(ctx context.Context) {
	for !b.stopped.Load() {
		if err := b.runWorkItemStream(ctx); err != nil {
			if b.stopped.Load() || ctx.Err() != nil {
				return
			}
			log.Warnf("DTS work item stream error, reconnecting: %v", err)
			time.Sleep(time.Second)
		}
	}
}

func (b *Backend) runWorkItemStream(ctx context.Context) error {
	stream, err := b.client.GetWorkItems(b.withTaskHub(ctx), &protos.GetWorkItemsRequest{})
	if err != nil {
		return fmt.Errorf("failed to open GetWorkItems stream: %w", err)
	}

	for {
		wi, err := stream.Recv()
		if err != nil {
			return fmt.Errorf("GetWorkItems stream error: %w", err)
		}

		token := wi.GetCompletionToken()

		switch {
		case wi.GetOrchestratorRequest() != nil:
			oreq := wi.GetOrchestratorRequest()
			owi := &backend.OrchestrationWorkItem{
				InstanceID: api.InstanceID(oreq.GetInstanceId()),
				NewEvents:  oreq.GetNewEvents(),
				State: &protos.OrchestrationRuntimeState{
					OldEvents: oreq.GetPastEvents(),
				},
				Properties: map[string]interface{}{
					completionTokenKey: token,
				},
			}
			select {
			case b.orchCh <- owi:
			case <-ctx.Done():
				return ctx.Err()
			}

		case wi.GetActivityRequest() != nil:
			areq := wi.GetActivityRequest()
			event := &protos.HistoryEvent{
				EventId: areq.GetTaskId(),
				EventType: &protos.HistoryEvent_TaskScheduled{
					TaskScheduled: &protos.TaskScheduledEvent{
						Name:               areq.GetName(),
						Version:            areq.GetVersion(),
						Input:              areq.GetInput(),
						TaskExecutionId:    areq.GetTaskExecutionId(),
						ParentTraceContext: areq.GetParentTraceContext(),
					},
				},
			}
			awi := &backend.ActivityWorkItem{
				InstanceID: api.InstanceID(areq.GetOrchestrationInstance().GetInstanceId()),
				NewEvent:   event,
				Properties: map[string]interface{}{
					completionTokenKey: token,
				},
			}
			select {
			case b.actCh <- awi:
			case <-ctx.Done():
				return ctx.Err()
			}
		}
	}
}

// NextOrchestrationWorkItem implements backend.Backend.
func (b *Backend) NextOrchestrationWorkItem(ctx context.Context) (*backend.OrchestrationWorkItem, error) {
	select {
	case wi := <-b.orchCh:
		log.Debugf("DTS backend received orchestration work item for '%s'", wi.InstanceID)
		return wi, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

// NextActivityWorkItem implements backend.Backend.
func (b *Backend) NextActivityWorkItem(ctx context.Context) (*backend.ActivityWorkItem, error) {
	select {
	case wi := <-b.actCh:
		log.Debugf("DTS backend received activity work item for '%s'", wi.InstanceID)
		return wi, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

// CompleteOrchestrationWorkItem implements backend.Backend.
func (b *Backend) CompleteOrchestrationWorkItem(ctx context.Context, wi *backend.OrchestrationWorkItem) error {
	token, _ := wi.Properties[completionTokenKey].(string)

	req := &protos.CompleteOrchestrationWorkItemRequest{
		CompletionToken: token,
		Instance:        &protos.OrchestrationInstance{InstanceId: string(wi.InstanceID)},
	}

	_, err := b.client.CompleteOrchestrationWorkItem(b.withTaskHub(ctx), req)
	if err != nil {
		return fmt.Errorf("DTS CompleteOrchestrationWorkItem failed: %w", err)
	}
	return nil
}

// AbandonOrchestrationWorkItem implements backend.Backend.
func (b *Backend) AbandonOrchestrationWorkItem(ctx context.Context, wi *backend.OrchestrationWorkItem) error {
	token, _ := wi.Properties[completionTokenKey].(string)

	_, err := b.client.AbandonOrchestrationWorkItem(b.withTaskHub(ctx), &protos.AbandonOrchestrationWorkItemRequest{
		CompletionToken: token,
	})
	if err != nil {
		return fmt.Errorf("DTS AbandonOrchestrationWorkItem failed: %w", err)
	}
	return nil
}

// CompleteActivityWorkItem implements backend.Backend.
func (b *Backend) CompleteActivityWorkItem(ctx context.Context, wi *backend.ActivityWorkItem) error {
	token, _ := wi.Properties[completionTokenKey].(string)

	_, err := b.client.CompleteActivityWorkItem(b.withTaskHub(ctx), &protos.CompleteActivityWorkItemRequest{
		CompletionToken: token,
		ResponseEvent:   wi.Result,
	})
	if err != nil {
		return fmt.Errorf("DTS CompleteActivityWorkItem failed: %w", err)
	}
	return nil
}

// AbandonActivityWorkItem implements backend.Backend.
func (b *Backend) AbandonActivityWorkItem(ctx context.Context, wi *backend.ActivityWorkItem) error {
	token, _ := wi.Properties[completionTokenKey].(string)

	_, err := b.client.AbandonActivityWorkItem(b.withTaskHub(ctx), &protos.AbandonActivityWorkItemRequest{
		CompletionToken: token,
	})
	if err != nil {
		return fmt.Errorf("DTS AbandonActivityWorkItem failed: %w", err)
	}
	return nil
}

// PurgeOrchestrationState implements backend.Backend.
func (b *Backend) PurgeOrchestrationState(ctx context.Context, id api.InstanceID, force bool) error {
	resp, err := b.client.PurgeInstances(b.withTaskHub(ctx), &protos.PurgeInstancesRequest{
		Request: &protos.PurgeInstancesRequest_InstanceId{InstanceId: string(id)},
		Force:   &force,
	})
	if err != nil {
		return fmt.Errorf("DTS PurgeInstances failed: %w", err)
	}
	if resp.GetDeletedInstanceCount() == 0 {
		return api.ErrInstanceNotFound
	}
	return nil
}

// CompleteOrchestratorTask implements backend.Backend.
func (b *Backend) CompleteOrchestratorTask(ctx context.Context, response *protos.OrchestratorResponse) error {
	return b.localTasks.CompleteOrchestratorTask(ctx, response)
}

// CancelOrchestratorTask implements backend.Backend.
func (b *Backend) CancelOrchestratorTask(ctx context.Context, instanceID api.InstanceID) error {
	return b.localTasks.CancelOrchestratorTask(ctx, instanceID)
}

// WaitForOrchestratorCompletion implements backend.Backend.
func (b *Backend) WaitForOrchestratorCompletion(request *protos.OrchestratorRequest) func(context.Context) (*protos.OrchestratorResponse, error) {
	return b.localTasks.WaitForOrchestratorCompletion(request)
}

// CompleteActivityTask implements backend.Backend.
func (b *Backend) CompleteActivityTask(ctx context.Context, response *protos.ActivityResponse) error {
	return b.localTasks.CompleteActivityTask(ctx, response)
}

// CancelActivityTask implements backend.Backend.
func (b *Backend) CancelActivityTask(ctx context.Context, instanceID api.InstanceID, taskID int32) error {
	return b.localTasks.CancelActivityTask(ctx, instanceID, taskID)
}

// WaitForActivityCompletion implements backend.Backend.
func (b *Backend) WaitForActivityCompletion(request *protos.ActivityRequest) func(context.Context) (*protos.ActivityResponse, error) {
	return b.localTasks.WaitForActivityCompletion(request)
}

// ListInstanceIDs implements backend.Backend.
func (b *Backend) ListInstanceIDs(ctx context.Context, req *protos.ListInstanceIDsRequest) (*protos.ListInstanceIDsResponse, error) {
	resp, err := b.client.ListInstanceIDs(b.withTaskHub(ctx), req)
	if err != nil {
		return nil, fmt.Errorf("DTS ListInstanceIDs failed: %w", err)
	}
	return resp, nil
}

// GetInstanceHistory implements backend.Backend.
func (b *Backend) GetInstanceHistory(ctx context.Context, req *protos.GetInstanceHistoryRequest) (*protos.GetInstanceHistoryResponse, error) {
	resp, err := b.client.GetInstanceHistory(b.withTaskHub(ctx), req)
	if err != nil {
		return nil, fmt.Errorf("DTS GetInstanceHistory failed: %w", err)
	}
	return resp, nil
}

// Close closes the gRPC connection.
func (b *Backend) Close() error {
	if b.conn != nil {
		return b.conn.Close()
	}
	return nil
}

// SuspendOrchestration is a helper that adds a suspend event.
func (b *Backend) SuspendOrchestration(ctx context.Context, id api.InstanceID, reason string) error {
	var input *wrapperspb.StringValue
	if reason != "" {
		input = wrapperspb.String(reason)
	}
	return b.AddNewOrchestrationEvent(ctx, id, &protos.HistoryEvent{
		EventId: -1,
		EventType: &protos.HistoryEvent_ExecutionSuspended{
			ExecutionSuspended: &protos.ExecutionSuspendedEvent{Input: input},
		},
	})
}

// ResumeOrchestration is a helper that adds a resume event.
func (b *Backend) ResumeOrchestration(ctx context.Context, id api.InstanceID, reason string) error {
	var input *wrapperspb.StringValue
	if reason != "" {
		input = wrapperspb.String(reason)
	}
	return b.AddNewOrchestrationEvent(ctx, id, &protos.HistoryEvent{
		EventId: -1,
		EventType: &protos.HistoryEvent_ExecutionResumed{
			ExecutionResumed: &protos.ExecutionResumedEvent{Input: input},
		},
	})
}
