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

package conversation

import (
	"context"

	"github.com/dapr/components-contrib/conversation"
	"github.com/dapr/dapr/pkg/components/pluggable"
	proto "github.com/dapr/dapr/pkg/proto/components/v1"
	"github.com/dapr/kit/logger"
)

type grpcConversation struct {
	*pluggable.GRPCConnector[proto.ConversationClient]
}

// Init initializes the grpc secret store passing out the metadata to the grpc component.
func (gss *grpcConversation) Init(ctx context.Context, metadata conversation.Metadata) error {
	if err := gss.Dial(metadata.Name); err != nil {
		return err
	}

	protoMetadata := &proto.MetadataRequest{
		Properties: metadata.Properties,
	}

	_, err := gss.Client.Init(gss.Context, &proto.ConversationInitRequest{
		Metadata: protoMetadata,
	})
	if err != nil {
		return err
	}

	return nil
}

func (gss *grpcConversation) Converse(ctx context.Context, req *conversation.ConversationRequest) (*conversation.ConversationResponse, error) {
	inputs := make([]*proto.ConversationInput, len(req.Inputs))
	for k := range req.Inputs {
		inputs[k] = &proto.ConversationInput{
			Message: req.Inputs[k].Message,
			Role: (*string)(&req.Inputs[k].Role),
		}
	}

	resp, err := gss.Client.Converse(gss.Context, &proto.ConversationRequest{
		Inputs: inputs,
		Parameters: req.Parameters,
		ConversationContext: &req.ConversationContext,
		Temperature: &req.Temperature,
		Key: &req.Key,
		Model: &req.Model,
		Endpoints: req.Endpoints,
		Policy: &req.Policy,
	})
	if err != nil {
		return &conversation.ConversationResponse{}, err
	}

	outputs := make([]conversation.ConversationResult, len(resp.Outputs))
	for k := range(resp.Outputs) {
		outputs[k] = conversation.ConversationResult{
			Result: resp.Outputs[k].Result,
			Parameters: resp.Outputs[k].Parameters,
		}
	}

	return &conversation.ConversationResponse{
		ConversationContext: *resp.ConversationContext,
		Outputs: outputs,
	}, nil
}

// fromConnector creates a new GRPC pubsub using the given underlying connector.
func fromConnector(l logger.Logger, connector *pluggable.GRPCConnector[proto.ConversationClient]) *grpcConversation {
	return &grpcConversation{
		GRPCConnector: connector,
	}
}

// NewGRPCConversation creates a new grpc pubsub using the given socket factory.
func NewGRPCConversation(l logger.Logger, socket string) *grpcConversation {
	return fromConnector(l, pluggable.NewGRPCConnector(socket, proto.NewConversationClient))
}

// newGRPCConversation creates a new grpc pubsub for the given pluggable component.
func newGRPCConversation(dialer pluggable.GRPCConnectionDialer) func(l logger.Logger) conversation.Conversation {
	return func(l logger.Logger) conversation.Conversation {
		return fromConnector(l, pluggable.NewGRPCConnectorWithDialer(dialer, proto.NewConversationClient))
	}
}

func init() {
	//nolint:nosnakecase
	pluggable.AddServiceDiscoveryCallback(proto.Conversation_ServiceDesc.ServiceName, func(name string, dialer pluggable.GRPCConnectionDialer) {
		DefaultRegistry.RegisterComponent(newGRPCConversation(dialer), name)
	})
}
