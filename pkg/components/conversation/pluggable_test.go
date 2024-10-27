//go:build !windows
// +build !windows

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
	"errors"
	"fmt"
	"net"
	"os"
	"sync/atomic"
	"testing"

	guuid "github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"

	contribMetadata "github.com/dapr/components-contrib/metadata"
	"github.com/dapr/components-contrib/conversation"
	"github.com/dapr/dapr/pkg/components/pluggable"
	proto "github.com/dapr/dapr/pkg/proto/components/v1"
	testingGrpc "github.com/dapr/dapr/pkg/testing/grpc"
	"github.com/dapr/kit/logger"
)

var testLogger = logger.NewLogger("conversation-pluggable-logger")

type server struct {
	proto.UnimplementedConversationServer
	initCalled         atomic.Int64
	onInitCalled       func(*proto.ConversationInitRequest)
	initErr            error
	converseCalled     atomic.Int64
	onConverse         func(*proto.ConversationRequest)
	converseErr        error
	pingCalled         atomic.Int64
	pingErr            error
}

func (s *server) Init(ctx context.Context, req *proto.ConversationInitRequest) (*proto.ConversationInitResponse, error) {
	s.initCalled.Add(1)
	if s.onInitCalled != nil {
		s.onInitCalled(req)
	}
	return &proto.ConversationInitResponse{}, s.initErr
}

func (s *server) Converse(ctx context.Context, req *proto.ConversationRequest) (*proto.ConversationResponse, error) {
	s.converseCalled.Add(1)
	if s.onConverse != nil {
		s.onConverse(req)
	}
	return &proto.ConversationResponse{}, s.converseErr
}

func (s *server) Ping(ctx context.Context, req *proto.PingRequest) (*proto.PingResponse, error) {
	s.pingCalled.Add(1)
	return &proto.PingResponse{}, s.pingErr
}

func TestComponentCalls(t *testing.T) {
	getSecretStores := testingGrpc.TestServerFor(testLogger, func(s *grpc.Server, svc *server) {
		proto.RegisterConversationServer(s, svc)
	}, func(cci grpc.ClientConnInterface) *grpcConversation {
		client := proto.NewConversationClient(cci)
		secretStore := fromConnector(testLogger, pluggable.NewGRPCConnector("/tmp/socket.sock", proto.NewConversationClient))
		secretStore.Client = client
		return secretStore
	})

	t.Run("init should call grpc init", func(t *testing.T) {
		const (
			fakeName          = "name"
			fakeType          = "type"
			fakeVersion       = "v1"
			fakeComponentName = "component"
			fakeSocketFolder  = "/tmp"
		)

		uniqueID := guuid.New().String()
		socket := fmt.Sprintf("%s/%s.sock", fakeSocketFolder, uniqueID)
		defer os.Remove(socket)

		connector := pluggable.NewGRPCConnector(socket, proto.NewConversationClient)
		defer connector.Close()

		listener, err := net.Listen("unix", socket)
		require.NoError(t, err)
		defer listener.Close()
		s := grpc.NewServer()
		srv := &server{}
		proto.RegisterConversationServer(s, srv)
		go func() {
			if serveErr := s.Serve(listener); serveErr != nil {
				testLogger.Debugf("failed to serve: %v", serveErr)
			}
		}()

		secretStore := fromConnector(testLogger, connector)
		err = secretStore.Init(context.Background(), conversation.Metadata{
			Base: contribMetadata.Base{},
		})
		require.NoError(t, err)
		assert.Equal(t, int64(1), srv.initCalled.Load())
	})

	// t.Run("converse should call grpc converse", func(t *testing.T) {
	// 	key := "secretName"
	// 	errStr := "secret not found"
	// 	svc := &server{
	// 		onGetSecret: func(req *proto.GetSecretRequest) {
	// 			assert.Equal(t, key, req.GetKey())
	// 		},
	// 		getSecretErr: errors.New(errStr),
	// 	}

	// 	secretStore, cleanup, err := getSecretStores(svc)
	// 	require.NoError(t, err)
	// 	defer cleanup()

	// 	resp, err := secretStore.GetSecret(context.Background(), secretstores.GetSecretRequest{
	// 		Name: key,
	// 	})
	// 	assert.Equal(t, int64(1), svc.getSecretCalled.Load())
	// 	str := err.Error()
	// 	assert.Equal(t, err.Error(), str)
	// 	assert.Equal(t, secretstores.GetSecretResponse{}, resp)
	// })

	t.Run("ping should not return an err when grpc not returns an error", func(t *testing.T) {
		svc := &server{}
		gSecretStores, cleanup, err := getSecretStores(svc)
		require.NoError(t, err)
		defer cleanup()

		err = gSecretStores.Ping()

		require.NoError(t, err)
		assert.Equal(t, int64(1), svc.pingCalled.Load())
	})

	t.Run("ping should return an err when grpc returns an error", func(t *testing.T) {
		svc := &server{
			pingErr: errors.New("fake-ping-err"),
		}
		gSecretStores, cleanup, err := getSecretStores(svc)
		require.NoError(t, err)
		defer cleanup()

		err = gSecretStores.Ping()

		require.Error(t, err)
		assert.Equal(t, int64(1), svc.pingCalled.Load())
	})
}
