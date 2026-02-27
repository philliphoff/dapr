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

// workflowdts is a sample application that demonstrates using the Durable Task
// Scheduler (DTS) as the workflow backend for Dapr. It registers a simple
// order-processing workflow with an activity, starts an instance, and waits for
// completion.
//
// Prerequisites:
//   - Dapr sidecar configured with a workflowbackend.durabletaskscheduler component
//   - DTS emulator or service running (see components/workflowbackend-dts.yaml)
//
// Usage:
//
//	# Start the DTS emulator
//	docker run -d -p 8080:8080 -p 8082:8082 mcr.microsoft.com/dts/dts-emulator:latest
//
//	# Run with Dapr
//	dapr run --app-id workflow-dts-sample --resources-path ./components -- go run .
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"time"

	"github.com/dapr/durabletask-go/api"
	"github.com/dapr/durabletask-go/backend"
	"github.com/dapr/durabletask-go/client"
	"github.com/dapr/durabletask-go/task"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// OrderPayload represents the input to the order-processing workflow.
type OrderPayload struct {
	ItemName string `json:"itemName"`
	Quantity int    `json:"quantity"`
}

// OrderResult represents the output of the order-processing workflow.
type OrderResult struct {
	Processed bool   `json:"processed"`
	Message   string `json:"message"`
}

func main() {
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt)
	defer cancel()

	// Register the workflow and activities with the task registry.
	r := task.NewTaskRegistry()
	if err := r.AddOrchestratorN("ProcessOrder", processOrderWorkflow); err != nil {
		log.Fatalf("Failed to register orchestrator: %v", err)
	}
	if err := r.AddActivityN("ValidateOrder", validateOrderActivity); err != nil {
		log.Fatalf("Failed to register activity: %v", err)
	}

	// Connect to the Dapr sidecar via gRPC.
	daprPort := os.Getenv("DAPR_GRPC_PORT")
	if daprPort == "" {
		daprPort = "50001"
	}

	conn, err := grpc.NewClient("localhost:"+daprPort,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		log.Fatalf("Failed to connect to Dapr sidecar: %v", err)
	}
	defer conn.Close()

	// Create the workflow client (talks to Dapr which proxies to DTS backend).
	backendClient := client.NewTaskHubGrpcClient(conn, backend.DefaultLogger())
	if err := backendClient.StartWorkItemListener(ctx, r); err != nil {
		log.Fatalf("Failed to start work item listener: %v", err)
	}

	// Start a simple HTTP server for health checks.
	go func() {
		mux := http.NewServeMux()
		mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusOK)
		})
		port := os.Getenv("APP_PORT")
		if port == "" {
			port = "3000"
		}
		log.Printf("App listening on port %s", port)
		if err := http.ListenAndServe(":"+port, mux); err != nil {
			log.Printf("HTTP server error: %v", err)
		}
	}()

	// Give the sidecar a moment to initialize.
	time.Sleep(5 * time.Second)

	// Schedule an order-processing workflow.
	payload := OrderPayload{ItemName: "Widget", Quantity: 3}
	id, err := backendClient.ScheduleNewOrchestration(ctx, "ProcessOrder",
		api.WithInstanceID("order-001"),
		api.WithInput(payload),
	)
	if err != nil {
		log.Fatalf("Failed to schedule workflow: %v", err)
	}
	log.Printf("Scheduled workflow with instance ID: %s", id)

	// Wait for completion.
	metadata, err := backendClient.WaitForOrchestrationCompletion(ctx, id,
		api.WithFetchPayloads(true),
	)
	if err != nil {
		log.Fatalf("Failed waiting for workflow: %v", err)
	}

	log.Printf("Workflow completed! Status: %s", metadata.GetRuntimeStatus())
	if output := metadata.GetOutput(); output != nil {
		var result OrderResult
		if err := json.Unmarshal([]byte(output.GetValue()), &result); err == nil {
			log.Printf("Result: %+v", result)
		} else {
			log.Printf("Raw output: %s", output.GetValue())
		}
	}
}

func processOrderWorkflow(ctx *task.OrchestrationContext) (any, error) {
	var payload OrderPayload
	if err := ctx.GetInput(&payload); err != nil {
		return nil, fmt.Errorf("failed to get input: %w", err)
	}

	// Call the validation activity.
	var result OrderResult
	if err := ctx.CallActivity("ValidateOrder", task.WithActivityInput(payload)).Await(&result); err != nil {
		return nil, fmt.Errorf("ValidateOrder activity failed: %w", err)
	}

	return result, nil
}

func validateOrderActivity(ctx task.ActivityContext) (any, error) {
	var payload OrderPayload
	if err := ctx.GetInput(&payload); err != nil {
		return nil, err
	}

	if payload.Quantity <= 0 {
		return OrderResult{
			Processed: false,
			Message:   fmt.Sprintf("Invalid quantity %d for item %s", payload.Quantity, payload.ItemName),
		}, nil
	}

	return OrderResult{
		Processed: true,
		Message:   fmt.Sprintf("Successfully processed order: %d x %s", payload.Quantity, payload.ItemName),
	}, nil
}
