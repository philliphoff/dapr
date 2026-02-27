# Workflow DTS Sample

This sample demonstrates using the **Durable Task Scheduler (DTS)** as the workflow backend for Dapr, instead of the default actor-based implementation.

## Prerequisites

- [Docker](https://www.docker.com/) (for the DTS emulator)
- [Dapr CLI](https://docs.dapr.io/getting-started/install-dapr-cli/)
- Go 1.24+

## Running

1. **Start the DTS emulator:**

   ```bash
   docker run -d --name dts-emulator \
     -p 8080:8080 \
     -p 8082:8082 \
     mcr.microsoft.com/dts/dts-emulator:latest
   ```

   The emulator dashboard is available at `http://localhost:8082`.

2. **Run the sample with Dapr:**

   ```bash
   dapr run --app-id workflow-dts-sample \
     --resources-path ./components \
     -- go run .
   ```

3. **Expected output:**

   ```
   Scheduled workflow with instance ID: order-001
   Workflow completed! Status: ORCHESTRATION_STATUS_COMPLETED
   Result: {Processed:true Message:Successfully processed order: 3 x Widget}
   ```

4. **Cleanup:**

   ```bash
   docker rm -f dts-emulator
   ```

## Component Configuration

The DTS workflow backend is configured via a Dapr component file (`components/workflowbackend-dts.yaml`):

```yaml
apiVersion: dapr.io/v1alpha1
kind: Component
metadata:
  name: dts
spec:
  type: workflowbackend.durabletaskscheduler
  version: v1
  metadata:
  - name: endpoint
    value: "Endpoint=http://localhost:8080;Authentication=None"
  - name: taskhub
    value: "default"
```

### Metadata Fields

| Field | Description | Required |
|-------|-------------|----------|
| `endpoint` | DTS connection string (`Endpoint=<url>;Authentication=<type>`) | Yes |
| `taskhub` | Name of the task hub to use | No (default: `default`) |

## Architecture

When a `workflowbackend.durabletaskscheduler` component is configured, the Dapr runtime uses DTS as the backend for the workflow engine instead of the default actor-based backend. This means:

- **No state store required** for workflow state (DTS manages it)
- **No placement service required** for workflow actors
- **Scalability** is managed by the DTS service
- The **Dapr workflow API** (HTTP/gRPC) works identically regardless of backend
