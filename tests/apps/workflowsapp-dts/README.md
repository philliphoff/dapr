# Workflows App — DTS Backend

This sample is a variant of [workflowsapp](../workflowsapp) that uses the **Durable Task Scheduler (DTS)** as the workflow backend instead of the default actor-based implementation. It is a .NET 6 ASP.NET Core application that exposes HTTP endpoints for starting, querying, pausing, resuming, terminating, and purging Dapr workflows.

## Key Differences from workflowsapp

- **No actor state store** — DTS manages all workflow state, so there is no `sqlite.yaml` component.
- **DTS workflow backend component** — A `workflowbackend.durabletaskscheduler` component (`resources/workflowbackend-dts.yaml`) configures the connection to the DTS emulator.
- **Workflow component name** — API calls use `dts` as the workflow component name (instead of `dapr`).

## Prerequisites

- [Docker](https://www.docker.com/) (for the DTS emulator)
- [.NET 6 SDK](https://dotnet.microsoft.com/download/dotnet/6.0)
- A locally built `daprd` binary (see [Building Daprd](../../../pkg/runtime/wfengine/README.md#building-daprd))

## Running

### 1. Start the DTS emulator

```bash
docker run -d --name dts-emulator \
  -p 8080:8080 \
  -p 8082:8082 \
  mcr.microsoft.com/dts/dts-emulator:latest
```

The emulator dashboard is available at http://localhost:8082.

### 2. Build daprd and create the Dapr CLI layout

From the repository root:

```bash
make build-dapr-layout
```

This builds all binaries and places `daprd` at `dist/<os>_<arch>/release/.dapr/bin/daprd`, which is the layout the Dapr CLI expects when using `--runtime-path`.

### 3. Run the sample with the Dapr CLI

```bash
cd tests/apps/workflowsapp-dts

dapr run \
  --app-id workflowsapp-dts \
  --app-port 5000 \
  --dapr-http-port 3500 \
  --dapr-grpc-port 50001 \
  --resources-path ./dts-resources \
  --runtime-path ../../../dist/darwin_arm64/release \
  -- dotnet bin/Debug/net8.0/WorkflowActor.dll
```

To use Dapr actors for workflows:

```bash
cd tests/apps/workflowsapp-dts

dapr run \
  --app-id workflowsapp-actors \
  --app-port 5000 \
  --dapr-http-port 3500 \
  --dapr-grpc-port 50001 \
  --resources-path ./actor-resources \
  --runtime-path ../../../dist/darwin_arm64/release \
  -- dotnet bin/Debug/net8.0/WorkflowActor.dll
```


Replace `darwin_arm64` with your OS/arch (e.g., `linux_amd64`). The `--dapr-grpc-port 50001` flag ensures the Dapr CLI sets `DAPR_GRPC_PORT` explicitly, which the .NET SDK reads to locate the daprd gRPC endpoint.

No placement service or state store is required — DTS handles all workflow state.

## Using the Sample

All endpoints accept the workflow component name as a route parameter. Use `dts` to target the DTS backend.

### Start a PlaceOrder workflow

```bash
curl -X POST http://localhost:5000/StartWorkflow/dts/PlaceOrder/order-001
```

Returns the workflow instance ID.

### Get workflow status

```bash
curl http://localhost:5000/dts/order-001
```

Returns the runtime status (e.g., `Running`, `Completed`).

### Send events to the workflow

The `PlaceOrder` workflow waits for several external events before completing. Send them in order:

```bash
# 1. Change the purchase item
curl -X POST http://localhost:5000/RaiseWorkflowEvent/dts/order-001/ChangePurchaseItem/stapler

# 2. Confirm size, color, and address (all three required — WhenAll)
curl -X POST http://localhost:5000/RaiseWorkflowEvent/dts/order-001/ConfirmColor/red
curl -X POST http://localhost:5000/RaiseWorkflowEvent/dts/order-001/ConfirmSize/large
curl -X POST http://localhost:5000/RaiseWorkflowEvent/dts/order-001/ConfirmAddress/123-Main-St

# 3. Choose a payment method (any one — WhenAny)
curl -X POST http://localhost:5000/RaiseWorkflowEvent/dts/order-001/PayByCard/visa
```

After all events are received, the workflow calls the `ShipProduct` activity and completes.

### Start a Monitor workflow

The `Monitor` workflow periodically checks the status of another workflow instance:

```bash
# Start a PlaceOrder workflow to monitor
curl -X POST http://localhost:5000/StartWorkflow/dts/PlaceOrder/order-002

# Start a monitor that watches order-002
curl -X POST http://localhost:5000/StartMonitorWorkflow/dts/order-002/monitor-001
```

### Pause and resume a workflow

```bash
curl -X POST http://localhost:5000/PauseWorkflow/dts/order-001
curl -X POST http://localhost:5000/ResumeWorkflow/dts/order-001
```

### Terminate a workflow

```bash
curl -X POST http://localhost:5000/TerminateWorkflow/dts/order-001
```

### Purge a workflow

```bash
curl -X POST http://localhost:5000/PurgeWorkflow/dts/order-001
```

## Component Configuration

The DTS workflow backend is configured via `resources/workflowbackend-dts.yaml`:

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

| Field | Description | Required |
|-------|-------------|----------|
| `endpoint` | DTS connection string (`Endpoint=<url>;Authentication=<type>`) | Yes |
| `taskhub` | Name of the task hub to use | No (default: `default`) |

## Cleanup

```bash
docker rm -f dts-emulator
```
