# Workflows App

A .NET 8 ASP.NET Core application that demonstrates Dapr workflows using either the **actors backend** (default) or the **Durable Task Scheduler (DTS)** backend. It exposes HTTP endpoints for starting, querying, pausing, resuming, terminating, and purging workflows.

The same application code works with both backends — only the Dapr component configuration differs.

## Workflow Backends

| Backend | Resources directory | Requirements |
|---------|-------------------|--------------|
| **Actors** (default) | `actor-resources/` | Placement service + actor-compatible state store (SQLite included) |
| **DTS** | `dts-resources/` | DTS emulator or service |

See [pkg/runtime/wfengine/README.md](../../../pkg/runtime/wfengine/README.md#workflow-backends) for more details on workflow backends.

## Prerequisites

- [.NET 8 SDK](https://dotnet.microsoft.com/download/dotnet/8.0)
- [Dapr CLI](https://docs.dapr.io/getting-started/install-dapr-cli/)
- A locally built `daprd` binary (see [Building Daprd](../../../pkg/runtime/wfengine/README.md#building-daprd))
- [Docker](https://www.docker.com/) (only for DTS backend — runs the emulator)

## Building

### Build the sample app

```bash
cd tests/apps/workflowsapp
dotnet build
```

### Build daprd and create the Dapr CLI layout

From the repository root:

```bash
make build-dapr-layout
```

This places `daprd` at `dist/<os>_<arch>/release/.dapr/bin/daprd`, which is the layout the Dapr CLI expects when using `--runtime-path`.

## Running with the Actors Backend

```bash
cd tests/apps/workflowsapp

dapr run \
  --app-id workflowsapp \
  --app-port 5000 \
  --dapr-http-port 3500 \
  --dapr-grpc-port 50001 \
  --resources-path ./actor-resources \
  --runtime-path ../../../dist/darwin_arm64/release \
  -- dotnet bin/Debug/net8.0/WorkflowActor.dll
```

This uses the built-in actors workflow engine with a local SQLite state store. A placement service is required (the Dapr CLI starts one automatically).

## Running with the DTS Backend

### 1. Start the DTS emulator

```bash
docker run -d --name dts-emulator \
  -p 8080:8080 \
  -p 8082:8082 \
  mcr.microsoft.com/dts/dts-emulator:latest
```

The emulator dashboard is available at http://localhost:8082.

### 2. Run the sample

```bash
cd tests/apps/workflowsapp

dapr run \
  --app-id workflowsapp \
  --app-port 5000 \
  --dapr-http-port 3500 \
  --dapr-grpc-port 50001 \
  --resources-path ./dts-resources \
  --runtime-path ../../../dist/darwin_arm64/release \
  -- dotnet bin/Debug/net8.0/WorkflowActor.dll
```

No placement service or state store is required — DTS manages all workflow state.

> Replace `darwin_arm64` with your OS/arch (e.g., `linux_amd64`). The `--dapr-grpc-port 50001` flag ensures the Dapr CLI sets `DAPR_GRPC_PORT` explicitly, which the .NET SDK reads to locate the daprd gRPC endpoint.

## Using the Sample

All endpoints accept a `workflowComponent` route parameter. Use `dapr` for the actors backend or `dts` for the DTS backend. The curl examples below use `dts`; substitute `dapr` if running with actors.

> You can also use the `workflowsapp.http` file directly in VS Code with the REST Client extension.

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
curl -X POST http://localhost:5000/RaiseWorkflowEvent/dts/order-001/ConfirmSize/large
curl -X POST http://localhost:5000/RaiseWorkflowEvent/dts/order-001/ConfirmColor/red
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

### Actors backend (`actor-resources/sqlite.yaml`)

```yaml
apiVersion: dapr.io/v1alpha1
kind: Component
metadata:
  name: statestore
spec:
  type: state.sqlite
  version: v1
  metadata:
    - name: actorStateStore
      value: "true"
    - name: connectionString
      value: "data.db"
```

### DTS backend (`dts-resources/workflowbackend-dts.yaml`)

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
# Stop the DTS emulator (if running)
docker rm -f dts-emulator
```
