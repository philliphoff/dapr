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

using Microsoft.AspNetCore.Mvc;
using System.Threading.Tasks;
using Dapr.Client;
using Dapr.Workflow;

namespace DaprDemoActor
{
  [ApiController]
  [Route("/")]
  public class Controller : ControllerBase
  {
    private readonly DaprWorkflowClient _workflowClient;
    private readonly DaprClient _daprClient;

    public Controller(DaprWorkflowClient workflowClient, DaprClient daprClient)
    {
      _workflowClient = workflowClient;
      _daprClient = daprClient;
    }

    [HttpGet("{workflowComponent}/{instanceID}")]
    public async Task<ActionResult<string>> GetWorkflow([FromRoute] string instanceID, string workflowComponent)
    {
      await _daprClient.WaitForSidecarAsync();
      var state = await _workflowClient.GetWorkflowStateAsync(instanceID);
      return state.RuntimeStatus.ToString();
    }

    [HttpPost("StartWorkflow/{workflowComponent}/{workflowName}/{instanceID}")]
    public async Task<ActionResult<string>> StartWorkflow([FromRoute] string instanceID, string workflowName, string workflowComponent)
    {
      await _daprClient.WaitForSidecarAsync();
      var startedInstanceId = await _workflowClient.ScheduleNewWorkflowAsync(
              name: "PlaceOrder",
              instanceId: instanceID,
              input: "paperclips");

      return startedInstanceId;
    }

    [HttpPost("StartMonitorWorkflow/{workflowComponent}/{watchInstanceID}/{instanceID}")]
    public async Task<ActionResult<string>> StartMonitorWorkflow([FromRoute] string watchInstanceID, string instanceID, string workflowComponent)
    {
      await _daprClient.WaitForSidecarAsync();
      var startedInstanceId = await _workflowClient.ScheduleNewWorkflowAsync(
              name: "Monitor",
              instanceId: instanceID,
              input: watchInstanceID);

      return startedInstanceId;
    }

    [HttpPost("PurgeWorkflow/{workflowComponent}/{instanceID}")]
    public async Task<ActionResult<bool>> PurgeWorkflow([FromRoute] string instanceID, string workflowComponent)
    {
      await _workflowClient.PurgeInstanceAsync(instanceID);
      return true;
    }

    [HttpPost("TerminateWorkflow/{workflowComponent}/{instanceID}")]
    public async Task<ActionResult<bool>> TerminateWorkflow([FromRoute] string instanceID, string workflowComponent)
    {
      await _workflowClient.TerminateWorkflowAsync(instanceID);
      return true;
    }

    [HttpPost("PauseWorkflow/{workflowComponent}/{instanceID}")]
    public async Task<ActionResult<bool>> PauseWorkflow([FromRoute] string instanceID, string workflowComponent)
    {
      await _workflowClient.SuspendWorkflowAsync(instanceID);
      return true;
    }

    [HttpPost("ResumeWorkflow/{workflowComponent}/{instanceID}")]
    public async Task<ActionResult<bool>> ResumeWorkflow([FromRoute] string instanceID, string workflowComponent)
    {
      await _workflowClient.ResumeWorkflowAsync(instanceID);
      return true;
    }

    [HttpPost("RaiseWorkflowEvent/{workflowComponent}/{instanceID}/{eventName}/{eventInput}")]
    public async Task<ActionResult<bool>> RaiseWorkflowEvent([FromRoute] string instanceID, string workflowComponent, string eventName, string eventInput)
    {
      await _workflowClient.RaiseEventAsync(instanceID, eventName, eventInput);
      return true;
    }
  }
}
