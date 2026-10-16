using Temporalio.Common;
using Temporalio.Workflows;
using TemporalLab.Activities;
using TemporalLab.Models;

namespace TemporalLab.Workflows;

[Workflow]
public sealed class OrderWorkflow
{
    private bool _approved;
    private string _status = "Created";

    [WorkflowRun]
    public async Task<string> RunAsync(OrderInput input)
    {
        if (input.RequiresApproval)
        {
            _status = "WaitingForApproval";
            await Workflow.WaitConditionAsync(() => _approved);
        }

        _status = "ProcessingItems";

        var processingOptions = new ActivityOptions
        {
            StartToCloseTimeout = TimeSpan.FromMinutes(2),
            ScheduleToCloseTimeout = TimeSpan.FromMinutes(5),
            HeartbeatTimeout = TimeSpan.FromSeconds(15),
            RetryPolicy = new RetryPolicy
            {
                InitialInterval = TimeSpan.FromSeconds(1),
                BackoffCoefficient = 2,
                MaximumInterval = TimeSpan.FromSeconds(10),
                MaximumAttempts = 3
            }
        };

        var processingResult = await Workflow.ExecuteActivityAsync(
            (OrderActivities activities) => activities.ProcessItemsAsync(input),
            processingOptions);

        _status = "CallingExternalService";

        var externalServiceOptions = new ActivityOptions
        {
            StartToCloseTimeout = TimeSpan.FromSeconds(20),
            ScheduleToCloseTimeout = TimeSpan.FromMinutes(1),
            RetryPolicy = new RetryPolicy
            {
                InitialInterval = TimeSpan.FromSeconds(1),
                BackoffCoefficient = 2,
                MaximumInterval = TimeSpan.FromSeconds(5),
                MaximumAttempts = 3
            }
        };

        var externalServiceResult = await Workflow.ExecuteActivityAsync(
            (OrderActivities activities) =>
                activities.CallUnstableServiceAsync(input.OrderId),
            externalServiceOptions);

        _status = "Completed";
        return $"{input.OrderId}: {processingResult}; {externalServiceResult}";
    }

    [WorkflowQuery]
    public string GetStatus() => _status;

    [WorkflowSignal]
    public Task ApproveAsync()
    {
        _approved = true;
        return Task.CompletedTask;
    }
}