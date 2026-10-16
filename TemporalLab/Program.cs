using Temporalio.Client;
using Temporalio.Worker;
using TemporalLab.Activities;
using TemporalLab.Models;
using TemporalLab.Workflows;

const string TaskQueue = "order-processing";

var mode = args.FirstOrDefault()?.Trim().ToLowerInvariant() ?? "help";
var temporalAddress =
    Environment.GetEnvironmentVariable("TEMPORAL_ADDRESS") ?? "localhost:7233";

if (mode is "help" or "--help" or "-h")
{
    PrintHelp();
    return;
}

var client = await TemporalClient.ConnectAsync(new(temporalAddress));

switch (mode)
{
    case "worker":
        await RunWorkerAsync(client);
        break;

    case "start":
        {
            var workflowId = args.ElementAtOrDefault(1) ??
                             $"order-{Guid.NewGuid():N}";

            var input = new OrderInput(
                OrderId: workflowId,
                ItemCount: 10,
                RequiresApproval: true);

            var handle = await client.StartWorkflowAsync(
                (OrderWorkflow wf) => wf.RunAsync(input),
                new(id: workflowId, taskQueue: TaskQueue));

            Console.WriteLine($"Workflow started. WorkflowId: {handle.Id}");
            Console.WriteLine("Next commands: status <workflowId> | approve <workflowId>");
            break;
        }

    case "status":
        {
            if (args.Length < 2)
            {
                Console.WriteLine("Usage: status <workflowId>");
                return;
            }

            var handle = client.GetWorkflowHandle<OrderWorkflow>(args[1]);
            var status = await handle.QueryAsync(wf => wf.GetStatus());
            Console.WriteLine($"WorkflowId: {args[1]}");
            Console.WriteLine($"Status: {status}");
            break;
        }

    case "approve":
        {
            if (args.Length < 2)
            {
                Console.WriteLine("Usage: approve <workflowId>");
                return;
            }

            var handle = client.GetWorkflowHandle<OrderWorkflow>(args[1]);
            await handle.SignalAsync(wf => wf.ApproveAsync());
            Console.WriteLine($"Approval Signal sent to WorkflowId: {args[1]}");
            break;
        }

    default:
        Console.WriteLine($"Unknown command: {mode}");
        PrintHelp();
        break;
}

static async Task RunWorkerAsync(TemporalClient client)
{
    // Demo switches intentionally enable transient failures so Retry is observable.
    var activities = new OrderActivities(
        simulateHeartbeatRetry: true,
        simulateExternalServiceRetry: true,
        itemProcessingDelay: TimeSpan.FromSeconds(2));

    var workerOptions = new TemporalWorkerOptions("order-processing")
        .AddWorkflow<OrderWorkflow>()
        .AddActivity(activities.ProcessItemsAsync)
        .AddActivity(activities.CallUnstableServiceAsync);

    using var worker = new TemporalWorker(client, workerOptions);
    using var cancellation = new CancellationTokenSource();

    Console.CancelKeyPress += (_, eventArgs) =>
    {
        eventArgs.Cancel = true;
        cancellation.Cancel();
    };

    Console.WriteLine($"Worker polling Task Queue '{workerOptions.TaskQueue}'...");
    Console.WriteLine("Demo mode: first processing attempt and first external call intentionally fail.");
    Console.WriteLine("Press Ctrl+C to stop.");

    try
    {
        await worker.ExecuteAsync(cancellation.Token);
    }
    catch (OperationCanceledException) when (cancellation.IsCancellationRequested)
    {
        Console.WriteLine("Worker shutdown requested.");
    }
}

static void PrintHelp()
{
    Console.WriteLine("TemporalLab commands:");
    Console.WriteLine("  worker                 Start a Temporal Worker");
    Console.WriteLine("  start [workflowId]     Start an order Workflow (waits for approval)");
    Console.WriteLine("  status <workflowId>    Query Workflow status");
    Console.WriteLine("  approve <workflowId>   Send an approval Signal");
    Console.WriteLine();
    Console.WriteLine("Environment variable: TEMPORAL_ADDRESS (default: localhost:7233)");
}