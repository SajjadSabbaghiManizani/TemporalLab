using Temporalio.Activities;
using TemporalLab.Models;

namespace TemporalLab.Activities;

public sealed class OrderActivities(
    bool simulateHeartbeatRetry = false,
    bool simulateExternalServiceRetry = false,
    TimeSpan? itemProcessingDelay = null)
{
    private readonly TimeSpan _itemProcessingDelay =
        itemProcessingDelay ?? TimeSpan.FromSeconds(2);

    [Activity]
    public async Task<string> ProcessItemsAsync(OrderInput input)
    {
        var context = ActivityExecutionContext.Current;

        var completedItems = context.Info.HeartbeatDetails.Count == 0
            ? 0
            : await context.Info.HeartbeatDetailAtAsync<int>(0);

        var attempt = context.Info.Attempt;

        Console.WriteLine(
            $"[{input.OrderId}] ProcessItems attempt #{attempt}; " +
            $"last recorded checkpoint: item {completedItems}.");

        for (var itemNumber = completedItems + 1;
             itemNumber <= input.ItemCount;
             itemNumber++)
        {
            context.CancellationToken.ThrowIfCancellationRequested();

            await Task.Delay(_itemProcessingDelay, context.CancellationToken);

            context.Heartbeat(itemNumber);

            Console.WriteLine(
                $"[{input.OrderId}] Processed item {itemNumber}/{input.ItemCount}");

            if (simulateHeartbeatRetry && attempt == 1 && itemNumber == 3)
            {
                throw new InvalidOperationException(
                    "Demo-only transient failure after item 3 to demonstrate heartbeat recovery.");
            }
        }

        return $"Processed {input.ItemCount} items " +
               $"(attempt #{attempt}; resumed after item {completedItems})";
    }

    [Activity]
    public Task<string> CallUnstableServiceAsync(string orderId)
    {
        var attempt = ActivityExecutionContext.Current.Info.Attempt;

        Console.WriteLine(
            $"[{orderId}] Calling simulated external service, attempt #{attempt}...");

        if (simulateExternalServiceRetry && attempt == 1)
        {
            throw new InvalidOperationException(
                "Demo-only transient failure on the first external-service attempt.");
        }

        return Task.FromResult(
            $"Simulated external service succeeded on attempt #{attempt}");
    }
}