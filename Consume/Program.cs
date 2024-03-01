using EventStore.Client;

using var client = new EventStoreClient(EventStoreClientSettings.Create("esdb://localhost:2113?tls=false"));
var count = 0;
FromAll checkpoint = default;

_ = Consume(FromAll.End);

async Task Consume(FromAll position) 
{
    Console.WriteLine("Consuming...");

    try
    {
        await using var subscription = client.SubscribeToAll(
            position,
            filterOptions: new SubscriptionFilterOptions(EventTypeFilter.ExcludeSystemEvents()));

        await foreach (var message in subscription.Messages)
        {
            count++;

            if (message is StreamMessage.Event(var resolved))
            {
                if (resolved.OriginalPosition is not null)
                {
                    checkpoint = FromAll.After(resolved.OriginalPosition.Value);
                }
            }
        }
    }
    catch (OperationCanceledException)
    {
        Console.WriteLine($"Subscription was canceled.");
    }
    catch (ObjectDisposedException)
    {
        Console.WriteLine($"Subscription was canceled by the user.");
    }
    catch (Exception ex)
    {
        Console.WriteLine($"Subscription dropped: {ex}");
        Console.WriteLine($"Events Handled Count: {count}");

        await Consume(checkpoint);
    }
}

Console.ReadLine();
Console.WriteLine($"Total Events Handled Count: {count}");