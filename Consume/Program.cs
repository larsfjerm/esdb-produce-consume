using EventStore.Client;

using var client = new EventStoreClient(EventStoreClientSettings.Create("esdb://localhost:2113?tls=false"));
var count = 0;
Position checkpoint = default;
StreamSubscription? subscription = default;

await Consume(FromAll.End);

async Task Consume(FromAll position) 
{
    Console.WriteLine("Consuming...");

    subscription = await client.SubscribeToAllAsync(
        position,
        (_, resolved, ct) => Appeared(resolved),
        subscriptionDropped: (_, reason, exc) => SubscriptionDropped(reason, exc),
        filterOptions: new SubscriptionFilterOptions(EventTypeFilter.ExcludeSystemEvents()));
}

Task Appeared(ResolvedEvent evt)
{
    count++;
    checkpoint = evt.Event.Position;
    return Task.CompletedTask;
}

Task SubscriptionDropped(SubscriptionDroppedReason reason, Exception? exception)
{
    Console.WriteLine("SubscriptionDropped");
    Console.WriteLine($"Events Handled Count: {count}");

    subscription?.Dispose();

    if (reason != SubscriptionDroppedReason.Disposed)
        return Consume(FromAll.After(checkpoint));

    return Task.CompletedTask;
}

Console.ReadLine();
Console.WriteLine($"Total Events Handled Count: {count}");