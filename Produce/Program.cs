﻿using System.Buffers;
using EventStore.Client;

using var client = new EventStoreClient(EventStoreClientSettings.Create("esdb://localhost:2113?tls=false"));
using var bufferPool = MemoryPool<byte>.Shared;

Random random = new();

const int MAX_EVENT_SIZE = 256;

while (true) 
{
    Console.WriteLine("Producing...");

    await Produce("space");

    await Task.Delay(1_000);
}

async Task Produce(string streamNamespace, int streamsCount = 100, int eventsPerStream = 10) 
{
    var tasks = new Task[streamsCount];

    for (var i = 0; i < tasks.Length; i++) 
    {
        var streamName = $"test.{streamNamespace}-{i+1}";
        var events = new EventData[eventsPerStream];

        for (var j = 0; j < events.Length; j++) 
        {
            events[j] = GenerateRandomEventData(eventType: $"TestEvent{j+1}");
        }

        tasks[i] = client.AppendToStreamAsync(streamName, StreamState.Any, events);
    }

    await Task.WhenAll(tasks);

    EventData GenerateRandomEventData(string eventType, string contentType = "application/octet-stream") 
    {
        var eventId = Uuid.NewUuid();

        var size = random.Next(MAX_EVENT_SIZE);
        var buffer = bufferPool.Rent(size);
        var data = buffer.Memory[..size];
        random.NextBytes(data.Span);

        return new EventData(eventId, eventType, data, contentType: contentType);
    }
}