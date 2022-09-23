using System.Text;
using Azure.Storage.Blobs;
using Azure.Messaging.EventHubs;
using Azure.Messaging.EventHubs.Consumer;
using Azure.Messaging.EventHubs.Processor;

namespace Azure.EventHub.Samples.Subscriber
{
    internal class Program
    {
        const string eventHubConnectionString = "<connection string>";
        const string eventHubName = "<event hub name>";
        const string blobStorageConnectionString = "<blob connection string>";
        const string blobContainerName = "<blob container name>";

        static async Task Main()
        {
            Console.Write("Use EventProcessor for receiving events (y/n)? ");
            var result = Console.ReadLine();

            if (result == "y")
                await ReceiveEventsUsingEventProcessor();
            else if (result == "n")
                await ReceiveEventsUsingConsumerClient();
        }

        static async Task ReceiveEventsUsingConsumerClient()
        {
            var consumer = new EventHubConsumerClient(EventHubConsumerClient.DefaultConsumerGroupName, eventHubConnectionString, eventHubName);
            Console.WriteLine("Press escape to exit...");
            
            try
            {
                var options = new ReadEventOptions
                {
                    MaximumWaitTime = TimeSpan.FromSeconds(1)
                };

                await foreach (PartitionEvent partitionEvent in consumer.ReadEventsAsync(readOptions: options))
                {
                    if (partitionEvent.Data is not null)
                        Console.WriteLine($"Partition: {partitionEvent.Partition.PartitionId} Received event: {partitionEvent.Data.EventBody}");

                    if (Console.KeyAvailable && Console.ReadKey().Key == ConsoleKey.Escape)
                        break;

                    await Task.Delay(1000);
                }
            }
            finally
            {
                await consumer.CloseAsync();
            }
        }

        static async Task ReceiveEventsUsingEventProcessor()
        {
            var storageClient = new BlobContainerClient(blobStorageConnectionString, blobContainerName);
            var processor = new EventProcessorClient(storageClient, EventHubConsumerClient.DefaultConsumerGroupName, eventHubConnectionString, eventHubName);

            processor.ProcessEventAsync += ProcessEventHandler;
            processor.ProcessErrorAsync += ProcessErrorHandler;

            Console.WriteLine("Press escape to exit...");
            await processor.StartProcessingAsync();

            while (true)
            {
                if (Console.ReadKey().Key == ConsoleKey.Escape)
                {
                    await processor.StopProcessingAsync();
                    break;
                }
            }
        }

        static async Task ProcessEventHandler(ProcessEventArgs eventArgs)
        {
            var message = Encoding.UTF8.GetString(eventArgs.Data.Body.ToArray());
            Console.WriteLine($"Partition: {eventArgs.Partition.PartitionId} Received event: {message}");

            await eventArgs.UpdateCheckpointAsync(eventArgs.CancellationToken);
            await Task.Delay(1000);
        }

        static Task ProcessErrorHandler(ProcessErrorEventArgs eventArgs)
        {
            Console.WriteLine($"Partition '{eventArgs.PartitionId}': {eventArgs.Exception.Message}");
            return Task.CompletedTask;
        }
    }
}