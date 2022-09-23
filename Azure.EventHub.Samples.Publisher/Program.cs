using Azure.Messaging.EventHubs;
using Azure.Messaging.EventHubs.Producer;

namespace Azure.EventHub.Samples.Publisher
{
    internal class Program
    {
        const string connectionString = "<connection string>";
        const string eventHubName = "<event hub name>";

        static async Task Main()
        {
            var countEvents = 0;
            var producerClient = new EventHubProducerClient(connectionString, eventHubName);

            Console.WriteLine("Press escape to exit...");

            try
            {
                while (true)
                {

                    var batch = new List<EventData>();

                    for (var i = 0; i < 5; i++)
                    {
                        countEvents++;
                        var body = $"Event time: {DateTime.Now}";
                        batch.Add(new EventData(body));
                        Console.WriteLine($"Published event {countEvents}: {body}");
                    }
                    
                    await producerClient.SendAsync(batch);

                    if (Console.KeyAvailable && Console.ReadKey().Key == ConsoleKey.Escape)
                        break;

                    await Task.Delay(5000);
                }
            } finally
            {
                await producerClient.DisposeAsync();
            }
        }
    }
}