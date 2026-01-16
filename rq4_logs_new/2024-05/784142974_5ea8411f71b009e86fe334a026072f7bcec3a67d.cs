using BasketConfirmed;
using Confluent.Kafka;
using DeliveryApp.Core.Application.Commands.Orders.CreateOrder;
using MediatR;
using Newtonsoft.Json;

namespace DeliveryApp.Api.Adapters.Kafka.BasketConfirmed;

public class ConsumerService : BackgroundService
{
    private readonly IServiceProvider _serviceProvider;
    private readonly IConsumer<Ignore, string> _consumer;

    public ConsumerService(IServiceProvider serviceProvider, string messageBrokerHost)
    {
        _serviceProvider = serviceProvider ?? throw new ArgumentNullException(nameof(serviceProvider));
        if (string.IsNullOrWhiteSpace(messageBrokerHost)) throw new ArgumentException(nameof(messageBrokerHost));

        var consumerConfig = new ConsumerConfig
        {
            BootstrapServers = messageBrokerHost,
            GroupId = "DeliveryConsumerGroup",
            EnableAutoOffsetStore = false,
            EnableAutoCommit = true,
            AutoOffsetReset = AutoOffsetReset.Earliest,
            EnablePartitionEof = true
        };
        _consumer = new ConsumerBuilder<Ignore, string>(consumerConfig).Build();
    }

    protected override async Task ExecuteAsync(CancellationToken cancellationToken)
    {
        _consumer.Subscribe("basket.confirmed");
        try
        {
            while (!cancellationToken.IsCancellationRequested)
            {
                await Task.Delay(TimeSpan.FromSeconds(5), cancellationToken);
                var consumeResult = _consumer.Consume(cancellationToken);

                if (consumeResult.IsPartitionEOF)
                {
                    continue;
                }

                Console.WriteLine(
                    $"Received message at {consumeResult.TopicPartitionOffset}: {consumeResult.Message.Value}");
                var basketConfirmedIntegrationEvent =
                    JsonConvert.DeserializeObject<BasketConfirmedIntegrationEvent>(consumeResult.Message.Value);

                using var scope = _serviceProvider.CreateScope();
                var mediatr = scope.ServiceProvider.GetRequiredService<IMediator>();
                
                var command = new Command
                {
                    BasketId = Guid.Parse(basketConfirmedIntegrationEvent!.BasketId),
                    Address = basketConfirmedIntegrationEvent.Address,
                    Weight = basketConfirmedIntegrationEvent.Weight
                };

                try
                {
                    var result = await mediatr.Send(command, cancellationToken);
                
                    if(result != true)
                        continue; // Try process again later
                }
                catch (Exception e)
                {
                    Console.WriteLine(e); // Skip :)
                }

                try
                {
                    _consumer.StoreOffset(consumeResult);
                }
                catch (KafkaException e)
                {
                    Console.WriteLine($"Store Offset error: {e.Error.Reason}");
                }
            }
        }
        catch (OperationCanceledException)
        {
            _consumer.Close();
        }
    }
}