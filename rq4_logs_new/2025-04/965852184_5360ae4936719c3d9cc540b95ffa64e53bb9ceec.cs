using Confluent.Kafka;
using DeliveryRentals.Domain.Events;
using DeliveryRentals.Infrastructure.Repositories;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using System.Text.Json;

namespace DeliveryRentals.Infrastructure.Messaging
{
	public class KafkaEventConsumerService : BackgroundService
	{
		private readonly ILogger<KafkaEventConsumerService> _logger;
		private readonly IServiceScopeFactory _serviceScopeFactory;
		private readonly string _bootstrapServers = "host.docker.internal:9092";
		private readonly string _topic = "motos";
		private readonly string _groupId = "moto-consumer-group";

		public KafkaEventConsumerService(
			ILogger<KafkaEventConsumerService> logger,
			IServiceScopeFactory serviceScopeFactory)
		{
			_logger = logger;
			_serviceScopeFactory = serviceScopeFactory;
		}

		protected override async Task ExecuteAsync(CancellationToken stoppingToken)
		{
			var config = new ConsumerConfig
			{
				BootstrapServers = _bootstrapServers,
				GroupId = _groupId,
				AutoOffsetReset = AutoOffsetReset.Earliest,
				AllowAutoCreateTopics = true,
			};

			using var consumer = new ConsumerBuilder<Ignore, string>(config).Build();
			consumer.Subscribe(_topic);

			_logger.LogInformation("Kafka consumer started. Listening to topic: {Topic}", _topic);

			try
			{
				// Topic to consume messages asynchronously
				await Task.Run(async () =>
				{
					while (!stoppingToken.IsCancellationRequested)
					{
						try
						{
							// Consuming the message synchronously, but with Task.Run to not block the main thread
							var result = consumer.Consume(stoppingToken);
							if (result?.Message?.Value is null)
								continue;

							var json = result.Message.Value;
							var motoEvent = JsonSerializer.Deserialize<MotoRegisterEvent>(json);

							if (motoEvent?.Year == 2024)
							{
								using (var scope = _serviceScopeFactory.CreateScope())
								{
									var eventRepository = scope.ServiceProvider.GetRequiredService<IEventRepository>();
									await eventRepository.SaveEventAsync(json);
								}

								_logger.LogInformation("Event persisted (Year 2024): {MotoId}", motoEvent.Id);
							}
						}
						catch (ConsumeException ex)
						{
							_logger.LogError(ex, "Kafka consume error.");
						}
					}
				}, stoppingToken);
			}
			catch (OperationCanceledException)
			{
				_logger.LogInformation("Kafka consumer stopped.");
			}
			finally
			{
				consumer.Close();
			}
		}
	}
}