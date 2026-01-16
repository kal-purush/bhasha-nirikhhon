using MessageBroker.Kafka.Producer.Abstractions;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;

namespace MessageBroker.Kafka.Producer.Extensions;

public static class ProducerExtensions
{
    public static void AddKafkaProducer<TMessage>(this IServiceCollection serviceCollection, IConfiguration configuration)
    {
        serviceCollection.Configure<KafkaSettings>(configuration);
        serviceCollection.AddSingleton<IKafkaProducer<TMessage>, KafkaOllamaProducer<TMessage>>();
    }
}