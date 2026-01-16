using System;
using System.Threading;
using System.Threading.Tasks;

using Confluent.Kafka;

using HomeBudget.Accounting.Infrastructure.Clients.Interfaces;

namespace HomeBudget.Accounting.Infrastructure.Clients
{
    public class BaseKafkaDependentProducer<K, V>(IKafkaClientHandler handle)
        : IKafkaDependentProducer<K, V>
    {
        private readonly IProducer<K, V> _kafkaHandle = new DependentProducerBuilder<K, V>(handle.Handle).Build();

        public void Produce(
            string topic,
            Message<K, V> message,
            Action<DeliveryReport<K, V>> deliveryHandler = null)
            => _kafkaHandle.Produce(topic, message, deliveryHandler);

        public Task<DeliveryResult<K, V>> ProduceAsync(
            string topic,
            Message<K, V> message,
            CancellationToken token)
            => _kafkaHandle.ProduceAsync(topic, message, token);

        public void Flush(TimeSpan timeout) => _kafkaHandle.Flush(timeout);
    }
}