using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Runtime.Serialization.Formatters.Binary;
using System.Text;
using System.Threading.Tasks;
using Common.Helpers.IHelpers;
using Newtonsoft.Json;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;

namespace Common.Helpers
{

    public class RabbitMQHelper : IMessageQueueHelper
    {
        public void PushMessage<T>(IApplicationConfig config, T message, string queueName)
        {
            var factory = new ConnectionFactory() { HostName = config.RabbitConnection };

            using (var connection = factory.CreateConnection())
            using (var channel = connection.CreateModel())
            {
                channel.QueueDeclare(queue: queueName, durable: false, exclusive: false, autoDelete: false, arguments: null);

                var body = Encoding.UTF8.GetBytes(JsonConvert.SerializeObject(message));

                var properties = channel.CreateBasicProperties();
                properties.Persistent = true;

                channel.BasicPublish(exchange: "", routingKey: config.QueueName, basicProperties: properties, body: body);
            }
        }

        public async Task ReadMessages<T>(IApplicationConfig config, Action<T> actionOnReceive, string queueName)
        {
            var factory = new ConnectionFactory() { HostName = config.RabbitConnection };
            using (var connection = factory.CreateConnection())
            {
                using (var channel = connection.CreateModel())
                {
                    channel.QueueDeclare(queue: queueName, durable: false, exclusive: false, autoDelete: false, arguments: null);

                    var consumer = new EventingBasicConsumer(channel);

                    consumer.Received += (model, ea) =>
                    {
                        var body = ea.Body;
                        var message = JsonConvert.DeserializeObject<T>(Encoding.UTF8.GetString(body));
                        actionOnReceive.Invoke(message);
                    };


                    channel.BasicConsume(queue: config.QueueName,
                                         autoAck: true,
                                         consumer: consumer);

                    Console.ReadKey();
                }
            }
        }

        private byte[] ObjectToByteArray(object obj)
        {
            if (obj == null)
                return null;

            BinaryFormatter bf = new BinaryFormatter();

            using (MemoryStream ms = new MemoryStream())
            {
                bf.Serialize(ms, obj);
                return ms.ToArray();
            }
        }
    }
}