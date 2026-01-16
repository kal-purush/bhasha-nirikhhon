using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;

namespace MessagingExample
{
    class Program
    {
        static void Main(string[] args)
        {
            var uri = "amqp://guest:guest@localhost:5672/%2f";
            var queueName = "test";

            var command = args.Length > 0 ? args[0] : "produce";

            var connectionFactory = new ConnectionFactory { Uri = uri };
            using (var connection = connectionFactory.CreateConnection())
            using (var channel = connection.CreateModel())
            {
                channel.QueueDeclare(queue: queueName,
                                     durable: false,
                                     exclusive: false,
                                     autoDelete: false,
                                     arguments: null);

                switch (command)
                {
                    case "produce":
                        channel.BasicPublish(exchange: "",
                                             routingKey: queueName,
                                             basicProperties: null,
                                             body: Encoding.UTF8.GetBytes("Hello World!"));

                        Console.WriteLine("Added 1 new message to the queue {0} with content: {1}", queueName, "Hello World!");
                        break;

                    case "consume":
                        var consumer = new EventingBasicConsumer(channel);
                        consumer.Received += (model, ea) =>
                        {
                            Console.WriteLine("Received 1 new message from the queue {0} with content: {1}", queueName, Encoding.UTF8.GetString(ea.Body));
                        };
                        channel.BasicConsume(queue: queueName,
                                             noAck: true,
                                             consumer: consumer);

                        Console.WriteLine("Waiting for new messages... Press [enter] to exit.");
                        Console.ReadLine();
                        break;

                    default:
                        Console.Error.WriteLine("Unknown command {0}. Only the commands 'consume' and 'produce' are supported.", command);
                        Environment.Exit(1);
                        break;
                }
            }
        }
    }
}