using BeeFor.Application.Interfaces;
using BeeFor.Application.IoC;
using BeeFor.Application.Models;
using Microsoft.Extensions.DependencyInjection;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using System;
using System.Text;
using System.Text.Json;

namespace ChangelogConsumer
{
    class Program
    {
        static void Main(string[] args)
        {
            var serviceCollection = new ServiceCollection();
            ResolveDependencies(serviceCollection);

            var serviceProvider = serviceCollection.BuildServiceProvider();

            var factory = new ConnectionFactory() { HostName = "localhost" };
            using (var connection = factory.CreateConnection())
            using (var channel = connection.CreateModel())
            {
                channel.QueueDeclare(queue: "ProjetoChangelogQueue",
                                     durable: false,
                                     exclusive: false,
                                     autoDelete: false,
                                     arguments: null);

                var consumer = new EventingBasicConsumer(channel);
                consumer.Received += (model, ea) =>
                {
                    try
                    {
                        var body = ea.Body.ToArray();

                        var message = Encoding.UTF8.GetString(body);

                        var projeto = JsonSerializer.Deserialize<ProjetoViewModel>(message);

                        if (projeto != null)
                        {
                            var t = serviceProvider.GetService<IProjetoAppService>().UpdateProjeto(projeto);

                            Console.WriteLine("Processado....");
                            string json = JsonSerializer.Serialize(projeto);
                            Console.WriteLine(json);
                        }

                        channel.BasicAck(ea.DeliveryTag, false);
                    }
                    catch (Exception e)
                    {
                        channel.BasicNack(ea.DeliveryTag, false, true);

                        Console.WriteLine(e);
                    }
                };

                channel.BasicConsume(queue: "ProjetoChangelogQueue",
                                     autoAck: false,
                                     consumer: consumer);

                Console.WriteLine(" Press [enter] to exit.");
                Console.ReadLine();
            }
        }

        public static IServiceCollection ResolveDependencies(IServiceCollection services)
        {
            services.ResolveDependencies();
            return services;
        }
    }
}