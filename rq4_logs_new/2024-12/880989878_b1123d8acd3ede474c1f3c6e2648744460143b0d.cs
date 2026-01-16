using MarGate.Core.MessageBus.Publisher;
using MassTransit;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;

namespace MarGate.Core.MessageBus.Extension;
public static class ServiceCollectionExtensions
{

    public class MessageBusSettings
    {
        public string Host { get; set; }
        public string UserName { get; set; }
        public string Password { get; set; }
        public string VirtualHost { get; set; }
    }

    public static IServiceCollection AddMessageBus(this IServiceCollection services, IConfiguration configuration)
    {
        var messageBusSettings = configuration.GetSection("MessageBus").Get<MessageBusSettings>()!;
        services.AddMassTransit(x =>
        {

            x.UsingRabbitMq((context, cfg) =>
            {
                cfg.Host(messageBusSettings.Host, messageBusSettings.VirtualHost, h =>
                {
                    h.Username(messageBusSettings.UserName);
                    h.Password(messageBusSettings.Password);
                });

                cfg.ConfigureEndpoints(context);
            });

            var consumers = AppDomain.CurrentDomain.GetAssemblies().SelectMany(x => x.GetTypes())
            .Where(x => x.IsClass && !x.IsAbstract && typeof(IMessageConsumer).IsAssignableFrom(x) && !x.IsGenericType);

            foreach (var consumer in consumers)
            {
                x.AddConsumer(consumer);
            }

        });

        services.AddScoped<IMessagePublisher, MessagePublisher>();

        return services;
    }
}