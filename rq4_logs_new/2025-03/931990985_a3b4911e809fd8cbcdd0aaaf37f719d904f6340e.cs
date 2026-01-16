using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using OrderService.Application.Specifications.Services;
using OrderService.Infrastructure.Implementations.Services.RabbitMQ;
using OrderService.Infrastructure.Options;

namespace OrderService.Infrastructure.DI
{
    public static class RabbitMQConfiguration
    {
        public static void ConfigureRabbitMQ(this IServiceCollection services, IConfiguration configuration)
        {
            services.Configure<RabbitMQOptions>(configuration.GetSection("MessageBroker"));

            services.AddSingleton<IMessageService, RabbitMQService>();
        }
    }
}