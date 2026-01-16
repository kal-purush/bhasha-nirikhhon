using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using MongoDB.Driver;
using OrderService.Application.Specifications.Repositories;
using OrderService.Application.Specifications.Services;
using OrderService.Infrastructure.Implementations.Repositories;
using OrderService.Infrastructure.Implementations.Services;

namespace OrderService.Infrastructure.DI
{
    public static class DbConfiguration
    {
        public static void ConfigureDbServices(this IServiceCollection services, IConfiguration configuration)
        {
            var mongoConnectionString = configuration.GetConnectionString("MongoDb");

            var client = new MongoClient(mongoConnectionString);
            var database = client.GetDatabase("OrderDatabase");

            services.AddSingleton<IMongoDatabase>(database);

            services.AddScoped<IOrderRepository, OrderRepository>();

            services.Configure<StripeSettings>(configuration.GetSection("StripeSettings"));

            services.AddScoped<IPaymentService, StripePaymentService>();
        }
    }
}