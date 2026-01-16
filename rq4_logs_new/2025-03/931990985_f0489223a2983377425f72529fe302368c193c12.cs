using Microsoft.Extensions.DependencyInjection;
using MongoDB.Driver;
using OrderService.Application.Specifications.Repositories;
using OrderService.Infrastructure.Implementations.Repositories;
using Microsoft.Extensions.Configuration;

namespace OrderService.Infrastructure
{
    public static class DependencyInjection
    {
        public static void ConfigureDbConnection(this IServiceCollection services, IConfiguration configuration)
        {
            var mongoConnectionString = configuration.GetConnectionString("MongoDb");

            var client = new MongoClient(mongoConnectionString);
            var database = client.GetDatabase("OrderDatabase");

            services.AddSingleton<IMongoDatabase>(database);
            services.AddScoped<IOrderRepository, OrderRepository>();
        }
    }
}