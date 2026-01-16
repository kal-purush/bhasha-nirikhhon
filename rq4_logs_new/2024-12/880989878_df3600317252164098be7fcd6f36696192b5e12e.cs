using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;

namespace MarGate.Core.Mongo.Extension;

public static class ServiceCollectionExtensions
{
    public static IServiceCollection AddMongo(this IServiceCollection services, IConfiguration configuration, string databaseName)
    {
        services.AddSingleton<IMongoDbContext>(new MongoDbContext(configuration.GetConnectionString("Mongo"), databaseName));

        services.AddSingleton<IMongoRepositoryFactory, MongoRepositoryFactory>();

        return services;
    }

}