using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using MongoDB.Driver;

namespace Library.Common.Mongo
{
    public static class Extensions
    {
        public static void AddMongoDb(this IServiceCollection services, IConfiguration configuration)
        {
            services.Configure<MongoOptions>(configuration.GetSection("Mongo"));
            services.AddSingleton<MongoClient>(client => 
            {
                var options = client.GetService<IOptions<MongoOptions>>();
                return new MongoClient(options.Value.ConnectionString);
            });
            services.AddScoped<IMongoDatabase>(database => 
            {
                var options = database.GetService<IOptions<MongoOptions>>();
                var client = database.GetService<MongoClient>();
                return client.GetDatabase(options.Value.Database);
            });
            services.AddScoped<IDatabaseInitializer, MongoInitializer>();
        }
    }
}