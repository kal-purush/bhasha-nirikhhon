using GiftSuggester.Web.HostedServices;
using Microsoft.OpenApi.Models;

namespace GiftSuggester.Web;

public static class ServiceCollectionExtensions
{
    public static IServiceCollection AddWeb(this IServiceCollection serviceCollection)
    {
        serviceCollection.AddHostedService<MigrationHostedService>();
        
        serviceCollection.AddControllers();
        
        serviceCollection.AddSwaggerGen(options =>
        {
            options.SwaggerDoc("v1", new OpenApiInfo { Title = "GiftSuggester", Version = "v1" });
        });

        return serviceCollection;
    }
}