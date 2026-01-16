using Goal.Application.Interfaces.Product;
using Goal.Application.Mappings;
using Goal.Application.Services.Product;
using Goal.Infrastructure;

namespace Goal.Api;

public static class ServiceExtensions
{
    /// <summary>
    /// Adds include business implementations
    /// </summary>
    /// <param name="services"></param>
    /// <param name="configuration"></param>
    /// <returns></returns>
    public static IServiceCollection AddServices(this IServiceCollection services, IConfiguration configuration)
    {
        // Business Services
        services.AddScoped<IProductService, ProductService>();

        // Mapping
        services.AddSingleton<IApplicationMapper, ApplicationMapper>();

        // Db Services
        services.AddDbExtensions(configuration);

        return services;
    }
}