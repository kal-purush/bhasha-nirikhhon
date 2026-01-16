using Microsoft.Extensions.DependencyInjection;

namespace Infrastructure;

public static class ConfigureServiceCollection
{
    public static IServiceCollection AddApplication(this IServiceCollection services)
    {

        return services;
    }
}