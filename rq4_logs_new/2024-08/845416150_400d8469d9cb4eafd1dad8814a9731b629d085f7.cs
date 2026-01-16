using CyberManager.Application.Services;
using Microsoft.Extensions.DependencyInjection;

namespace CyberManager.Application;

public static class DependencyInjection
{
    public static IServiceCollection AddApplication(this IServiceCollection services)
    {
        //Add application service
        services.AddScoped<IGreeting, Greeting>();

        return services;
    }
}