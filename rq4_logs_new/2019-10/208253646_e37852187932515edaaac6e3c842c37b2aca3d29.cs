using Microsoft.Extensions.DependencyInjection;

namespace TheFipster.Munchkin.AppInsights.Core
{
    public static class ServiceExtension
    {
        public static IServiceCollection AddMunchkinInsights(this IServiceCollection services)
        {
            services.AddApplicationInsightsTelemetry();
            return services;
        }
    }
}