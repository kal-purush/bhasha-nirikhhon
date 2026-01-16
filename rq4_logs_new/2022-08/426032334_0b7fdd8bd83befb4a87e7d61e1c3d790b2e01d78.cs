using System.Diagnostics;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;

namespace TS.StandaloneKestrelServer.Extensions
{
    public static class ServiceCollectionExtensions
    {
        public static IServiceCollection AddStandaloneKestrelServerServices(this IServiceCollection service)
        {
            service.AddHostedService<StandaloneKestrelServerService>();

            service.AddOptions<StandaloneKestrelServerOptions>();
            service
                .AddTransient<IConfigureOptions<StandaloneKestrelServerOptions>, StandaloneKestrelServerOptionsSetup>();

            var listener = new DiagnosticListener("TS.StandaloneKestrelServer");
            service.AddSingleton<DiagnosticListener>(listener);
            service.AddSingleton<DiagnosticSource>(listener);
            return service;
        }
    }
}