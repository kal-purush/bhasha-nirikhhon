using ApplicationBuilderHelpers;
using Infrastructure.Serilog.Common;
using Microsoft.AspNetCore.Builder;
using Microsoft.Extensions.DependencyInjection;
using Serilog;

namespace Infrastructure.Serilog;

public class SerilogInfrastructure : ApplicationDependency
{
    public override void AddServices(ApplicationHostBuilder applicationBuilder, IServiceCollection services)
    {
        base.AddServices(applicationBuilder, services);

        (applicationBuilder.Builder as WebApplicationBuilder)!.Host
            .UseSerilog((context, loggerConfiguration) => LoggerBuilder.Configure(loggerConfiguration, applicationBuilder.Configuration));

        Log.Logger = LoggerBuilder.Configure(new LoggerConfiguration(), applicationBuilder.Configuration).CreateLogger();
    }
}