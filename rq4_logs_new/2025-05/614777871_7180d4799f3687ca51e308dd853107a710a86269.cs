using Microsoft.Extensions.DependencyInjection;
using SFA.DAS.Funding.ApprenticeshipPayments.Functions.HealthChecks;
using SFA.DAS.Funding.ApprenticeshipPayments.Infrastructure.Configuration;

namespace SFA.DAS.Funding.ApprenticeshipPayments.Functions.AppStart;

public static class HealthChecks
{
    public static IServiceCollection AddFunctionHealthChecks(this IServiceCollection services, ApplicationSettings applicationSettings)
    {
        services.AddSingleton(sp => new FunctionHealthChecker(
            new DbHealthCheck(applicationSettings.DbConnectionString, sp.GetService<ILogger<DbHealthCheck>>()!),
            new ServiceBusHealthCheck(applicationSettings.NServiceBusConnectionString, "DasServiceBus", sp.GetService<ILogger<ServiceBusHealthCheck>>()!),
            new ServiceBusHealthCheck(applicationSettings.DCServiceBusConnectionString, "Pv2ServiceBus", sp.GetService<ILogger<ServiceBusHealthCheck>>()!)
            ));


        return services;
    }
}