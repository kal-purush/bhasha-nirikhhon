using System;
using FACommon.DependencyInjection;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;

namespace Entry.Register;

public sealed class EntryRegister : IServiceRegister
{
    public IServiceCollection Register(IServiceCollection services, IConfiguration configuration)
    {
        services.AddLogging(config =>
        {
            config.ClearProviders().AddConsole();
        });

        services.AddControllers(config =>
        {
            config.SuppressAsyncSuffixInActionNames = false;
        });

        services.AddHttpContextAccessor().MakeSingletonLazy<IHttpContextAccessor>();

        return services;
    }
}