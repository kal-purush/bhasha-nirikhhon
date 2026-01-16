using System;
using FA3.Src.Generator;
using FCommon.Src.DependencyInjection;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;

namespace FA3.Src;

public sealed class FA3Register : IServiceRegister
{
    public IServiceCollection Register(IServiceCollection services, IConfiguration configuration)
    {
        AddAppDefinedServices(services);

        return services;
    }

    private static void AddAppDefinedServices(IServiceCollection services)
    {
        services.AddSingleton<IIdGenerator, SnowFlakeGenerator>().MakeScopedLazy<IIdGenerator>();
    }
}