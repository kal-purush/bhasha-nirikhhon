using FACommon.Src.DependencyInjection;
using FCommon.Src.AccessToken;
using FCommon.Src.IdGeneration;
using FCommon.Src.RefreshToken;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;

namespace FCommon.Src;

public sealed class CommonServiceRegister : IServiceRegister
{
    public IServiceCollection Register(IServiceCollection services, IConfiguration configuration)
    {
        AddAppDefinedServices(services, configuration);

        return services;
    }

    public void AddAppDefinedServices(IServiceCollection services, IConfiguration configuration)
    {
        services
            .AddSingleton<IAppIdGenerator, SnowflakeIdGenerator>()
            .MakeSingletonLazy<IAppIdGenerator>()
            .AddSingleton<IAppAccessTokenHandler, AppAccessTokenHandler>()
            .MakeSingletonLazy<IAppAccessTokenHandler>()
            .MakeSingletonLazy<IAppRefreshTokenHandler>()
            .MakeSingletonLazy<IAppRefreshTokenHandler>();
    }
}