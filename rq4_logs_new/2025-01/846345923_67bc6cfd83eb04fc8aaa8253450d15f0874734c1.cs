using F1.Src.BusinessLogic;
using F1.Src.DataAccess;
using FCommon.Src.DependencyInjection;
using FluentValidation;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;

namespace F2.Src;

public sealed class F1Register : IServiceRegister
{
    public IServiceCollection Register(IServiceCollection services, IConfiguration configuration)
    {
        var currentAssembly = typeof(F1Register).Assembly;

        #region Filters
        services.RegisterFiltersFromAssembly(currentAssembly);
        #endregion

        #region Validation
        services.AddValidatorsFromAssembly(currentAssembly, ServiceLifetime.Singleton);
        #endregion

        #region Core
        services
            .AddScoped<IF1Repository, F1Repository>()
            .MakeScopedLazy<IF1Repository>()
            .AddScoped<F1Service>()
            .MakeScopedLazy<F1Service>();
        #endregion

        return services;
    }
}