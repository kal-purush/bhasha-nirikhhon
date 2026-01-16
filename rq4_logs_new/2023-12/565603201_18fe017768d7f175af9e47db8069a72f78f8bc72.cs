using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;

namespace SmallsOnline.Web.Lib.Services;

/// <summary>
/// Provides extension methods for configuring and adding the CosmosDbService to the IServiceCollection.
/// </summary>
public static class CosmosDbServiceExtensions
{
    /// <summary>
    /// Adds the <see cref="CosmosDbService"/> to the <see cref="IServiceCollection"/> with the specified configuration.
    /// </summary>
    /// <param name="services">The <see cref="IServiceCollection"/> to add the <see cref="CosmosDbService"/> to.</param>
    /// <param name="configure">The configuration action for the <see cref="CosmosDbServiceOptions"/>.</param>
    /// <returns>The modified <see cref="IServiceCollection"/>.</returns>
    public static IServiceCollection AddCosmosDbService(this IServiceCollection services, Action<CosmosDbServiceOptions> configure)
    {
        services.Configure(configure);

        services.TryAddSingleton<ICosmosDbService, CosmosDbService>();

        return services;
    }
}