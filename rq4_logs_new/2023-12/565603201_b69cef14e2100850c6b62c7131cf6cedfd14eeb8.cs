using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;

namespace SmallsOnline.Web.Lib.Services;

/// <summary>
/// Extension methods for configuring and adding the BlobStorageService to the service collection.
/// </summary>
public static class BlobStorageServiceExtensions
{
    /// <summary>
    /// Adds the BlobStorageService to the service collection and configures it using the specified options.
    /// </summary>
    /// <param name="services">The service collection to add the service to.</param>
    /// <param name="configure">The action used to configure <see cref="BlobStorageServiceOptions"/> the <see cref="BlobStorageService"/>.</param>
    /// <returns>The modified service collection.</returns>
    public static IServiceCollection AddBlobStorageService(this IServiceCollection services, Action<BlobStorageServiceOptions> configure)
    {
        services.Configure(configure);

        services.TryAddSingleton<IBlobStorageService, BlobStorageService>();

        return services;
    }
}