using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Configuration.CommandLine;
using Soenneker.Enums.DeployEnvironment;

namespace Soenneker.Extensions.ConfigurationBuilder;

/// <summary>
/// A collection of helpful ConfigurationBuilder extension methods
/// </summary>
public static class ConfigurationBuilderExtension
{
    /// <summary>
    /// Initializes the <see cref="IConfigurationBuilder"/> by removing undesired configuration sources
    /// and adding environment-specific app settings and environment variables.
    /// </summary>
    /// <param name="builder">The configuration builder to initialize.</param>
    /// <param name="environment">The current deployment environment (e.g., Development, Staging, Production).</param>
    /// <remarks>
    /// This method ensures deterministic configuration order and removes automatic inclusion of certain
    /// sources like <c>appsettings.Development.json</c> that ASP.NET Core may add by default.
    /// </remarks>
    // https://devblogs.microsoft.com/premier-developer/order-of-precedence-when-configuring-asp-net-core/
    public static IConfigurationBuilder Initialize(this IConfigurationBuilder builder, string? environment)
    {
        // Strip out all sources except the ones we want to keep
        for (int i = builder.Sources.Count - 1; i >= 0; i--)
        {
            if (builder.Sources[i] is not ChainedConfigurationSource and not CommandLineConfigurationSource)
            {
                builder.Sources.RemoveAt(i);
            }
        }

        builder.AddAppSettings(environment);

        // this is last because we want azure app settings to take priority
        builder.AddEnvironmentVariables();

        return builder;
    }

    /// <summary>
    /// Adds the appropriate <c>appsettings</c> JSON configuration file based on the provided environment.
    /// </summary>
    /// <param name="builder">The configuration builder to add the JSON file to.</param>
    /// <param name="environment">The current deployment environment (e.g., Development, Staging, Production).</param>
    /// <returns>The modified <see cref="IConfigurationBuilder"/> instance for chaining.</returns>
    /// <remarks>
    /// This adds either an environment-specific <c>appsettings.{environment}.json</c> file or
    /// falls back to <c>appsettings.json</c> if no known environment is detected.
    /// </remarks>
    public static IConfigurationBuilder AddAppSettings(this IConfigurationBuilder builder, string? environment)
    {
        if (environment == DeployEnvironment.Production.Name || environment == DeployEnvironment.Staging.Name ||
            environment == DeployEnvironment.Development.Name)
        {
            builder.AddJsonFile($"appsettings.{environment}.json", true, true);
        }
        else
        {
            builder.AddJsonFile("appsettings.json", true, true);
        }

        return builder;
    }

    /// <summary>
    /// Adds the appropriate Ocelot configuration JSON file based on the provided environment.
    /// </summary>
    /// <param name="builder">The configuration builder to add the Ocelot JSON file to.</param>
    /// <param name="environment">The current deployment environment (e.g., Development, Staging, Production).</param>
    /// <returns>The modified <see cref="IConfigurationBuilder"/> instance for chaining.</returns>
    /// <remarks>
    /// This adds either <c>ocelot.{environment}.json</c> or <c>ocelot.json</c> depending on the current environment.
    /// This method is useful when setting up Ocelot API Gateway configuration in an ASP.NET Core app.
    /// </remarks>
    public static IConfigurationBuilder AddOcelotConfig(this IConfigurationBuilder builder, string? environment)
    {
        if (environment == DeployEnvironment.Production.Name || environment == DeployEnvironment.Staging.Name ||
            environment == DeployEnvironment.Development.Name)
        {
            builder.AddJsonFile($"ocelot.{environment}.json", true, true);
        }
        else
        {
            builder.AddJsonFile("ocelot.json", true, true);
        }

        return builder;
    }
}