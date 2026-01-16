using Mortein.Mqtt.Services;
using MQTTnet.Client;

namespace Mortein.Mqtt.Extensions;

/// <summary>
/// Extension methods for setting up MQTT client related services in an
/// <see cref="IServiceCollection" />.
/// </summary>
public static class ServiceCollectionExtension
{
    /// <summary>
    /// Registers a hosted MQTT client as a service in the <see cref="IServiceCollection" />.
    /// </summary>
    /// 
    /// <param name="services">The <see cref="IServiceCollection" /> to add services to.</param>
    /// 
    /// <returns>The same service collection so that multiple calls can be chained.</returns>
    public static IServiceCollection AddMqttClientHostedService(this IServiceCollection services)
    {
        services.AddMqttClientServiceWithConfig(_ => { });
        return services;
    }

    /// <summary>
    /// Registers a hosted MQTT client as a service in the <see cref="IServiceCollection" />.
    /// </summary>
    /// 
    /// <param name="services">The <see cref="IServiceCollection" /> to add services to.</param>
    /// <param name="configure">
    /// A required action to configure the <see cref="MqttClientOptionsBuilder" /> for the client.
    /// </param>
    /// 
    /// <returns>The same service collection so that multiple calls can be chained.</returns>
    private static IServiceCollection AddMqttClientServiceWithConfig(this IServiceCollection services, Action<MqttClientOptionsBuilder> configure)
    {
        services.AddSingleton(_ =>
        {
            var optionBuilder = new MqttClientOptionsBuilder();
            configure(optionBuilder);
            return optionBuilder.Build();
        });
        services.AddSingleton<MqttClientService>();
        services.AddSingleton<IHostedService>(serviceProvider =>
        {
            return serviceProvider.GetService<MqttClientService>()!;
        });
        services.AddSingleton(serviceProvider =>
        {
            var mqttClientService = serviceProvider.GetService<MqttClientService>();
            var mqttClientServiceProvider = new MqttClientServiceProvider(mqttClientService!);
            return mqttClientServiceProvider;
        });
        return services;
    }
}