using Microsoft.Extensions.DependencyInjection;

namespace Mediator.Switch.Extensions.Microsoft.DependencyInjection;

public class MicrosoftDependencyInjectionServiceProvider : ISwitchMediatorServiceProvider
{
    private readonly IServiceProvider _serviceProvider;

    public MicrosoftDependencyInjectionServiceProvider(IServiceProvider serviceProvider) => 
        _serviceProvider = serviceProvider;

    public T Get<T>() where T : notnull => 
        _serviceProvider.GetRequiredService<T>();
}