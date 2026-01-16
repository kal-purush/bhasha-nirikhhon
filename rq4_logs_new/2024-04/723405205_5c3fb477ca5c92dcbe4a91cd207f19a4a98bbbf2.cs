namespace SmartHome.Common.Interfaces;

/// <summary>
/// Simple interface for avoid using the service provider directly
/// The implemntation can use the service provider to resolve the service from the provider
/// </summary>
/// <typeparam name="TService"></typeparam>
/// <typeparam name="TKey"></typeparam>
public interface ITypedProvider<TService, TKey>
{
    TService? GetService(TKey topic);
}