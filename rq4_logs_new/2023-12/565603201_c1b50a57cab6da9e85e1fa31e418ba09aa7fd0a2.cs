using System.Reflection;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;

namespace SmallsOnline.Web.Lib.Services;

public static class ItunesApiServiceExtensions
{
    public static IServiceCollection AddItunesApiService(this IServiceCollection services, string userAgentName)
    {
        services.AddHttpClient(
            name: "ItunesApiClient",
            configureClient: (serviceProvider, httpClient) =>
            {
                httpClient.DefaultRequestHeaders.UserAgent.Add(new(userAgentName, Assembly.GetExecutingAssembly().GetName().Version!.ToString()));
                httpClient.BaseAddress = new("https://itunes.apple.com/");
            }
        );

        services.AddSingleton<IItunesApiService, ItunesApiService>();

        return services;
    }
}