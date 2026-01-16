using AspNetCore.Proxy;

namespace SelfService.Infrastructure.Api;

public static class ApiProxy
{
    
    private const string TimeseriesFinoutRoute = "/api/data/timeseries/finout";
    private const string TimeseriesByGroupFinoutRoute = "/api/data/timeseriesbygroup/finout";

    public static void MapProxies(this IApplicationBuilder app)
    {
        app.UseProxies(proxies =>
        {
            // TODO: Figure out how to add parameters
            proxies.Map(TimeseriesFinoutRoute,
                proxy => proxy.UseHttp((_, args) => TimeseriesFinoutRoute));
            proxies.Map(TimeseriesByGroupFinoutRoute,
                proxy => proxy.UseHttp((_, args) => TimeseriesByGroupFinoutRoute));
        });
    }
}