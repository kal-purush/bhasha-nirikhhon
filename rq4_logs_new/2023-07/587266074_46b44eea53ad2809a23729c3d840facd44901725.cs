using System.Net.Http.Headers;
using AspNetCore.Proxy;
using AspNetCore.Proxy.Options;
using Microsoft.AspNetCore.Mvc;

namespace SelfService.Infrastructure.Api.PlatformDataMetrics;

[Route("platformdatametrics")]
[Produces("application/json")]
[ApiController]
public class PlatformDataMetricsController : ControllerBase
{
    private readonly HttpProxyOptions _httpOptions;

    public PlatformDataMetricsController()
    {
        _httpOptions = HttpProxyOptionsBuilder.Instance
            .WithShouldAddForwardedHeaders(false)
            .WithBeforeSend(
                (c, hrm) =>
                {
                    // TODO: figure out auth here
                    // hrm.Headers.Authorization

                    // TODO: convert request to platform data endpoint
                    return Task.CompletedTask;
                }).Build();
    }

    [Route("timeseries/finout/{capabilityid}")]
    public Task GetTimeSeries()
    {
        return this.HttpProxyAsync("http://localhost:8070/api/data/timeseries/finout", _httpOptions);
    }
}