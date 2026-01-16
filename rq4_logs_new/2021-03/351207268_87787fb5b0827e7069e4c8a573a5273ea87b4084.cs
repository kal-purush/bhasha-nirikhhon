using System;
using System.Collections.Generic;
using System.Linq;
using System.Net.Http;
using System.Text;
using System.Threading.Tasks;
using Microsoft.Extensions.DependencyInjection;
using Polly;
using Polly.Extensions.Http;
using Polly.Timeout;
using RedBoarder.NetDataClient.Clients;
using RedBoarder.NetDataClient.Services;

namespace RedBoarder.NetDataClient
{
    public static class ServicesNetDataClientExtension
    {
        public static void AddNetDataClient(this IServiceCollection services)
        {
            services
                .AddHttpClient("NetDataClient",
                    c =>
                    {
                        c.BaseAddress = new Uri("http://127.0.0.1:19999/api/v1/");
                        c.DefaultRequestHeaders.Add("Accept", "application/json");
                        c.DefaultRequestHeaders.Add("User-Agent", "NetDataClient");
                        c.Timeout = TimeSpan.FromSeconds(10);
                    })
                .ConfigurePrimaryHttpMessageHandler(() => new HttpClientHandler()
                {
                    AllowAutoRedirect = false,
                    UseCookies = false,
                })
                .AddPolicyHandler(GetRetryPolicy());

            services.AddTransient<NetDataChartClient>();
            services.AddTransient<NetDataStatusService>();
        }

        private static IAsyncPolicy<HttpResponseMessage> GetRetryPolicy()
        {

            AsyncTimeoutPolicy timeout = Policy.TimeoutAsync(7);
            return HttpPolicyExtensions
                // Handle HttpRequestExceptions, 408 and 5xx status codes
                .HandleTransientHttpError()
                // Handle 404 not found
                .OrResult(msg => msg.StatusCode == System.Net.HttpStatusCode.NotFound)
                // Handle 401 Unauthorized
                .OrResult(msg => msg.StatusCode == System.Net.HttpStatusCode.Unauthorized)
                // What to do if any of the above erros occur:
                // Retry 3 times, each time wait 1,2 and 4 seconds before retrying.
                .WaitAndRetryAsync(3, retryAttempt => TimeSpan.FromSeconds(Math.Pow(2, retryAttempt))).WrapAsync(timeout);
        }
    }
}