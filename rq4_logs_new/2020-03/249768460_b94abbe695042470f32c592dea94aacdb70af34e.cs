using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Jellyfin.Plugin.Prowl.Configuration;
using Jellyfin.Plugin.Prowl.Utils;
using MediaBrowser.Common.Net;
using MediaBrowser.Model.Serialization;
using MediaBrowser.Model.Services;
using Microsoft.Extensions.Logging;

namespace Jellyfin.Plugin.Prowl.Api
{
    [Route("/Notification/Prowl/Test/{UserId}", "POST", Summary = "Tests Prowl")]
    public class TestNotification : IReturnVoid
    {
        [ApiMember(Name = "UserId", Description = "User Id", IsRequired = true, DataType = "string",
            ParameterType = "path", Verb = "GET")]
        public string UserId { get; set; }
    }

    public class ServerApiEndpoints : IService
    {
        private readonly IHttpClient _httpClient;
        private readonly ILogger _logger;

        public ServerApiEndpoints(ILoggerFactory loggerFactory, IHttpClient httpClient)
        {
            _logger = loggerFactory.CreateLogger<ServerApiEndpoints>();
            _httpClient = httpClient;
        }

        private static ProwlOptions GetOptions(string userId)
        {
            return Plugin.Instance.Configuration.Options
                .FirstOrDefault(i => string.Equals(i.UserId, userId, StringComparison.OrdinalIgnoreCase));
        }

        public void Post(TestNotification request)
        {
            PostAsync(request)
                .GetAwaiter()
                .GetResult();
        }

        private async Task PostAsync(TestNotification request)
        {
            var options = GetOptions(request.UserId);

            var parameters = new Dictionary<string, string>
            {
                {"apikey", options.Token},
                {"application", "Jellyfin"},
                {"event", "Test Notification"},
                {"description", "This is a test notification from Jellyfin"},
            };

            _logger.LogDebug("Prowl <TEST> to {0}", options.Token);

            var url = string.Format(PluginConfiguration.Url, parameters.ToQueryString());
            var requestOptions = new HttpRequestOptions
            {
                Url = url,
                LogErrorResponseBody = true
            };

            await _httpClient.Get(requestOptions).ConfigureAwait(false);
        }
    }
}