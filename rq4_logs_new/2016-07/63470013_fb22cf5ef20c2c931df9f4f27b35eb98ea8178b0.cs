using System;
using System.Threading.Tasks;
using Newtonsoft.Json;
using System.Net.Http;
using System.Net;
using Microsoft.Extensions.Options;
using Paynter.WitAi.Configuration;
using Microsoft.Extensions.Logging;

namespace Paynter.WitAi.Services
{
    public class WitAiService
    {
        private WitAiOptions _options;
        private ILogger<WitAiService> _logger;
        private HttpClient _httpClient;

        public WitAiService(IOptions<WitAiOptions> options, ILogger<WitAiService> logger)
        {
            _options = options.Value;
            _logger = logger;
        }

        public HttpClient HttpClient 
        { 
            get
            {
                if(_httpClient == null)
                {
                    _httpClient = new HttpClient();
                    _httpClient.BaseAddress = new Uri(_options.ApiUrl);
                    _httpClient.DefaultRequestHeaders.Accept.Clear();
                    _httpClient.DefaultRequestHeaders.Add("Accept", $"application/vnd.wit.{_options.ApiVersion}+json");
                    _httpClient.DefaultRequestHeaders.Add("Authorization", $" Bearer {_options.ApiToken}");
                }

                return _httpClient;
            }
        }

        public async Task<WitResponse> Message(string message)
        {
            var queryString = $"q={WebUtility.UrlEncode(message)}";
            var response = await HttpClient.GetAsync($"message?{queryString}");
            var content = await response.Content.ReadAsStringAsync();
            return JsonConvert.DeserializeObject<WitResponse>(content);
        }
    }
}