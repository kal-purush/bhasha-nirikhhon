using Interfaces.Clients;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using System.Net.Http;
using System.Security.Cryptography;
using System.Text;
using System.Web;

namespace Clients
{
    public class BinanceApiClient : IBinanceApiClient
    {
        private readonly IHttpClientFactory _httpClientFactory;
        private readonly string _apiKey;
        private readonly string _apiSecret;
        private readonly ILogger<BinanceApiClient> _logger;

        public BinanceApiClient(IHttpClientFactory httpClientFactory, IConfiguration configuration, ILogger<BinanceApiClient> logger)
        {
            _httpClientFactory = httpClientFactory;
            _logger = logger;

            // Check secrets hierarchy
            _apiKey = GetConfigurationValue(configuration, "Binance:ApiKey");
            _apiSecret = GetConfigurationValue(configuration, "Binance:ApiSecret");

            if (string.IsNullOrEmpty(_apiKey) || string.IsNullOrEmpty(_apiSecret))
                throw new Exception("Binance API credentials not found in any configuration source.");
        }

        public async Task<string> GetAsync(string endpoint, Dictionary<string, string> queryParams = null)
        {
            queryParams ??= new Dictionary<string, string>();
            queryParams["timestamp"] = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds().ToString();
            queryParams["signature"] = GenerateSignature(queryParams);

            var queryString = BuildQueryString(queryParams);
            var requestUrl = $"{endpoint}?{queryString}";

            HttpClient client = _httpClientFactory.CreateClient("BinanceClient");
            client.DefaultRequestHeaders.Clear();
            client.DefaultRequestHeaders.Add("X-MBX-APIKEY", _apiKey);


            _logger.LogInformation($"Making GET request to Binance: {client.BaseAddress.ToString()}{requestUrl}");

            var response = await client.GetAsync(requestUrl);
            response.EnsureSuccessStatusCode();
            return await response.Content.ReadAsStringAsync();
        }

        public async Task<string> PostAsync(string endpoint, HttpContent content)
        {
            var timestamp = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds().ToString();
            var queryParams = new Dictionary<string, string> { { "timestamp", timestamp } };
            queryParams["signature"] = GenerateSignature(queryParams);

            var queryString = BuildQueryString(queryParams);
            var requestUrl = $"{endpoint}?{queryString}";

            var client = _httpClientFactory.CreateClient("BinanceClient");
            client.DefaultRequestHeaders.Clear();
            client.DefaultRequestHeaders.Add("X-MBX-APIKEY", _apiKey);

            _logger.LogInformation($"Making POST request to Binance: {requestUrl}");

            var response = await client.PostAsync(requestUrl, content);
            response.EnsureSuccessStatusCode();
            return await response.Content.ReadAsStringAsync();
        }

        private string GenerateSignature(Dictionary<string, string> queryParams)
        {
            var queryString = BuildQueryString(queryParams);
            using var hmac = new HMACSHA256(Encoding.UTF8.GetBytes(_apiSecret));
            var hash = hmac.ComputeHash(Encoding.UTF8.GetBytes(queryString));
            return BitConverter.ToString(hash).Replace("-", "").ToLower();
        }

        private static string BuildQueryString(Dictionary<string, string> queryParams)
        {
            return string.Join("&", queryParams.Select(kvp => $"{HttpUtility.UrlEncode(kvp.Key)}={HttpUtility.UrlEncode(kvp.Value)}"));
        }


        private string GetConfigurationValue(IConfiguration configuration, string key)
        {
            // Check environment variables first
            var value = Environment.GetEnvironmentVariable(key.Replace(":", "__"));

            // Check secrets from config (appsettings, user-secrets, etc.)
            if (string.IsNullOrEmpty(value))
            {
                value = configuration[key];
            }

            _logger.LogInformation($"Config key '{key}' loaded: {(!string.IsNullOrEmpty(value) ? "Yes" : "No")}");
            return value;
        }
    }
}