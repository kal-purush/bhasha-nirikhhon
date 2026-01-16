using ActivityFinder.Server.OtherTools.Extensions;
using System.Text.Json;

namespace ActivityFinder.Server.Models
{
    public class AppHttpClient
    {
        private readonly HttpClient _httpClient;

        public AppHttpClient() 
        {
            _httpClient = new HttpClient();
            _httpClient.SetBasicHeaders();
        }

        public async Task<T> GetResonse<T>(string url) 
        {
            HttpRequestMessage requestMessage = new HttpRequestMessage(HttpMethod.Get, url);
            requestMessage.Headers.Add("User-Agent", "User-Agent-Here");

            HttpResponseMessage response = await _httpClient.SendAsync(requestMessage);
            var jsonResponse = await response.Content.ReadAsStringAsync();

            return JsonSerializer.Deserialize<T>(jsonResponse);
        }
    }
}