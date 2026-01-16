using System.Net.Http;
using System.Net.Http.Headers;
using System.Text.Json;
using WordPrediction.Application.Interfaces;

namespace WordPrediction.Infrastructure
{
    public class PredictedWordsService : IPredictedWordsService
    {
        private const string ACCESS_TOKEN = "";
        private readonly IHttpClientFactory _httpClientFactory;

        public PredictedWordsService(IHttpClientFactory httpClientFactory) => _httpClientFactory = httpClientFactory;


        private HttpClient CreateClient()
        {
            var client = _httpClientFactory.CreateClient("Wizkids");
            client.DefaultRequestHeaders.Authorization = new AuthenticationHeaderValue("Bearer", ACCESS_TOKEN);
            return client;
        }

        public async Task<string[]> GetPredictions(string text)
        {
            var httpClient = CreateClient();
            var httpResponseMessage = await httpClient.GetAsync(
            $"misc/getPredictions?locale=en-GB&text={text}");
            if (!httpResponseMessage.IsSuccessStatusCode) throw new Exception("");
            using var contentStream =
                await httpResponseMessage.Content.ReadAsStreamAsync();
            return await JsonSerializer.DeserializeAsync<string[]>(contentStream);

        }
    }
}