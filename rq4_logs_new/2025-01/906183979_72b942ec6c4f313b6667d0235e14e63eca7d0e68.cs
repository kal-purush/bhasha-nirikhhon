using Application.Interfaces.AnalyzeInterface;
using Microsoft.Extensions.Configuration;

namespace Infrastructure.Services.AnalyzeService
{
    public class AnalyzeService : IAnalyze
    {
        private readonly HttpClient _httpClient;
        private readonly string _apiKey;

        public AnalyzeService(IConfiguration configuration, HttpClient httpClient)
        {
            var apiKey = configuration["OpenAI:ApiKey"];

            if (string.IsNullOrEmpty(apiKey))
            {
                throw new InvalidOperationException("OpenAI API key is missing.");
            }

            _httpClient = httpClient;
            _apiKey = apiKey;
        }

        public Task<string> AnalyzeCVAsync(string fileContent)
        {
            throw new NotImplementedException();
        }
    }
}