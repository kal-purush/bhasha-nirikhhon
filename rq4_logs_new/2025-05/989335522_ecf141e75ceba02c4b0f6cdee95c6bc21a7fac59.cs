using SearchEntities;
using DataEntities;
using System.Text.Json;

namespace Store.Services;

public class UserInsightService
{
    HttpClient httpClient;
    private readonly ILogger<UserInsightService> _logger;

    public UserInsightService(HttpClient httpClient, ILogger<UserInsightService> logger)
    {
        _logger = logger;
        this.httpClient = httpClient;
    }
    public async Task<List<UserQuestionInsight>> GetUserInsights()
    {
        List<UserQuestionInsight>? insights = null;
        try
        {
            var response = await httpClient.GetAsync("/api/Insights");
            var responseText = await response.Content.ReadAsStringAsync();

            _logger.LogInformation($"Http status code: {response.StatusCode}");
            _logger.LogInformation($"Http response content: {responseText}");

            if (response.IsSuccessStatusCode)
            {
                var options = new JsonSerializerOptions
                {
                    PropertyNameCaseInsensitive = true
                };

                insights = await response.Content.ReadFromJsonAsync<List<UserQuestionInsight>>(options);
            }
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error during GetUserInsights.");
        }

        return insights ?? new List<UserQuestionInsight>();
    }

    public async Task<string?> GenerateInsights(string searchTerm)
    {
        try
        {
            // call the desired Endpoint
            HttpResponseMessage response = null;
            response = await httpClient.GetAsync($"/api/generateinsights/{searchTerm}");

            var responseText = await response.Content.ReadAsStringAsync();

            _logger.LogInformation($"Http status code: {response.StatusCode}");
            _logger.LogInformation($"Http response content: {responseText}");

            if (response.IsSuccessStatusCode)
            {
                return responseText;
            }
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error during Search.");
        }
        return "No response";
    }
}