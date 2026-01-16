using System.Net.Http;

namespace SelfService.Infrastructure.Api.Apis;

public static class ConfluentGatewayClient
{
    public static async Task<(IEnumerable<string> consumers, string? resultstring)> GetConsumersForKafkaTopic(string cluster, string topic)
    {
        HttpClient client = new HttpClient();
        string? confluentGateway = Environment.GetEnvironmentVariable("SS_CONFLUENT_GATEWAY_API_ENDPOINT");
        if (string.IsNullOrEmpty(confluentGateway))
        {
            return (new List<string>(), "SS_CONFLUENT_GATEWAY_API_ENDPOINT is not set");
        }
        string url = $"{confluentGateway}/cluster/{cluster}/topic/{topic}/consumers";
        HttpResponseMessage response = await client.GetAsync(url);

        if (response.IsSuccessStatusCode)
        {
            string json = await response.Content.ReadAsStringAsync();
            client.Dispose();
            return (json.ToString()!.Split(",").ToList(), null);
        }
        else {
            client.Dispose();
            string error = await response.Content.ReadAsStringAsync();
            return (new List<string>(), $"Failed to get consumers for topic");
        }   
    }
}