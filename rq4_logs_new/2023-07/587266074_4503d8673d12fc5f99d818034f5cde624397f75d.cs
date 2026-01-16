using System.Net.Http;
using Newtonsoft.Json;

namespace SelfService.Infrastructure.Api.Prometheus;


public static class PrometheusClient
{    
    private static IEnumerable<string> GetConsumersFromResponse(Response? response, string topic)
    {
        List<string> consumers = new List<string>();
        if (response == null) {
            return new List<string>();
        }
        foreach (Result result in response.data.result) {
            if (result.metric.topic == topic)
            {
                consumers.Add(result.metric.consumergroup);
            }
        }
        return consumers;
    }

    public static async Task<(IEnumerable<string> consumers, string? errorString)> GetConsumersForKafkaTopic(string topic)
    {
        HttpClient client = new HttpClient();
        string? prometheus = Environment.GetEnvironmentVariable("SS_PROMETHEUS_API_ENDPOINT");
        if (string.IsNullOrEmpty(prometheus))
        {
            return (new List<string>(), "SS_PROMETHEUS_API_ENDPOINT is not set");
        }
        string url = $"{prometheus}/api/v1/query?query=kafka_consumergroup_lag"; // consider time parameter: "&time=1689844553.339"
        HttpResponseMessage response = await client.GetAsync(url);

        if (response.IsSuccessStatusCode)
        {
            string jsonstring = await response.Content.ReadAsStringAsync();
            if (jsonstring == null)
            {
                client.Dispose();
                return (new List<string>(), "Prometheus response is null");
            }
            Response? promResponse = JsonConvert.DeserializeObject<Response>(jsonstring);
            client.Dispose();
            return (GetConsumersFromResponse(promResponse, topic), null);
        }
        else {
            client.Dispose();
            return (new List<string>(), $"Prometheus Statuscode: {response.StatusCode}");
        }   
    }
}