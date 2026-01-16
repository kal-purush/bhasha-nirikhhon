using System.Text.Json.Serialization;

namespace ReportPortal_APIClient.Domain.Request;

public class CreateDashboardRequest
{
    [JsonPropertyName("description")]
    public string Description { get; set; }

    [JsonPropertyName("name")]
    public string Name { get; set; }

    [JsonPropertyName("share")]
    public bool Share { get; set; }
}