using System.Text.Json.Serialization;

namespace Falu.Client.Realtime;

public class RealtimeConnectionNegotiation
{
    [JsonPropertyName("type")]
    public string? Type { get; set; }

    [JsonPropertyName("url")]
    public Uri Url { get; set; } = default!;

    [JsonPropertyName("expires")]
    public DateTimeOffset Expires { get; set; }

    [JsonPropertyName("token")]
    public string Token { get; set; } = default!;

    [JsonPropertyName("workspace")]
    public string Workspace { get; set; } = default!;

    [JsonPropertyName("live")]
    public bool Live { get; set; }
}