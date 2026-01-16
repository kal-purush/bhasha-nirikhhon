using System.Text.Json.Nodes;
using System.Text.Json.Serialization;

namespace Falu.Websockets;

internal class RealtimeConnectionMessage
{
    [JsonPropertyName("type")]
    public string? Type { get; set; }
}

internal class RealtimeConnectionIncomingMessage : RealtimeConnectionMessage
{
    [JsonPropertyName("event")]
    public string? Event { get; set; }

    [JsonPropertyName("object")]
    public JsonObject? Object { get; set; }
}

internal class RealtimeConnectionOutgoingMessage : RealtimeConnectionMessage
{
    public RealtimeConnectionOutgoingMessage(string type, RealtimeConnectionFilters? filters)
    {
        Type = type ?? throw new ArgumentNullException(nameof(type));
        Filters = filters;
    }

    [JsonPropertyName("filters")]
    public RealtimeConnectionFilters? Filters { get; set; }
}