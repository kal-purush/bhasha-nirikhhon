using Newtonsoft.Json;

using System.Runtime.Serialization;

namespace ShareInvest.Entities.Google.Firebase;

public class CloudMessage
{
    [DataMember, JsonProperty("notification")]
    public Notification Notification
    {
        get; set;
    }
    [DataMember, JsonProperty("token")]
    public string? Token
    {
        get; set;
    }
    [DataMember, JsonProperty("topic")]
    public string? Topic
    {
        get; set;
    }
    [DataMember, JsonProperty("condition")]
    public string? Condition
    {
        get; set;
    }
    [DataMember, JsonProperty("data")]
    public IReadOnlyDictionary<string, string>? Data
    {
        get; set;
    }
}