using Newtonsoft.Json;

using System.Runtime.Serialization;

namespace ShareInvest.Entities.Google.Maps;

public struct MapStatus
{
    [DataMember, JsonProperty("center")]
    public Coordinate Center
    {
        get; set;
    }
    [DataMember, JsonProperty("tilt")]
    public double Tilt
    {
        get; set;
    }
    [DataMember, JsonProperty("heading")]
    public double Heading
    {
        get; set;
    }
    [DataMember, JsonProperty("zoom")]
    public double Zoom
    {
        get; set;
    }
}