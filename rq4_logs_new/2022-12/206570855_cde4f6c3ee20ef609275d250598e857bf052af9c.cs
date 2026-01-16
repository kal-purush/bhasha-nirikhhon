using Newtonsoft.Json;
using System;

namespace CoronaVirus.Models;
public partial class CovidDTO
{
    [JsonProperty("data")]
    public CovidData[] Data { get; set; }
}

public partial class CovidData
{
    [JsonProperty("date")]
    public DateTimeOffset Date { get; set; }

    [JsonProperty("confirmed")]
    public long Confirmed { get; set; }

    [JsonProperty("deaths")]
    public long Deaths { get; set; }

    [JsonProperty("recovered")]
    public long Recovered { get; set; }

    [JsonProperty("confirmed_diff")]
    public long ConfirmedDiff { get; set; }

    [JsonProperty("deaths_diff")]
    public long DeathsDiff { get; set; }

    [JsonProperty("recovered_diff")]
    public long RecoveredDiff { get; set; }

    [JsonProperty("last_update")]
    public DateTimeOffset LastUpdate { get; set; }

    [JsonProperty("active")]
    public long Active { get; set; }

    [JsonProperty("active_diff")]
    public long ActiveDiff { get; set; }

    [JsonProperty("fatality_rate")]
    public double FatalityRate { get; set; }

    [JsonProperty("region")]
    public Region Region { get; set; }
}

public partial class Region
{
    [JsonProperty("iso")]
    public string Iso { get; set; }

    [JsonProperty("name")]
    public string Name { get; set; }

    [JsonProperty("province")]
    public string Province { get; set; }

    [JsonProperty("lat")]
    public string Lat { get; set; }

    [JsonProperty("long")]
    public string Long { get; set; }

    [JsonProperty("cities")]
    public object[] Cities { get; set; }
}