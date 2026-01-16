using Newtonsoft.Json;

using System.Runtime.Serialization;

namespace ShareInvest.Entities.Kakao.Geo;

public struct Address
{
    [DataMember, JsonProperty("address_name")]
    public string Name
    {
        get; set;
    }
    [DataMember, JsonProperty("region_1depth_name")]
    public string RegionName1
    {
        get; set;
    }
    [DataMember, JsonProperty("region_2depth_name")]
    public string RegionName2
    {
        get; set;
    }
    [DataMember, JsonProperty("region_3depth_name")]
    public string RegionName3
    {
        get; set;
    }
    [DataMember, JsonProperty("region_3depth_h_name")]
    public string RegionNameH
    {
        get; set;
    }
    [DataMember, JsonProperty("h_code")]
    public string AdministrationCode
    {
        get; set;
    }
    [DataMember, JsonProperty("b_code")]
    public string CourtCode
    {
        get; set;
    }
    [DataMember, JsonProperty("mountain_yn")]
    public string Mountain
    {
        get; set;
    }
    [DataMember, JsonProperty("main_address_no")]
    public string Main
    {
        get; set;
    }
    [DataMember, JsonProperty("sub_address_no")]
    public string Sub
    {
        get; set;
    }
    [DataMember, JsonProperty("x")]
    public string Longitude
    {
        get; set;
    }
    [DataMember, JsonProperty("y")]
    public string Latitude
    {
        get; set;
    }
}