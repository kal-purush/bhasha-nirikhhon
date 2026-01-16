using Newtonsoft.Json;

using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Runtime.Serialization;

namespace ShareInvest.Entities;

public class KakaoCallback
{
    [DatabaseGenerated(DatabaseGeneratedOption.Identity), Key]
    public int Id
    {
        get; set;
    }
    [DataMember, JsonProperty("CHAT_TYPE"), StringLength(0x10)]
    public string? ChatType
    {
        get; set;
    }
    [DataMember, JsonProperty("HASH_CHAT_ID"), StringLength(0x20)]
    public string? ChatId
    {
        get; set;
    }
    [DataMember, JsonProperty("TEMPLATE_ID")]
    public long TemplateId
    {
        get; set;
    }
    [DataMember, JsonProperty("lat")]
    public double Latitude
    {
        get; set;
    }
    [DataMember, JsonProperty("lng")]
    public double Longitude
    {
        get; set;
    }
    [DataMember, JsonProperty("X-Kakao-Resource-ID"), StringLength(0x20)]
    public string? ResourceId
    {
        get; set;
    }
}