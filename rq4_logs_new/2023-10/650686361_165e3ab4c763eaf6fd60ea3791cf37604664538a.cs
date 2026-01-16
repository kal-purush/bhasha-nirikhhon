using Newtonsoft.Json;

using System.ComponentModel.DataAnnotations;
using System.Runtime.Serialization;

namespace ShareInvest.Entities.Dart;

public class Disclousure
{
    [DataMember, JsonProperty("rcept_no"), Key, StringLength(0xE)]
    public string? SerialNumber
    {
        get; set;
    }
    [DataMember, JsonProperty("rcept_dt"), Required, StringLength(8)]
    public string? Date
    {
        get; set;
    }
    [DataMember, JsonProperty("stock_code"), Required, StringLength(6)]
    public string? Code
    {
        get; set;
    }
    [DataMember, JsonProperty("corp_name"), StringLength(0x40)]
    public string? Name
    {
        get; set;
    }
    [DataMember, JsonProperty("corp_code"), Required, StringLength(8)]
    public string? CorpCode
    {
        get; set;
    }
    [DataMember, JsonProperty("report_nm"), Required, StringLength(0x100)]
    public string? ReportName
    {
        get; set;
    }
    [DataMember, JsonProperty("corp_cls"), StringLength(1)]
    public string? CorpClassification
    {
        get; set;
    }
    [DataMember, JsonProperty("flr_nm"), StringLength(0x40)]
    public string? Submitter
    {
        get; set;
    }
    [DataMember, JsonProperty("rm"), StringLength(2)]
    public string? Remarks
    {
        get; set;
    }
}