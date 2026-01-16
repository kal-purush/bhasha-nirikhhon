using Newtonsoft.Json;

using System.ComponentModel.DataAnnotations;
using System.Runtime.Serialization;

namespace ShareInvest.Entities.Dart;

public class CompanyOverview : UniqueNumber
{
    public DateTime Date
    {
        get; set;
    }
    public double Latitude
    {
        get; set;
    }
    public double Longitude
    {
        get; set;
    }
    [DataMember, JsonProperty("status"), StringLength(0x10)]
    public string? Status
    {
        get; set;
    }
    [DataMember, JsonProperty("message"), StringLength(0x80)]
    public string? Message
    {
        get; set;
    }
    [DataMember, JsonProperty("corp_code"), StringLength(8)]
    public override string? CorpCode
    {
        get; set;
    }
    [DataMember, JsonProperty("corp_name"), StringLength(0x40)]
    public override string? CorpName
    {
        get; set;
    }
    [DataMember, JsonProperty("modify_date"), StringLength(8)]
    public override string? ModifyDate
    {
        get; set;
    }
    [DataMember, JsonProperty("corp_name_eng"), StringLength(0x80)]
    public string? CorpEngName
    {
        get; set;
    }
    [DataMember, JsonProperty("stock_name"), StringLength(0x80)]
    public string? Name
    {
        get; set;
    }
    [DataMember, JsonProperty("stock_code"), Key, StringLength(6)]
    public override string? Code
    {
        get; set;
    }
    [DataMember, JsonProperty("ceo_nm"), StringLength(0x40)]
    public string? CEO
    {
        get; set;
    }
    [DataMember, JsonProperty("corp_cls"), StringLength(1)]
    public string? Classification
    {
        get; set;
    }
    [DataMember, JsonProperty("jurir_no"), StringLength(0x10)]
    public string? LegalRegistrationNumber
    {
        get; set;
    }
    [DataMember, JsonProperty("bizr_no"), StringLength(0x10)]
    public string? CorporateRegistrationNumber
    {
        get; set;
    }
    [DataMember, JsonProperty("adres"), StringLength(0x80)]
    public string? Address
    {
        get; set;
    }
    [DataMember, JsonProperty("hm_url"), StringLength(0x100)]
    public string? Url
    {
        get; set;
    }
    [DataMember, JsonProperty("ir_url"), StringLength(0x100)]
    public string? IR
    {
        get; set;
    }
    [DataMember, JsonProperty("phn_no"), StringLength(0x20)]
    public string? Phone
    {
        get; set;
    }
    [DataMember, JsonProperty("fax_no"), StringLength(0x20)]
    public string? Fax
    {
        get; set;
    }
    [DataMember, JsonProperty("induty_code"), StringLength(8)]
    public string? IndutyCode
    {
        get; set;
    }
    [DataMember, JsonProperty("est_dt"), StringLength(8)]
    public string? FoundingDate
    {
        get; set;
    }
    [DataMember, JsonProperty("acc_mt"), StringLength(2)]
    public string? SettlementMonth
    {
        get; set;
    }
}