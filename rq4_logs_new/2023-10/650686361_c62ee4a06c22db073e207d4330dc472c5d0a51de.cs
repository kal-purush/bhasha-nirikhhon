using Newtonsoft.Json;

using System.Runtime.Serialization;

namespace ShareInvest.Entities.Naver;

public struct DetectLangage
{
    [DataMember, JsonProperty("langCode")]
    public string Langage
    {
        get; set;
    }
    public static Dictionary<string, string> LangageCode => new()
    {
        { "ko", "한국어" },
        { "ja", "일본어" },
        { "zh-CN", "중국어 간체" },
        { "zh-TW", "중국어 번체" },
        { "hi", "힌디어" },
        { "en", "영어" },
        { "es", "스페인어" },
        { "fr", "프랑스어" },
        { "de", "독일어" },
        { "pt", "포르투갈어" },
        { "vi", "베트남어" },
        { "id", "인도네시아어" },
        { "fa", "페르시아어" },
        { "ar", "아랍어" },
        { "mm", "미얀마어" },
        { "th", "태국어" },
        { "ru", "러시아어" },
        { "it", "이탈리아어" },
        { "unk", "알 수 없음" }
    };
}