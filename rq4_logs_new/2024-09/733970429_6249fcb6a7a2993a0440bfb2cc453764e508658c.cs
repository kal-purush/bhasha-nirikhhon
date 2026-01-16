using DFC.Common.SharedContent.Pkg.Netcore.Model.Common;
using System.Text.Json.Serialization;

namespace DFC.Common.SharedContent.Pkg.Netcore.Model.ContentItems;

public class TriageFilterAdviceGroupImage
{
    [JsonPropertyName("triageLevelOne")]
    public TriageLevelOne? TriageLevelOne { get; set; }

    [JsonPropertyName("filterAdviceGroup")]
    public FilterAdviceGroup? FilterAdviceGroup { get; set; }

    [JsonPropertyName("imageHtml")]
    public FilterAdviceGroupHtml? ImageHtml { get; set; }
}