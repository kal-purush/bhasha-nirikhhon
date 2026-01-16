using DFC.Common.SharedContent.Pkg.Netcore.Model.Common;
using System.Text.Json.Serialization;

namespace DFC.Common.SharedContent.Pkg.Netcore.Model.ContentItems;

public class TriageResultTile
{
    [JsonPropertyName("contentItems")]
    public List<TriageResultTile>? ContentItems { get; set; }

    [JsonPropertyName("contentItemId")]
    public string? ContentItemId { get; set; }

    [JsonPropertyName("title")]
    public string? Title { get; set; }

    [JsonPropertyName("triageLevelOne")]
    public TriageLevelOne? TriageLevelOne { get; set; }

    [JsonPropertyName("triageResult")]
    public TriageResult? TriageResult { get; set; }

    [JsonPropertyName("triageTileHtml")]
    public TriageTileHtml? TriageTileHtml { get; set; }
}