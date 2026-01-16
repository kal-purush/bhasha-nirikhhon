using System.Collections.Immutable;
using WebScrap.Modules.Exporting.Json;

namespace WebScrap.API.Data;

/// <summary>
/// Represents the result of scrapping.
/// </summary>
public readonly ref struct ScrapResult
{
    private readonly ReadOnlySpan<char> html;
    private readonly ImmutableArray<Range> tagRanges;

    internal ScrapResult(
        ReadOnlySpan<char> html,
        ImmutableArray<Range> tagRanges)
    {
        this.html = html;
        this.tagRanges = tagRanges;
    }

    /// <summary>
    /// Gets the stored value in JSON format.
    /// </summary>
    /// <returns>
    /// Array of json objects in string format.
    /// { value: %JsonValue% }
    /// where %JsonValue% is any JsonObject.
    /// </returns>
    public ImmutableArray<string> AsJson()
    {
        var tagStrings = ExtractStrings(html.ToString(), tagRanges);
        return JsonApi.Export(tagStrings)
            .Select(x => x!.ToJsonString())
            .ToImmutableArray();
    }

    public ImmutableArray<string> AsHtml()
        => ExtractStrings(html.ToString(), tagRanges)
            .Select(x => x.ToString())
            .ToImmutableArray();

    private static ImmutableArray<ReadOnlyMemory<char>> ExtractStrings(
        string html,
        IEnumerable<Range> tagRanges)
        => tagRanges
            .Select(range => html[range])
            .Select(x => x.ToString())
            .Select(x => x.Trim())
            .Select(x => x.AsMemory())
            .ToImmutableArray();
}