using System.Collections.Immutable;

namespace WebScrap.API.Data;

public record struct CssTagRanges(string Css, ImmutableArray<Range> TagRanges);