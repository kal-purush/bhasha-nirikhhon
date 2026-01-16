namespace Atmoos.Sphere.Text;

using static Atmoos.Sphere.Text.LineTag;

public readonly record struct LineTag()
{
    public required String Start { get; init; }
    public required String End { get; init; }
    public void Deconstruct(out String start, out String end) => (start, end) = (Start, End);
    public static LineTag FromName(String tag, String name) => new() { Start = $"{tag} {name}", End = tag };
}

public static class LineTags
{
    public static class Markdown
    {
        public static LineTag Header(String name) => FromName("#", name);
        public static LineTag SubHeader(String name) => FromName("##", name);
        public static LineTag SubSubHeader(String name) => FromName("###", name);
        public static LineTag Code(String name) => FromName("```", name);
    }

    public static class CSharp
    {
        public static LineTag LineComment(String name)
        {
            var lineTag = $"// {name}";
            return new() { Start = lineTag, End = lineTag };
        }

        public static LineTag BlockComment(String name) => new() { Start = $"/* {name}", End = $"{name} */" };
        public static LineTag Region(String name) => new() { Start = $"#region {name}", End = "#endregion" };
    }
}