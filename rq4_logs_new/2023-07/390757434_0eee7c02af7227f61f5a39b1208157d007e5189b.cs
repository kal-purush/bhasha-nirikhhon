namespace Falu;

internal static class SpectreFormatter
{
    public static string ColouredRed(object value) => Coloured("red", value);
    public static string ColouredYellow(object value) => Coloured("yellow", value);
    public static string ColouredGreen(object value) => Coloured("green", value);
    public static string ColouredLightGreen(object value) => Coloured("lightgreen", value);
    public static string ColouredGrey(object value) => Coloured("grey", value);
    public static string Coloured(string color, object value) => $"[{color}]{value}[/]";

    public static string ForColorizedStatus(int code)
    {
        return code switch
        {
            >= 500 => ColouredRed(code),
            >= 300 => ColouredYellow(code),
            _ => ColouredGreen(code),
        };
    }

    public static string ForLink(string text, string url) => $"[link={url}]{text}[/]";

    public static string EscapeSquares(string text) => $"[[{text}]]";
}