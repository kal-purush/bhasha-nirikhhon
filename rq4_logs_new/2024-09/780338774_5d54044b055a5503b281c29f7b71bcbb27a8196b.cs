using System.Text;

namespace MB.API.Helpers;

public class LoggingHelper
{
    public static List<string> WordWrap(string text, int maxWidth)
    {
        List<string> lines = [];
        string[] words = text.Split(' ');

        StringBuilder currentLine = new();

        foreach (var word in words)
        {
            if (currentLine.Length + word.Length + (currentLine.Length > 0 ? 1 : 0) > maxWidth)
            {
                lines.Add(currentLine.ToString());
                currentLine.Clear();
            }

            if (currentLine.Length > 0)
            {
                currentLine.Append(' ');
            }

            currentLine.Append(word);
        }

        if (currentLine.Length > 0)
        {
            lines.Add(currentLine.ToString());
        }

        return lines;
    }
}