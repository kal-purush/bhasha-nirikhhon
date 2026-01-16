using System.Text.RegularExpressions;

namespace MDLibrary.Extensions;

public static class StringExtensions
{
    /// <summary>
    /// Elimina los espacios entre palabras y los limita a uno
    /// </summary>
    /// <param name="line"></param>
    /// <seealso cref="https://github.com/alca259/Bya-Extensions/blob/main/src/Bya.System/StringExtensions.cs"/>
    /// <returns></returns>
    public static string TrimSpacesBetweenString(this string line)
    {
        if (string.IsNullOrEmpty(line)) return line;
        var regex = new Regex(@"[ ]{2,}", RegexOptions.None);
        line = regex.Replace(line, @" ").Trim();
        return line;
    }
}